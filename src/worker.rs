use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{mpsc, OnceCell};
use tokio::task::JoinSet;

use crate::client;
use crate::spool;
use crate::types::HttpVersion;

static REMOTE_DUMP_TX: OnceCell<mpsc::Sender<DumpJob>> = OnceCell::const_new();

/// Approximate number of bytes currently sitting in the queue + batch drain.
/// Incremented on enqueue, decremented when the worker consumes a job.
static QUEUE_BYTES: AtomicUsize = AtomicUsize::new(0);

/// Runtime toggle: whether the HTTP (skip_browser) dump path is active.
/// Callers flip this via [`set_skip_browser_dumps_enabled`] — the crawl
/// machinery reads it with [`skip_browser_dumps_enabled`] before enqueuing.
/// Wait-free on the hot path.
static SKIP_BROWSER_DUMPS_ENABLED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Runtime toggle: whether [`try_enqueue`] / [`enqueue_best_effort`] should
/// spill to the on-disk spool when the in-memory channel is full or the
/// memory budget is exceeded. Default `false` preserves the pre-spool
/// drop-on-overflow behavior — existing users see zero regression.
static SPOOL_ENABLED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Enable or disable the HTTP (skip_browser) dump path globally.
///
/// Wiring from a per-crawl [`Configuration`] sets this on crawl start.
/// Safe to call from any thread; no allocation, no lock, no panic.
///
/// [`Configuration`]: https://docs.rs/spider/latest/spider/configuration/struct.Configuration.html
#[inline]
pub fn set_skip_browser_dumps_enabled(enabled: bool) {
    SKIP_BROWSER_DUMPS_ENABLED.store(enabled, std::sync::atomic::Ordering::Relaxed);
}

/// Whether the HTTP (skip_browser) dump path should enqueue jobs.
/// Wait-free read; safe to call from the hot crawl path.
#[inline]
pub fn skip_browser_dumps_enabled() -> bool {
    SKIP_BROWSER_DUMPS_ENABLED.load(std::sync::atomic::Ordering::Relaxed)
}

/// Enable or disable the disk-backed overflow spool. Default `false`.
///
/// When `false`, `try_enqueue` / `enqueue_best_effort` drop jobs on
/// channel-full or memory-budget-exceeded exactly like prior releases —
/// zero behavior change. When `true`, overflow jobs atomically spill to
/// the spool directory (see [`crate::spool`]) and a background drain
/// reloads them when the channel has headroom.
#[inline]
pub fn set_spool_enabled(enabled: bool) {
    SPOOL_ENABLED.store(enabled, std::sync::atomic::Ordering::Relaxed);
}

/// Whether the disk-backed overflow spool is active.
#[inline]
pub fn spool_enabled() -> bool {
    SPOOL_ENABLED.load(std::sync::atomic::Ordering::Relaxed)
}

/// How often the worker wakes to check the disk spool even if no new jobs
/// are arriving on the channel. Keeps spool drainage progressing during
/// idle periods.
const SPOOL_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Maximum spool entries to pull back into the channel in a single drain.
/// Chosen small so we never stall the upload loop; the drain repeats on
/// each outer iteration.
const SPOOL_DRAIN_BATCH: usize = 32;

/// A job representing a single cached response to upload to the remote cache.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct DumpJob {
    pub cache_key: String,
    pub cache_site: String,
    pub url: String,
    pub method: String,
    pub status: u16,
    pub request_headers: HashMap<String, String>,
    pub response_headers: HashMap<String, String>,
    pub body: Vec<u8>,
    pub http_version: HttpVersion,
    /// None => default endpoint
    /// Some("true") => default endpoint
    /// Some("http://...") => override base URL
    pub dump_remote: Option<String>,
}

impl DumpJob {
    /// Rough estimate of the memory this job occupies (body dominates).
    #[inline]
    pub fn estimated_bytes(&self) -> usize {
        self.body.len()
            + self.cache_key.len()
            + self.cache_site.len()
            + self.url.len()
            + self.method.len()
            + 256 // headers overhead estimate
    }
}

/// Maximum number of items to send in a single batch POST.
const MAX_BATCH_SIZE: usize = 16;

async fn init_inner(queue_cap: usize, qps: u32, timeout_ms: u64) -> mpsc::Sender<DumpJob> {
    let (tx, mut rx) = mpsc::channel::<DumpJob>(queue_cap.max(1));
    let drain_tx = tx.clone();
    let max_concurrent = default_max_concurrent();

    tokio::spawn(async move {
        let sem = Arc::new(tokio::sync::Semaphore::new(max_concurrent));
        let inflight: Arc<dashmap::DashSet<String>> = Arc::new(dashmap::DashSet::new());
        let mut join_set = JoinSet::<Vec<String>>::new();

        let qps = qps.max(1);
        let spawn_interval = Duration::from_micros(1_000_000 / qps as u64);
        let mut last_spawn = tokio::time::Instant::now() - spawn_interval;

        let timeout = Duration::from_millis(timeout_ms);

        // Cap how many jobs we drain in a single batch to bound the Vec
        // allocation and prevent one burst from monopolising memory.
        let max_drain: usize = (queue_cap / 4).clamp(64, 1024);

        loop {
            reap_completed(&mut join_set, &inflight);

            // Opportunistic spool drain — only active when the spool is
            // enabled. Preserves the prior recv-blocks-forever behavior
            // for existing users who haven't opted into the spool.
            let spool_on = spool_enabled();
            if spool_on && spool::spool_bytes() > 0 && queue_has_headroom_for_drain() {
                for _ in 0..SPOOL_DRAIN_BATCH {
                    match spool::spool_pop_one().await {
                        Some(job) => {
                            let est = job.estimated_bytes();
                            match drain_tx.try_send(job) {
                                Ok(()) => {
                                    QUEUE_BYTES.fetch_add(est, Ordering::Relaxed);
                                }
                                Err(mpsc::error::TrySendError::Full(returned)) => {
                                    // Channel filled before we could finish
                                    // draining — re-spool this one and try
                                    // again next loop iteration.
                                    if let Err(err) = spool::spool_write(&returned).await {
                                        tracing::warn!(
                                            "remote dump: re-spool failed after drain refusal: {err}"
                                        );
                                    }
                                    break;
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => {
                                    return;
                                }
                            }
                        }
                        None => break,
                    }
                }
            }

            // When the spool is on, use a bounded timeout so an idle
            // channel still wakes periodically to check for spooled work.
            // When the spool is off, preserve the original indefinite
            // blocking recv so we don't change CPU behavior for existing
            // users.
            let first_job = if spool_on {
                match tokio::time::timeout(SPOOL_POLL_INTERVAL, rx.recv()).await {
                    Ok(Some(job)) => job,
                    Ok(None) => break,
                    Err(_) => continue,
                }
            } else {
                match rx.recv().await {
                    Some(job) => job,
                    None => break,
                }
            };

            // Track bytes released from the queue as we drain.
            let mut drained_bytes = first_job.estimated_bytes();

            // Batch-drain pending jobs, but cap the drain to avoid
            // building an enormous Vec when the channel is backed up.
            let mut batch = Vec::with_capacity(max_drain.min(64));
            batch.push(first_job);
            while batch.len() < max_drain {
                match rx.try_recv() {
                    Ok(job) => {
                        drained_bytes += job.estimated_bytes();
                        batch.push(job);
                    }
                    Err(_) => break,
                }
            }

            // Release the tracked queue bytes now that we own the jobs.
            QUEUE_BYTES.fetch_sub(drained_bytes, Ordering::Relaxed);

            // Dedup against in-flight keys and group by endpoint.
            let mut groups: HashMap<String, Vec<DumpJob>> = HashMap::new();
            for job in batch {
                if !inflight.insert(job.cache_key.clone()) {
                    continue;
                }
                let base_url =
                    client::resolve_base_url(job.dump_remote.as_deref()).to_string();
                groups.entry(base_url).or_default().push(job);
            }

            // Spawn upload tasks per endpoint group, chunked into batches.
            for (base_url, jobs) in groups {
                for chunk in chunks_into_vec(jobs, MAX_BATCH_SIZE) {
                    let elapsed = last_spawn.elapsed();
                    if elapsed < spawn_interval {
                        tokio::time::sleep(spawn_interval - elapsed).await;
                    }
                    last_spawn = tokio::time::Instant::now();

                    let permit = match sem.clone().acquire_owned().await {
                        Ok(p) => p,
                        Err(_) => break,
                    };

                    let inflight_ref = inflight.clone();
                    let base_url = base_url.clone();
                    let keys: Vec<String> =
                        chunk.iter().map(|j| j.cache_key.clone()).collect();

                    join_set.spawn(async move {
                        let _permit = permit;

                        let result = tokio::time::timeout(
                            timeout,
                            upload_chunk(chunk, &base_url),
                        )
                        .await;

                        if result.is_err() {
                            tracing::warn!(
                                "remote cache dump: batch of {} timed out after {}ms",
                                keys.len(),
                                timeout.as_millis(),
                            );
                        }

                        for key in &keys {
                            inflight_ref.remove(key);
                        }

                        keys
                    });
                }
            }

            reap_completed(&mut join_set, &inflight);
        }

        // Graceful shutdown — drain remaining queue bytes.
        QUEUE_BYTES.store(0, Ordering::Relaxed);
        while let Some(result) = join_set.join_next().await {
            if let Ok(keys) = result {
                for key in keys {
                    inflight.remove(&key);
                }
            }
        }
    });

    tx
}

fn reap_completed(
    join_set: &mut JoinSet<Vec<String>>,
    inflight: &Arc<dashmap::DashSet<String>>,
) {
    while let Some(result) = join_set.try_join_next() {
        if let Ok(keys) = result {
            for key in keys {
                inflight.remove(&key);
            }
        }
    }
}

async fn upload_chunk(jobs: Vec<DumpJob>, base_url: &str) {
    if jobs.len() == 1 {
        let job = jobs.into_iter().next().unwrap();
        client::dump_to_remote(
            &job.cache_key,
            &job.cache_site,
            &job.url,
            &job.body,
            &job.method,
            job.status,
            &job.request_headers,
            &job.response_headers,
            &job.http_version,
            job.dump_remote.as_deref(),
        )
        .await;
    } else {
        let payloads: Vec<_> = jobs
            .iter()
            .map(|job| {
                client::build_payload(
                    &job.cache_key,
                    &job.url,
                    &job.body,
                    &job.method,
                    job.status,
                    &job.request_headers,
                    &job.response_headers,
                    &job.http_version,
                )
            })
            .collect();
        client::dump_batch_to_remote(payloads, base_url).await;
    }
}

/// Split a Vec into owned sub-Vecs of at most `chunk_size`.
fn chunks_into_vec<T>(vec: Vec<T>, chunk_size: usize) -> Vec<Vec<T>> {
    let mut result = Vec::new();
    let mut current = Vec::with_capacity(chunk_size);
    for item in vec {
        current.push(item);
        if current.len() >= chunk_size {
            result.push(std::mem::replace(
                &mut current,
                Vec::with_capacity(chunk_size),
            ));
        }
    }
    if !current.is_empty() {
        result.push(current);
    }
    result
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Manual init (optional). Safe to call multiple times.
pub async fn init_remote_dump_worker(
    queue_cap: usize,
    qps: u32,
    timeout_ms: u64,
) -> mpsc::Sender<DumpJob> {
    REMOTE_DUMP_TX
        .get_or_init(|| init_inner(queue_cap, qps, timeout_ms))
        .await
        .clone()
}

/// Returns `true` if the job's body is within the configured size limit.
/// Oversized bodies are dropped outright — they're too big for the remote
/// cache server as well, so disk-spooling them would just waste space.
#[inline]
fn body_within_size(job: &DumpJob) -> bool {
    let max = default_max_body_size();
    if max > 0 && job.body.len() > max {
        tracing::debug!(
            "remote dump dropped: body {} bytes exceeds max {} for {}",
            job.body.len(),
            max,
            job.cache_key,
        );
        return false;
    }
    true
}

/// Returns `true` if the in-memory queue has room within its byte budget.
/// When this returns false we fall back to the disk spool rather than
/// dropping the job.
#[inline]
fn queue_has_memory_headroom() -> bool {
    let budget = default_queue_memory_budget();
    budget == 0 || QUEUE_BYTES.load(Ordering::Relaxed) <= budget
}

/// Returns `true` if the queue is below half its memory budget — the
/// threshold at which the worker is willing to refill from the spool.
/// Prevents oscillation between memory and disk.
#[inline]
fn queue_has_headroom_for_drain() -> bool {
    let budget = default_queue_memory_budget();
    budget == 0 || QUEUE_BYTES.load(Ordering::Relaxed) < budget / 2
}

/// Fire-and-forget spool write. Returns `true` if the spool task was
/// scheduled (best-effort — success of the write itself is logged).
/// Returns `false` if the spool is disabled, no worker exists to drain
/// it, or we are not running under a tokio runtime — in which case the
/// caller falls back to the prior drop-on-overflow behavior.
fn spool_fire_and_forget(job: DumpJob) -> bool {
    if !spool_enabled() {
        return false;
    }
    if REMOTE_DUMP_TX.get().is_none() {
        tracing::debug!("remote dump dropped: no worker to drain spool");
        return false;
    }
    if tokio::runtime::Handle::try_current().is_err() {
        tracing::debug!("remote dump dropped: no tokio runtime for spool write");
        return false;
    }
    tokio::spawn(async move {
        match spool::spool_write(&job).await {
            Ok(n) => {
                tracing::debug!(
                    "remote dump spooled to disk: {n} bytes for {}",
                    job.cache_key
                );
            }
            Err(err) => {
                tracing::warn!(
                    "remote dump: spool failed for {} ({}B body): {err}",
                    job.cache_key,
                    job.body.len(),
                );
            }
        }
    });
    true
}

/// Auto-init on first use + best-effort enqueue.
///
/// On full channel or over-budget in-memory queue, the job spills to the
/// on-disk spool instead of being dropped. Still returns `false` only when
/// the body is oversized or no tokio runtime is available.
pub async fn enqueue_best_effort(job: DumpJob) -> bool {
    if !body_within_size(&job) {
        return false;
    }
    let tx =
        init_remote_dump_worker(default_queue_cap(), default_qps(), default_timeout_ms()).await;

    if !queue_has_memory_headroom() {
        return spool_fire_and_forget(job);
    }

    let est = job.estimated_bytes();
    match tx.try_send(job) {
        Ok(()) => {
            QUEUE_BYTES.fetch_add(est, Ordering::Relaxed);
            true
        }
        Err(mpsc::error::TrySendError::Full(returned)) => spool_fire_and_forget(returned),
        Err(mpsc::error::TrySendError::Closed(_)) => false,
    }
}

/// Enqueue with backpressure (awaits channel room).
///
/// Does not involve the disk spool — this variant is for callers that
/// explicitly want to wait rather than spill. Use `enqueue_best_effort`
/// or `try_enqueue` for the disk-backed fast path.
pub async fn enqueue(job: DumpJob) -> Result<(), mpsc::error::SendError<DumpJob>> {
    if !body_within_size(&job) {
        return Ok(());
    }
    let tx =
        init_remote_dump_worker(default_queue_cap(), default_qps(), default_timeout_ms()).await;
    let est = job.estimated_bytes();
    match tx.send(job).await {
        Ok(()) => {
            QUEUE_BYTES.fetch_add(est, Ordering::Relaxed);
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// Returns true if the worker has been initialized.
pub fn worker_inited() -> bool {
    REMOTE_DUMP_TX.initialized()
}

/// Non-async enqueue (fast path).
///
/// On full channel or over-budget in-memory queue, the job spills to the
/// on-disk spool rather than being dropped. Returns `false` only when the
/// body is oversized, no worker has been initialized, or no tokio runtime
/// is available.
pub fn try_enqueue(job: DumpJob) -> bool {
    if !body_within_size(&job) {
        return false;
    }
    // Fast path: worker + channel + memory budget.
    if queue_has_memory_headroom() {
        if let Some(tx) = REMOTE_DUMP_TX.get() {
            let est = job.estimated_bytes();
            match tx.try_send(job) {
                Ok(()) => {
                    QUEUE_BYTES.fetch_add(est, Ordering::Relaxed);
                    return true;
                }
                Err(mpsc::error::TrySendError::Full(returned)) => {
                    return spool_fire_and_forget(returned);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => return false,
            }
        }
        return false; // worker not initialized
    }
    // Over memory budget — spool instead of dropping.
    spool_fire_and_forget(job)
}

/// Returns the approximate number of bytes currently queued for upload.
pub fn queue_bytes() -> usize {
    QUEUE_BYTES.load(Ordering::Relaxed)
}

/// Init the worker with default settings.
pub async fn init_default_worker() {
    init_remote_dump_worker(default_queue_cap(), default_qps(), default_timeout_ms()).await;
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

pub fn default_queue_cap() -> usize {
    std::env::var("HYBRID_CACHE_REMOTE_QUEUE_CAP")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2_000)
}

pub fn default_qps() -> u32 {
    std::env::var("HYBRID_CACHE_REMOTE_QPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50)
}

pub fn default_timeout_ms() -> u64 {
    std::env::var("HYBRID_CACHE_REMOTE_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2_500)
}

pub fn default_max_concurrent() -> usize {
    std::env::var("HYBRID_CACHE_REMOTE_MAX_CONCURRENT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8)
}

/// Maximum body size (in bytes) for a single dump job.
/// Bodies larger than this are silently dropped.
/// Set to 0 to disable the limit.
/// Default: 5 MiB.
pub fn default_max_body_size() -> usize {
    std::env::var("HYBRID_CACHE_REMOTE_MAX_BODY_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5 * 1024 * 1024)
}

/// Approximate memory budget (in bytes) for the dump queue.
/// When the queue exceeds this, new jobs are dropped until it drains.
/// Set to 0 to disable the budget.
/// Default: 256 MiB.
pub fn default_queue_memory_budget() -> usize {
    std::env::var("HYBRID_CACHE_REMOTE_MEMORY_BUDGET")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(256 * 1024 * 1024)
}
