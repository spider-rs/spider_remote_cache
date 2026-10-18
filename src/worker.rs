use std::{
    collections::HashMap,
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{mpsc, OnceCell};
use tokio::task::{JoinHandle, JoinSet};

use crate::client;
use crate::spool;
use crate::types::HttpVersion;

static REMOTE_DUMP_TX: OnceCell<mpsc::Sender<DumpJob>> = OnceCell::const_new();

/// Approximate bytes held by jobs the worker has accepted and not yet
/// finished: queued in the channel, drained into a batch, or in an upload
/// that has not returned. Incremented on enqueue, decremented when the
/// upload for the job ends (success, failure or timeout) or the job is
/// discarded as a duplicate.
static QUEUE_BYTES: AtomicUsize = AtomicUsize::new(0);

#[inline]
fn add_queue_bytes(n: usize) {
    let now = QUEUE_BYTES
        .fetch_add(n, Ordering::Relaxed)
        .saturating_add(n);
    crate::metrics::queue_bytes(now);
}

#[inline]
fn release_queue_bytes(n: usize) {
    let prev = QUEUE_BYTES
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
            Some(cur.saturating_sub(n))
        })
        .unwrap_or(0);
    crate::metrics::queue_bytes(prev.saturating_sub(n));
}

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
static SPOOL_ENABLED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

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

/// Queue admission has its own deadline; upload deadlines still start
/// after a concurrency permit is acquired.
const QUEUE_ADMISSION_TIMEOUT: Duration = Duration::from_millis(2_500);

async fn init_inner(queue_cap: usize, qps: u32, timeout_ms: u64) -> mpsc::Sender<DumpJob> {
    let (tx, _worker) = spawn_worker(
        queue_cap,
        qps,
        timeout_ms,
        default_max_concurrent(),
        |chunk, cache_site, base_url| async move {
            upload_chunk(chunk, &cache_site, &base_url).await;
        },
    );
    tx
}

// Keep construction and scheduling shared with offline tests, which supply
// an uploader without HTTP or process-global client/worker initialization.
fn spawn_worker<U, F>(
    queue_cap: usize,
    qps: u32,
    timeout_ms: u64,
    max_concurrent: usize,
    upload: U,
) -> (mpsc::Sender<DumpJob>, JoinHandle<()>)
where
    U: Fn(Vec<DumpJob>, String, String) -> F + Clone + Send + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    // Validate every capacity and construct the semaphore before spawning
    // or publishing the sender through REMOTE_DUMP_TX. Invalid configuration
    // must never leave that OnceCell pointing at a dead or parked worker.
    let queue_cap = QUEUE_CAPACITY.clamp(queue_cap);
    let max_concurrent = UPLOAD_CONCURRENCY.clamp(max_concurrent);
    let sem = Arc::new(tokio::sync::Semaphore::new(max_concurrent));
    let (tx, mut rx) = mpsc::channel::<DumpJob>(queue_cap);
    let drain_tx = tx.clone();

    let worker = tokio::spawn(async move {
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
                            // Count the bytes before the send so the worker
                            // can never release them first.
                            add_queue_bytes(est);
                            match drain_tx.try_send(job) {
                                Ok(()) => {}
                                Err(mpsc::error::TrySendError::Full(returned)) => {
                                    release_queue_bytes(est);
                                    // Channel filled before we could finish
                                    // draining — re-spool this one and try
                                    // again next loop iteration.
                                    if let Err(err) = spool::spool_write(&returned).await {
                                        crate::metrics::dropped(
                                            crate::metrics::reason::SPOOL_ERROR,
                                        );
                                        tracing::warn!(
                                            "remote dump: re-spool failed after drain refusal: {err}"
                                        );
                                    }
                                    break;
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => {
                                    release_queue_bytes(est);
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

            // Batch-drain pending jobs, but cap the drain to avoid
            // building an enormous Vec when the channel is backed up.
            // Drained jobs stay counted in QUEUE_BYTES until their upload
            // ends, so the memory budget covers them too.
            let mut batch = Vec::with_capacity(max_drain.min(64));
            batch.push(first_job);
            while batch.len() < max_drain {
                match rx.try_recv() {
                    Ok(job) => batch.push(job),
                    Err(_) => break,
                }
            }

            // Dedup against in-flight keys and group by (endpoint, site).
            // The server files a whole batch under one `x-cache-site`, so
            // only jobs with the same cache_site may share a request.
            let mut groups: HashMap<(String, String), Vec<DumpJob>> = HashMap::new();
            for job in batch {
                if !inflight.insert(job.cache_key.clone()) {
                    release_queue_bytes(job.estimated_bytes());
                    crate::metrics::dropped(crate::metrics::reason::INFLIGHT_DUP);
                    continue;
                }
                let base_url = client::resolve_base_url(job.dump_remote.as_deref()).to_string();
                groups
                    .entry((base_url, job.cache_site.clone()))
                    .or_default()
                    .push(job);
            }

            // Spawn upload tasks per group, chunked into batches.
            let mut groups = groups.into_iter();
            'groups: while let Some(((base_url, cache_site), jobs)) = groups.next() {
                let mut chunks = chunks_into_vec(jobs, MAX_BATCH_SIZE).into_iter();
                while let Some(chunk) = chunks.next() {
                    let elapsed = last_spawn.elapsed();
                    if elapsed < spawn_interval {
                        tokio::time::sleep(spawn_interval - elapsed).await;
                    }
                    last_spawn = tokio::time::Instant::now();

                    let permit = match sem.clone().acquire_owned().await {
                        Ok(p) => p,
                        Err(_) => {
                            // Semaphore closed: nothing more will upload.
                            // Release everything still held.
                            let rest = std::iter::once(chunk)
                                .chain(chunks)
                                .flatten()
                                .chain(groups.flat_map(|(_, jobs)| jobs));
                            for job in rest {
                                inflight.remove(&job.cache_key);
                                release_queue_bytes(job.estimated_bytes());
                            }
                            break 'groups;
                        }
                    };

                    let inflight_ref = inflight.clone();
                    let base_url = base_url.clone();
                    let cache_site = cache_site.clone();
                    let keys: Vec<String> = chunk.iter().map(|j| j.cache_key.clone()).collect();
                    let chunk_bytes: usize = chunk.iter().map(DumpJob::estimated_bytes).sum();
                    let upload = upload.clone();

                    join_set.spawn(async move {
                        let _permit = permit;
                        let started = std::time::Instant::now();

                        let result =
                            tokio::time::timeout(timeout, upload(chunk, cache_site, base_url)).await;

                        if result.is_err() {
                            crate::metrics::upload(
                                crate::metrics::outcome::TIMEOUT,
                                started.elapsed().as_secs_f64() * 1000.0,
                            );
                            tracing::warn!(
                                "remote cache dump: batch of {} timed out after {}ms",
                                keys.len(),
                                timeout.as_millis(),
                            );
                        }

                        // The chunk (and its bodies) is gone now.
                        release_queue_bytes(chunk_bytes);

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

    (tx, worker)
}

fn reap_completed(join_set: &mut JoinSet<Vec<String>>, inflight: &Arc<dashmap::DashSet<String>>) {
    while let Some(result) = join_set.try_join_next() {
        if let Ok(keys) = result {
            for key in keys {
                inflight.remove(&key);
            }
        }
    }
}

async fn upload_chunk(jobs: Vec<DumpJob>, cache_site: &str, base_url: &str) {
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
        client::dump_site_batch_to_remote(payloads, cache_site, base_url).await;
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
///
/// `queue_cap` is clamped to 1..=65,536, with a warning when changed.
/// Queue and upload capacities are validated before the sender is published;
/// the first initialization's settings apply for the lifetime of the worker.
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

async fn default_worker_sender() -> &'static mpsc::Sender<DumpJob> {
    REMOTE_DUMP_TX
        // Read and warn about default configuration only during initialization,
        // rather than logging the same invalid environment on every enqueue.
        .get_or_init(|| init_inner(default_queue_cap(), default_qps(), default_timeout_ms()))
        .await
}

/// Returns `true` if the job's body is within the configured size limit.
/// Oversized bodies are dropped outright — they're too big for the remote
/// cache server as well, so disk-spooling them would just waste space.
#[inline]
fn body_within_size(job: &DumpJob) -> bool {
    let max = default_max_body_size();
    if max > 0 && job.body.len() > max {
        crate::metrics::dropped(crate::metrics::reason::OVERSIZE);
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

/// Bounds concurrent spool writes. Each pending write holds its job (body
/// included) in memory, so without this a burst of overflow would pin an
/// unbounded number of bodies while the disk catches up.
static SPOOL_WRITERS: std::sync::OnceLock<Arc<tokio::sync::Semaphore>> = std::sync::OnceLock::new();

fn spool_writers() -> &'static Arc<tokio::sync::Semaphore> {
    SPOOL_WRITERS.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(default_spool_writers())))
}

/// Maximum concurrent spool writes. Overflow beyond this is dropped
/// (counted as `spool_busy`).
///
/// Override via env `HYBRID_CACHE_REMOTE_SPOOL_WRITERS`. Default 16.
/// Clamped to 1..=64, with a warning for invalid values.
pub fn default_spool_writers() -> usize {
    SPOOL_WRITER_CAPACITY.read_env()
}

/// Fire-and-forget spool write. Returns `true` if the spool task was
/// scheduled (best-effort — success of the write itself is logged).
/// Returns `false` if the spool is disabled, no worker exists to drain
/// it, every spool writer slot is busy, or we are not running under a
/// tokio runtime — in which case the job is dropped as before the spool
/// existed. `overflow_reason` labels the drop when the spool is off.
fn spool_fire_and_forget(job: DumpJob, overflow_reason: &'static str) -> bool {
    if !spool_enabled() {
        crate::metrics::dropped(overflow_reason);
        return false;
    }
    if REMOTE_DUMP_TX.get().is_none() {
        crate::metrics::dropped(crate::metrics::reason::NO_WORKER);
        tracing::debug!("remote dump dropped: no worker to drain spool");
        return false;
    }
    if tokio::runtime::Handle::try_current().is_err() {
        crate::metrics::dropped(crate::metrics::reason::NO_RUNTIME);
        tracing::debug!("remote dump dropped: no tokio runtime for spool write");
        return false;
    }
    let permit = match spool_writers().clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            crate::metrics::dropped(crate::metrics::reason::SPOOL_BUSY);
            tracing::debug!(
                "remote dump dropped: all spool writers busy for {}",
                job.cache_key
            );
            return false;
        }
    };
    tokio::spawn(async move {
        let _permit = permit;
        match spool::spool_write(&job).await {
            Ok(n) => {
                tracing::debug!(
                    "remote dump spooled to disk: {n} bytes for {}",
                    job.cache_key
                );
            }
            Err(err) => {
                crate::metrics::dropped(crate::metrics::reason::SPOOL_ERROR);
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
    let tx = default_worker_sender().await;

    if !queue_has_memory_headroom() {
        return spool_fire_and_forget(job, crate::metrics::reason::MEMORY_BUDGET);
    }

    let est = job.estimated_bytes();
    add_queue_bytes(est);
    match tx.try_send(job) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(returned)) => {
            release_queue_bytes(est);
            spool_fire_and_forget(returned, crate::metrics::reason::QUEUE_FULL)
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            release_queue_bytes(est);
            crate::metrics::dropped(crate::metrics::reason::CLOSED);
            false
        }
    }
}

/// Enqueue with backpressure (waits up to 2,500 ms for channel room).
///
/// Does not involve the disk spool — this variant is for callers that
/// explicitly want to wait rather than spill. Use `enqueue_best_effort`
/// or `try_enqueue` for the disk-backed fast path.
/// Returns the original job in `SendError` if the receiver closes or the
/// admission deadline expires. Cancelling the wait does not retain queue
/// capacity or count the unaccepted job toward the queue's byte budget.
pub async fn enqueue(job: DumpJob) -> Result<(), mpsc::error::SendError<DumpJob>> {
    if !body_within_size(&job) {
        return Ok(());
    }
    let tx = default_worker_sender().await;
    enqueue_with_timeout(tx, job, QUEUE_ADMISSION_TIMEOUT).await
}

async fn enqueue_with_timeout(
    tx: &mpsc::Sender<DumpJob>,
    job: DumpJob,
    timeout: Duration,
) -> Result<(), mpsc::error::SendError<DumpJob>> {
    // Reserve without transferring the job: timeout can return it through
    // the existing error type, and cancellation cannot leak byte accounting.
    let permit = match tokio::time::timeout(timeout, tx.reserve()).await {
        Ok(Ok(permit)) => permit,
        Ok(Err(_)) => {
            crate::metrics::dropped(crate::metrics::reason::CLOSED);
            return Err(mpsc::error::SendError(job));
        }
        Err(_) => {
            crate::metrics::dropped(crate::metrics::reason::QUEUE_TIMEOUT);
            tracing::warn!(
                "remote dump: queue admission timed out after {}ms",
                timeout.as_millis(),
            );
            return Err(mpsc::error::SendError(job));
        }
    };
    // No await between counting and sending: neither cancellation nor the
    // receiving worker can race ahead of the accounting increment.
    add_queue_bytes(job.estimated_bytes());
    permit.send(job);
    Ok(())
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
            add_queue_bytes(est);
            match tx.try_send(job) {
                Ok(()) => return true,
                Err(mpsc::error::TrySendError::Full(returned)) => {
                    release_queue_bytes(est);
                    return spool_fire_and_forget(returned, crate::metrics::reason::QUEUE_FULL);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    release_queue_bytes(est);
                    crate::metrics::dropped(crate::metrics::reason::CLOSED);
                    return false;
                }
            }
        }
        crate::metrics::dropped(crate::metrics::reason::NO_WORKER);
        return false; // worker not initialized
    }
    // Over memory budget — spool instead of dropping.
    spool_fire_and_forget(job, crate::metrics::reason::MEMORY_BUDGET)
}

/// Returns the approximate number of bytes currently queued for upload.
pub fn queue_bytes() -> usize {
    QUEUE_BYTES.load(Ordering::Relaxed)
}

/// Init the worker with default settings.
pub async fn init_default_worker() {
    default_worker_sender().await;
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

// Practical bounds protect memory/task counts as well as Tokio's much larger
// hard semaphore limit. Public APIs return plain values, so invalid settings
// are clamped (or fall back on parse failure) with a visible warning.
#[derive(Clone, Copy)]
struct CapacitySetting {
    name: &'static str,
    default: usize,
    max: usize,
}

const QUEUE_CAPACITY: CapacitySetting = CapacitySetting {
    name: "HYBRID_CACHE_REMOTE_QUEUE_CAP",
    default: 2_000,
    max: 65_536,
};
const UPLOAD_CONCURRENCY: CapacitySetting = CapacitySetting {
    name: "HYBRID_CACHE_REMOTE_MAX_CONCURRENT",
    default: 8,
    max: 256,
};
const SPOOL_WRITER_CAPACITY: CapacitySetting = CapacitySetting {
    name: "HYBRID_CACHE_REMOTE_SPOOL_WRITERS",
    default: 16,
    max: 64,
};

impl CapacitySetting {
    fn clamp(self, value: usize) -> usize {
        let max = self.max.min(tokio::sync::Semaphore::MAX_PERMITS);
        let clamped = value.clamp(1, max);
        if clamped != value {
            tracing::warn!(
                setting = self.name,
                requested = value,
                effective = clamped,
                "remote dump: capacity outside supported range 1..={max}; clamping",
            );
        }
        clamped
    }

    fn parse(self, value: Option<&str>) -> usize {
        let value = match value {
            Some(value) => match value.parse::<usize>() {
                Ok(value) => value,
                Err(_) => {
                    tracing::warn!(
                        setting = self.name,
                        effective = self.default,
                        "remote dump: invalid capacity; using default",
                    );
                    self.default
                }
            },
            None => self.default,
        };
        self.clamp(value)
    }

    fn read_env(self) -> usize {
        match std::env::var(self.name) {
            Ok(value) => self.parse(Some(&value)),
            Err(std::env::VarError::NotPresent) => self.parse(None),
            Err(std::env::VarError::NotUnicode(_)) => {
                tracing::warn!(
                    setting = self.name,
                    effective = self.default,
                    "remote dump: capacity is not Unicode; using default",
                );
                self.parse(None)
            }
        }
    }
}

/// Queue capacity from `HYBRID_CACHE_REMOTE_QUEUE_CAP`. Default 2,000.
/// Clamped to 1..=65,536, with a warning for invalid values.
pub fn default_queue_cap() -> usize {
    QUEUE_CAPACITY.read_env()
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

/// Upload concurrency from `HYBRID_CACHE_REMOTE_MAX_CONCURRENT`. Default 8.
/// Clamped to 1..=256, with a warning for invalid values.
pub fn default_max_concurrent() -> usize {
    UPLOAD_CONCURRENCY.read_env()
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

#[cfg(test)]
mod tests {
    use super::*;

    const CAPACITIES: [CapacitySetting; 3] =
        [QUEUE_CAPACITY, UPLOAD_CONCURRENCY, SPOOL_WRITER_CAPACITY];
    const TEST_TIMEOUT: Duration = Duration::from_secs(1);

    fn job(key: &str) -> DumpJob {
        DumpJob {
            cache_key: key.into(),
            cache_site: "offline-site".into(),
            url: "https://offline.invalid/page".into(),
            method: "GET".into(),
            status: 200,
            request_headers: HashMap::new(),
            response_headers: HashMap::new(),
            body: b"offline body".to_vec(),
            http_version: HttpVersion::Http11,
            // Avoid even initializing the global endpoint in these tests.
            dump_remote: Some("http://offline.invalid".into()),
        }
    }

    #[test]
    fn capacity_defaults_are_preserved() {
        for (setting, expected) in CAPACITIES.into_iter().zip([2_000, 8, 16]) {
            assert_eq!(setting.parse(None), expected, "{}", setting.name);
        }
        assert_eq!(QUEUE_ADMISSION_TIMEOUT, Duration::from_millis(2_500));
    }

    #[test]
    fn valid_capacities_are_preserved() {
        for setting in CAPACITIES {
            for value in [1, setting.default, setting.max] {
                assert_eq!(setting.clamp(value), value, "{}", setting.name);
                assert_eq!(setting.parse(Some(&value.to_string())), value);
            }
        }
    }

    #[test]
    fn zero_capacities_are_nonzero_before_construction() {
        for setting in CAPACITIES {
            assert_eq!(setting.clamp(0), 1, "{}", setting.name);
            let capacity = setting.parse(Some("0"));
            assert_eq!(capacity, 1);
            assert_eq!(tokio::sync::Semaphore::new(capacity).available_permits(), 1);
        }
        let (tx, _rx) = mpsc::channel::<DumpJob>(QUEUE_CAPACITY.clamp(0));
        assert_eq!(tx.max_capacity(), 1);
    }

    #[test]
    fn oversized_capacities_are_bounded_before_construction() {
        for setting in CAPACITIES {
            for value in [
                setting.max + 1,
                tokio::sync::Semaphore::MAX_PERMITS + 1,
                usize::MAX,
            ] {
                assert_eq!(setting.clamp(value), setting.max, "{}", setting.name);
                let capacity = setting.parse(Some(&value.to_string()));
                assert_eq!(capacity, setting.max);
                assert_eq!(
                    tokio::sync::Semaphore::new(capacity).available_permits(),
                    setting.max,
                );
            }
        }
        let (tx, _rx) = mpsc::channel::<DumpJob>(QUEUE_CAPACITY.clamp(usize::MAX));
        assert_eq!(tx.max_capacity(), 65_536);
    }

    #[test]
    fn malformed_capacity_values_use_valid_defaults() {
        for setting in CAPACITIES {
            // Include a number too large to parse even as a 64-bit usize.
            for value in ["", "invalid", "-1", "18446744073709551616"] {
                assert_eq!(setting.parse(Some(value)), setting.default);
            }
        }
    }

    #[test]
    fn spool_writer_limits_keep_overflow_nonblocking() {
        for value in [0, 16, usize::MAX] {
            let capacity = SPOOL_WRITER_CAPACITY.parse(Some(&value.to_string()));
            let sem = Arc::new(tokio::sync::Semaphore::new(capacity));
            let permits = sem.clone().try_acquire_many_owned(capacity as u32).unwrap();
            assert!(matches!(
                sem.clone().try_acquire_owned(),
                Err(tokio::sync::TryAcquireError::NoPermits),
            ));
            drop(permits);
            assert!(sem.try_acquire_owned().is_ok());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn closed_receiver_returns_the_original_job_immediately() {
        let (tx, rx) = mpsc::channel(1);
        drop(rx);
        let started = tokio::time::Instant::now();
        let err = enqueue_with_timeout(&tx, job("closed"), TEST_TIMEOUT)
            .await
            .unwrap_err();
        assert_eq!(err.0.cache_key, "closed");
        assert_eq!(err.0.body, b"offline body");
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn receiver_closure_wakes_pending_admission() {
        let (tx, rx) = mpsc::channel(1);
        tx.try_send(job("occupant")).unwrap();
        let mut pending = Box::pin(enqueue_with_timeout(&tx, job("waiting"), TEST_TIMEOUT));
        assert!(
            std::future::poll_fn(|cx| {
                std::task::Poll::Ready(pending.as_mut().poll(cx).is_pending())
            })
            .await
        );
        drop(rx);
        let started = tokio::time::Instant::now();
        let err = pending.await.unwrap_err();
        assert_eq!(err.0.cache_key, "waiting");
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn closing_receiver_rejects_pending_admission_without_discarding_queued_jobs() {
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(job("occupant")).unwrap();
        let mut pending = Box::pin(enqueue_with_timeout(&tx, job("waiting"), TEST_TIMEOUT));
        assert!(
            std::future::poll_fn(|cx| {
                std::task::Poll::Ready(pending.as_mut().poll(cx).is_pending())
            })
            .await
        );

        // close() must wake the reservation even while the receiver and its
        // previously accepted job stay alive for a graceful drain.
        rx.close();
        let started = tokio::time::Instant::now();
        let err = pending.await.unwrap_err();
        assert_eq!(err.0.cache_key, "waiting");
        assert_eq!(err.0.body, b"offline body");
        assert_eq!(started.elapsed(), Duration::ZERO);
        assert!(tx.is_closed());
        assert_eq!(rx.try_recv().unwrap().cache_key, "occupant");
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn full_queue_times_out_without_losing_the_job_or_capacity() {
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(job("occupant")).unwrap();
        let started = tokio::time::Instant::now();
        let err = tokio::time::timeout(
            QUEUE_ADMISSION_TIMEOUT + TEST_TIMEOUT,
            enqueue_with_timeout(&tx, job("timed-out"), QUEUE_ADMISSION_TIMEOUT),
        )
        .await
        .expect("queue admission must have a deadline")
        .unwrap_err();
        assert!(started.elapsed() >= QUEUE_ADMISSION_TIMEOUT);
        assert!(started.elapsed() < QUEUE_ADMISSION_TIMEOUT + TEST_TIMEOUT);
        assert_eq!(err.0.cache_key, "timed-out");
        assert_eq!(err.0.body, b"offline body");
        assert_eq!(rx.try_recv().unwrap().cache_key, "occupant");
        assert_eq!(tx.capacity(), 1);
        // The timed-out reservation must not steal the newly available slot.
        tx.try_send(err.0).unwrap();
        assert_eq!(rx.try_recv().unwrap().cache_key, "timed-out");
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_admission_releases_its_waiter() {
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(job("occupant")).unwrap();
        let mut pending = Box::pin(enqueue_with_timeout(&tx, job("cancelled"), TEST_TIMEOUT));
        assert!(
            std::future::poll_fn(|cx| {
                std::task::Poll::Ready(pending.as_mut().poll(cx).is_pending())
            })
            .await
        );
        drop(pending);
        assert_eq!(rx.try_recv().unwrap().cache_key, "occupant");
        assert_eq!(tx.capacity(), 1);
        tx.try_send(job("replacement")).unwrap();
        assert_eq!(rx.try_recv().unwrap().cache_key, "replacement");
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_woken_admission_returns_its_assigned_capacity() {
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(job("occupant")).unwrap();
        let mut pending = Box::pin(enqueue_with_timeout(&tx, job("cancelled"), TEST_TIMEOUT));
        assert!(
            std::future::poll_fn(|cx| {
                std::task::Poll::Ready(pending.as_mut().poll(cx).is_pending())
            })
            .await
        );

        assert_eq!(rx.try_recv().unwrap().cache_key, "occupant");
        // Tokio has assigned the slot to the waiting reservation, but the
        // admission future has not been polled again to accept its job.
        assert_eq!(tx.capacity(), 0);
        drop(pending);
        assert_eq!(tx.capacity(), 1);

        enqueue_with_timeout(&tx, job("replacement"), TEST_TIMEOUT)
            .await
            .unwrap();
        let accepted = rx.try_recv().unwrap();
        assert_eq!(accepted.cache_key, "replacement");
        release_queue_bytes(accepted.estimated_bytes());
        assert_eq!(tx.capacity(), 1);
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn admission_succeeds_when_capacity_becomes_available() {
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(job("occupant")).unwrap();
        let mut pending = Box::pin(enqueue_with_timeout(&tx, job("accepted"), TEST_TIMEOUT));
        assert!(
            std::future::poll_fn(|cx| {
                std::task::Poll::Ready(pending.as_mut().poll(cx).is_pending())
            })
            .await
        );
        assert_eq!(rx.try_recv().unwrap().cache_key, "occupant");
        pending.await.unwrap();
        let accepted = rx.try_recv().unwrap();
        assert_eq!(accepted.cache_key, "accepted");
        // Release only this accepted job's bytes, without resetting globals.
        release_queue_bytes(accepted.estimated_bytes());
        assert_eq!(tx.capacity(), 1);
    }

    async fn assert_worker_schedules(queue_cap: usize, concurrency: usize) {
        let (seen_tx, mut seen_rx) = mpsc::unbounded_channel();
        let (tx, worker) = spawn_worker(
            queue_cap,
            u32::MAX,
            2_500,
            concurrency,
            move |jobs, site, base| {
                let seen_tx = seen_tx.clone();
                async move { seen_tx.send((jobs, site, base)).unwrap() }
            },
        );
        assert_eq!(tx.max_capacity(), QUEUE_CAPACITY.clamp(queue_cap));
        for key in ["first", "second", "third"] {
            enqueue_with_timeout(&tx, job(key), TEST_TIMEOUT)
                .await
                .unwrap();
        }
        let mut keys = Vec::new();
        while keys.len() < 3 {
            let (jobs, site, base) = tokio::time::timeout(TEST_TIMEOUT, seen_rx.recv())
                .await
                .expect("worker must schedule uploads")
                .unwrap();
            assert_eq!(site, "offline-site");
            assert_eq!(base, "http://offline.invalid");
            keys.extend(jobs.into_iter().map(|job| job.cache_key));
        }
        keys.sort();
        assert_eq!(keys, ["first", "second", "third"]);
        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());
    }

    #[tokio::test(start_paused = true)]
    async fn zero_queue_and_concurrency_still_schedule_uploads() {
        assert_worker_schedules(0, 0).await;
    }

    #[tokio::test(start_paused = true)]
    async fn oversized_queue_and_concurrency_still_schedule_uploads() {
        assert_worker_schedules(usize::MAX, usize::MAX).await;
    }

    #[tokio::test(start_paused = true)]
    async fn default_capacities_schedule_uploads() {
        assert_worker_schedules(QUEUE_CAPACITY.parse(None), UPLOAD_CONCURRENCY.parse(None)).await;
    }

    #[tokio::test(start_paused = true)]
    async fn upload_deadline_releases_the_only_permit_for_the_next_job() {
        struct DropSignal(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for DropSignal {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Relaxed);
            }
        }

        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancelled_upload = cancelled.clone();
        let (seen_tx, mut seen_rx) = mpsc::unbounded_channel();
        let (tx, worker) = spawn_worker(1, u32::MAX, 25, 1, move |jobs, _, _| {
            let seen_tx = seen_tx.clone();
            let cancelled = cancelled_upload.clone();
            async move {
                let key = jobs[0].cache_key.clone();
                seen_tx.send(key.clone()).unwrap();
                if key == "slow" {
                    let _signal = DropSignal(cancelled);
                    std::future::pending::<()>().await;
                }
            }
        });
        enqueue_with_timeout(&tx, job("slow"), TEST_TIMEOUT)
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(TEST_TIMEOUT, seen_rx.recv())
                .await
                .unwrap()
                .unwrap(),
            "slow",
        );
        let started = tokio::time::Instant::now();
        enqueue_with_timeout(&tx, job("next"), TEST_TIMEOUT)
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(TEST_TIMEOUT, seen_rx.recv())
                .await
                .expect("timed-out upload must release its permit")
                .unwrap(),
            "next",
        );
        assert!(started.elapsed() >= Duration::from_millis(25));
        assert!(started.elapsed() < TEST_TIMEOUT);
        assert!(cancelled.load(Ordering::Relaxed));
        worker.abort();
        assert!(worker.await.unwrap_err().is_cancelled());
    }
}
