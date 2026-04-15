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
use crate::types::HttpVersion;

static REMOTE_DUMP_TX: OnceCell<mpsc::Sender<DumpJob>> = OnceCell::const_new();

/// Approximate number of bytes currently sitting in the queue + batch drain.
/// Incremented on enqueue, decremented when the worker consumes a job.
static QUEUE_BYTES: AtomicUsize = AtomicUsize::new(0);

/// A job representing a single cached response to upload to the remote cache.
#[derive(Debug)]
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

            let first_job = match rx.recv().await {
                Some(job) => job,
                None => break,
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

/// Returns `true` if the job is within the configured body size limit.
/// Oversized jobs are silently dropped to protect memory and CPU.
#[inline]
fn within_limits(job: &DumpJob) -> bool {
    let max = default_max_body_size();
    if max > 0 && job.body.len() > max {
        tracing::debug!(
            "remote dump skipped: body {} bytes exceeds max {} for {}",
            job.body.len(),
            max,
            job.cache_key,
        );
        return false;
    }
    let budget = default_queue_memory_budget();
    if budget > 0 && QUEUE_BYTES.load(Ordering::Relaxed) > budget {
        tracing::debug!(
            "remote dump skipped: queue memory budget {}B exceeded",
            budget,
        );
        return false;
    }
    true
}

/// Auto-init on first use + best-effort enqueue (never blocks on full queue).
pub async fn enqueue_best_effort(job: DumpJob) -> bool {
    if !within_limits(&job) {
        return false;
    }
    let tx =
        init_remote_dump_worker(default_queue_cap(), default_qps(), default_timeout_ms()).await;
    let est = job.estimated_bytes();
    match tx.try_send(job) {
        Ok(()) => {
            QUEUE_BYTES.fetch_add(est, Ordering::Relaxed);
            true
        }
        Err(_) => false,
    }
}

/// Enqueue with backpressure (blocks if queue is full).
pub async fn enqueue(job: DumpJob) -> Result<(), mpsc::error::SendError<DumpJob>> {
    if !within_limits(&job) {
        return Ok(()); // silently drop oversized/over-budget jobs
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

/// Non-async enqueue (fast path). Drops if queue is full or worker not initialized.
pub fn try_enqueue(job: DumpJob) -> bool {
    if !within_limits(&job) {
        return false;
    }
    let est = job.estimated_bytes();
    let ok = REMOTE_DUMP_TX
        .get()
        .and_then(|tx| tx.try_send(job).ok())
        .is_some();
    if ok {
        QUEUE_BYTES.fetch_add(est, Ordering::Relaxed);
    }
    ok
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
