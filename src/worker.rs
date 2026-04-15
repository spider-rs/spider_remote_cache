use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::{mpsc, OnceCell};
use tokio::task::JoinSet;

use crate::client;
use crate::types::HttpVersion;

static REMOTE_DUMP_TX: OnceCell<mpsc::Sender<DumpJob>> = OnceCell::const_new();

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

/// Maximum number of items to send in a single batch POST.
const MAX_BATCH_SIZE: usize = 64;

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

        loop {
            reap_completed(&mut join_set, &inflight);

            let first_job = match rx.recv().await {
                Some(job) => job,
                None => break,
            };

            // Batch-drain all pending jobs.
            let mut batch = Vec::with_capacity(64);
            batch.push(first_job);
            while let Ok(job) = rx.try_recv() {
                batch.push(job);
            }

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

        // Graceful shutdown.
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

/// Auto-init on first use + best-effort enqueue (never blocks on full queue).
pub async fn enqueue_best_effort(job: DumpJob) -> bool {
    let tx =
        init_remote_dump_worker(default_queue_cap(), default_qps(), default_timeout_ms()).await;
    tx.try_send(job).is_ok()
}

/// Enqueue with backpressure (blocks if queue is full).
pub async fn enqueue(job: DumpJob) -> Result<(), mpsc::error::SendError<DumpJob>> {
    let tx =
        init_remote_dump_worker(default_queue_cap(), default_qps(), default_timeout_ms()).await;
    tx.send(job).await
}

/// Returns true if the worker has been initialized.
pub fn worker_inited() -> bool {
    REMOTE_DUMP_TX.initialized()
}

/// Non-async enqueue (fast path). Drops if queue is full or worker not initialized.
pub fn try_enqueue(job: DumpJob) -> bool {
    REMOTE_DUMP_TX
        .get()
        .and_then(|tx| tx.try_send(job).ok())
        .is_some()
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
        .unwrap_or(10_000)
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
        .unwrap_or(32)
}
