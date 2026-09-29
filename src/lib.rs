//! Shared remote cache upload worker for spider and chromey.
//!
//! This crate provides a high-performance, concurrent batch upload worker
//! for sending cached HTTP responses to a remote `hybrid_cache_server`.
//!
//! # Client injection
//!
//! Call [`set_client`] at startup, before the first upload or read, to
//! share your application's `reqwest::Client`. This avoids compiling a
//! second TLS stack and reuses the existing connection pool. If nothing is
//! injected before first use, the crate builds a default client
//! ([`default_client_builder`]: 250 ms connect timeout, request timeout
//! equal to the batch timeout) and a later [`set_client`] is ignored.
//! [`client_source`] reports which one is active.
//!
//! # Site keys
//!
//! Every upload carries the job's `cache_site` as the `x-cache-site`
//! header and as the payload's `website_key`. A batch request only ever
//! holds jobs with the same `cache_site`.
//!
//! # Features
//!
//! - **Batch-drain**: Collects pending jobs from the channel before uploading
//! - **Concurrent uploads**: Configurable parallelism via semaphore (default 8)
//! - **Lock-free dedup**: DashSet for in-flight key tracking (no Mutex, no deadlocks)
//! - **Rate limiting**: Per-spawn pacing without blocking the drain loop
//! - **Batch POST**: Uses `/cache/index/batch` when multiple jobs are ready
//! - **Best-effort**: `try_enqueue` never blocks; drops on full queue
//! - **Metrics**: with the default `metrics` feature, counters, a gauge and
//!   a histogram go through the `metrics` facade (see [`metrics`])

pub mod client;
pub mod metrics;
pub mod spool;
pub mod types;
pub mod worker;

// Re-export the primary public API at crate root.
pub use client::{
    build_payload, client_source, default_client_builder, dump_batch_to_remote,
    dump_site_batch_to_remote, dump_to_remote, get_client, get_endpoint, resolve_base_url,
    set_client, set_endpoint, try_set_client, ClientSource,
};
pub use spool::{spool_bytes, spool_dir, spool_max_bytes};
pub use types::{HttpVersion, HybridCachePayload};
pub use worker::{
    default_max_body_size, default_max_concurrent, default_qps, default_queue_cap,
    default_queue_memory_budget, default_spool_writers, default_timeout_ms, enqueue,
    enqueue_best_effort, init_default_worker, init_remote_dump_worker, queue_bytes,
    set_skip_browser_dumps_enabled, set_spool_enabled, skip_browser_dumps_enabled, spool_enabled,
    try_enqueue, worker_inited, DumpJob,
};
