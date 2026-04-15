//! Shared remote cache upload worker for spider and chromey.
//!
//! This crate provides a high-performance, concurrent batch upload worker
//! for sending cached HTTP responses to a remote `hybrid_cache_server`.
//!
//! # Client injection
//!
//! Call [`set_client`] at startup to share your application's
//! `reqwest::Client` — this avoids compiling a second TLS stack and
//! reuses the existing connection pool.  If never called, a minimal
//! default client is created on first use.
//!
//! # Features
//!
//! - **Batch-drain**: Collects pending jobs from the channel before uploading
//! - **Concurrent uploads**: Configurable parallelism via semaphore (default 32)
//! - **Lock-free dedup**: DashSet for in-flight key tracking (no Mutex, no deadlocks)
//! - **Rate limiting**: Per-spawn pacing without blocking the drain loop
//! - **Batch POST**: Uses `/cache/index/batch` when multiple jobs are ready
//! - **Best-effort**: `try_enqueue` never blocks; drops on full queue

pub mod client;
pub mod types;
pub mod worker;

// Re-export the primary public API at crate root.
pub use client::{
    build_payload, dump_batch_to_remote, dump_to_remote, get_client, get_endpoint,
    resolve_base_url, set_client, set_endpoint,
};
pub use types::{HttpVersion, HybridCachePayload};
pub use worker::{
    default_max_concurrent, default_queue_cap, default_qps, default_timeout_ms, enqueue,
    enqueue_best_effort, init_default_worker, init_remote_dump_worker, try_enqueue,
    worker_inited, DumpJob,
};
