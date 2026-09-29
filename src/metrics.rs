//! Metric names and thin recording helpers.
//!
//! With the `metrics` cargo feature (on by default) these write through the
//! [`metrics`](https://docs.rs/metrics) facade, so they land in whatever
//! recorder the host process installed and cost one atomic load when none
//! is installed. Without the feature every helper compiles to nothing.
//!
//! | name | kind | labels |
//! |------|------|--------|
//! | `spider_remote_cache_upload_total` | counter | `outcome` = `ok`, `http_error`, `send_error`, `timeout` |
//! | `spider_remote_cache_upload_duration_ms` | histogram | `outcome` |
//! | `spider_remote_cache_queue_bytes` | gauge | none |
//! | `spider_remote_cache_dropped_total` | counter | `reason` |

/// Counter of upload POSTs (single or batch) by outcome.
pub const UPLOAD_TOTAL: &str = "spider_remote_cache_upload_total";
/// Histogram of upload request latency in milliseconds.
pub const UPLOAD_DURATION_MS: &str = "spider_remote_cache_upload_duration_ms";
/// Gauge of bytes held by queued and in-flight jobs.
pub const QUEUE_BYTES: &str = "spider_remote_cache_queue_bytes";
/// Counter of jobs that were never uploaded, by reason.
pub const DROPPED_TOTAL: &str = "spider_remote_cache_dropped_total";

/// Upload outcome label values.
pub mod outcome {
    /// The server answered 2xx.
    pub const OK: &str = "ok";
    /// The server answered with a non-2xx status.
    pub const HTTP_ERROR: &str = "http_error";
    /// Connect or transport failure before a response arrived.
    pub const SEND_ERROR: &str = "send_error";
    /// The worker's per-batch timeout fired first.
    pub const TIMEOUT: &str = "timeout";
}

/// Drop reason label values.
pub mod reason {
    /// Body larger than `HYBRID_CACHE_REMOTE_MAX_BODY_SIZE`.
    pub const OVERSIZE: &str = "oversize";
    /// Channel full and the spool is off.
    pub const QUEUE_FULL: &str = "queue_full";
    /// Memory budget exceeded and the spool is off.
    pub const MEMORY_BUDGET: &str = "memory_budget";
    /// Worker channel closed.
    pub const CLOSED: &str = "closed";
    /// No worker was ever initialized.
    pub const NO_WORKER: &str = "no_worker";
    /// No tokio runtime to run the spool write on.
    pub const NO_RUNTIME: &str = "no_runtime";
    /// Every spool writer slot was busy.
    pub const SPOOL_BUSY: &str = "spool_busy";
    /// The spool write itself failed (full, I/O error).
    pub const SPOOL_ERROR: &str = "spool_error";
    /// Same cache key already in flight.
    pub const INFLIGHT_DUP: &str = "inflight_dup";
}

#[inline]
#[allow(unused_variables)]
pub(crate) fn upload(outcome: &'static str, elapsed_ms: f64) {
    #[cfg(feature = "metrics")]
    {
        ::metrics::counter!(UPLOAD_TOTAL, "outcome" => outcome).increment(1);
        ::metrics::histogram!(UPLOAD_DURATION_MS, "outcome" => outcome).record(elapsed_ms);
    }
}

#[inline]
#[allow(unused_variables)]
pub(crate) fn dropped(reason: &'static str) {
    #[cfg(feature = "metrics")]
    ::metrics::counter!(DROPPED_TOTAL, "reason" => reason).increment(1);
}

#[inline]
#[allow(unused_variables)]
pub(crate) fn queue_bytes(bytes: usize) {
    #[cfg(feature = "metrics")]
    ::metrics::gauge!(QUEUE_BYTES).set(bytes as f64);
}
