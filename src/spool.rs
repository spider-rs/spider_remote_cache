//! Disk-backed overflow spool for [`DumpJob`].
//!
//! The in-memory [`mpsc`] channel + [`QUEUE_BYTES`] budget is the fast path.
//! When either refuses a job — channel full or memory budget exceeded — we
//! fall through to here: the job is serialized with `bincode` and atomically
//! renamed into a spool directory. A background drain task inside the worker
//! loop reloads spooled jobs back into the channel when the queue has
//! headroom.
//!
//! Guarantees:
//!
//! - **No panics.** Every filesystem op is matched and logged via `tracing`.
//!   Corrupt files are deleted (never retried in a loop).
//! - **Bounded disk.** [`spool_max_bytes`] caps total spool size; over-budget
//!   writes return [`std::io::ErrorKind::StorageFull`].
//! - **At-most-once replay.** Files are removed *before* deserialize, so a
//!   corrupt or partial file cannot cause an infinite retry.
//! - **Atomic durability on disk.** `.tmp` then rename — drain never sees a
//!   half-written file.
//! - **Dedup by key.** Filename is `blake3(cache_key)` truncated — duplicate
//!   enqueues of the same key overwrite in place rather than piling up.
//!
//! [`QUEUE_BYTES`]: crate::worker::queue_bytes
//! [`mpsc`]: tokio::sync::mpsc

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::fs;

use crate::worker::DumpJob;

/// Approximate bytes currently persisted to disk across all spool files.
static SPOOL_BYTES: AtomicU64 = AtomicU64::new(0);

/// Directory where spool files live.
///
/// Override via env `HYBRID_CACHE_REMOTE_SPOOL_DIR`.
/// Default: `/tmp/spider_remote_cache_spool`.
pub fn spool_dir() -> PathBuf {
    std::env::var("HYBRID_CACHE_REMOTE_SPOOL_DIR")
        .unwrap_or_else(|_| "/tmp/spider_remote_cache_spool".into())
        .into()
}

/// Maximum total bytes the spool is allowed to occupy on disk.
///
/// Override via env `HYBRID_CACHE_REMOTE_SPOOL_MAX_BYTES`.
/// Default: 512 MiB.
pub fn spool_max_bytes() -> u64 {
    std::env::var("HYBRID_CACHE_REMOTE_SPOOL_MAX_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(512 * 1024 * 1024)
}

/// Approximate bytes currently on disk. Wait-free observability.
pub fn spool_bytes() -> u64 {
    SPOOL_BYTES.load(Ordering::Relaxed)
}

fn file_name_for(cache_key: &str) -> String {
    let hash = blake3::hash(cache_key.as_bytes()).to_hex();
    let short: String = hash.as_str().chars().take(32).collect();
    format!("{short}.job")
}

fn tmp_name_for(cache_key: &str) -> String {
    let hash = blake3::hash(cache_key.as_bytes()).to_hex();
    let short: String = hash.as_str().chars().take(32).collect();
    format!("{short}.job.tmp")
}

/// Serialize a job and atomically rename it into the spool directory.
///
/// Returns the number of bytes written on success, or an I/O error. The
/// caller is responsible for dropping the job on error.
pub async fn spool_write(job: &DumpJob) -> std::io::Result<u64> {
    let dir = spool_dir();
    fs::create_dir_all(&dir).await?;

    let est = job.estimated_bytes() as u64;
    let max = spool_max_bytes();
    if max > 0 && SPOOL_BYTES.load(Ordering::Relaxed).saturating_add(est) > max {
        return Err(std::io::Error::new(
            std::io::ErrorKind::StorageFull,
            "spool over budget",
        ));
    }

    let serialized = bincode::serialize(job)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let bytes_len = serialized.len() as u64;

    let final_path = dir.join(file_name_for(&job.cache_key));
    let tmp_path = dir.join(tmp_name_for(&job.cache_key));

    fs::write(&tmp_path, &serialized).await?;
    fs::rename(&tmp_path, &final_path).await?;

    SPOOL_BYTES.fetch_add(bytes_len, Ordering::Relaxed);
    Ok(bytes_len)
}

/// Pop one spooled job.
///
/// Iterates the spool directory, reads the first `.job` file, deletes it,
/// then deserializes. Deleting before deserializing ensures a corrupt file
/// cannot cause an infinite retry.
pub async fn spool_pop_one() -> Option<DumpJob> {
    let dir = spool_dir();
    let mut rd = match fs::read_dir(&dir).await {
        Ok(rd) => rd,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return None,
        Err(err) => {
            tracing::warn!("spool: read_dir failed: {err}");
            return None;
        }
    };

    loop {
        let entry = match rd.next_entry().await {
            Ok(Some(e)) => e,
            Ok(None) => return None,
            Err(err) => {
                tracing::warn!("spool: next_entry failed: {err}");
                return None;
            }
        };

        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("job") {
            continue;
        }

        match try_take(&path).await {
            Some(job) => return Some(job),
            None => continue,
        }
    }
}

async fn try_take(path: &Path) -> Option<DumpJob> {
    let bytes = match fs::read(path).await {
        Ok(b) => b,
        Err(err) => {
            tracing::warn!("spool: read {:?} failed: {err}", path);
            // Try to remove it — if read failed, the file may be corrupt.
            let _ = fs::remove_file(path).await;
            return None;
        }
    };
    let file_len = bytes.len() as u64;

    if let Err(err) = fs::remove_file(path).await {
        tracing::warn!("spool: remove {:?} failed: {err}", path);
        // We still accept the job — filesystem will retry deletion later.
    }
    SPOOL_BYTES.fetch_sub(
        file_len.min(SPOOL_BYTES.load(Ordering::Relaxed)),
        Ordering::Relaxed,
    );

    match bincode::deserialize::<DumpJob>(&bytes) {
        Ok(job) => Some(job),
        Err(err) => {
            tracing::warn!("spool: deserialize {:?} failed: {err}", path);
            None
        }
    }
}

#[cfg(test)]
// The std Mutex guard is held across awaits on purpose: it serializes the
// process-wide env vars these tests set.
#[allow(clippy::await_holding_lock)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tempfile::TempDir;

    // Env vars are process-global; serialize tests that mutate them so
    // they don't race each other under the default multi-threaded runner.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn scoped_spool_dir(dir: &TempDir) {
        std::env::set_var("HYBRID_CACHE_REMOTE_SPOOL_DIR", dir.path());
    }

    fn sample_job(key: &str) -> DumpJob {
        DumpJob {
            cache_key: key.to_string(),
            cache_site: "example.com".into(),
            url: "https://example.com/".into(),
            method: "GET".into(),
            status: 200,
            request_headers: HashMap::new(),
            response_headers: HashMap::new(),
            body: b"hello".to_vec(),
            http_version: crate::types::HttpVersion::Http11,
            dump_remote: None,
        }
    }

    #[tokio::test]
    async fn round_trip() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let td = TempDir::new().unwrap();
        scoped_spool_dir(&td);
        std::env::remove_var("HYBRID_CACHE_REMOTE_SPOOL_MAX_BYTES");
        SPOOL_BYTES.store(0, Ordering::Relaxed);

        let job = sample_job("round-trip-key");
        let wrote = spool_write(&job).await.unwrap();
        assert!(wrote > 0);
        assert!(spool_bytes() >= wrote);

        let popped = spool_pop_one().await.expect("expected a job");
        assert_eq!(popped.cache_key, "round-trip-key");
        assert_eq!(popped.body, b"hello");

        // Directory should now be empty (next pop is None).
        assert!(spool_pop_one().await.is_none());
    }

    #[tokio::test]
    async fn over_budget_returns_storage_full() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let td = TempDir::new().unwrap();
        scoped_spool_dir(&td);
        std::env::set_var("HYBRID_CACHE_REMOTE_SPOOL_MAX_BYTES", "1");
        SPOOL_BYTES.store(0, Ordering::Relaxed);

        let job = sample_job("big-key");
        let err = spool_write(&job).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::StorageFull);

        std::env::remove_var("HYBRID_CACHE_REMOTE_SPOOL_MAX_BYTES");
    }

    #[tokio::test]
    async fn corrupt_file_skipped_not_panicked() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let td = TempDir::new().unwrap();
        scoped_spool_dir(&td);
        std::env::remove_var("HYBRID_CACHE_REMOTE_SPOOL_MAX_BYTES");
        SPOOL_BYTES.store(0, Ordering::Relaxed);

        let path = td.path().join("garbage.job");
        fs::write(&path, b"not bincode").await.unwrap();

        // Should return None (file removed + deserialize failed), not panic.
        let got = spool_pop_one().await;
        assert!(got.is_none());
        assert!(!path.exists(), "corrupt spool file should be removed");
    }
}
