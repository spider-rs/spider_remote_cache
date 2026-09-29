use base64::engine::general_purpose;
use base64::prelude::Engine as _;
use reqwest::header::HeaderValue;
use reqwest::Client;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use crate::types::{HttpVersion, HybridCachePayload};

/// Global HTTP client for remote cache operations.
///
/// Set once via [`set_client`]. If never set, [`default_client_builder`]
/// builds one on first use. Whichever happens first wins for the life of
/// the process, so call [`set_client`] before the first upload or read.
static CACHE_CLIENT: OnceLock<Client> = OnceLock::new();

/// Which client [`CACHE_CLIENT`] holds. See [`ClientSource`].
static CLIENT_SOURCE: AtomicU8 = AtomicU8::new(CLIENT_UNSET);

const CLIENT_UNSET: u8 = 0;
const CLIENT_INJECTED: u8 = 1;
const CLIENT_DEFAULT: u8 = 2;

/// Where the active remote cache client came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientSource {
    /// Installed by [`set_client`] / [`try_set_client`].
    Injected,
    /// Built by [`default_client_builder`] because nothing was injected
    /// before first use.
    Default,
}

/// Connect timeout of the default client. The cache server sits on the
/// internal VPC, so a connect that takes longer than this is a dead or
/// blackholed host, not a slow one.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_millis(250);

/// Idle connections kept per host by the default client.
pub const DEFAULT_POOL_MAX_IDLE_PER_HOST: usize = 32;

/// Base URL of your remote hybrid cache server.
///
/// Override via env: `HYBRID_CACHE_ENDPOINT=http://remote-cache:8080`
static CACHE_ENDPOINT: OnceLock<String> = OnceLock::new();

/// Inject the HTTP client that all remote cache operations will use.
///
/// Call this once at startup, before the first upload or read, passing the
/// same `reqwest::Client` your application already uses. This shares the
/// connection pool and TLS stack. Give the client a connect timeout and a
/// request timeout: the crate does not wrap reads in its own deadline.
///
/// Only the first call wins. If the default client was already built
/// because something used the crate first, this call is ignored and a
/// warning is logged; check [`client_source`] or use [`try_set_client`].
pub fn set_client(client: Client) {
    if !try_set_client(client) {
        tracing::warn!(
            "spider_remote_cache::set_client ignored: a {:?} client is already active",
            client_source()
        );
    }
}

/// Like [`set_client`], but returns `true` only if this call installed the
/// client. Returns `false` when a client (injected or default) was already
/// active.
pub fn try_set_client(client: Client) -> bool {
    let installed = CACHE_CLIENT.set(client).is_ok();
    if installed {
        CLIENT_SOURCE.store(CLIENT_INJECTED, Ordering::Release);
    }
    installed
}

/// Which client is active, or `None` if no client has been installed or
/// built yet.
pub fn client_source() -> Option<ClientSource> {
    match CLIENT_SOURCE.load(Ordering::Acquire) {
        CLIENT_INJECTED => Some(ClientSource::Injected),
        CLIENT_DEFAULT => Some(ClientSource::Default),
        _ => None,
    }
}

/// Override the default remote cache endpoint.
///
/// Safe to call multiple times — only the first call wins.
pub fn set_endpoint(endpoint: String) {
    let _ = CACHE_ENDPOINT.set(endpoint);
}

/// The builder behind the default client: 250 ms connect timeout, a
/// request timeout equal to the worker's batch timeout
/// (`HYBRID_CACHE_REMOTE_TIMEOUT_MS`, default 2500 ms), 32 idle
/// connections per host, `TCP_NODELAY`, 90 s idle timeout.
///
/// Public so callers can start from these settings and add their own.
pub fn default_client_builder() -> reqwest::ClientBuilder {
    Client::builder()
        .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
        .timeout(Duration::from_millis(
            crate::worker::default_timeout_ms().max(1),
        ))
        .pool_max_idle_per_host(DEFAULT_POOL_MAX_IDLE_PER_HOST)
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_nodelay(true)
}

/// Returns the configured client, or builds the default one.
pub fn get_client() -> &'static Client {
    CACHE_CLIENT.get_or_init(|| {
        CLIENT_SOURCE.store(CLIENT_DEFAULT, Ordering::Release);
        default_client_builder()
            .build()
            .expect("failed to build default remote cache client")
    })
}

/// Returns the configured endpoint.
pub fn get_endpoint() -> &'static str {
    CACHE_ENDPOINT
        .get_or_init(|| {
            std::env::var("HYBRID_CACHE_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:8080".to_string())
        })
        .as_str()
}

/// Resolve the effective base URL from a `dump_remote` option.
///
/// - `None` => default endpoint
/// - `Some("true")` => default endpoint
/// - `Some("http://...")` => override base URL
pub fn resolve_base_url(dump_remote: Option<&str>) -> &str {
    match dump_remote {
        Some(remote) if remote != "true" => remote.trim_ascii(),
        _ => get_endpoint(),
    }
}

/// Build a `HybridCachePayload` from raw parts.
///
/// `website_key` is set to the URL host. Uploads made through
/// [`dump_to_remote`] and the worker overwrite it with the job's
/// `cache_site`, which is the key reads look up.
#[allow(clippy::too_many_arguments)]
pub fn build_payload(
    cache_key: &str,
    url_str: &str,
    body: &[u8],
    method: &str,
    status: u16,
    request_headers: &HashMap<String, String>,
    response_headers: &HashMap<String, String>,
    http_version: &HttpVersion,
) -> HybridCachePayload {
    let website_key = url::Url::parse(url_str)
        .ok()
        .and_then(|u| u.host_str().map(|h| h.to_string()));

    let body_base64 = general_purpose::STANDARD.encode(body);

    HybridCachePayload {
        website_key,
        resource_key: cache_key.to_string(),
        url: url_str.to_string(),
        method: method.to_string(),
        status,
        http_version: *http_version,
        request_headers: request_headers.clone(),
        response_headers: response_headers.clone(),
        body_base64,
    }
}

/// Set the payload's own site key to `cache_site` so a server that reads
/// the per-item key files it where reads look. An empty `cache_site`
/// leaves the host default in place.
#[inline]
pub(crate) fn stamp_site(payload: &mut HybridCachePayload, cache_site: &str) {
    if !cache_site.is_empty() {
        payload.website_key = Some(cache_site.to_string());
    }
}

#[inline]
fn site_header(cache_site: &str) -> HeaderValue {
    HeaderValue::from_str(cache_site).unwrap_or(HeaderValue::from_static(""))
}

/// Small jitter for the connect retry, 10 to 49 ms, without pulling in
/// an RNG crate.
fn retry_jitter() -> Duration {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    Duration::from_millis(10 + u64::from(nanos % 40))
}

/// Result of one upload POST, for logging and metrics.
pub(crate) enum PostOutcome {
    Ok,
    Http(reqwest::StatusCode),
    Send(reqwest::Error),
}

/// Send a request built by `build`, retrying exactly once after a short
/// jitter when the first attempt failed to connect. A connect failure
/// means no byte of the request reached the server, so the retry cannot
/// double-write. Any other error (including a timeout after the request
/// was sent) is returned as is.
pub(crate) async fn send_with_connect_retry<F>(
    build: F,
) -> Result<reqwest::Response, reqwest::Error>
where
    F: Fn() -> reqwest::RequestBuilder,
{
    match build().send().await {
        Err(err) if err.is_connect() => {
            tokio::time::sleep(retry_jitter()).await;
            build().send().await
        }
        other => other,
    }
}

async fn post_json<T: serde::Serialize + ?Sized>(
    endpoint: &str,
    body: &T,
    cache_site: &str,
) -> PostOutcome {
    let client = get_client();
    let started = Instant::now();
    let result = send_with_connect_retry(|| {
        client
            .post(endpoint)
            .json(body)
            .header("x-cache-site", site_header(cache_site))
    })
    .await;
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;

    let outcome = match result {
        Ok(resp) if resp.status().is_success() => PostOutcome::Ok,
        Ok(resp) => PostOutcome::Http(resp.status()),
        Err(err) => PostOutcome::Send(err),
    };
    let label = match &outcome {
        PostOutcome::Ok => crate::metrics::outcome::OK,
        PostOutcome::Http(_) => crate::metrics::outcome::HTTP_ERROR,
        PostOutcome::Send(_) => crate::metrics::outcome::SEND_ERROR,
    };
    crate::metrics::upload(label, elapsed_ms);
    outcome
}

/// POST a single cached response to the remote cache server's `/cache/index`
/// endpoint with best-effort error handling.
///
/// `cache_site` is sent as the `x-cache-site` header and as the payload's
/// `website_key`.
#[allow(clippy::too_many_arguments)]
pub async fn dump_to_remote(
    cache_key: &str,
    cache_site: &str,
    url_str: &str,
    body: &[u8],
    method: &str,
    status: u16,
    request_headers: &HashMap<String, String>,
    response_headers: &HashMap<String, String>,
    http_version: &HttpVersion,
    dump_remote: Option<&str>,
) {
    let mut payload = build_payload(
        cache_key,
        url_str,
        body,
        method,
        status,
        request_headers,
        response_headers,
        http_version,
    );
    stamp_site(&mut payload, cache_site);

    let base_url = resolve_base_url(dump_remote);
    let endpoint = format!("{}/cache/index", base_url);

    match post_json(&endpoint, &payload, cache_site).await {
        PostOutcome::Ok => {
            tracing::debug!("remote cache dump: stored {}", cache_key);
        }
        PostOutcome::Http(status) => {
            tracing::warn!(
                "remote cache dump: non-success status for {}: {}",
                cache_key,
                status
            );
        }
        PostOutcome::Send(err) => {
            tracing::warn!(
                "remote cache dump: failed to POST {} to {}: {}",
                cache_key,
                endpoint,
                err
            );
        }
    }
}

/// POST payloads that all share one site key to `/cache/index/batch`,
/// sending `cache_site` as `x-cache-site` and stamping it on every payload.
///
/// This is the call the worker makes. Every item in `payloads` must belong
/// to `cache_site`: the server files the whole request under the header.
pub async fn dump_site_batch_to_remote(
    mut payloads: Vec<HybridCachePayload>,
    cache_site: &str,
    base_url: &str,
) {
    if payloads.is_empty() {
        return;
    }
    for p in payloads.iter_mut() {
        stamp_site(p, cache_site);
    }

    let endpoint = format!("{}/cache/index/batch", base_url);

    match post_json(&endpoint, &payloads, cache_site).await {
        PostOutcome::Ok => {
            tracing::debug!(
                "remote cache batch dump: stored {} items for site {}",
                payloads.len(),
                cache_site
            );
        }
        PostOutcome::Http(status) => {
            tracing::warn!(
                "remote cache batch dump: non-success for {} items: {}",
                payloads.len(),
                status
            );
        }
        PostOutcome::Send(err) => {
            tracing::warn!(
                "remote cache batch dump: failed to POST {} items to {}: {}",
                payloads.len(),
                endpoint,
                err
            );
        }
    }
}

/// POST multiple cached responses to the remote cache server's
/// `/cache/index/batch` endpoint.
///
/// Payloads are grouped by their `website_key` and each group goes out as
/// its own request with that key as `x-cache-site`, so no item is filed
/// under another item's site. Order within a group is preserved. Before
/// 0.5 this sent one request keyed by the first item's site.
pub async fn dump_batch_to_remote(payloads: Vec<HybridCachePayload>, base_url: &str) {
    for (site, group) in group_by_site(payloads) {
        dump_site_batch_to_remote(group, &site, base_url).await;
    }
}

/// Split payloads into per-site groups, keeping first-seen order.
fn group_by_site(payloads: Vec<HybridCachePayload>) -> Vec<(String, Vec<HybridCachePayload>)> {
    let mut groups: Vec<(String, Vec<HybridCachePayload>)> = Vec::new();
    for p in payloads {
        let site = p.website_key.clone().unwrap_or_default();
        match groups.iter_mut().find(|(s, _)| *s == site) {
            Some((_, g)) => g.push(p),
            None => groups.push((site, vec![p])),
        }
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(site: Option<&str>, key: &str) -> HybridCachePayload {
        HybridCachePayload {
            website_key: site.map(str::to_string),
            resource_key: key.to_string(),
            url: "https://example.com/".into(),
            method: "GET".into(),
            ..Default::default()
        }
    }

    #[test]
    fn group_by_site_keeps_sites_apart_and_order() {
        let groups = group_by_site(vec![
            payload(Some("a"), "1"),
            payload(Some("b"), "2"),
            payload(Some("a"), "3"),
            payload(None, "4"),
        ]);
        let shape: Vec<(String, Vec<String>)> = groups
            .into_iter()
            .map(|(s, g)| (s, g.into_iter().map(|p| p.resource_key).collect()))
            .collect();
        assert_eq!(
            shape,
            vec![
                ("a".to_string(), vec!["1".to_string(), "3".to_string()]),
                ("b".to_string(), vec!["2".to_string()]),
                (String::new(), vec!["4".to_string()]),
            ]
        );
    }

    #[test]
    fn stamp_site_overrides_host_but_not_with_empty() {
        let mut p = payload(Some("example.com"), "k");
        stamp_site(&mut p, "");
        assert_eq!(p.website_key.as_deref(), Some("example.com"));
        stamp_site(&mut p, "abc123");
        assert_eq!(p.website_key.as_deref(), Some("abc123"));
    }

    #[tokio::test]
    async fn connect_error_retries_once() {
        // Bind then drop a listener so the port refuses connections.
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let url = format!("http://127.0.0.1:{port}/cache/index");
        let client = Client::new();
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let res = send_with_connect_retry(|| {
            calls.fetch_add(1, Ordering::SeqCst);
            client.post(&url)
        })
        .await;
        assert!(res.unwrap_err().is_connect());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn error_after_send_is_not_retried() {
        use tokio::io::AsyncReadExt;
        // Accept, read the request, then hang up without answering.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut s, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = s.read(&mut buf).await;
                drop(s);
            }
        });
        let url = format!("http://{addr}/cache/index");
        let client = Client::new();
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let res = send_with_connect_retry(|| {
            calls.fetch_add(1, Ordering::SeqCst);
            client.post(&url).body("x")
        })
        .await;
        let err = res.unwrap_err();
        assert!(!err.is_connect(), "{err:?}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
