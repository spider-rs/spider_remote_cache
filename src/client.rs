use base64::engine::general_purpose;
use base64::prelude::Engine as _;
use reqwest::header::HeaderValue;
use reqwest::Client;
use std::collections::HashMap;
use std::sync::OnceLock;

use crate::types::{HttpVersion, HybridCachePayload};

/// Global HTTP client for remote cache operations.
///
/// Set once via [`set_client`]. If never set, a minimal default is created
/// on first use — but callers should prefer injecting their own so the
/// connection pool and TLS stack are shared with the rest of the application.
static CACHE_CLIENT: OnceLock<Client> = OnceLock::new();

/// Base URL of your remote hybrid cache server.
///
/// Override via env: `HYBRID_CACHE_ENDPOINT=http://remote-cache:8080`
static CACHE_ENDPOINT: OnceLock<String> = OnceLock::new();

/// Inject the HTTP client that all remote cache operations will use.
///
/// Call this once at startup, passing the same `reqwest::Client` your
/// application already uses (spider's global client, chromey's JSON
/// client, etc.).  This avoids compiling a second TLS stack and shares
/// the connection pool.
///
/// Safe to call multiple times — only the first call wins.
pub fn set_client(client: Client) {
    let _ = CACHE_CLIENT.set(client);
}

/// Override the default remote cache endpoint.
///
/// Safe to call multiple times — only the first call wins.
pub fn set_endpoint(endpoint: String) {
    let _ = CACHE_ENDPOINT.set(endpoint);
}

/// Returns the configured client, or a minimal default.
pub fn get_client() -> &'static Client {
    CACHE_CLIENT.get_or_init(|| {
        Client::builder()
            .pool_idle_timeout(std::time::Duration::from_secs(90))
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

/// POST a single cached response to the remote cache server's `/cache/index`
/// endpoint with best-effort error handling.
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
    let payload = build_payload(
        cache_key,
        url_str,
        body,
        method,
        status,
        request_headers,
        response_headers,
        http_version,
    );

    let base_url = resolve_base_url(dump_remote);
    let endpoint = format!("{}/cache/index", base_url);
    let client = get_client();

    let result = client
        .post(&endpoint)
        .json(&payload)
        .header(
            "x-cache-site",
            HeaderValue::from_str(cache_site).unwrap_or(HeaderValue::from_static("")),
        )
        .send()
        .await;

    match result {
        Ok(resp) => {
            if !resp.status().is_success() {
                tracing::warn!(
                    "remote cache dump: non-success status for {}: {}",
                    cache_key,
                    resp.status()
                );
            } else {
                tracing::info!(
                    "remote cache dump: success status for {}: {}",
                    cache_key,
                    resp.status()
                );
            }
        }
        Err(err) => {
            tracing::warn!(
                "remote cache dump: failed to POST {} to {}: {}",
                cache_key,
                endpoint,
                err
            );
        }
    }
}

/// POST multiple cached responses to the remote cache server's
/// `/cache/index/batch` endpoint in a single HTTP request.
pub async fn dump_batch_to_remote(payloads: Vec<HybridCachePayload>, base_url: &str) {
    if payloads.is_empty() {
        return;
    }

    let endpoint = format!("{}/cache/index/batch", base_url);
    let client = get_client();

    let site_header = payloads
        .first()
        .and_then(|p| p.website_key.as_deref())
        .unwrap_or("");

    let result = client
        .post(&endpoint)
        .json(&payloads)
        .header(
            "x-cache-site",
            HeaderValue::from_str(site_header).unwrap_or(HeaderValue::from_static("")),
        )
        .send()
        .await;

    match result {
        Ok(resp) => {
            if !resp.status().is_success() {
                tracing::warn!(
                    "remote cache batch dump: non-success for {} items: {}",
                    payloads.len(),
                    resp.status()
                );
            } else {
                tracing::info!(
                    "remote cache batch dump: success for {} items: {}",
                    payloads.len(),
                    resp.status()
                );
            }
        }
        Err(err) => {
            tracing::warn!(
                "remote cache batch dump: failed to POST {} items to {}: {}",
                payloads.len(),
                endpoint,
                err
            );
        }
    }
}
