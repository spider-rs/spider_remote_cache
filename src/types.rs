use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// HTTP version shared across spider and chromey.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub enum HttpVersion {
    /// HTTP Version 0.9
    Http09,
    /// HTTP Version 1.0
    Http10,
    #[default]
    /// HTTP Version 1.1
    Http11,
    /// HTTP Version 2.0
    H2,
    /// HTTP Version 3.0
    H3,
}

impl HttpVersion {
    /// Parse a protocol string (e.g. "h2", "HTTP/1.1") into an `HttpVersion`.
    #[inline]
    pub fn parse_protocol(s: &str) -> Self {
        let t = s.trim();
        match t {
            "h2" | "H2" | "HTTP/2" | "HTTP/2.0" => Self::H2,
            "h3" | "H3" | "HTTP/3" | "HTTP/3.0" => Self::H3,
            "HTTP/1.0" => Self::Http10,
            "HTTP/0.9" => Self::Http09,
            _ => {
                let lower = t.to_ascii_lowercase();
                if lower.contains("h2") || lower.contains("http/2") {
                    Self::H2
                } else if lower.contains("h3") || lower.contains("http/3") {
                    Self::H3
                } else if lower.contains("1.0") || lower.contains("http/1.0") {
                    Self::Http10
                } else if lower.contains("0.9") || lower.contains("http/0.9") {
                    Self::Http09
                } else {
                    Self::Http11
                }
            }
        }
    }
}

impl From<&str> for HttpVersion {
    #[inline]
    fn from(s: &str) -> Self {
        Self::parse_protocol(s)
    }
}

impl From<Option<&str>> for HttpVersion {
    #[inline]
    fn from(s: Option<&str>) -> Self {
        s.map(HttpVersion::from).unwrap_or(HttpVersion::Http11)
    }
}

/// Payload shape for the remote hybrid cache server `/cache/index` endpoint.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct HybridCachePayload {
    /// Optional website-level key (defaults to URL host if None).
    #[serde(default)]
    pub website_key: Option<String>,
    pub resource_key: String,
    pub url: String,
    pub method: String,
    pub status: u16,
    pub request_headers: HashMap<String, String>,
    pub response_headers: HashMap<String, String>,
    pub http_version: HttpVersion,
    /// Base64-encoded HTTP body for JSON transport.
    pub body_base64: String,
}
