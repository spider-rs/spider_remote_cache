//! The worker must file every item under its own `cache_site`.
//!
//! A mock HTTP server records the `x-cache-site` header of each request
//! and the payloads it carried. Jobs for two sites share one URL host, so
//! the pre-0.5 behaviour (header taken from the first item's URL host)
//! fails every assertion here.
//!
//! This file is its own test binary so the global worker lives on this
//! test's runtime alone.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use spider_remote_cache::{DumpJob, HttpVersion, HybridCachePayload};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug, Clone)]
struct Seen {
    path: String,
    site_header: String,
    /// `(resource_key, website_key)` for every item in the request.
    items: Vec<(String, Option<String>)>,
}

async fn read_request(
    stream: &mut TcpStream,
) -> Option<(String, HashMap<String, String>, Vec<u8>)> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    let header_end = loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = head.split("\r\n");
    let path = lines.next()?.split(' ').nth(1)?.to_string();
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    let len: usize = headers
        .get("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let mut body = buf[header_end..].to_vec();
    while body.len() < len {
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    Some((path, headers, body))
}

async fn mock_server() -> (String, Arc<Mutex<Vec<Seen>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_srv = seen.clone();
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(s) => s,
                Err(_) => return,
            };
            let seen = seen_srv.clone();
            tokio::spawn(async move {
                while let Some((path, headers, body)) = read_request(&mut stream).await {
                    let items: Vec<(String, Option<String>)> = if path.ends_with("/batch") {
                        serde_json::from_slice::<Vec<HybridCachePayload>>(&body)
                            .unwrap()
                            .into_iter()
                            .map(|p| (p.resource_key, p.website_key))
                            .collect()
                    } else {
                        let p: HybridCachePayload = serde_json::from_slice(&body).unwrap();
                        vec![(p.resource_key, p.website_key)]
                    };
                    seen.lock().unwrap().push(Seen {
                        path,
                        site_header: headers.get("x-cache-site").cloned().unwrap_or_default(),
                        items,
                    });
                    let _ = stream
                        .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                        .await;
                }
            });
        }
    });
    (format!("http://{addr}"), seen)
}

fn job(key: &str, site: &str, base: &str) -> DumpJob {
    DumpJob {
        cache_key: key.to_string(),
        cache_site: site.to_string(),
        // Same host for every job: the host must not decide the site.
        url: format!("https://example.com/{key}"),
        method: "GET".into(),
        status: 200,
        request_headers: HashMap::new(),
        response_headers: HashMap::new(),
        body: format!("body-{key}").into_bytes(),
        http_version: HttpVersion::Http11,
        dump_remote: Some(base.to_string()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_item_is_filed_under_its_own_cache_site() {
    let (base, seen) = mock_server().await;
    let site_a = "a".repeat(64);
    let site_b = "b".repeat(64);

    // Large qps so pacing does not slow the test.
    let tx = spider_remote_cache::init_remote_dump_worker(64, 10_000, 5_000).await;

    let mut expected: HashMap<String, String> = HashMap::new();
    let mut jobs = Vec::new();
    for i in 0..6 {
        let site = if i % 2 == 0 { &site_a } else { &site_b };
        let key = format!("k{i}");
        expected.insert(key.clone(), site.clone());
        jobs.push(job(&key, site, &base));
    }
    drop(tx);
    // `enqueue` counts the bytes, so the release check below means something.
    for j in jobs {
        spider_remote_cache::enqueue(j).await.unwrap();
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let n: usize = seen.lock().unwrap().iter().map(|s| s.items.len()).sum();
        if n >= expected.len() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "uploads did not arrive"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let snapshot = seen.lock().unwrap().clone();
    let mut filed = 0;
    for req in &snapshot {
        assert!(req.path.starts_with("/cache/index"), "{req:?}");
        for (key, website_key) in &req.items {
            let want = &expected[key];
            assert_eq!(&req.site_header, want, "header for {key} in {req:?}");
            assert_eq!(
                website_key.as_deref(),
                Some(want.as_str()),
                "website_key for {key}"
            );
            filed += 1;
        }
    }
    assert_eq!(filed, expected.len());
    assert!(snapshot.iter().any(|s| s.site_header == site_a));
    assert!(snapshot.iter().any(|s| s.site_header == site_b));

    // Every accepted job's bytes are released once its upload ends.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while spider_remote_cache::queue_bytes() != 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "queue bytes never released"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // The public batch API splits a mixed batch per site. Deterministic
    // cover for the batch path, whatever the worker's drain happened to do.
    seen.lock().unwrap().clear();
    let mk = |key: &str, site: &str| HybridCachePayload {
        website_key: Some(site.to_string()),
        resource_key: key.to_string(),
        url: format!("https://example.com/{key}"),
        method: "GET".into(),
        status: 200,
        ..Default::default()
    };
    spider_remote_cache::dump_batch_to_remote(
        vec![mk("x1", &site_a), mk("x2", &site_b), mk("x3", &site_a)],
        &base,
    )
    .await;
    let batch = seen.lock().unwrap().clone();
    assert_eq!(batch.len(), 2, "{batch:?}");
    for req in &batch {
        assert_eq!(req.path, "/cache/index/batch");
        for (_, website_key) in &req.items {
            assert_eq!(website_key.as_deref(), Some(req.site_header.as_str()));
        }
    }
    let a = batch.iter().find(|r| r.site_header == site_a).unwrap();
    assert_eq!(
        a.items.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>(),
        vec!["x1", "x3"]
    );
    let b = batch.iter().find(|r| r.site_header == site_b).unwrap();
    assert_eq!(b.items.len(), 1);
    assert_eq!(b.items[0].0, "x2");
}
