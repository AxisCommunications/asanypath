//! Shared HTTP client infrastructure used by all cloud backends.
//!
//! Maintains two connection pools:
//! - **H1**: HTTP/1.1-only — lower per-request overhead, best for single calls
//!   and small batches where parallel TCP connections beat multiplexing.
//! - **H2**: HTTP/2-capable (with ALPN negotiation) — multiplexes many streams
//!   on fewer connections, best for large batches (≥ threshold).

/**
 * Copyright (C) 2026 Axis Communications AB, Lund, Sweden
 *
 * Use of this source code is governed by an MIT-style
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use bytes::Bytes;
use pyo3::PyErr;
use pyo3::exceptions::{PyFileNotFoundError, PyPermissionError, PyRuntimeError};
use reqwest::{Client, Method, StatusCode};

/// Check whether a reqwest error is transient and worth retrying.
fn is_retryable(e: &String) -> bool {
    let lower = e.to_lowercase();
    lower.contains("connection")
        || lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("reset")
        || lower.contains("broken pipe")
        || lower.contains("eof")
}

/// Check whether an HTTP status code is transient.
fn is_retryable_status(status: StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504)
}

/// Backoff config: jittered exponential, 3 attempts, 1s..30s.
fn retry_backoff() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_min_delay(Duration::from_secs(1))
        .with_max_delay(Duration::from_secs(30))
        .with_max_times(3)
        .with_jitter()
}

// ---------------------------------------------------------------------------
// Dual HTTP clients (H1 for low concurrency, H2 for high concurrency)
// ---------------------------------------------------------------------------

static CLIENT_H1: OnceLock<Client> = OnceLock::new();
static CLIENT_H2: OnceLock<Client> = OnceLock::new();

fn base_builder() -> reqwest::ClientBuilder {
    Client::builder()
        .pool_max_idle_per_host(100)
        .pool_idle_timeout(std::time::Duration::from_secs(90))
        .tcp_nodelay(true)
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .no_proxy()
}

pub fn get_client(use_h2: bool) -> &'static Client {
    if use_h2 {
        CLIENT_H2.get_or_init(|| base_builder().build().expect("failed to build H2 client"))
    } else {
        CLIENT_H1.get_or_init(|| {
            base_builder()
                .http1_only()
                .build()
                .expect("failed to build H1 client")
        })
    }
}

// ---------------------------------------------------------------------------
// Generic HTTP request executor
// ---------------------------------------------------------------------------

/// Execute an HTTP request and return (status, body, response_headers).
/// Retries transient network errors and 408/429/5xx with jittered exponential backoff.
pub async fn do_request(
    method: &str,
    url: &str,
    headers: &[(String, String)],
    body_bytes: Option<Bytes>,
    use_h2: bool,
) -> Result<(StatusCode, Bytes, HashMap<String, String>), String> {
    let method_parsed = method.parse::<Method>().map_err(|e| e.to_string())?;
    let client = get_client(use_h2);
    let headers = headers.to_vec();
    let body = body_bytes.clone();

    (async || {
        let mut req = client.request(method_parsed.clone(), url);
        for (k, v) in &headers {
            req = req.header(k.as_str(), v.as_str());
        }
        if let Some(b) = body.clone() {
            req = req.body(b);
        }
        let resp = req.send().await.map_err(|e| e.to_string())?;
        let status = resp.status();
        if is_retryable_status(status) {
            return Err(format!("HTTP {} (retryable)", status.as_u16()));
        }
        let resp_headers: HashMap<String, String> = resp
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        let resp_body = resp.bytes().await.map_err(|e| e.to_string())?;
        Ok((status, resp_body, resp_headers))
    })
    .retry(retry_backoff())
    .when(|e: &String| is_retryable(e))
    .await
}

/// Lightweight request — returns only (status, body), skipping header collection.
/// Retries transient network errors and 408/429/5xx with jittered exponential backoff.
pub async fn do_request_light(
    method: &str,
    url: &str,
    headers: &[(String, String)],
    body_bytes: Option<Bytes>,
    use_h2: bool,
) -> Result<(StatusCode, Bytes), String> {
    let method_parsed = method.parse::<Method>().map_err(|e| e.to_string())?;
    let client = get_client(use_h2);
    let headers = headers.to_vec();
    let body = body_bytes.clone();

    (async || {
        let mut req = client.request(method_parsed.clone(), url);
        for (k, v) in &headers {
            req = req.header(k.as_str(), v.as_str());
        }
        if let Some(b) = body.clone() {
            req = req.body(b);
        }
        let resp = req.send().await.map_err(|e| e.to_string())?;
        let status = resp.status();
        if is_retryable_status(status) {
            return Err(format!("HTTP {} (retryable)", status.as_u16()));
        }
        let resp_body = resp.bytes().await.map_err(|e| e.to_string())?;
        Ok((status, resp_body))
    })
    .retry(retry_backoff())
    .when(|e: &String| is_retryable(e))
    .await
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract the host portion from an endpoint URL.
pub fn extract_host(endpoint: &str) -> String {
    endpoint
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .split('/')
        .next()
        .unwrap_or("")
        .to_string()
}

/// Map an HTTP status code to the appropriate Python exception.
pub fn status_to_pyerr(status: StatusCode, path: &str, _body: &[u8]) -> PyErr {
    match status.as_u16() {
        404 => PyFileNotFoundError::new_err(format!("No such file or directory: '{path}'")),
        403 => PyPermissionError::new_err(format!("Permission denied: '{path}'")),
        _ => PyRuntimeError::new_err(format!("HTTP {}: '{path}'", status.as_u16())),
    }
}

/// Build a Bearer Authorization header pair.
pub fn bearer_headers(token: &str) -> Vec<(String, String)> {
    vec![("authorization".to_string(), format!("Bearer {token}"))]
}

// ---------------------------------------------------------------------------
// Generic HTTP functions (unsigned, exposed to Python)
// ---------------------------------------------------------------------------

use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::PyDict;

/// Execute a generic HTTP request, returning (body_bytes, headers_dict).
/// Used by all verb-specific wrappers below.
async fn _http_request(
    method: &str,
    url: &str,
    body: Option<Vec<u8>>,
    headers: Vec<(String, String)>,
) -> Result<(Vec<u8>, HashMap<String, String>), PyErr> {
    let body_bytes = body.map(Bytes::from);
    let (status, resp_body, resp_headers) = do_request(method, url, &headers, body_bytes, false)
        .await
        .map_err(|e| PyRuntimeError::new_err(e))?;
    if !status.is_success() {
        return Err(status_to_pyerr(status, url, &resp_body));
    }
    Ok((resp_body.to_vec(), resp_headers))
}

/// GET an unsigned URL, returning the response body as bytes.
#[pyfunction]
#[pyo3(signature = (url, headers=None))]
pub fn http_get(
    py: Python<'_>,
    url: String,
    headers: Option<Vec<(String, String)>>,
) -> PyResult<Bound<'_, pyo3::PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let (body, _) = _http_request("GET", &url, None, headers.unwrap_or_default()).await?;
        Ok(body)
    })
}

/// HEAD an unsigned URL, returning response headers as a dict.
#[pyfunction]
#[pyo3(signature = (url, headers=None))]
pub fn http_head(
    py: Python<'_>,
    url: String,
    headers: Option<Vec<(String, String)>>,
) -> PyResult<Bound<'_, pyo3::PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let (_, resp_headers) =
            _http_request("HEAD", &url, None, headers.unwrap_or_default()).await?;
        Python::try_attach(|py| {
            let dict = PyDict::new(py);
            for (k, v) in &resp_headers {
                dict.set_item(k.as_str(), v.as_str())?;
            }
            Ok(dict.into_py_any(py)?)
        })
        .expect("Python GIL must be available")
    })
}

/// PUT to an unsigned URL.
#[pyfunction]
#[pyo3(signature = (url, body=None, headers=None))]
pub fn http_put(
    py: Python<'_>,
    url: String,
    body: Option<Vec<u8>>,
    headers: Option<Vec<(String, String)>>,
) -> PyResult<Bound<'_, pyo3::PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let (resp_body, _) = _http_request("PUT", &url, body, headers.unwrap_or_default()).await?;
        Ok(resp_body)
    })
}

/// POST to an unsigned URL.
#[pyfunction]
#[pyo3(signature = (url, body=None, headers=None))]
pub fn http_post(
    py: Python<'_>,
    url: String,
    body: Option<Vec<u8>>,
    headers: Option<Vec<(String, String)>>,
) -> PyResult<Bound<'_, pyo3::PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let (resp_body, _) = _http_request("POST", &url, body, headers.unwrap_or_default()).await?;
        Ok(resp_body)
    })
}

/// DELETE an unsigned URL.
#[pyfunction]
#[pyo3(signature = (url, headers=None))]
pub fn http_delete(
    py: Python<'_>,
    url: String,
    headers: Option<Vec<(String, String)>>,
) -> PyResult<Bound<'_, pyo3::PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let (resp_body, _) =
            _http_request("DELETE", &url, None, headers.unwrap_or_default()).await?;
        Ok(resp_body)
    })
}

/// PATCH an unsigned URL.
#[pyfunction]
#[pyo3(signature = (url, body=None, headers=None))]
pub fn http_patch(
    py: Python<'_>,
    url: String,
    body: Option<Vec<u8>>,
    headers: Option<Vec<(String, String)>>,
) -> PyResult<Bound<'_, pyo3::PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let (resp_body, _) =
            _http_request("PATCH", &url, body, headers.unwrap_or_default()).await?;
        Ok(resp_body)
    })
}

/// OPTIONS on an unsigned URL.
#[pyfunction]
#[pyo3(signature = (url, headers=None))]
pub fn http_options(
    py: Python<'_>,
    url: String,
    headers: Option<Vec<(String, String)>>,
) -> PyResult<Bound<'_, pyo3::PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let (_, resp_headers) =
            _http_request("OPTIONS", &url, None, headers.unwrap_or_default()).await?;
        Python::try_attach(|py| {
            let dict = PyDict::new(py);
            for (k, v) in &resp_headers {
                dict.set_item(k.as_str(), v.as_str())?;
            }
            Ok(dict.into_py_any(py)?)
        })
        .expect("Python GIL must be available")
    })
}

/// Check if a URL exists (HEAD, returns true if 2xx).
///
/// Presigned URLs (AWS SigV4, GCS signed URLs, Azure SAS) are typically only
/// valid for the method they were signed for — a HEAD on a GET-signed URL
/// returns 403. When HEAD returns 403/405, retry with a ranged GET so that
/// exists() works for the common "presigned GET" case.
#[pyfunction]
#[pyo3(signature = (url, headers=None))]
pub fn http_exists(
    py: Python<'_>,
    url: String,
    headers: Option<Vec<(String, String)>>,
) -> PyResult<Bound<'_, pyo3::PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let hdrs = headers.unwrap_or_default();
        match do_request("HEAD", &url, &hdrs, None, false).await {
            Ok((status, _, _)) if status.is_success() => Ok(true),
            Ok((status, _, _))
                if status == StatusCode::FORBIDDEN || status == StatusCode::METHOD_NOT_ALLOWED =>
            {
                let mut probe = hdrs.clone();
                probe.push(("range".into(), "bytes=0-0".into()));
                match do_request("GET", &url, &probe, None, false).await {
                    Ok((s, _, _)) => Ok(s.is_success()),
                    Err(_) => Ok(false),
                }
            }
            Ok(_) => Ok(false),
            Err(_) => Ok(false),
        }
    })
}

/// Execute a generic HTTP request, returning (status_code, body_bytes, headers_dict).
/// Does NOT raise on non-2xx — the caller decides how to handle the status.
#[pyfunction]
#[pyo3(signature = (method, url, body=None, headers=None))]
pub fn http_request(
    py: Python<'_>,
    method: String,
    url: String,
    body: Option<Vec<u8>>,
    headers: Option<Vec<(String, String)>>,
) -> PyResult<Bound<'_, pyo3::PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let body_bytes = body.map(Bytes::from);
        let (status, resp_body, resp_headers) = do_request(
            &method,
            &url,
            &headers.unwrap_or_default(),
            body_bytes,
            false,
        )
        .await
        .map_err(|e| PyRuntimeError::new_err(e))?;
        Python::try_attach(|py| {
            let dict = PyDict::new(py);
            for (k, v) in &resp_headers {
                dict.set_item(k.as_str(), v.as_str())?;
            }
            let result = (status.as_u16(), resp_body.to_vec(), dict.into_py_any(py)?);
            result.into_py_any(py)
        })
        .expect("Python GIL must be available")
    })
}

/// Fetch an HTML page and extract absolute URLs from elements matching `selector`.
///
/// Reads the value of `attr` (default "href") from each matched element, resolves
/// it against `base_url` (defaulting to the fetched URL), and filters out empty,
/// fragment-only, parent (`..`) and self (`.`) links.
#[pyfunction]
#[pyo3(signature = (url, selector, attr=None, base_url=None, headers=None))]
pub fn http_scrape_links(
    py: Python<'_>,
    url: String,
    selector: String,
    attr: Option<String>,
    base_url: Option<String>,
    headers: Option<Vec<(String, String)>>,
) -> PyResult<Bound<'_, pyo3::PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let attr = attr.unwrap_or_else(|| "href".to_string());
        let base_url_str = base_url.unwrap_or_else(|| url.clone());

        let (status, body, _) = do_request("GET", &url, &headers.unwrap_or_default(), None, false)
            .await
            .map_err(|e| PyRuntimeError::new_err(e))?;
        if !status.is_success() {
            return Err(status_to_pyerr(status, &url, &body));
        }

        let html_str = String::from_utf8_lossy(&body).into_owned();
        let parsed_sel = scraper::Selector::parse(&selector).map_err(|e| {
            PyRuntimeError::new_err(format!("invalid CSS selector '{selector}': {e:?}"))
        })?;
        let base = url::Url::parse(&base_url_str)
            .map_err(|e| PyRuntimeError::new_err(format!("invalid base_url: {e}")))?;

        let doc = scraper::Html::parse_document(&html_str);
        let mut out: Vec<String> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for el in doc.select(&parsed_sel) {
            let raw = el.value().attr(&attr).unwrap_or("").trim();
            if raw.is_empty() || raw.starts_with('#') || raw.starts_with('?') {
                continue;
            }
            // Skip parent / self / explicitly-relative parent paths
            let stripped = raw.trim_end_matches('/');
            if stripped.is_empty() || stripped == "." || stripped == ".." {
                continue;
            }
            // Skip "Parent Directory" style absolute parent refs
            if raw.starts_with("../") {
                continue;
            }
            let resolved = match base.join(raw) {
                Ok(u) => u.to_string(),
                Err(_) => continue,
            };
            // Skip URLs that resolve to the page itself or above it
            if resolved.trim_end_matches('/') == base_url_str.trim_end_matches('/') {
                continue;
            }
            if seen.insert(resolved.clone()) {
                out.push(resolved);
            }
        }
        Ok(out)
    })
}
