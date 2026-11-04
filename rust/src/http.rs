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
use reqwest::header::{CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, LOCATION};
use reqwest::{Client, Method, Response, StatusCode, Url};

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
        // Redirects are followed by `send_following_redirects`, which drops
        // credential-bearing headers when a redirect leaves the origin.
        .redirect(reqwest::redirect::Policy::none())
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
// Redirects and error messages
// ---------------------------------------------------------------------------

const MAX_REDIRECTS: usize = 10;

/// Request headers that may follow a redirect to another origin. Anything
/// else (Authorization, API-key headers, signed-request headers, ...) may
/// carry credentials for the original origin only.
const CROSS_ORIGIN_HEADERS: &[&str] = &[
    "accept",
    "accept-encoding",
    "accept-language",
    "if-match",
    "if-modified-since",
    "if-none-match",
    "if-range",
    "if-unmodified-since",
    "range",
    "user-agent",
];

/// `url` without userinfo, query or fragment, for error messages: SAS
/// tokens and presigned signatures live in the query string.
pub fn redact_url(url: &str) -> String {
    match Url::parse(url) {
        Ok(mut u) => {
            let _ = u.set_username("");
            let _ = u.set_password(None);
            u.set_query(None);
            u.set_fragment(None);
            u.to_string()
        }
        Err(_) => url.split(['?', '#']).next().unwrap_or("").to_string(),
    }
}

/// Describe a reqwest error without the request URL it embeds.
fn error_string(e: reqwest::Error, url: &str) -> String {
    format!("{} for url ({})", e.without_url(), redact_url(url))
}

/// Send a request, following redirects the way reqwest would, except that
/// headers outside `CROSS_ORIGIN_HEADERS` are dropped once a redirect leaves
/// the original scheme, host and port.
async fn send_following_redirects(
    client: &Client,
    method: Method,
    url: &str,
    headers: &[(String, String)],
    body: Option<Bytes>,
) -> Result<Response, String> {
    let mut method = method;
    let mut url = Url::parse(url).map_err(|e| format!("invalid url ({}): {e}", redact_url(url)))?;
    let origin = url.origin();
    let mut headers = headers.to_vec();
    let mut body = body;
    for _ in 0..=MAX_REDIRECTS {
        let mut req = client.request(method.clone(), url.clone());
        for (k, v) in &headers {
            req = req.header(k.as_str(), v.as_str());
        }
        if let Some(b) = body.clone() {
            req = req.body(b);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| error_string(e, url.as_str()))?;
        let status = resp.status();
        let next = match status {
            StatusCode::MOVED_PERMANENTLY
            | StatusCode::FOUND
            | StatusCode::SEE_OTHER
            | StatusCode::TEMPORARY_REDIRECT
            | StatusCode::PERMANENT_REDIRECT => resp
                .headers()
                .get(LOCATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|loc| url.join(loc).ok())
                .filter(|u| matches!(u.scheme(), "http" | "https")),
            _ => None,
        };
        let Some(next) = next else {
            return Ok(resp);
        };
        if matches!(
            status,
            StatusCode::MOVED_PERMANENTLY | StatusCode::FOUND | StatusCode::SEE_OTHER
        ) && method != Method::HEAD
        {
            method = Method::GET;
            body = None;
            headers.retain(|(k, _)| {
                let k = k.to_ascii_lowercase();
                k != CONTENT_TYPE.as_str()
                    && k != CONTENT_LENGTH.as_str()
                    && k != CONTENT_ENCODING.as_str()
            });
        }
        if next.origin() != origin {
            headers
                .retain(|(k, _)| CROSS_ORIGIN_HEADERS.contains(&k.to_ascii_lowercase().as_str()));
        }
        url = next;
    }
    Err(format!(
        "too many redirects for url ({})",
        redact_url(url.as_str())
    ))
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
        let resp =
            send_following_redirects(client, method_parsed.clone(), url, &headers, body.clone())
                .await?;
        let status = resp.status();
        if is_retryable_status(status) {
            return Err(format!("HTTP {} (retryable)", status.as_u16()));
        }
        let resp_headers: HashMap<String, String> = resp
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        let resp_body = resp.bytes().await.map_err(|e| error_string(e, url))?;
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
        let resp =
            send_following_redirects(client, method_parsed.clone(), url, &headers, body.clone())
                .await?;
        let status = resp.status();
        if is_retryable_status(status) {
            return Err(format!("HTTP {} (retryable)", status.as_u16()));
        }
        let resp_body = resp.bytes().await.map_err(|e| error_string(e, url))?;
        Ok((status, resp_body))
    })
    .retry(retry_backoff())
    .when(|e: &String| is_retryable(e))
    .await
}

/// Stream a request body from a file (bounded memory). An explicit
/// `content_length` frames the body with Content-Length instead of chunked
/// encoding; retries reopen the file so a consumed stream can be replayed.
pub async fn do_request_stream_file(
    method: &str,
    url: &str,
    headers: &[(String, String)],
    file_path: &str,
    content_length: u64,
    use_h2: bool,
) -> Result<(StatusCode, Bytes), String> {
    let method_parsed = method.parse::<Method>().map_err(|e| e.to_string())?;
    let client = get_client(use_h2);
    let headers = headers.to_vec();

    (async || {
        let file = tokio::fs::File::open(file_path)
            .await
            .map_err(|e| e.to_string())?;
        let stream = tokio_util::io::ReaderStream::new(file);
        let mut req = client.request(method_parsed.clone(), url);
        for (k, v) in &headers {
            req = req.header(k.as_str(), v.as_str());
        }
        req = req
            .header("content-length", content_length.to_string())
            .body(reqwest::Body::wrap_stream(stream));
        let resp = req.send().await.map_err(|e| error_string(e, url))?;
        let status = resp.status();
        if is_retryable_status(status) {
            return Err(format!("HTTP {} (retryable)", status.as_u16()));
        }
        let resp_body = resp.bytes().await.map_err(|e| error_string(e, url))?;
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

pyo3::import_exception!(asanypath.exceptions, AuthenticationError);

/// Map an HTTP status code to the appropriate Python exception. 401 and 403
/// carry the server's reason when the error body has one.
pub fn status_to_pyerr(status: StatusCode, path: &str, body: &[u8]) -> PyErr {
    let reason = error_reason(body).map_or_else(String::new, |r| format!(" ({r})"));
    match status.as_u16() {
        404 => PyFileNotFoundError::new_err(format!("No such file or directory: '{path}'")),
        401 => AuthenticationError::new_err((
            format!("Authentication failed{reason}: '{path}'"),
            path.to_string(),
        )),
        403 => PyPermissionError::new_err(format!("Permission denied{reason}: '{path}'")),
        _ => PyRuntimeError::new_err(format!("HTTP {}: '{path}'", status.as_u16())),
    }
}

/// The message of an error response: Artifactory and GCS JSON
/// (`errors[0].message`, `error.message`) or S3 and Azure XML (`<Message>`,
/// else `<Code>`). Only the first line, at most 200 characters.
pub fn error_reason(body: &[u8]) -> Option<String> {
    use sonic_rs::JsonValueTrait;
    let from_json = || {
        let doc: sonic_rs::Value = sonic_rs::from_slice(body).ok()?;
        let message = doc
            .get("errors")
            .and_then(|errors| errors.get(0))
            .or_else(|| doc.get("error"))
            .unwrap_or(&doc)
            .get("message")?;
        message.as_str().map(str::to_string)
    };
    let from_xml = || {
        let text = std::str::from_utf8(body).ok()?;
        ["Message", "Code"].into_iter().find_map(|tag| {
            let start = text.find(&format!("<{tag}>"))? + tag.len() + 2;
            let end = start + text[start..].find(&format!("</{tag}>"))?;
            quick_xml::escape::unescape(&text[start..end])
                .ok()
                .map(|s| s.into_owned())
        })
    };
    let reason = from_json().or_else(from_xml)?;
    let line = reason.lines().next().unwrap_or_default().trim();
    let line: String = line.chars().filter(|c| !c.is_control()).take(200).collect();
    (!line.is_empty()).then_some(line)
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
use pyo3::types::{PyBytes, PyDict};

use crate::backend::PyBytesBuf;

/// Execute a generic HTTP request, returning (body_bytes, headers_dict).
/// Used by all verb-specific wrappers below.
async fn _http_request(
    method: &str,
    url: &str,
    body: Option<Vec<u8>>,
    headers: Vec<(String, String)>,
) -> Result<(Bytes, HashMap<String, String>), PyErr> {
    let body_bytes = body.map(Bytes::from);
    let (status, resp_body, resp_headers) = do_request(method, url, &headers, body_bytes, false)
        .await
        .map_err(|e| PyRuntimeError::new_err(e))?;
    if !status.is_success() {
        return Err(status_to_pyerr(status, &redact_url(url), &resp_body));
    }
    Ok((resp_body, resp_headers))
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
        Ok(PyBytesBuf(body))
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
        Ok(PyBytesBuf(resp_body))
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
        Ok(PyBytesBuf(resp_body))
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
        Ok(PyBytesBuf(resp_body))
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
        Ok(PyBytesBuf(resp_body))
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
            let result = (
                status.as_u16(),
                PyBytes::new(py, &resp_body),
                dict.into_py_any(py)?,
            );
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
            return Err(status_to_pyerr(status, &redact_url(&url), &body));
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

#[cfg(test)]
mod tests {
    use super::error_reason;

    #[test]
    fn error_reason_reads_json_and_xml_error_bodies() {
        let artifactory = br#"{"errors" : [ {"status" : 401, "message" : "Props Authentication Token not found"} ]}"#;
        assert_eq!(
            error_reason(artifactory).as_deref(),
            Some("Props Authentication Token not found")
        );
        let gcs = br#"{"error": {"code": 401, "message": "Invalid Credentials"}}"#;
        assert_eq!(error_reason(gcs).as_deref(), Some("Invalid Credentials"));
        let s3 = b"<?xml version=\"1.0\"?><Error><Code>InvalidAccessKeyId</Code>\
<Message>The key &amp; id do not exist</Message></Error>";
        assert_eq!(
            error_reason(s3).as_deref(),
            Some("The key & id do not exist")
        );
        let azure = b"<Error><Code>AuthenticationFailed</Code><Message>Server failed to \
authenticate the request.\nRequestId:abc\nTime:2026</Message></Error>";
        assert_eq!(
            error_reason(azure).as_deref(),
            Some("Server failed to authenticate the request.")
        );
        let code_only = b"<Error><Code>AccessDenied</Code></Error>";
        assert_eq!(error_reason(code_only).as_deref(), Some("AccessDenied"));
    }

    #[test]
    fn error_reason_ignores_bodies_without_a_message() {
        assert_eq!(error_reason(b""), None);
        assert_eq!(error_reason(b"<html><body>401</body></html>"), None);
        assert_eq!(error_reason(br#"{"errors": []}"#), None);
        let long = format!(r#"{{"message": "{}"}}"#, "x".repeat(500));
        assert_eq!(error_reason(long.as_bytes()).map(|r| r.len()), Some(200));
    }
}
