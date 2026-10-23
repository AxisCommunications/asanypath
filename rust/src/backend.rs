//! Shared `CloudBackend` trait that unifies get/put/delete/exists/head across
//! all cloud backends (S3, GCS, Azure, Artifactory).
//!
//! Each backend only needs to implement `do_request` (lightweight) and
//! `do_request_with_headers` (for HEAD), plus `error_path` for error formatting.
//! Default implementations handle the common status-check → PyErr mapping.

/**
 * Copyright (C) 2026 Axis Communications AB, Lund, Sweden
 *
 * Use of this source code is governed by an MIT-style
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use pyo3::PyErr;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::types::PyBytes;
use pyo3::{Bound, IntoPyObject, Python};
use reqwest::StatusCode;
use serde::Deserialize;
use std::collections::HashMap;

use crate::http::status_to_pyerr;

/// Bytes payload bridged to Python with a single copy: wraps reqwest `Bytes`
/// and materializes `bytes` directly (no intermediate `Vec<u8>` copy).
pub struct PyBytesBuf(pub Bytes);

impl<'py> IntoPyObject<'py> for PyBytesBuf {
    type Target = PyBytes;
    type Output = Bound<'py, PyBytes>;
    type Error = std::convert::Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(PyBytes::new(py, &self.0))
    }
}

/// Per-upload options received from Python as JSON.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct UploadOptions {
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default)]
    pub query: HashMap<String, String>,
    #[serde(default)]
    pub provider: HashMap<String, sonic_rs::Value>,
}

/// Reject request fields owned by the transport or backend authentication.
pub fn user_headers(options: &UploadOptions) -> Result<Vec<(String, String)>, PyErr> {
    const RESERVED: &[&str] = &["authorization", "host", "content-length"];
    let mut headers = Vec::with_capacity(options.headers.len());
    for (name, value) in &options.headers {
        if RESERVED
            .iter()
            .any(|reserved| name.eq_ignore_ascii_case(reserved))
        {
            return Err(PyValueError::new_err(format!(
                "backend_options cannot override reserved header {name:?}"
            )));
        }
        headers.push((name.clone(), value.clone()));
    }
    Ok(headers)
}

/// Apply safe user headers, replacing a backend default with the same name.
pub fn merge_user_headers(
    headers: &mut Vec<(String, String)>,
    options: &UploadOptions,
) -> Result<(), PyErr> {
    for (name, value) in user_headers(options)? {
        headers.retain(|(existing, _)| !existing.eq_ignore_ascii_case(&name));
        headers.push((name, value));
    }
    Ok(())
}

/// Reject opaque provider data on backends without an object-resource body.
pub fn require_empty_provider(options: &UploadOptions) -> Result<(), PyErr> {
    if options.provider.is_empty() {
        return Ok(());
    }
    Err(PyValueError::new_err(
        "backend_options.provider is not supported by this backend; use headers or query",
    ))
}

/// Add caller-provided query parameters while preserving an existing query string.
pub fn append_query(url: &str, query: &HashMap<String, String>) -> String {
    if query.is_empty() {
        return url.to_string();
    }
    let encoded = query
        .iter()
        .map(|(name, value)| {
            format!(
                "{}={}",
                urlencoding::encode(name),
                urlencoding::encode(value)
            )
        })
        .collect::<Vec<_>>()
        .join("&");
    let separator = if url.contains('?') { "&" } else { "?" };
    format!("{url}{separator}{encoded}")
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Common cloud object operations. Backends implement the request dispatch;
/// default methods provide the status-check → error mapping.
pub trait CloudBackend {
    /// Execute a request returning (status, body). Includes auth/signing.
    /// `extra_headers` allows injecting additional headers (e.g. Range) before
    /// the request is dispatched; pass `&[]` when none are needed.
    fn do_request(
        &self,
        method: &str,
        path: &str,
        body: Option<Bytes>,
        use_h2: bool,
        extra_headers: &[(String, String)],
    ) -> impl std::future::Future<Output = Result<(StatusCode, Bytes), String>>;

    /// Execute a request returning (status, body, response_headers).
    /// Needed for HEAD-style operations that return headers to Python.
    fn do_request_with_headers(
        &self,
        method: &str,
        path: &str,
        body: Option<Bytes>,
        use_h2: bool,
    ) -> impl std::future::Future<Output = Result<(StatusCode, Bytes, HashMap<String, String>), String>>;

    /// Format the error path for `status_to_pyerr` (e.g. "s3://bucket/key").
    fn error_path(&self, path: &str) -> String;

    // -----------------------------------------------------------------------
    // Default provided methods
    // -----------------------------------------------------------------------

    /// GET an object, returning its body as bytes.
    fn get(
        &self,
        path: &str,
        use_h2: bool,
    ) -> impl std::future::Future<Output = Result<PyBytesBuf, PyErr>> {
        async move {
            let (status, body) = self
                .do_request("GET", path, None, use_h2, &[])
                .await
                .map_err(|e| PyRuntimeError::new_err(e))?;
            if !status.is_success() {
                return Err(status_to_pyerr(status, &self.error_path(path), &body));
            }
            Ok(PyBytesBuf(body))
        }
    }

    /// GET a byte range of an object. Returns the requested slice.
    fn range_get(
        &self,
        path: &str,
        start: u64,
        end: u64,
        use_h2: bool,
        conditions: &[(String, String)],
    ) -> impl std::future::Future<Output = Result<PyBytesBuf, PyErr>> {
        async move {
            let mut headers = conditions.to_vec();
            headers.push(("range".to_string(), format!("bytes={}-{}", start, end)));
            headers.push(("accept-encoding".to_string(), "identity".to_string()));
            let (status, body) = self
                .do_request("GET", path, None, use_h2, &headers)
                .await
                .map_err(|e| PyRuntimeError::new_err(e))?;
            // A range entirely past end-of-object yields 416; treat as empty (EOF)
            // so sequential readers can stop without knowing the size in advance.
            if status.as_u16() == 416 {
                return Ok(PyBytesBuf(Bytes::new()));
            }
            if !status.is_success() {
                return Err(status_to_pyerr(status, &self.error_path(path), &body));
            }
            if status.as_u16() != 206 {
                return Err(pyo3::exceptions::PyOSError::new_err(
                    "Server ignored range request",
                ));
            }
            Ok(PyBytesBuf(body))
        }
    }

    /// PUT an object.
    fn put(
        &self,
        path: &str,
        data: Bytes,
        options: &UploadOptions,
        use_h2: bool,
    ) -> impl std::future::Future<Output = Result<(), PyErr>> {
        async move {
            let headers = user_headers(options)?;
            let (status, resp_body) = self
                .do_request("PUT", path, Some(data), use_h2, &headers)
                .await
                .map_err(|e| PyRuntimeError::new_err(e))?;
            if !status.is_success() {
                return Err(status_to_pyerr(status, &self.error_path(path), &resp_body));
            }
            Ok(())
        }
    }

    /// DELETE an object.
    fn delete(
        &self,
        path: &str,
        use_h2: bool,
    ) -> impl std::future::Future<Output = Result<(), PyErr>> {
        async move {
            let (status, resp_body) = self
                .do_request("DELETE", path, None, use_h2, &[])
                .await
                .map_err(|e| PyRuntimeError::new_err(e))?;
            if !status.is_success() {
                return Err(status_to_pyerr(status, &self.error_path(path), &resp_body));
            }
            Ok(())
        }
    }

    /// Check if an object exists (HEAD → 2xx).
    fn exists(
        &self,
        path: &str,
        use_h2: bool,
    ) -> impl std::future::Future<Output = Result<bool, PyErr>> {
        async move {
            let (status, _body) = self
                .do_request("HEAD", path, None, use_h2, &[])
                .await
                .map_err(|e| PyRuntimeError::new_err(e))?;
            Ok(status.is_success())
        }
    }

    /// HEAD an object, returning response headers.
    fn head(
        &self,
        path: &str,
        use_h2: bool,
    ) -> impl std::future::Future<Output = Result<HashMap<String, String>, PyErr>> {
        async move {
            let (status, resp_body, headers) = self
                .do_request_with_headers("HEAD", path, None, use_h2)
                .await
                .map_err(|e| PyRuntimeError::new_err(e))?;
            if !status.is_success() {
                return Err(status_to_pyerr(status, &self.error_path(path), &resp_body));
            }
            Ok(headers)
        }
    }

    /// Server-side copy `src` → `dst` within this backend/credentials.
    /// Each backend implements the native copy verb (S3 CopyObject, Azure Copy
    /// Blob, GCS copyTo, Artifactory copy).
    fn copy(
        &self,
        src: &str,
        dst: &str,
        options: &UploadOptions,
        use_h2: bool,
    ) -> impl std::future::Future<Output = Result<(), PyErr>>;
}

/// Object DELETEs in flight per tree delete. An unbounded fan-out opens one
/// connection per object; at a 20 ms round trip, 64 deletes about 3,200 keys a
/// second, under S3's 3,500 writes per second per prefix.
pub const TREE_DELETE_CONCURRENCY: usize = 64;

/// Batch requests in flight per tree-delete page: a 1,000-key GCS page is 10
/// batches of 100. 1/2/4/8 in flight took 0.37/0.21/0.17/0.15 s for 1,000
/// objects at a 20 ms round trip; 4 keeps at most 400 deletes in flight.
pub const BATCHES_IN_FLIGHT: usize = 4;

/// Run `batches` (one future per batch request), at most
/// [`BATCHES_IN_FLIGHT`] at a time.
pub async fn run_batches<Fut>(batches: Vec<Fut>) -> Result<(), PyErr>
where
    Fut: std::future::Future<Output = Result<(), PyErr>>,
{
    futures::stream::iter(batches)
        .buffer_unordered(BATCHES_IN_FLIGHT)
        .try_for_each(|()| async { Ok(()) })
        .await
}

/// One DELETE per key, at most [`TREE_DELETE_CONCURRENCY`] in flight; a key
/// that is already gone counts as deleted.
pub async fn delete_each<B: CloudBackend + ?Sized>(
    backend: &B,
    keys: Vec<String>,
    use_h2: bool,
) -> Result<(), PyErr> {
    futures::stream::iter(keys)
        .map(|key| async move {
            let (status, body) = backend
                .do_request("DELETE", &key, None, use_h2, &[])
                .await
                .map_err(PyRuntimeError::new_err)?;
            if status.is_success() || status == StatusCode::NOT_FOUND {
                Ok(())
            } else {
                Err(status_to_pyerr(status, &backend.error_path(&key), &body))
            }
        })
        .buffer_unordered(TREE_DELETE_CONCURRENCY)
        .try_for_each(|()| async { Ok(()) })
        .await
}

/// Recursive delete for object stores addressed by key prefix.
pub trait TreeDelete: CloudBackend {
    /// One page of every object key under `search_prefix`, listed without a
    /// delimiter, and the token of the next page.
    fn list_keys_page(
        &self,
        search_prefix: &str,
        token: Option<String>,
        use_h2: bool,
    ) -> impl std::future::Future<Output = Result<(Vec<String>, Option<String>), PyErr>>;

    /// Delete `keys`; a key that is already gone counts as deleted.
    fn delete_keys(
        &self,
        keys: Vec<String>,
        use_h2: bool,
    ) -> impl std::future::Future<Output = Result<(), PyErr>> {
        delete_each(self, keys, use_h2)
    }

    /// Delete every object under `prefix` (marker keys included), deleting
    /// each listing page while the next one is fetched. Returns the keys left
    /// in place because a request path could not address them (see
    /// [`crate::listing::plain_key`]).
    fn delete_tree(
        &self,
        prefix: &str,
        use_h2: bool,
    ) -> impl std::future::Future<Output = Result<Vec<String>, PyErr>> {
        async move {
            let search = crate::listing::search_prefix(prefix);
            let mut skipped = Vec::new();
            let (mut keys, mut token) = self.list_keys_page(&search, None, use_h2).await?;
            loop {
                let (plain, other): (Vec<_>, Vec<_>) = std::mem::take(&mut keys)
                    .into_iter()
                    .partition(|key| crate::listing::plain_key(key, &search));
                skipped.extend(other);
                let Some(next) = token.take() else {
                    self.delete_keys(plain, use_h2).await?;
                    return Ok(skipped);
                };
                let (deleted, listed) = tokio::join!(
                    self.delete_keys(plain, use_h2),
                    self.list_keys_page(&search, Some(next), use_h2),
                );
                deleted?;
                (keys, token) = listed?;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Batch macros — eliminate repetitive join_all boilerplate in pyfunctions
// ---------------------------------------------------------------------------

/// Collect results from concurrent operations that each return `Result<T, PyErr>`.
/// Usage: `batch_collect!(creds, items, h2, get)` → `Result<Vec<T>, PyErr>`
#[macro_export]
macro_rules! batch_collect {
    ($creds:expr, $items:expr, $h2:expr, $method:ident) => {{
        let futs: Vec<_> = $items.iter().map(|p| $creds.$method(p, $h2)).collect();
        let results = futures::future::join_all(futs).await;
        let mut out = Vec::with_capacity(results.len());
        for r in results {
            out.push(r?);
        }
        Ok(out)
    }};
}

/// Fire-and-forget concurrent operations that return `Result<(), PyErr>`.
/// Usage: `batch_fire!(creds, items, h2, delete)` → `Result<(), PyErr>`
#[macro_export]
macro_rules! batch_fire {
    ($creds:expr, $items:expr, $h2:expr, $method:ident) => {{
        let futs: Vec<_> = $items.iter().map(|p| $creds.$method(p, $h2)).collect();
        let results = futures::future::join_all(futs).await;
        for r in results {
            r?;
        }
        Ok(())
    }};
}

/// Batch PUT: items are `(path, data, options_json)` tuples.
/// Usage: `batch_put!(creds, items, h2)` → `Result<(), PyErr>`
#[macro_export]
macro_rules! batch_put {
    ($creds:expr, $items:expr, $h2:expr) => {{
        let futs: Vec<_> = $items
            .into_iter()
            .map(|(path, data, options_json)| {
                let creds_ref = &$creds;
                async move {
                    let options: $crate::backend::UploadOptions = sonic_rs::from_str(&options_json)
                        .map_err(|error| {
                            pyo3::exceptions::PyValueError::new_err(format!(
                                "invalid backend_options: {error}"
                            ))
                        })?;
                    creds_ref
                        .put(&path, bytes::Bytes::from(data), &options, $h2)
                        .await
                }
            })
            .collect();
        let results = futures::future::join_all(futs).await;
        for r in results {
            r?;
        }
        Ok(())
    }};
}

/// Batch server-side copy: items are `(src, dst)` pairs within one backend.
/// Usage: `batch_copy!(creds, pairs, h2)` → `Result<(), PyErr>`
#[macro_export]
macro_rules! batch_copy {
    ($creds:expr, $pairs:expr, $options_json:expr, $h2:expr) => {{
        let options: $crate::backend::UploadOptions =
            sonic_rs::from_str(&$options_json).map_err(|error| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "invalid destination_backend_options: {error}"
                ))
            })?;
        let futs: Vec<_> = $pairs
            .iter()
            .map(|(src, dst)| $creds.copy(src, dst, &options, $h2))
            .collect();
        let results = futures::future::join_all(futs).await;
        for r in results {
            r?;
        }
        Ok(())
    }};
}

/// Implement a `#[pyfunction]` range-read body.
/// Usage: `range_read_impl!(py, creds, path, start, end)`
#[macro_export]
macro_rules! range_read_impl {
    ($py:expr, $creds:expr, $path:expr, $start:expr, $end:expr, $conditions:expr) => {
        pyo3_async_runtimes::tokio::future_into_py($py, async move {
            $creds
                .range_get(&$path, $start, $end, false, &$conditions)
                .await
        })
    };
}

// ---------------------------------------------------------------------------
// Unified range_read pyfunction — dispatches on kwargs
// ---------------------------------------------------------------------------

use pyo3::prelude::*;

/// Read a byte range from any supported cloud backend.
///
/// Dispatches based on the credential kwargs provided:
/// - `region` present → S3
/// - `container` present → Azure
/// - `base_url` present → Artifactory
/// - otherwise → GCS
#[pyfunction]
#[pyo3(signature = (path, start, end, endpoint=None, bucket=None, region=None,
    access_key=None, secret_key=None, session_token=None,
    container=None, account_name=None, account_key=None, sas_token=None,
    access_token=None, base_url=None, token=None,
    if_match=None, if_unmodified_since=None, if_generation_match=None))]
pub fn range_read<'py>(
    py: Python<'py>,
    path: String,
    start: u64,
    end: u64,
    // S3 / GCS / Azure
    endpoint: Option<String>,
    bucket: Option<String>,
    // S3
    region: Option<String>,
    access_key: Option<String>,
    secret_key: Option<String>,
    session_token: Option<String>,
    // Azure
    container: Option<String>,
    account_name: Option<String>,
    account_key: Option<String>,
    sas_token: Option<String>,
    // GCS
    access_token: Option<String>,
    // Artifactory
    base_url: Option<String>,
    token: Option<String>,
    if_match: Option<String>,
    if_unmodified_since: Option<String>,
    if_generation_match: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    let mut conditions = Vec::new();
    if let Some(value) = if_match {
        conditions.push(("if-match".to_string(), value));
    }
    if let Some(value) = if_unmodified_since {
        conditions.push(("if-unmodified-since".to_string(), value));
    }
    if let Some(value) = if_generation_match {
        conditions.push(("x-asanypath-generation-match".to_string(), value));
    }
    if let Some(region) = region {
        // S3
        let creds = crate::s3::S3Creds {
            endpoint: endpoint.unwrap_or_default(),
            bucket: bucket.unwrap_or_default(),
            region,
            access_key: access_key.unwrap_or_default(),
            secret_key: secret_key.unwrap_or_default(),
            session_token,
        };
        range_read_impl!(py, creds, path, start, end, conditions)
    } else if let Some(container) = container {
        // Azure
        use crate::azure::AzCreds;
        let creds = AzCreds::new(
            endpoint.unwrap_or_default(),
            container,
            account_name.unwrap_or_default(),
            account_key,
            sas_token,
        );
        range_read_impl!(py, creds, path, start, end, conditions)
    } else if let Some(base_url) = base_url {
        // Artifactory
        use crate::artifactory::ArtCreds;
        let creds = ArtCreds::new(base_url, token.unwrap_or_default());
        range_read_impl!(py, creds, path, start, end, conditions)
    } else {
        // GCS
        use crate::gcs::GcsCreds;
        let creds = GcsCreds::new(
            endpoint.unwrap_or_default(),
            bucket.unwrap_or_default(),
            access_token,
        );
        range_read_impl!(py, creds, path, start, end, conditions)
    }
}
