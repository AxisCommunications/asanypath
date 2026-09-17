//! JFrog Artifactory native operations via REST API with Bearer token auth.
//!
//! Reuses shared HTTP client from `http.rs` and `bearer_headers` helper.
//! Artifactory uses:
//! - Standard PUT/GET/DELETE for file content at the repository path
//! - Storage API (GET /api/storage/repo/path) for metadata/listing (returns JSON)

/**
 * Copyright (C) 2026 Axis Communications AB, Lund, Sweden
 *
 * Use of this source code is governed by an MIT-style
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
use bytes::Bytes;
use futures::future::join_all;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use serde::Deserialize;
use std::collections::HashMap;

use crate::http::{bearer_headers, do_request_light, status_to_pyerr};

// ---------------------------------------------------------------------------
// JSON response types for Artifactory Storage API
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct StorageInfo {
    #[serde(default)]
    children: Option<Vec<StorageChild>>,
    #[serde(default)]
    size: Option<String>,
    #[serde(default)]
    created: Option<String>,
    #[serde(rename = "lastModified", default)]
    last_modified: Option<String>,
    #[serde(default)]
    checksums: Option<HashMap<String, String>>,
}

#[derive(Deserialize)]
struct StorageChild {
    uri: String,
    folder: bool,
}

/// Entry from the repositories API (GET /api/repositories).
#[derive(Deserialize)]
struct RepoEntry {
    key: String,
}

// ---------------------------------------------------------------------------
// Credentials & URL building
// ---------------------------------------------------------------------------

pub(crate) struct ArtCreds {
    /// Base URL without protocol, e.g. "artifacts.example.com/artifactory"
    base_url: String,
    token: String,
}

impl ArtCreds {
    pub(crate) fn new(base_url: String, token: String) -> Self {
        Self { base_url, token }
    }

    fn auth_headers(&self) -> Vec<(String, String)> {
        bearer_headers(&self.token)
    }

    /// Build the content URL: https://{base_url}/{path}
    fn content_url(&self, path: &str) -> String {
        format!("https://{}/{}", self.base_url, path.trim_start_matches('/'))
    }

    /// Build the storage API URL: https://{base}/api/storage/{repo_path}
    /// Input path is like "repo/folder/file.txt"
    fn storage_url(&self, path: &str) -> String {
        // Insert /api/storage/ after the artifactory base
        // base_url = "host/artifactory", path = "repo/path"
        // result = "https://host/artifactory/api/storage/repo/path"
        let base = self.base_url.trim_end_matches('/');
        let trimmed_path = path.trim_start_matches('/');
        format!("https://{}/api/storage/{}", base, trimmed_path)
    }

    fn permission_target_url(&self, name: &str) -> String {
        let base = self.base_url.trim_end_matches('/');
        format!(
            "https://{}/api/security/permissions/{}",
            base,
            urlencoding::encode(name)
        )
    }
}

macro_rules! art_creds {
    ($base_url:expr, $token:expr) => {
        ArtCreds {
            base_url: $base_url,
            token: $token,
        }
    };
}

// ---------------------------------------------------------------------------
// CloudBackend trait implementation
// ---------------------------------------------------------------------------

use crate::backend::CloudBackend;

impl CloudBackend for ArtCreds {
    fn do_request(
        &self,
        method: &str,
        path: &str,
        body: Option<Bytes>,
        use_h2: bool,
        extra_headers: &[(String, String)],
    ) -> impl std::future::Future<Output = Result<(reqwest::StatusCode, Bytes), String>> {
        let extras = extra_headers.to_vec();
        async move {
            let url = self.content_url(path);
            let mut headers = self.auth_headers();
            if body.is_some() {
                headers.push((
                    "content-type".to_string(),
                    "application/octet-stream".to_string(),
                ));
            }
            headers.extend(extras);
            do_request_light(method, &url, &headers, body, use_h2).await
        }
    }

    fn do_request_with_headers(
        &self,
        _method: &str,
        _path: &str,
        _body: Option<Bytes>,
        _use_h2: bool,
    ) -> impl std::future::Future<
        Output = Result<(reqwest::StatusCode, Bytes, HashMap<String, String>), String>,
    > {
        async { Err("Artifactory does not use do_request_with_headers".to_string()) }
    }

    fn error_path(&self, path: &str) -> String {
        format!("art://{}/{}", self.base_url, path)
    }

    fn put(
        &self,
        path: &str,
        data: Bytes,
        options: &crate::backend::UploadOptions,
        use_h2: bool,
    ) -> impl std::future::Future<Output = Result<(), PyErr>> {
        let options = options.clone();
        async move {
            crate::backend::require_empty_provider(&options)?;
            let url = crate::backend::append_query(&self.content_url(path), &options.query);
            let mut headers = self.auth_headers();
            headers.push((
                "content-type".to_string(),
                "application/octet-stream".to_string(),
            ));
            crate::backend::merge_user_headers(&mut headers, &options)?;
            let (status, body) = do_request_light("PUT", &url, &headers, Some(data), use_h2)
                .await
                .map_err(PyRuntimeError::new_err)?;
            if !status.is_success() {
                return Err(status_to_pyerr(status, &self.error_path(path), &body));
            }
            Ok(())
        }
    }

    // Override: Artifactory native server-side copy via POST /api/copy/{src}?to=/{dst}.
    fn copy(
        &self,
        src: &str,
        dst: &str,
        options: &crate::backend::UploadOptions,
        use_h2: bool,
    ) -> impl std::future::Future<Output = Result<(), PyErr>> {
        let options = options.clone();
        async move {
            crate::backend::require_empty_provider(&options)?;
            let base = self.base_url.trim_end_matches('/');
            let src_trim = src.trim_start_matches('/');
            let dst_trim = dst.trim_start_matches('/');
            let url = crate::backend::append_query(
                &format!("https://{}/api/copy/{}?to=/{}", base, src_trim, dst_trim),
                &options.query,
            );
            let mut headers = self.auth_headers();
            headers.push(("content-length".to_string(), "0".to_string()));
            crate::backend::merge_user_headers(&mut headers, &options)?;
            let (status, resp_body) = do_request_light("POST", &url, &headers, None, use_h2)
                .await
                .map_err(|e| PyRuntimeError::new_err(e))?;
            if !status.is_success() {
                return Err(status_to_pyerr(status, &self.error_path(dst), &resp_body));
            }
            Ok(())
        }
    }

    // Override: delete tolerates 404
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
            if !status.is_success() && status.as_u16() != 404 {
                return Err(status_to_pyerr(status, &self.error_path(path), &resp_body));
            }
            Ok(())
        }
    }

    // Override: exists uses the storage API, not the content URL
    fn exists(
        &self,
        path: &str,
        use_h2: bool,
    ) -> impl std::future::Future<Output = Result<bool, PyErr>> {
        async move {
            let url = self.storage_url(path);
            let headers = self.auth_headers();
            let (status, _body) = do_request_light("GET", &url, &headers, None, use_h2)
                .await
                .map_err(|e| PyRuntimeError::new_err(e))?;
            Ok(status.is_success())
        }
    }

    // Override: head uses storage_info API and returns parsed metadata
    fn head(
        &self,
        path: &str,
        use_h2: bool,
    ) -> impl std::future::Future<Output = Result<HashMap<String, String>, PyErr>> {
        async move {
            let info = do_art_storage_info(self, path, use_h2).await?;
            let mut result: HashMap<String, String> = HashMap::new();
            if let Some(size) = info.size {
                result.insert("size".to_string(), size);
            }
            if let Some(created) = info.created {
                result.insert("created".to_string(), created);
            }
            if let Some(lm) = info.last_modified {
                result.insert("lastModified".to_string(), lm);
            }
            if let Some(checksums) = info.checksums {
                for (k, v) in checksums {
                    result.insert(format!("checksum_{}", k), v);
                }
            }
            if info.children.is_some() {
                result.insert("is_dir".to_string(), "true".to_string());
            }
            Ok(result)
        }
    }
}

async fn do_art_storage_info(
    creds: &ArtCreds,
    path: &str,
    use_h2: bool,
) -> Result<StorageInfo, PyErr> {
    let url = creds.storage_url(path);
    let headers = creds.auth_headers();
    let (status, body) = do_request_light("GET", &url, &headers, None, use_h2)
        .await
        .map_err(|e| PyRuntimeError::new_err(e))?;
    if !status.is_success() {
        return Err(status_to_pyerr(
            status,
            &format!("art://{}/{}", creds.base_url, path),
            &body,
        ));
    }
    let info: StorageInfo = sonic_rs::from_slice(&body)
        .map_err(|e| PyRuntimeError::new_err(format!("JSON parse error: {e}")))?;
    Ok(info)
}

async fn do_art_list(
    creds: &ArtCreds,
    path: &str,
    use_h2: bool,
) -> Result<Vec<(String, bool)>, PyErr> {
    let info = do_art_storage_info(creds, path, use_h2).await?;
    match info.children {
        Some(children) => {
            let base = format!("art://{}/{}", creds.base_url, path.trim_end_matches('/'));
            let results: Vec<(String, bool)> = children
                .iter()
                .map(|child| {
                    let name = child.uri.trim_start_matches('/');
                    (format!("{}/{}", base, name), child.folder)
                })
                .collect();
            Ok(results)
        }
        None => Err(pyo3::exceptions::PyNotADirectoryError::new_err(format!(
            "Not a directory: 'art://{}/{}'",
            creds.base_url, path
        ))),
    }
}

/// List all repositories (GET /api/repositories). Returns bare repository keys.
async fn do_art_list_repos(creds: &ArtCreds, use_h2: bool) -> Result<Vec<String>, PyErr> {
    let base = creds.base_url.trim_end_matches('/');
    let url = format!("https://{}/api/repositories", base);
    let headers = creds.auth_headers();
    let (status, body) = do_request_light("GET", &url, &headers, None, use_h2)
        .await
        .map_err(|e| PyRuntimeError::new_err(e))?;
    if !status.is_success() {
        return Err(status_to_pyerr(
            status,
            &format!("art://{}", creds.base_url),
            &body,
        ));
    }
    let repos: Vec<RepoEntry> = sonic_rs::from_slice(&body)
        .map_err(|e| PyRuntimeError::new_err(format!("JSON parse error: {e}")))?;
    Ok(repos.into_iter().map(|r| r.key).collect())
}

async fn do_art_get_permission_target(creds: &ArtCreds, name: &str) -> Result<String, PyErr> {
    let url = creds.permission_target_url(name);
    let headers = creds.auth_headers();
    let (status, body) = do_request_light("GET", &url, &headers, None, false)
        .await
        .map_err(PyRuntimeError::new_err)?;
    if !status.is_success() {
        return Err(status_to_pyerr(status, &creds.error_path(name), &body));
    }
    std::str::from_utf8(&body)
        .map(str::to_owned)
        .map_err(|error| {
            PyRuntimeError::new_err(format!("invalid permission target response: {error}"))
        })
}

async fn do_art_put_permission_target(
    creds: &ArtCreds,
    name: &str,
    target_json: &str,
) -> Result<(), PyErr> {
    let _: sonic_rs::Value = sonic_rs::from_str(target_json)
        .map_err(|error| PyValueError::new_err(format!("invalid permission target: {error}")))?;
    let url = creds.permission_target_url(name);
    let mut headers = creds.auth_headers();
    headers.push(("content-type".to_string(), "application/json".to_string()));
    let (status, body) = do_request_light(
        "PUT",
        &url,
        &headers,
        Some(Bytes::from(target_json.to_owned())),
        false,
    )
    .await
    .map_err(PyRuntimeError::new_err)?;
    if !status.is_success() {
        return Err(status_to_pyerr(status, &creds.error_path(name), &body));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Single-operation Python-exposed functions
// ---------------------------------------------------------------------------

/// Get storage info (metadata). Returns dict with size, created, lastModified, checksums.
#[pyfunction]
pub fn art_storage_info<'py>(
    py: Python<'py>,
    base_url: String,
    path: String,
    token: String,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = art_creds!(base_url, token);
        creds.head(&path, false).await
    })
}

/// Fetch a named Artifactory permission target as JSON.
#[pyfunction]
pub fn art_get_permission_target<'py>(
    py: Python<'py>,
    base_url: String,
    name: String,
    token: String,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = art_creds!(base_url, token);
        do_art_get_permission_target(&creds, &name).await
    })
}

/// Replace a named Artifactory permission target from its complete JSON document.
#[pyfunction]
pub fn art_put_permission_target<'py>(
    py: Python<'py>,
    base_url: String,
    name: String,
    target_json: String,
    token: String,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = art_creds!(base_url, token);
        do_art_put_permission_target(&creds, &name, &target_json).await
    })
}

// ---------------------------------------------------------------------------
// Batch operations — one bridge call, N concurrent requests
// ---------------------------------------------------------------------------

/// Read multiple files concurrently. Returns list of bytes.
#[pyfunction]
pub fn art_get_batch<'py>(
    py: Python<'py>,
    base_url: String,
    paths: Vec<String>,
    token: String,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = art_creds!(base_url, token);
        batch_collect!(creds, paths, h2, get)
    })
}

/// Upload multiple files concurrently.
#[pyfunction]
pub fn art_put_batch<'py>(
    py: Python<'py>,
    base_url: String,
    items: Vec<(String, Vec<u8>, String)>,
    token: String,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = art_creds!(base_url, token);
        batch_put!(creds, items, h2)
    })
}

/// Copy multiple files concurrently (server-side). Pairs are `(src, dst)`.
#[pyfunction]
pub fn art_copy_batch<'py>(
    py: Python<'py>,
    base_url: String,
    pairs: Vec<(String, String)>,
    token: String,
    options_json: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = art_creds!(base_url, token);
        batch_copy!(
            creds,
            pairs,
            options_json.unwrap_or_else(|| "{}".to_string()),
            h2
        )
    })
}

/// Delete multiple files concurrently.
#[pyfunction]
pub fn art_delete_batch<'py>(
    py: Python<'py>,
    base_url: String,
    paths: Vec<String>,
    token: String,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = art_creds!(base_url, token);
        batch_fire!(creds, paths, h2, delete)
    })
}

/// Check existence of multiple paths concurrently. Returns list of bools.
#[pyfunction]
pub fn art_exists_batch<'py>(
    py: Python<'py>,
    base_url: String,
    paths: Vec<String>,
    token: String,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = art_creds!(base_url, token);
        batch_collect!(creds, paths, h2, exists)
    })
}

/// List multiple directories concurrently. Returns list of Vec<(uri, is_folder)>.
#[pyfunction]
pub fn art_list_batch<'py>(
    py: Python<'py>,
    base_url: String,
    paths: Vec<String>,
    token: String,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = art_creds!(base_url, token);
        let futs: Vec<_> = paths.iter().map(|p| do_art_list(&creds, p, h2)).collect();
        let results = join_all(futs).await;
        let mut out = Vec::with_capacity(results.len());
        for r in results {
            out.push(r?);
        }
        Ok(out)
    })
}

/// List all repositories on the server. Returns bare repository keys.
#[pyfunction]
pub fn art_list_repos<'py>(
    py: Python<'py>,
    base_url: String,
    token: String,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = art_creds!(base_url, token);
        do_art_list_repos(&creds, h2).await
    })
}
