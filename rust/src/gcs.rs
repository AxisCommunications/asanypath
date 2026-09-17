//! Google Cloud Storage native operations via the GCS JSON API.
//!
//! Optimizations over the initial version:
//! - Typed serde structs instead of generic `Value` (fewer allocations)
//! - Batch functions (`gcs_get_batch`, `gcs_head_batch`, etc.) that amortize
//!   the PyO3 async bridge overhead across N concurrent requests in a single call
//! - Reuses shared HTTP client from `http.rs` (tcp_nodelay, no compression)

/**
 * Copyright (C) 2026 Axis Communications AB, Lund, Sweden
 *
 * Use of this source code is governed by an MIT-style
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
use bytes::Bytes;
use futures::future::join_all;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use sonic_rs::JsonValueTrait;
use std::collections::{HashMap, HashSet};

use crate::http::{bearer_headers, do_request_light, status_to_pyerr};

// ---------------------------------------------------------------------------
// Typed JSON response structs (avoids Value allocations)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ListResponse {
    #[serde(default)]
    items: Vec<ListItem>,
    #[serde(default)]
    prefixes: Vec<String>,
    #[serde(rename = "nextPageToken")]
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
struct ListItem {
    name: String,
}

/// Response for service-level bucket listing (GET /storage/v1/b).
#[derive(Deserialize)]
struct BucketListResponse {
    #[serde(default)]
    items: Vec<BucketItem>,
    #[serde(rename = "nextPageToken")]
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
struct BucketItem {
    name: String,
}

/// Minimal struct for is_dir check — just needs to know if items/prefixes exist.
#[derive(Deserialize)]
struct IsDirResponse {
    #[serde(default)]
    items: Vec<IsDirItem>,
    #[serde(default)]
    prefixes: Vec<String>,
}

#[derive(Deserialize)]
struct IsDirItem {
    #[allow(dead_code)]
    name: String,
}

#[derive(Deserialize)]
struct ObjectAclResponse {
    owner: Option<ObjectAclOwner>,
    #[serde(default)]
    acl: Vec<ObjectAclEntry>,
}

#[derive(Deserialize)]
struct ObjectAclOwner {
    entity: String,
}

#[derive(Deserialize)]
struct ObjectAclEntry {
    entity: String,
    role: String,
}

#[derive(Deserialize, Serialize)]
struct GcsAclGrant {
    principal: String,
    permission: String,
}

#[derive(Serialize)]
struct GcsAcl {
    owner: Option<String>,
    grants: Vec<GcsAclGrant>,
}

// ---------------------------------------------------------------------------
// GCS credentials & URL building (zero-copy where possible)
// ---------------------------------------------------------------------------

pub(crate) struct GcsCreds {
    endpoint: String,
    bucket: String,
    access_token: Option<String>,
}

impl GcsCreds {
    pub(crate) fn new(endpoint: String, bucket: String, access_token: Option<String>) -> Self {
        Self {
            endpoint,
            bucket,
            access_token,
        }
    }

    fn auth_headers(&self) -> Vec<(String, String)> {
        match &self.access_token {
            Some(token) if !token.is_empty() => bearer_headers(token),
            _ => vec![],
        }
    }

    fn storage_base(&self) -> String {
        let base = self.endpoint.trim_end_matches('/');
        format!("{}/storage/v1/b/{}/o", base, self.bucket)
    }

    fn object_url(&self, object_path: &str) -> String {
        let base = self.endpoint.trim_end_matches('/');
        let encoded = urlencoding::encode(object_path);
        format!("{}/storage/v1/b/{}/o/{}", base, self.bucket, encoded)
    }

    fn upload_base(&self) -> String {
        let base = self.endpoint.trim_end_matches('/');
        format!("{}/upload/storage/v1/b/{}/o", base, self.bucket)
    }
}

fn append_params(url: &str, params: &[(&str, &str)]) -> String {
    if params.is_empty() {
        return url.to_string();
    }
    let qs: String = params
        .iter()
        .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
        .collect::<Vec<_>>()
        .join("&");
    let sep = if url.contains('?') { "&" } else { "?" };
    format!("{}{}{}", url, sep, qs)
}

// ---------------------------------------------------------------------------
// CloudBackend trait implementation
// ---------------------------------------------------------------------------

use crate::backend::CloudBackend;

impl CloudBackend for GcsCreds {
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
            let url = match method {
                "GET" => append_params(&self.object_url(path), &[("alt", "media")]),
                "DELETE" => self.object_url(path),
                _ => self.object_url(path),
            };
            let mut headers = self.auth_headers();
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
        async { Err("GCS does not use do_request_with_headers".to_string()) }
    }

    fn error_path(&self, path: &str) -> String {
        format!("gs://{}/{}", self.bucket, path)
    }

    // Override: GCS server-side copy via the JSON API copyTo endpoint.
    fn copy(
        &self,
        src: &str,
        dst: &str,
        options: &crate::backend::UploadOptions,
        use_h2: bool,
    ) -> impl std::future::Future<Output = Result<(), PyErr>> {
        let options = options.clone();
        async move {
            let base = self.endpoint.trim_end_matches('/');
            let url = crate::backend::append_query(
                &format!(
                    "{}/storage/v1/b/{}/o/{}/copyTo/b/{}/o/{}",
                    base,
                    self.bucket,
                    urlencoding::encode(src),
                    self.bucket,
                    urlencoding::encode(dst),
                ),
                &options.query,
            );
            let mut headers = self.auth_headers();
            let body = if options.provider.is_empty() {
                headers.push(("content-length".to_string(), "0".to_string()));
                None
            } else {
                let body = sonic_rs::to_vec(&options.provider).map_err(|error| {
                    PyValueError::new_err(format!("invalid destination provider: {error}"))
                })?;
                headers.push(("content-type".to_string(), "application/json".to_string()));
                headers.push(("content-length".to_string(), body.len().to_string()));
                Some(Bytes::from(body))
            };
            crate::backend::merge_user_headers(&mut headers, &options)?;
            let (status, resp_body) = do_request_light("POST", &url, &headers, body, use_h2)
                .await
                .map_err(|e| PyRuntimeError::new_err(e))?;
            if !status.is_success() {
                return Err(status_to_pyerr(status, &self.error_path(dst), &resp_body));
            }
            Ok(())
        }
    }

    // Override: GCS PUT uses POST to the upload endpoint
    fn put(
        &self,
        path: &str,
        data: Bytes,
        options: &crate::backend::UploadOptions,
        use_h2: bool,
    ) -> impl std::future::Future<Output = Result<(), PyErr>> {
        let options = options.clone();
        async move {
            let upload_type = if options.provider.is_empty() {
                "media"
            } else {
                "multipart"
            };
            let url = crate::backend::append_query(
                &append_params(
                    &self.upload_base(),
                    &[("uploadType", upload_type), ("name", path)],
                ),
                &options.query,
            );
            let mut headers = self.auth_headers();
            let data = if options.provider.is_empty() {
                headers.push((
                    "content-type".to_string(),
                    "application/octet-stream".to_string(),
                ));
                data
            } else {
                let provider_json = sonic_rs::to_string(&options.provider)
                    .map_err(|error| PyValueError::new_err(format!("invalid provider: {error}")))?;
                let metadata_json = format!(
                    "{{\"name\":{},{}",
                    sonic_rs::to_string(path)
                        .map_err(|error| PyValueError::new_err(error.to_string()))?,
                    &provider_json[1..],
                );
                const BOUNDARY: &str = "asanypath-backend-options";
                let mut body = format!(
                    "--{BOUNDARY}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n{metadata_json}\r\n--{BOUNDARY}\r\nContent-Type: application/octet-stream\r\n\r\n"
                )
                .into_bytes();
                body.extend_from_slice(&data);
                body.extend_from_slice(format!("\r\n--{BOUNDARY}--\r\n").as_bytes());
                headers.push((
                    "content-type".to_string(),
                    format!("multipart/related; boundary={BOUNDARY}"),
                ));
                Bytes::from(body)
            };
            crate::backend::merge_user_headers(&mut headers, &options)?;
            let (status, resp_body) = do_request_light("POST", &url, &headers, Some(data), use_h2)
                .await
                .map_err(|e| PyRuntimeError::new_err(e))?;
            if !status.is_success() {
                return Err(status_to_pyerr(status, &self.error_path(path), &resp_body));
            }
            Ok(())
        }
    }

    // Override: GCS "head" is a metadata GET that returns JSON, parsed into a HashMap
    fn head(
        &self,
        path: &str,
        use_h2: bool,
    ) -> impl std::future::Future<Output = Result<HashMap<String, String>, PyErr>> {
        async move {
            let url = self.object_url(path);
            let headers = self.auth_headers();
            let (status, body) = do_request_light("GET", &url, &headers, None, use_h2)
                .await
                .map_err(|e| PyRuntimeError::new_err(e))?;
            if !status.is_success() {
                return Err(status_to_pyerr(status, &self.error_path(path), &body));
            }
            let data: HashMap<String, sonic_rs::Value> = sonic_rs::from_slice(&body)
                .map_err(|e| PyRuntimeError::new_err(format!("JSON parse error: {e}")))?;
            let mut result = HashMap::new();
            for (k, v) in data {
                if v.is_str() {
                    if let Some(s) = v.as_str() {
                        result.insert(k, s.to_string());
                    }
                } else if v.is_u64() {
                    if let Some(n) = v.as_u64() {
                        result.insert(k, n.to_string());
                    }
                } else if v.is_boolean() {
                    if let Some(b) = v.as_bool() {
                        result.insert(k, b.to_string());
                    }
                }
            }
            Ok(result)
        }
    }

    // Override: exists checks object metadata first, then falls back to is_dir
    fn exists(
        &self,
        path: &str,
        use_h2: bool,
    ) -> impl std::future::Future<Output = Result<bool, PyErr>> {
        async move {
            let url = self.object_url(path);
            let headers = self.auth_headers();
            let (status, _) = do_request_light("GET", &url, &headers, None, use_h2)
                .await
                .map_err(|e| PyRuntimeError::new_err(e))?;
            if status.is_success() {
                return Ok(true);
            }
            do_gcs_is_dir(self, path, use_h2).await
        }
    }
}

async fn do_gcs_is_dir(creds: &GcsCreds, prefix: &str, use_h2: bool) -> Result<bool, PyErr> {
    let search_prefix = if prefix.is_empty() {
        String::new()
    } else {
        format!("{}/", prefix.trim_end_matches('/'))
    };
    let url = append_params(
        &creds.storage_base(),
        &[("prefix", &search_prefix), ("maxResults", "1")],
    );
    let headers = creds.auth_headers();
    let (status, body) = do_request_light("GET", &url, &headers, None, use_h2)
        .await
        .map_err(|e| PyRuntimeError::new_err(e))?;
    if !status.is_success() {
        return Ok(false);
    }
    let resp: IsDirResponse = sonic_rs::from_slice(&body).unwrap_or(IsDirResponse {
        items: vec![],
        prefixes: vec![],
    });
    Ok(!resp.items.is_empty() || !resp.prefixes.is_empty())
}

async fn do_gcs_get_acl(creds: &GcsCreds, object_path: &str) -> Result<String, PyErr> {
    let url = creds.object_url(object_path);
    let headers = creds.auth_headers();
    let (status, body) = do_request_light("GET", &url, &headers, None, false)
        .await
        .map_err(PyRuntimeError::new_err)?;
    if !status.is_success() {
        return Err(status_to_pyerr(
            status,
            &creds.error_path(object_path),
            &body,
        ));
    }
    let response: ObjectAclResponse = sonic_rs::from_slice(&body)
        .map_err(|error| PyRuntimeError::new_err(format!("GCS ACL JSON parse error: {error}")))?;
    let acl = GcsAcl {
        owner: response.owner.map(|owner| owner.entity),
        grants: response
            .acl
            .into_iter()
            .map(|entry| GcsAclGrant {
                principal: entry.entity,
                permission: entry.role,
            })
            .collect(),
    };
    sonic_rs::to_string(&acl)
        .map_err(|error| PyRuntimeError::new_err(format!("could not serialize GCS ACL: {error}")))
}

#[derive(Deserialize)]
struct GcsAclInput {
    grants: Vec<GcsAclGrant>,
}

#[derive(Deserialize, Serialize)]
struct GcsAclGrantInput {
    entity: String,
    role: String,
}

#[derive(Serialize)]
struct GcsAclPayload {
    acl: Vec<GcsAclGrantInput>,
}

async fn do_gcs_put_acl(creds: &GcsCreds, object_path: &str, acl_json: &str) -> Result<(), PyErr> {
    let acl: GcsAclInput = sonic_rs::from_str(acl_json)
        .map_err(|error| PyValueError::new_err(format!("invalid GCS ACL: {error}")))?;
    let entries: Vec<GcsAclGrantInput> = acl
        .grants
        .into_iter()
        .map(|grant| GcsAclGrantInput {
            entity: grant.principal,
            role: grant.permission,
        })
        .collect();
    let body = sonic_rs::to_vec(&GcsAclPayload { acl: entries })
        .map_err(|error| PyValueError::new_err(format!("invalid GCS ACL: {error}")))?;
    let url = creds.object_url(object_path);
    let mut headers = creds.auth_headers();
    headers.push(("content-type".to_string(), "application/json".to_string()));
    let (status, response) =
        do_request_light("PATCH", &url, &headers, Some(Bytes::from(body)), false)
            .await
            .map_err(PyRuntimeError::new_err)?;
    if !status.is_success() {
        return Err(status_to_pyerr(
            status,
            &creds.error_path(object_path),
            &response,
        ));
    }
    Ok(())
}

async fn do_gcs_list(creds: &GcsCreds, prefix: &str, use_h2: bool) -> Result<Vec<String>, PyErr> {
    let current_key = prefix.trim_matches('/').to_string();
    let search_prefix = if current_key.is_empty() {
        String::new()
    } else {
        format!("{}/", current_key)
    };

    let mut found_all = HashSet::new();
    let mut uris = Vec::new();
    let mut page_token: Option<String> = None;
    let headers = creds.auth_headers();

    loop {
        let mut params_owned: Vec<(String, String)> = vec![
            ("prefix".to_string(), search_prefix.clone()),
            ("delimiter".to_string(), "/".to_string()),
        ];
        if let Some(ref token) = page_token {
            params_owned.push(("pageToken".to_string(), token.clone()));
        }
        let params_ref: Vec<(&str, &str)> = params_owned
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let url = append_params(&creds.storage_base(), &params_ref);

        let (status, body) = do_request_light("GET", &url, &headers, None, use_h2)
            .await
            .map_err(|e| PyRuntimeError::new_err(e))?;

        if !status.is_success() {
            return Err(status_to_pyerr(
                status,
                &format!("gs://{}/{}", creds.bucket, search_prefix),
                &body,
            ));
        }

        let resp: ListResponse = sonic_rs::from_slice(&body)
            .map_err(|e| PyRuntimeError::new_err(format!("JSON parse error: {e}")))?;

        // Prefixes (subdirectories)
        for p in &resp.prefixes {
            let value = p.trim_end_matches('/');
            if !value.is_empty() && value != current_key && found_all.insert(value.to_string()) {
                uris.push(format!("gs://{}/{}", creds.bucket, value));
            }
        }

        // Items (files)
        for item in &resp.items {
            let name = &item.name;
            if name.is_empty() || name.trim_end_matches('/') == current_key {
                continue;
            }
            let relative = if !search_prefix.is_empty() && name.starts_with(&search_prefix) {
                &name[search_prefix.len()..]
            } else {
                name.as_str()
            };
            let relative = relative.trim_start_matches('/');
            let resolved = if !relative.is_empty() {
                let first = relative.split('/').next().unwrap_or(relative);
                format!("{}{}", search_prefix, first)
            } else {
                name.to_string()
            };
            let value = resolved.trim_end_matches('/').to_string();
            if !value.is_empty() && value != current_key && found_all.insert(value.clone()) {
                uris.push(format!("gs://{}/{}", creds.bucket, value));
            }
        }

        page_token = resp.next_page_token;
        if page_token.is_none() {
            break;
        }
    }

    Ok(uris)
}

/// List all buckets in a project (GET /storage/v1/b?project=). Returns bare names.
async fn do_gcs_list_buckets(
    creds: &GcsCreds,
    project: &str,
    use_h2: bool,
) -> Result<Vec<String>, PyErr> {
    let base = creds.endpoint.trim_end_matches('/');
    let headers = creds.auth_headers();
    let mut out = Vec::new();
    let mut page_token: Option<String> = None;

    loop {
        let pt = page_token.clone().unwrap_or_default();
        let mut params: Vec<(&str, &str)> = vec![("project", project)];
        if !pt.is_empty() {
            params.push(("pageToken", &pt));
        }
        let url = append_params(&format!("{}/storage/v1/b", base), &params);

        let (status, body) = do_request_light("GET", &url, &headers, None, use_h2)
            .await
            .map_err(|e| PyRuntimeError::new_err(e))?;
        if !status.is_success() {
            return Err(status_to_pyerr(status, "gs://", &body));
        }

        let resp: BucketListResponse = sonic_rs::from_slice(&body)
            .map_err(|e| PyRuntimeError::new_err(format!("JSON parse error: {e}")))?;
        for item in &resp.items {
            out.push(item.name.clone());
        }

        page_token = resp.next_page_token;
        if page_token.is_none() {
            break;
        }
    }

    Ok(out)
}

// ---------------------------------------------------------------------------
// Single-operation Python-exposed functions
// ---------------------------------------------------------------------------

macro_rules! gcs_creds {
    ($endpoint:expr, $bucket:expr, $access_token:expr) => {
        GcsCreds {
            endpoint: $endpoint,
            bucket: $bucket,
            access_token: $access_token,
        }
    };
}

/// GET object metadata — returns JSON fields as a Python dict.
#[pyfunction]
pub fn gcs_head<'py>(
    py: Python<'py>,
    endpoint: String,
    bucket: String,
    object_path: String,
    access_token: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = gcs_creds!(endpoint, bucket, access_token);
        creds.head(&object_path, false).await
    })
}

/// Fetch an object's GCS ACL as JSON with its owner and grant list.
#[pyfunction]
pub fn gcs_get_acl<'py>(
    py: Python<'py>,
    endpoint: String,
    bucket: String,
    object_path: String,
    access_token: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = gcs_creds!(endpoint, bucket, access_token);
        do_gcs_get_acl(&creds, &object_path).await
    })
}

/// Replace an object's complete GCS ACL from a JSON policy document.
#[pyfunction]
pub fn gcs_put_acl<'py>(
    py: Python<'py>,
    endpoint: String,
    bucket: String,
    object_path: String,
    acl_json: String,
    access_token: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = gcs_creds!(endpoint, bucket, access_token);
        do_gcs_put_acl(&creds, &object_path, &acl_json).await
    })
}

/// Check if a prefix has any children (is a "directory").
#[pyfunction]
pub fn gcs_is_dir<'py>(
    py: Python<'py>,
    endpoint: String,
    bucket: String,
    prefix: String,
    access_token: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = gcs_creds!(endpoint, bucket, access_token);
        do_gcs_is_dir(&creds, &prefix, false).await
    })
}

// ---------------------------------------------------------------------------
// Batch operations — one bridge call, N concurrent requests
// ---------------------------------------------------------------------------

/// List multiple prefixes concurrently. Returns list of Vec<String> URIs.
#[pyfunction]
pub fn gcs_list_batch<'py>(
    py: Python<'py>,
    endpoint: String,
    bucket: String,
    prefixes: Vec<String>,
    access_token: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = gcs_creds!(endpoint, bucket, access_token);
        let futs: Vec<_> = prefixes
            .iter()
            .map(|p| do_gcs_list(&creds, p, h2))
            .collect();
        let results = join_all(futs).await;
        let mut out = Vec::with_capacity(results.len());
        for r in results {
            out.push(r?);
        }
        Ok(out)
    })
}

/// List all buckets in a project. Returns bare bucket names.
#[pyfunction]
pub fn gcs_list_buckets<'py>(
    py: Python<'py>,
    endpoint: String,
    project: String,
    access_token: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = gcs_creds!(endpoint, String::new(), access_token);
        do_gcs_list_buckets(&creds, &project, h2).await
    })
}

/// Read multiple objects concurrently. Returns list of bytes (or raises on first error).
#[pyfunction]
pub fn gcs_get_batch<'py>(
    py: Python<'py>,
    endpoint: String,
    bucket: String,
    object_paths: Vec<String>,
    access_token: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = gcs_creds!(endpoint, bucket, access_token);
        batch_collect!(creds, object_paths, h2, get)
    })
}

/// Get metadata for multiple objects concurrently. Returns list of dicts.
#[pyfunction]
pub fn gcs_head_batch<'py>(
    py: Python<'py>,
    endpoint: String,
    bucket: String,
    object_paths: Vec<String>,
    access_token: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = gcs_creds!(endpoint, bucket, access_token);
        batch_collect!(creds, object_paths, h2, head)
    })
}

/// Upload multiple objects concurrently.
#[pyfunction]
pub fn gcs_put_batch<'py>(
    py: Python<'py>,
    endpoint: String,
    bucket: String,
    items: Vec<(String, Vec<u8>, String)>,
    access_token: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = gcs_creds!(endpoint, bucket, access_token);
        batch_put!(creds, items, h2)
    })
}

/// Copy multiple objects concurrently (server-side). Pairs are `(src, dst)`.
#[pyfunction]
pub fn gcs_copy_batch<'py>(
    py: Python<'py>,
    endpoint: String,
    bucket: String,
    pairs: Vec<(String, String)>,
    access_token: Option<String>,
    options_json: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = gcs_creds!(endpoint, bucket, access_token);
        batch_copy!(
            creds,
            pairs,
            options_json.unwrap_or_else(|| "{}".to_string()),
            h2
        )
    })
}

/// Delete multiple objects concurrently.
#[pyfunction]
pub fn gcs_delete_batch<'py>(
    py: Python<'py>,
    endpoint: String,
    bucket: String,
    object_paths: Vec<String>,
    access_token: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = gcs_creds!(endpoint, bucket, access_token);
        batch_fire!(creds, object_paths, h2, delete)
    })
}

/// Check existence of multiple objects concurrently. Returns list of bools.
#[pyfunction]
pub fn gcs_exists_batch<'py>(
    py: Python<'py>,
    endpoint: String,
    bucket: String,
    object_paths: Vec<String>,
    access_token: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = gcs_creds!(endpoint, bucket, access_token);
        batch_collect!(creds, object_paths, h2, exists)
    })
}

/// Check is_dir for multiple prefixes concurrently. Returns list of bools.
#[pyfunction]
pub fn gcs_is_dir_batch<'py>(
    py: Python<'py>,
    endpoint: String,
    bucket: String,
    prefixes: Vec<String>,
    access_token: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = gcs_creds!(endpoint, bucket, access_token);
        let futs: Vec<_> = prefixes
            .iter()
            .map(|p| do_gcs_is_dir(&creds, p, h2))
            .collect();
        let results = join_all(futs).await;
        let mut out = Vec::with_capacity(results.len());
        for r in results {
            out.push(r?);
        }
        Ok(out)
    })
}

// ---------------------------------------------------------------------------
// Presigned URL generation (V4 RSA-SHA256)
// ---------------------------------------------------------------------------

/// Generate a V4 signed URL for a GCS object.
///
/// Requires the service account JSON key content (as a string).
/// Uses RSA-PKCS1-SHA256 signing via the `ring` crate.
#[pyfunction]
pub fn gcs_presign(
    bucket: String,
    object_path: String,
    service_account_json: String,
    method: Option<String>,
    expires: Option<u64>,
) -> PyResult<String> {
    use pem::parse as parse_pem;
    use ring::rand::SystemRandom;
    use ring::signature::{self, RsaKeyPair};
    use sha2::{Digest, Sha256};
    use std::time::SystemTime;

    let method_str = method.as_deref().unwrap_or("GET");
    let expires_secs = expires.unwrap_or(3600);

    // Parse the service account JSON
    let sa: sonic_rs::Value = sonic_rs::from_str(&service_account_json)
        .map_err(|e| PyRuntimeError::new_err(format!("invalid service account JSON: {e}")))?;

    let client_email = sa
        .get("client_email")
        .and_then(|v| v.as_str())
        .ok_or_else(|| PyRuntimeError::new_err("missing client_email in service account JSON"))?;

    let private_key_pem = sa
        .get("private_key")
        .and_then(|v| v.as_str())
        .ok_or_else(|| PyRuntimeError::new_err("missing private_key in service account JSON"))?;

    // Parse PEM and load RSA key
    let pem_data = parse_pem(private_key_pem.as_bytes())
        .map_err(|e| PyRuntimeError::new_err(format!("invalid PEM key: {e}")))?;
    let key_pair = RsaKeyPair::from_pkcs8(pem_data.contents())
        .map_err(|e| PyRuntimeError::new_err(format!("invalid RSA key: {e}")))?;

    // Timestamps
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let secs = now.as_secs();
    let (year, month, day, hours, minutes, seconds) = secs_to_datetime(secs);
    let datestamp = format!("{:04}{:02}{:02}", year, month, day);
    let request_timestamp = format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        year, month, day, hours, minutes, seconds
    );

    let host = "storage.googleapis.com";
    let canonical_uri = format!(
        "/{}/{}",
        bucket,
        urlencoding::encode(&object_path).replace("%2F", "/")
    );
    let credential_scope = format!("{}/auto/storage/goog4_request", datestamp);
    let credential = format!("{}/{}", client_email, credential_scope);

    // Build canonical query string (sorted)
    let mut params: Vec<(&str, String)> = vec![
        ("X-Goog-Algorithm", "GOOG4-RSA-SHA256".to_string()),
        ("X-Goog-Credential", credential),
        ("X-Goog-Date", request_timestamp.clone()),
        ("X-Goog-Expires", expires_secs.to_string()),
        ("X-Goog-SignedHeaders", "host".to_string()),
    ];
    params.sort_by_key(|(k, _)| *k);

    let canonical_querystring: String = params
        .iter()
        .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
        .collect::<Vec<_>>()
        .join("&");

    let canonical_headers = format!("host:{}\n", host);
    let signed_headers = "host";

    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\nUNSIGNED-PAYLOAD",
        method_str, canonical_uri, canonical_querystring, canonical_headers, signed_headers
    );

    let canon_hash = {
        let mut hasher = Sha256::new();
        hasher.update(canonical_request.as_bytes());
        hasher
            .finalize()
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>()
    };

    let string_to_sign = format!(
        "GOOG4-RSA-SHA256\n{}\n{}\n{}",
        request_timestamp, credential_scope, canon_hash
    );

    // RSA-PKCS1-SHA256 sign
    let rng = SystemRandom::new();
    let mut sig = vec![0u8; key_pair.public().modulus_len()];
    key_pair
        .sign(
            &signature::RSA_PKCS1_SHA256,
            &rng,
            string_to_sign.as_bytes(),
            &mut sig,
        )
        .map_err(|e| PyRuntimeError::new_err(format!("RSA signing failed: {e}")))?;

    let signature_hex: String = sig.iter().map(|b| format!("{:02x}", b)).collect();

    Ok(format!(
        "https://{}{}?{}&X-Goog-Signature={}",
        host, canonical_uri, canonical_querystring, signature_hex
    ))
}

/// Convert unix seconds to (year, month, day, hours, minutes, seconds).
fn secs_to_datetime(secs: u64) -> (i64, i64, i64, u64, u64, u64) {
    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;
    // Civil calendar algorithm
    let z = days as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d, hours, minutes, seconds)
}
