//! Azure Blob Storage native operations via the REST API.
//!
//! Uses SharedKey HMAC-SHA256 authentication and XML list parsing.
//! Reuses shared HTTP client from `http.rs` and XML helpers from `xml.rs`.

/**
 * Copyright (C) 2026 Axis Communications AB, Lund, Sweden
 *
 * Use of this source code is governed by an MIT-style
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
use base64::{Engine, engine::general_purpose::STANDARD as B64};
use bytes::Bytes;
use futures::future::join_all;
use hmac::{Hmac, KeyInit, Mac};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use quick_xml::Reader;
use quick_xml::events::Event;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashSet;

use crate::http::{do_request, do_request_light, status_to_pyerr};
use crate::xml::local_name;

type HmacSha256 = Hmac<Sha256>;

const API_VERSION: &str = "2023-11-03";

// ---------------------------------------------------------------------------
// Azure credentials & URL building
// ---------------------------------------------------------------------------

pub(crate) struct AzCreds {
    endpoint: String,
    container: String,
    account_name: String,
    /// Pre-decoded HMAC key bytes (cached from base64 account_key).
    key_bytes: Option<Vec<u8>>,
    sas_token: Option<String>,
}

impl AzCreds {
    pub(crate) fn new(
        endpoint: String,
        container: String,
        account_name: String,
        account_key: Option<String>,
        sas_token: Option<String>,
    ) -> Self {
        let key_bytes = account_key
            .as_ref()
            .filter(|k| !k.is_empty())
            .and_then(|k| B64.decode(k).ok());
        Self {
            endpoint,
            container,
            account_name,
            key_bytes,
            sas_token,
        }
    }
}

impl AzCreds {
    /// Build URL for a blob operation.
    fn blob_url(&self, blob_path: &str) -> String {
        let base = self.endpoint.trim_end_matches('/');
        let mut url = if blob_path.is_empty() {
            format!("{}/{}", base, self.container)
        } else {
            format!("{}/{}/{}", base, self.container, blob_path)
        };
        if let Some(ref sas) = self.sas_token {
            let sep = if url.contains('?') { "&" } else { "?" };
            url = format!("{}{}{}", url, sep, sas);
        }
        url
    }

    /// Build URL with query params (for list operations).
    fn list_url(&self, params: &[(&str, &str)]) -> String {
        let base = self.endpoint.trim_end_matches('/');
        let mut url = format!("{}/{}", base, self.container);
        if let Some(ref sas) = self.sas_token {
            url = format!("{}?{}", url, sas);
        }
        if !params.is_empty() {
            let qs: String = params
                .iter()
                .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
                .collect::<Vec<_>>()
                .join("&");
            let sep = if url.contains('?') { "&" } else { "?" };
            url = format!("{}{}{}", url, sep, qs);
        }
        url
    }

    /// Sign a request with Azure SharedKey. Mutates headers in-place.
    fn sign(
        &self,
        method: &str,
        url: &str,
        headers: &mut Vec<(String, String)>,
        params: Option<&[(&str, &str)]>,
    ) {
        let now = chrono_now_rfc2822();
        headers.push(("x-ms-date".to_string(), now));
        headers.push(("x-ms-version".to_string(), API_VERSION.to_string()));

        let key_bytes = match &self.key_bytes {
            Some(kb) => kb,
            None => return, // SAS auth or no key, no signing needed
        };

        // Canonicalized headers
        let mut ms_headers: Vec<(String, String)> = headers
            .iter()
            .filter(|(k, _)| k.starts_with("x-ms-"))
            .map(|(k, v)| (k.to_lowercase(), v.clone()))
            .collect();
        ms_headers.sort_by(|a, b| a.0.cmp(&b.0));
        let canon_headers: String = ms_headers
            .iter()
            .map(|(k, v)| format!("{}:{}", k, v))
            .collect::<Vec<_>>()
            .join("\n");

        // Canonicalized resource
        let url_path = url
            .split('?')
            .next()
            .unwrap_or("")
            .trim_start_matches("http://")
            .trim_start_matches("https://");
        let path_start = url_path.find('/').unwrap_or(url_path.len());
        let resource_path = &url_path[path_start..];
        let mut canon_resource = format!("/{}{}", self.account_name, resource_path);
        if let Some(p) = params {
            let mut sorted: Vec<(&str, &str)> = p.to_vec();
            sorted.sort_by_key(|(k, _)| *k);
            for (k, v) in sorted {
                canon_resource = format!("{}\n{}:{}", canon_resource, k, v);
            }
        }

        // Look up content-length and content-type from headers
        let content_length = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
            .map(|(_, v)| v.as_str())
            .unwrap_or("");
        let content_length = if content_length == "0" {
            ""
        } else {
            content_length
        };
        let content_type = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("content-type"))
            .map(|(_, v)| v.as_str())
            .unwrap_or("");

        let string_to_sign = format!(
            "{}\n\n\n{}\n\n{}\n\n\n\n\n\n\n{}\n{}",
            method, content_length, content_type, canon_headers, canon_resource
        );

        let mut mac = HmacSha256::new_from_slice(key_bytes).expect("HMAC key");
        mac.update(string_to_sign.as_bytes());
        let signature = B64.encode(mac.finalize().into_bytes());

        headers.push((
            "authorization".to_string(),
            format!("SharedKey {}:{}", self.account_name, signature),
        ));
    }
}

/// Format current UTC time as RFC 2822 for Azure x-ms-date header.
fn chrono_now_rfc2822() -> String {
    use std::time::SystemTime;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let secs = now.as_secs();

    // Manual formatting: "Day, DD Mon YYYY HH:MM:SS GMT"
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Day of week (Jan 1, 1970 was Thursday = 4)
    let dow = ((days_since_epoch + 4) % 7) as usize;
    let day_names = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

    // Date calculation (from days since epoch)
    let (year, month, day) = days_to_ymd(days_since_epoch as i64);
    let month_names = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];

    format!(
        "{}, {:02} {} {:04} {:02}:{:02}:{:02} GMT",
        day_names[dow],
        day,
        month_names[(month - 1) as usize],
        year,
        hours,
        minutes,
        seconds
    )
}

fn days_to_ymd(days: i64) -> (i64, i64, i64) {
    // Civil calendar algorithm
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

// ---------------------------------------------------------------------------
// CloudBackend trait implementation
// ---------------------------------------------------------------------------

use crate::backend::CloudBackend;
use std::collections::HashMap;

impl CloudBackend for AzCreds {
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
            let url = self.blob_url(path);
            let mut headers: Vec<(String, String)> = extras;
            self.sign(method, &url, &mut headers, None);
            do_request_light(method, &url, &headers, body, use_h2).await
        }
    }

    fn do_request_with_headers(
        &self,
        method: &str,
        path: &str,
        body: Option<Bytes>,
        use_h2: bool,
    ) -> impl std::future::Future<
        Output = Result<(reqwest::StatusCode, Bytes, HashMap<String, String>), String>,
    > {
        async move {
            let url = self.blob_url(path);
            let mut headers = Vec::new();
            self.sign(method, &url, &mut headers, None);
            do_request(method, &url, &headers, body, use_h2).await
        }
    }

    fn error_path(&self, path: &str) -> String {
        format!("az://{}/{}", self.container, path)
    }

    // Override: Azure server-side copy via PUT + x-ms-copy-source (signed x-ms- header).
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
            let base = self.endpoint.trim_end_matches('/');
            let mut src_url = format!("{}/{}/{}", base, self.container, src);
            if let Some(ref sas) = self.sas_token {
                let sep = if src_url.contains('?') { "&" } else { "?" };
                src_url = format!("{}{}{}", src_url, sep, sas);
            }
            let dst_url = crate::backend::append_query(&self.blob_url(dst), &options.query);
            let mut headers = vec![
                ("content-length".to_string(), "0".to_string()),
                ("x-ms-copy-source".to_string(), src_url),
            ];
            crate::backend::merge_user_headers(&mut headers, &options)?;
            self.sign("PUT", &dst_url, &mut headers, None);
            let (status, resp_body) = do_request_light("PUT", &dst_url, &headers, None, use_h2)
                .await
                .map_err(|e| PyRuntimeError::new_err(e))?;
            if !status.is_success() {
                return Err(status_to_pyerr(status, &self.error_path(dst), &resp_body));
            }
            Ok(())
        }
    }

    // Override: Azure PUT needs content-type + content-length + x-ms-blob-type headers
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
            let url = crate::backend::append_query(&self.blob_url(path), &options.query);
            let mut headers = vec![
                (
                    "content-type".to_string(),
                    "application/octet-stream".to_string(),
                ),
                ("content-length".to_string(), data.len().to_string()),
                ("x-ms-blob-type".to_string(), "BlockBlob".to_string()),
            ];
            crate::backend::merge_user_headers(&mut headers, &options)?;
            self.sign("PUT", &url, &mut headers, None);
            let (status, resp_body) = do_request_light("PUT", &url, &headers, Some(data), use_h2)
                .await
                .map_err(|e| PyRuntimeError::new_err(e))?;
            if !status.is_success() {
                return Err(status_to_pyerr(status, &self.error_path(path), &resp_body));
            }
            Ok(())
        }
    }

    // Override: Azure HEAD uses HTTP HEAD and returns response headers
    fn head(
        &self,
        path: &str,
        use_h2: bool,
    ) -> impl std::future::Future<Output = Result<HashMap<String, String>, PyErr>> {
        async move {
            let (status, _body, resp_headers) = self
                .do_request_with_headers("HEAD", path, None, use_h2)
                .await
                .map_err(|e| PyRuntimeError::new_err(e))?;
            if !status.is_success() {
                return Err(status_to_pyerr(status, &self.error_path(path), &[]));
            }
            Ok(resp_headers)
        }
    }

    // Override: exists checks HEAD first, then falls back to is_dir
    fn exists(
        &self,
        path: &str,
        use_h2: bool,
    ) -> impl std::future::Future<Output = Result<bool, PyErr>> {
        async move {
            let url = self.blob_url(path);
            let mut headers = Vec::new();
            self.sign("HEAD", &url, &mut headers, None);
            let (status, _body) = do_request_light("HEAD", &url, &headers, None, use_h2)
                .await
                .map_err(|e| PyRuntimeError::new_err(e))?;
            if status.is_success() {
                return Ok(true);
            }
            do_az_is_dir(self, path, use_h2).await
        }
    }
}

async fn do_az_is_dir(creds: &AzCreds, prefix: &str, use_h2: bool) -> Result<bool, PyErr> {
    let search_prefix = if prefix.is_empty() {
        String::new()
    } else {
        format!("{}/", prefix.trim_end_matches('/'))
    };
    let params: Vec<(&str, &str)> = vec![
        ("restype", "container"),
        ("comp", "list"),
        ("prefix", &search_prefix),
        ("maxresults", "1"),
    ];
    let url = creds.list_url(&params);
    let mut headers = Vec::new();
    creds.sign("GET", &url, &mut headers, Some(&params));
    let (status, body) = do_request_light("GET", &url, &headers, None, use_h2)
        .await
        .map_err(|e| PyRuntimeError::new_err(e))?;
    if !status.is_success() {
        return Ok(false);
    }
    // Parse XML: look for any <Name> inside <Blob> or <BlobPrefix>
    let mut reader = Reader::from_reader(body.as_ref());
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) | Ok(Event::Empty(e)) => {
                let name = e.name();
                let tag = local_name(name.as_ref());
                if tag == "Name" {
                    // Found at least one item
                    return Ok(true);
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => break,
            _ => {}
        }
        buf.clear();
    }
    Ok(false)
}

async fn do_az_list(creds: &AzCreds, prefix: &str, use_h2: bool) -> Result<Vec<String>, PyErr> {
    let current_key = prefix.trim_matches('/').to_string();
    let search_prefix = if current_key.is_empty() {
        String::new()
    } else {
        format!("{}/", current_key)
    };

    let mut found_all = HashSet::new();
    let mut uris = Vec::new();
    let mut marker: Option<String> = None;

    loop {
        let marker_str = marker.clone().unwrap_or_default();
        let mut params: Vec<(&str, &str)> = vec![
            ("restype", "container"),
            ("comp", "list"),
            ("prefix", &search_prefix),
            ("delimiter", "/"),
        ];
        if !marker_str.is_empty() {
            params.push(("marker", &marker_str));
        }
        let url = creds.list_url(&params);
        let mut headers = Vec::new();
        creds.sign("GET", &url, &mut headers, Some(&params));
        let (status, body) = do_request_light("GET", &url, &headers, None, use_h2)
            .await
            .map_err(|e| PyRuntimeError::new_err(e))?;
        if !status.is_success() {
            return Err(status_to_pyerr(
                status,
                &format!("az://{}/{}", creds.container, search_prefix),
                &body,
            ));
        }

        // Parse XML
        let mut reader = Reader::from_reader(body.as_ref());
        reader.config_mut().trim_text(true);
        let mut buf = Vec::new();
        let mut next_marker: Option<String> = None;
        let mut in_blob_prefix = false;

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    let name = e.name();
                    let tag = local_name(name.as_ref());
                    match tag {
                        "BlobPrefix" => {
                            in_blob_prefix = true;
                        }
                        "Name" => {
                            let text = reader
                                .read_text(e.name())
                                .ok()
                                .map(|text| text.xml10_content().into_owned())
                                .unwrap_or_default();
                            if in_blob_prefix {
                                let value = text.trim_end_matches('/').to_string();
                                if !value.is_empty()
                                    && value != current_key
                                    && found_all.insert(value.clone())
                                {
                                    uris.push(format!("az://{}/{}", creds.container, value));
                                }
                            } else {
                                // Regular blob
                                let name = &text;
                                if name.is_empty() || name.trim_end_matches('/') == current_key {
                                    // skip
                                } else {
                                    let relative = if !search_prefix.is_empty()
                                        && name.starts_with(&search_prefix)
                                    {
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
                                    if !value.is_empty()
                                        && value != current_key
                                        && found_all.insert(value.clone())
                                    {
                                        uris.push(format!("az://{}/{}", creds.container, value));
                                    }
                                }
                            }
                        }
                        "NextMarker" => {
                            let text = reader
                                .read_text(e.name())
                                .ok()
                                .map(|text| text.xml10_content().into_owned())
                                .unwrap_or_default();
                            if !text.is_empty() {
                                next_marker = Some(text);
                            }
                        }
                        _ => {}
                    }
                }
                Ok(Event::End(ref e)) => {
                    let name = e.name();
                    if local_name(name.as_ref()) == "BlobPrefix" {
                        in_blob_prefix = false;
                    }
                }
                Ok(Event::Eof) => break,
                Err(_) => break,
                _ => {}
            }
            buf.clear();
        }

        marker = next_marker;
        if marker.is_none() {
            break;
        }
    }

    Ok(uris)
}

/// List all containers in the account (GET /?comp=list). Returns bare names.
async fn do_az_list_containers(creds: &AzCreds, use_h2: bool) -> Result<Vec<String>, PyErr> {
    let mut out = Vec::new();
    let mut marker: Option<String> = None;

    loop {
        let marker_str = marker.clone().unwrap_or_default();
        let mut params: Vec<(&str, &str)> = vec![("comp", "list")];
        if !marker_str.is_empty() {
            params.push(("marker", &marker_str));
        }
        let url = creds.list_url(&params);
        let mut headers = Vec::new();
        creds.sign("GET", &url, &mut headers, Some(&params));
        let (status, body) = do_request_light("GET", &url, &headers, None, use_h2)
            .await
            .map_err(|e| PyRuntimeError::new_err(e))?;
        if !status.is_success() {
            return Err(status_to_pyerr(status, "az://", &body));
        }

        let mut reader = Reader::from_reader(body.as_ref());
        reader.config_mut().trim_text(true);
        let mut buf = Vec::new();
        let mut next_marker: Option<String> = None;
        let mut in_container = false;

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) => {
                    let name = e.name();
                    match local_name(name.as_ref()) {
                        "Container" => in_container = true,
                        "Name" if in_container => {
                            let text = reader
                                .read_text(e.name())
                                .ok()
                                .map(|text| text.xml10_content().into_owned())
                                .unwrap_or_default();
                            if !text.is_empty() {
                                out.push(text);
                            }
                        }
                        "NextMarker" => {
                            let text = reader
                                .read_text(e.name())
                                .ok()
                                .map(|text| text.xml10_content().into_owned())
                                .unwrap_or_default();
                            if !text.is_empty() {
                                next_marker = Some(text);
                            }
                        }
                        _ => {}
                    }
                }
                Ok(Event::End(ref e)) => {
                    if local_name(e.name().as_ref()) == "Container" {
                        in_container = false;
                    }
                }
                Ok(Event::Eof) => break,
                Err(_) => break,
                _ => {}
            }
            buf.clear();
        }

        marker = next_marker;
        if marker.is_none() {
            break;
        }
    }

    Ok(out)
}

#[derive(Deserialize, Serialize)]
struct AzSignedIdentifier {
    id: String,
    start: Option<String>,
    expiry: Option<String>,
    permissions: Option<String>,
}

#[derive(Deserialize, Serialize)]
struct AzContainerAcl {
    public_access: Option<String>,
    signed_identifiers: Vec<AzSignedIdentifier>,
}

async fn do_az_get_container_acl(creds: &AzCreds) -> Result<String, PyErr> {
    let params = [("restype", "container"), ("comp", "acl")];
    let url = creds.list_url(&params);
    let mut headers = Vec::new();
    creds.sign("GET", &url, &mut headers, Some(&params));
    let (status, body, response_headers) = do_request("GET", &url, &headers, None, false)
        .await
        .map_err(PyRuntimeError::new_err)?;
    if !status.is_success() {
        return Err(status_to_pyerr(status, &creds.error_path(""), &body));
    }

    let mut reader = Reader::from_reader(body.as_ref());
    reader.config_mut().trim_text(true);
    let mut buffer = Vec::with_capacity(256);
    let mut signed_identifiers = Vec::new();
    let mut identifier: Option<AzSignedIdentifier> = None;
    let mut field: Option<String> = None;
    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(element)) => {
                let element_name = element.name();
                match local_name(element_name.as_ref()) {
                    "SignedIdentifier" => {
                        identifier = Some(AzSignedIdentifier {
                            id: String::new(),
                            start: None,
                            expiry: None,
                            permissions: None,
                        });
                    }
                    "Id" | "Start" | "Expiry" | "Permission" if identifier.is_some() => {
                        field = Some(local_name(element_name.as_ref()).to_string());
                    }
                    _ => {}
                }
            }
            Ok(Event::Text(text)) => {
                if let (Some(identifier), Some(field_name), value) =
                    (identifier.as_mut(), field.as_deref(), text.xml10_content())
                {
                    match field_name {
                        "Id" => identifier.id = value.into_owned(),
                        "Start" => identifier.start = Some(value.into_owned()),
                        "Expiry" => identifier.expiry = Some(value.into_owned()),
                        "Permission" => identifier.permissions = Some(value.into_owned()),
                        _ => {}
                    }
                }
            }
            Ok(Event::End(element)) => match local_name(element.name().as_ref()) {
                "SignedIdentifier" => {
                    if let Some(identifier) = identifier.take() {
                        signed_identifiers.push(identifier);
                    }
                }
                "Id" | "Start" | "Expiry" | "Permission" => field = None,
                _ => {}
            },
            Ok(Event::Eof) | Err(_) => break,
            _ => {}
        }
        buffer.clear();
    }
    let public_access = response_headers.get("x-ms-blob-public-access").cloned();
    sonic_rs::to_string(&AzContainerAcl {
        public_access,
        signed_identifiers,
    })
    .map_err(|error| PyRuntimeError::new_err(format!("could not serialize Azure ACL: {error}")))
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

async fn do_az_put_container_acl(creds: &AzCreds, acl_json: &str) -> Result<(), PyErr> {
    let acl: AzContainerAcl = sonic_rs::from_str(acl_json)
        .map_err(|error| PyValueError::new_err(format!("invalid Azure ACL: {error}")))?;
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"utf-8\"?><SignedIdentifiers>");
    for identifier in acl.signed_identifiers {
        xml.push_str("<SignedIdentifier><Id>");
        xml.push_str(&xml_escape(&identifier.id));
        xml.push_str("</Id><AccessPolicy>");
        if let Some(start) = identifier.start {
            xml.push_str(&format!("<Start>{}</Start>", xml_escape(&start)));
        }
        if let Some(expiry) = identifier.expiry {
            xml.push_str(&format!("<Expiry>{}</Expiry>", xml_escape(&expiry)));
        }
        if let Some(permissions) = identifier.permissions {
            xml.push_str(&format!(
                "<Permission>{}</Permission>",
                xml_escape(&permissions)
            ));
        }
        xml.push_str("</AccessPolicy></SignedIdentifier>");
    }
    xml.push_str("</SignedIdentifiers>");
    let params = [("restype", "container"), ("comp", "acl")];
    let url = creds.list_url(&params);
    let mut headers = vec![
        ("content-type".to_string(), "application/xml".to_string()),
        ("content-length".to_string(), xml.len().to_string()),
    ];
    if let Some(public_access) = acl.public_access {
        headers.push(("x-ms-blob-public-access".to_string(), public_access));
    }
    creds.sign("PUT", &url, &mut headers, Some(&params));
    let (status, body, _) = do_request("PUT", &url, &headers, Some(Bytes::from(xml)), false)
        .await
        .map_err(PyRuntimeError::new_err)?;
    if !status.is_success() {
        return Err(status_to_pyerr(status, &creds.error_path(""), &body));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Single-operation Python-exposed functions
// ---------------------------------------------------------------------------

macro_rules! az_creds {
    ($endpoint:expr, $container:expr, $account_name:expr, $account_key:expr, $sas_token:expr) => {
        AzCreds::new(
            $endpoint,
            $container,
            $account_name,
            $account_key,
            $sas_token,
        )
    };
}

#[pyfunction]
pub fn az_is_dir<'py>(
    py: Python<'py>,
    endpoint: String,
    container: String,
    prefix: String,
    account_name: String,
    account_key: Option<String>,
    sas_token: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = az_creds!(endpoint, container, account_name, account_key, sas_token);
        do_az_is_dir(&creds, &prefix, false).await
    })
}

/// Fetch a container's public-access setting and stored access policies.
#[pyfunction]
pub fn az_get_container_acl<'py>(
    py: Python<'py>,
    endpoint: String,
    container: String,
    account_name: String,
    account_key: Option<String>,
    sas_token: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = az_creds!(endpoint, container, account_name, account_key, sas_token);
        do_az_get_container_acl(&creds).await
    })
}

/// Replace a container's public-access setting and stored access policies.
#[pyfunction]
pub fn az_put_container_acl<'py>(
    py: Python<'py>,
    endpoint: String,
    container: String,
    account_name: String,
    account_key: Option<String>,
    sas_token: Option<String>,
    acl_json: String,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = az_creds!(endpoint, container, account_name, account_key, sas_token);
        do_az_put_container_acl(&creds, &acl_json).await
    })
}

// ---------------------------------------------------------------------------
// Batch operations
// ---------------------------------------------------------------------------

#[pyfunction]
pub fn az_list_batch<'py>(
    py: Python<'py>,
    endpoint: String,
    container: String,
    prefixes: Vec<String>,
    account_name: String,
    account_key: Option<String>,
    sas_token: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = az_creds!(endpoint, container, account_name, account_key, sas_token);
        let futs: Vec<_> = prefixes.iter().map(|p| do_az_list(&creds, p, h2)).collect();
        let results = join_all(futs).await;
        let mut out = Vec::with_capacity(results.len());
        for r in results {
            out.push(r?);
        }
        Ok(out)
    })
}

/// List all containers in the account. Returns bare container names.
#[pyfunction]
pub fn az_list_containers<'py>(
    py: Python<'py>,
    endpoint: String,
    account_name: String,
    account_key: Option<String>,
    sas_token: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = az_creds!(
            endpoint,
            String::new(),
            account_name,
            account_key,
            sas_token
        );
        do_az_list_containers(&creds, h2).await
    })
}

#[pyfunction]
pub fn az_get_batch<'py>(
    py: Python<'py>,
    endpoint: String,
    container: String,
    blob_paths: Vec<String>,
    account_name: String,
    account_key: Option<String>,
    sas_token: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = az_creds!(endpoint, container, account_name, account_key, sas_token);
        batch_collect!(creds, blob_paths, h2, get)
    })
}

#[pyfunction]
pub fn az_head_batch<'py>(
    py: Python<'py>,
    endpoint: String,
    container: String,
    blob_paths: Vec<String>,
    account_name: String,
    account_key: Option<String>,
    sas_token: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = az_creds!(endpoint, container, account_name, account_key, sas_token);
        batch_collect!(creds, blob_paths, h2, head)
    })
}

#[pyfunction]
pub fn az_put_batch<'py>(
    py: Python<'py>,
    endpoint: String,
    container: String,
    items: Vec<(String, Vec<u8>, String)>,
    account_name: String,
    account_key: Option<String>,
    sas_token: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = az_creds!(endpoint, container, account_name, account_key, sas_token);
        batch_put!(creds, items, h2)
    })
}

#[pyfunction]
pub fn az_copy_batch<'py>(
    py: Python<'py>,
    endpoint: String,
    container: String,
    pairs: Vec<(String, String)>,
    account_name: String,
    account_key: Option<String>,
    sas_token: Option<String>,
    options_json: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = az_creds!(endpoint, container, account_name, account_key, sas_token);
        batch_copy!(
            creds,
            pairs,
            options_json.unwrap_or_else(|| "{}".to_string()),
            h2
        )
    })
}

#[pyfunction]
pub fn az_delete_batch<'py>(
    py: Python<'py>,
    endpoint: String,
    container: String,
    blob_paths: Vec<String>,
    account_name: String,
    account_key: Option<String>,
    sas_token: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = az_creds!(endpoint, container, account_name, account_key, sas_token);
        batch_fire!(creds, blob_paths, h2, delete)
    })
}

#[pyfunction]
pub fn az_exists_batch<'py>(
    py: Python<'py>,
    endpoint: String,
    container: String,
    blob_paths: Vec<String>,
    account_name: String,
    account_key: Option<String>,
    sas_token: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = az_creds!(endpoint, container, account_name, account_key, sas_token);
        batch_collect!(creds, blob_paths, h2, exists)
    })
}

#[pyfunction]
pub fn az_is_dir_batch<'py>(
    py: Python<'py>,
    endpoint: String,
    container: String,
    prefixes: Vec<String>,
    account_name: String,
    account_key: Option<String>,
    sas_token: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = az_creds!(endpoint, container, account_name, account_key, sas_token);
        let futs: Vec<_> = prefixes
            .iter()
            .map(|p| do_az_is_dir(&creds, p, h2))
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
// Presigned URL generation (Service SAS)
// ---------------------------------------------------------------------------

/// Generate a presigned URL for an Azure blob using a Service SAS token.
///
/// If `sas_token` is already provided, it is simply appended to the blob URL.
/// Otherwise, `account_key` is used to generate a new Service SAS.
#[pyfunction]
pub fn az_presign(
    endpoint: String,
    container: String,
    blob_path: String,
    account_name: String,
    account_key: Option<String>,
    sas_token: Option<String>,
    method: Option<String>,
    expires: Option<u64>,
) -> PyResult<String> {
    use std::time::SystemTime;

    let base = endpoint.trim_end_matches('/');
    let blob_url = if blob_path.is_empty() {
        format!("{}/{}", base, container)
    } else {
        format!("{}/{}/{}", base, container, blob_path)
    };

    // If SAS token already exists, just append it
    if let Some(ref sas) = sas_token {
        if !sas.is_empty() {
            let sep = if blob_url.contains('?') { "&" } else { "?" };
            return Ok(format!("{}{}{}", blob_url, sep, sas));
        }
    }

    let key_str = account_key
        .as_ref()
        .filter(|k| !k.is_empty())
        .ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(
                "presign requires account_key or an existing sas_token",
            )
        })?;

    let key_bytes = B64
        .decode(key_str)
        .map_err(|e| PyRuntimeError::new_err(format!("invalid account_key base64: {e}")))?;

    let method_str = method.as_deref().unwrap_or("GET");
    let expires_secs = expires.unwrap_or(3600);

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let start_secs = now.as_secs();
    let expiry_secs = start_secs + expires_secs;

    let start_time = format_iso8601(start_secs);
    let expiry_time = format_iso8601(expiry_secs);

    // Map HTTP method to SAS permission
    let permissions = match method_str.to_uppercase().as_str() {
        "PUT" => "cw",
        "DELETE" => "d",
        _ => "r",
    };

    let canonicalized_resource = format!("/blob/{}/{}/{}", account_name, container, blob_path);

    // Service SAS string-to-sign (v2023-11-03)
    let string_to_sign = format!(
        "{}\n{}\n{}\n{}\n\n\n\n{}\nb\n\n\n\n\n\n\n",
        permissions, start_time, expiry_time, canonicalized_resource, API_VERSION
    );

    let mut mac = HmacSha256::new_from_slice(&key_bytes)
        .map_err(|e| PyRuntimeError::new_err(format!("HMAC key error: {e}")))?;
    mac.update(string_to_sign.as_bytes());
    let signature = B64.encode(mac.finalize().into_bytes());

    let sig_encoded = urlencoding::encode(&signature);
    let st_encoded = urlencoding::encode(&start_time);
    let se_encoded = urlencoding::encode(&expiry_time);

    Ok(format!(
        "{}?sv={}&sr=b&sp={}&st={}&se={}&sig={}",
        blob_url, API_VERSION, permissions, st_encoded, se_encoded, sig_encoded
    ))
}

/// Format a UNIX timestamp as ISO 8601 UTC string (YYYY-MM-DDTHH:MM:SSZ).
fn format_iso8601(secs: u64) -> String {
    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;
    let (year, month, day) = days_to_ymd(days as i64);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, month, day, hours, minutes, seconds
    )
}
