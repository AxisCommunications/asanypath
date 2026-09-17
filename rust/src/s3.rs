//! S3-specific operations: SigV4 signing, XML listing, and Python-exposed functions.

/**
 * Copyright (C) 2026 Axis Communications AB, Lund, Sweden
 *
 * Use of this source code is governed by an MIT-style
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

use std::collections::HashMap;
use std::time::SystemTime;

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    SignableBody, SignableRequest, SigningParams, SigningSettings, sign,
};
use aws_sigv4::sign::v4;
use bytes::Bytes;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use quick_xml::Reader;
use quick_xml::{XmlVersion, events::Event};
use serde::{Deserialize, Serialize};

use crate::http::{do_request, extract_host, status_to_pyerr};
use crate::xml::local_name;
use futures::future::join_all;

// ---------------------------------------------------------------------------
// S3 credentials
// ---------------------------------------------------------------------------

pub(crate) struct S3Creds {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
}

// ---------------------------------------------------------------------------
// SigV4 signing
// ---------------------------------------------------------------------------

fn sign_request(
    creds: &S3Creds,
    method: &str,
    path: &str,
    query: &[(String, String)],
    headers: &mut Vec<(String, String)>,
    body: SignableBody<'_>,
) -> String {
    let query_string: String = query
        .iter()
        .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
        .collect::<Vec<_>>()
        .join("&");

    let url = if query_string.is_empty() {
        format!("{}/{}{}", creds.endpoint, creds.bucket, path)
    } else {
        format!(
            "{}/{}{}?{}",
            creds.endpoint, creds.bucket, path, query_string
        )
    };

    let credentials = Credentials::new(
        &creds.access_key,
        &creds.secret_key,
        creds.session_token.clone(),
        None,
        "asanypath",
    );

    let identity = credentials.into();
    let settings = SigningSettings::default();
    let signing_params = v4::SigningParams::builder()
        .identity(&identity)
        .region(&creds.region)
        .name("s3")
        .time(SystemTime::now())
        .settings(settings)
        .build()
        .expect("signing params");

    let signing_params = SigningParams::V4(signing_params);

    let mut builder = http::Request::builder().method(method).uri(&url);
    for (k, v) in headers.iter() {
        builder = builder.header(k.as_str(), v.as_str());
    }
    let http_req = builder.body("").unwrap();

    let signable = SignableRequest::new(
        http_req.method().as_str(),
        http_req.uri().to_string(),
        http_req
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str(), v.to_str().unwrap_or(""))),
        body,
    )
    .expect("signable request");

    let (signing_instructions, _signature) = sign(signable, &signing_params).unwrap().into_parts();

    let mut apply_req = http::Request::builder().method(method).uri(&url);
    for (k, v) in headers.iter() {
        apply_req = apply_req.header(k.as_str(), v.as_str());
    }
    let mut apply_http = apply_req.body("").unwrap();
    signing_instructions.apply_to_request_http1x(&mut apply_http);

    headers.clear();
    for (k, v) in apply_http.headers().iter() {
        headers.push((k.to_string(), v.to_str().unwrap_or("").to_string()));
    }

    url
}

// ---------------------------------------------------------------------------
// S3-specific signed request (wraps shared do_request + SigV4)
// ---------------------------------------------------------------------------

async fn do_s3_request(
    creds: &S3Creds,
    method: &str,
    path: &str,
    query: &[(String, String)],
    body_bytes: Option<Bytes>,
    use_h2: bool,
    extra_headers: &[(String, String)],
) -> Result<(reqwest::StatusCode, Bytes, HashMap<String, String>), String> {
    let signable_body = SignableBody::UnsignedPayload;

    let mut headers = vec![
        ("host".to_string(), extract_host(&creds.endpoint)),
        (
            "x-amz-content-sha256".to_string(),
            "UNSIGNED-PAYLOAD".to_string(),
        ),
    ];
    if let Some(ref b) = body_bytes {
        headers.push(("content-length".to_string(), b.len().to_string()));
    }
    if let Some(ref token) = creds.session_token {
        headers.push(("x-amz-security-token".to_string(), token.clone()));
    }
    for h in extra_headers {
        headers.push(h.clone());
    }
    let url = sign_request(creds, method, path, query, &mut headers, signable_body);

    do_request(method, &url, &headers, body_bytes, use_h2).await
}

// ---------------------------------------------------------------------------
// XML listing parser
// ---------------------------------------------------------------------------

pub(crate) fn parse_listing_page(
    body: &[u8],
    search_prefix: &str,
    current_key: &str,
    bucket: &str,
    found_all: &mut HashMap<String, ()>,
    uris: &mut Vec<String>,
) -> Option<String> {
    let mut reader = Reader::from_reader(body);
    reader.config_mut().trim_text(true);

    let mut buf = Vec::with_capacity(256);
    let mut in_tag: Option<String> = None;
    let mut text_buf = String::new();
    let mut continuation_token: Option<String> = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let name = e.name();
                let name_ref = name.as_ref();
                let local = local_name(name_ref);
                match local {
                    "Key" | "Prefix" | "NextContinuationToken" => {
                        in_tag = Some(local.to_string());
                        text_buf.clear();
                    }
                    _ => {}
                }
            }
            Ok(Event::Text(e)) => {
                if in_tag.is_some() {
                    if let Ok(t) = e.decode() {
                        text_buf.push_str(&t);
                    }
                }
            }
            Ok(Event::End(_)) => {
                if let Some(tag) = in_tag.take() {
                    let found = text_buf.clone();
                    match tag.as_str() {
                        "NextContinuationToken" => {
                            if !found.is_empty() {
                                continuation_token = Some(found);
                            }
                        }
                        "Prefix" => {
                            if found == search_prefix || found.is_empty() {
                                continue;
                            }
                            let trimmed = found.trim_end_matches('/');
                            add_uri(trimmed, current_key, bucket, found_all, uris);
                        }
                        "Key" => {
                            if !search_prefix.is_empty()
                                && found.trim_end_matches('/')
                                    == search_prefix.trim_end_matches('/')
                            {
                                continue;
                            }
                            let relative =
                                if !search_prefix.is_empty() && found.starts_with(search_prefix) {
                                    &found[search_prefix.len()..]
                                } else {
                                    &found
                                };
                            let relative = relative.trim_start_matches('/');
                            let resolved = if !relative.is_empty() {
                                let first_component =
                                    relative.split('/').next().unwrap_or(relative);
                                format!("{}{}", search_prefix, first_component)
                            } else {
                                found.clone()
                            };
                            let trimmed = resolved.trim_end_matches('/');
                            add_uri(trimmed, current_key, bucket, found_all, uris);
                        }
                        _ => {}
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => break,
            _ => {}
        }
        buf.clear();
    }

    continuation_token
}

fn add_uri(
    path: &str,
    current_key: &str,
    bucket: &str,
    found_all: &mut HashMap<String, ()>,
    uris: &mut Vec<String>,
) {
    if path.is_empty() || path.trim_end_matches('/') == current_key {
        return;
    }
    if found_all.contains_key(path) {
        return;
    }
    found_all.insert(path.to_string(), ());
    uris.push(format!("s3://{}/{}", bucket, path));
}

// ---------------------------------------------------------------------------
// Helpers to build S3Creds from Python keyword args
// ---------------------------------------------------------------------------

macro_rules! s3_creds {
    ($endpoint:expr, $bucket:expr, $region:expr,
     $access_key:expr, $secret_key:expr, $session_token:expr) => {
        S3Creds {
            endpoint: $endpoint,
            bucket: $bucket,
            region: $region,
            access_key: $access_key,
            secret_key: $secret_key,
            session_token: $session_token,
        }
    };
}

// ---------------------------------------------------------------------------
// CloudBackend trait implementation
// ---------------------------------------------------------------------------

use crate::backend::CloudBackend;

impl CloudBackend for S3Creds {
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
            let s3_path = format!("/{}", path);
            let (status, body, _headers) =
                do_s3_request(self, method, &s3_path, &[], body, use_h2, &extras).await?;
            Ok((status, body))
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
            let s3_path = format!("/{}", path);
            do_s3_request(self, method, &s3_path, &[], body, use_h2, &[]).await
        }
    }

    fn error_path(&self, path: &str) -> String {
        format!("s3://{}/{}", self.bucket, path)
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
            let headers = crate::backend::user_headers(&options)?;
            let query = options.query.into_iter().collect::<Vec<_>>();
            let s3_path = format!("/{path}");
            let (status, body, _) = do_s3_request(
                self,
                "PUT",
                &s3_path,
                &query,
                Some(data),
                use_h2,
                &headers,
            )
            .await
            .map_err(PyRuntimeError::new_err)?;
            if !status.is_success() {
                return Err(status_to_pyerr(status, &self.error_path(path), &body));
            }
            Ok(())
        }
    }

    // Override: S3 server-side copy via PUT + signed x-amz-copy-source header.
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
            let encoded_src: String = src
                .split('/')
                .map(|s| urlencoding::encode(s).into_owned())
                .collect::<Vec<_>>()
                .join("/");
            let copy_source = format!("/{}/{}", self.bucket, encoded_src);
            let dst_path = format!("/{}", dst);
            let mut headers = vec![
                ("host".to_string(), extract_host(&self.endpoint)),
                (
                    "x-amz-content-sha256".to_string(),
                    "UNSIGNED-PAYLOAD".to_string(),
                ),
                ("x-amz-copy-source".to_string(), copy_source),
            ];
            if let Some(ref token) = self.session_token {
                headers.push(("x-amz-security-token".to_string(), token.clone()));
            }
            if crate::backend::user_headers(&options)?
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case("x-amz-copy-source"))
            {
                return Err(PyValueError::new_err(
                    "destination_backend_options cannot override x-amz-copy-source",
                ));
            }
            crate::backend::merge_user_headers(&mut headers, &options)?;
            let url = sign_request(
                self,
                "PUT",
                &dst_path,
                &options.query.into_iter().collect::<Vec<_>>(),
                &mut headers,
                SignableBody::UnsignedPayload,
            );
            let (status, body, _headers) = do_request("PUT", &url, &headers, None, use_h2)
                .await
                .map_err(|e| PyRuntimeError::new_err(e))?;
            if !status.is_success() {
                return Err(status_to_pyerr(status, &self.error_path(dst), &body));
            }
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Internal async helpers (used by both single + batch pyfunctions)
// ---------------------------------------------------------------------------

async fn do_s3_list(creds: &S3Creds, prefix: &str, use_h2: bool) -> Result<Vec<String>, PyErr> {
    let current_key = prefix.trim_matches('/').to_string();
    let search_prefix = if current_key.is_empty() {
        String::new()
    } else {
        format!("{}/", current_key)
    };

    let mut found_all = HashMap::new();
    let mut uris = Vec::new();
    let mut continuation_token: Option<String> = None;

    loop {
        let mut query = vec![
            ("list-type".to_string(), "2".to_string()),
            ("prefix".to_string(), search_prefix.clone()),
            ("delimiter".to_string(), "/".to_string()),
        ];
        if let Some(ref token) = continuation_token {
            query.push(("continuation-token".to_string(), token.clone()));
        }

        let (status, body, _headers) = do_s3_request(creds, "GET", "", &query, None, use_h2, &[])
            .await
            .map_err(|e| PyRuntimeError::new_err(e))?;

        if !status.is_success() {
            return Err(status_to_pyerr(
                status,
                &format!("s3://{}/{}", creds.bucket, search_prefix),
                &body,
            ));
        }

        continuation_token = parse_listing_page(
            &body,
            &search_prefix,
            &current_key,
            &creds.bucket,
            &mut found_all,
            &mut uris,
        );

        if continuation_token.is_none() {
            break;
        }
    }

    Ok(uris)
}

/// List all buckets (service-level ListBuckets). Returns bare bucket names.
async fn do_s3_list_buckets(creds: &S3Creds, use_h2: bool) -> Result<Vec<String>, PyErr> {
    let (status, body, _headers) = do_s3_request(creds, "GET", "", &[], None, use_h2, &[])
        .await
        .map_err(|e| PyRuntimeError::new_err(e))?;

    if !status.is_success() {
        return Err(status_to_pyerr(status, "s3://", &body));
    }

    let mut reader = Reader::from_reader(body.as_ref());
    reader.config_mut().trim_text(true);
    let mut buf = Vec::with_capacity(256);
    let mut in_bucket = false;
    let mut in_name = false;
    let mut text_buf = String::new();
    let mut out = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => match local_name(e.name().as_ref()) {
                "Bucket" => in_bucket = true,
                "Name" if in_bucket => {
                    in_name = true;
                    text_buf.clear();
                }
                _ => {}
            },
            Ok(Event::Text(e)) => {
                if in_name {
                    if let Ok(t) = e.decode() {
                        text_buf.push_str(&t);
                    }
                }
            }
            Ok(Event::End(e)) => match local_name(e.name().as_ref()) {
                "Name" if in_name => {
                    in_name = false;
                    if !text_buf.is_empty() {
                        out.push(text_buf.clone());
                    }
                }
                "Bucket" => in_bucket = false,
                _ => {}
            },
            Ok(Event::Eof) => break,
            Err(_) => break,
            _ => {}
        }
        buf.clear();
    }

    Ok(out)
}

async fn do_s3_is_dir(creds: &S3Creds, prefix: &str, use_h2: bool) -> Result<bool, PyErr> {
    let exact_key = prefix.trim_matches('/').to_string();
    let search_prefix = if exact_key.is_empty() {
        String::new()
    } else {
        format!("{}/", exact_key)
    };

    let query = vec![
        ("list-type".to_string(), "2".to_string()),
        ("prefix".to_string(), search_prefix.clone()),
        ("delimiter".to_string(), "/".to_string()),
        ("max-keys".to_string(), "1".to_string()),
    ];

    let (status, body, _headers) = do_s3_request(creds, "GET", "", &query, None, use_h2, &[])
        .await
        .map_err(|e| PyRuntimeError::new_err(e))?;

    if !status.is_success() {
        return Err(status_to_pyerr(
            status,
            &format!("s3://{}/{}", creds.bucket, search_prefix),
            &body,
        ));
    }

    let mut reader = Reader::from_reader(body.as_ref());
    reader.config_mut().trim_text(true);
    let mut buf = Vec::with_capacity(256);
    let mut in_tag: Option<String> = None;
    let mut text_buf = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let name = e.name();
                let local = local_name(name.as_ref());
                if local == "Key" || local == "Prefix" {
                    in_tag = Some(local.to_string());
                    text_buf.clear();
                }
            }
            Ok(Event::Text(e)) => {
                if in_tag.is_some() {
                    if let Ok(t) = e.decode() {
                        text_buf.push_str(&t);
                    }
                }
            }
            Ok(Event::End(_)) => {
                if let Some(tag) = in_tag.take() {
                    let found = &text_buf;
                    if found.is_empty() {
                        continue;
                    }
                    match tag.as_str() {
                        "Prefix" => {
                            if found != &search_prefix && found.starts_with(&search_prefix) {
                                return Ok(true);
                            }
                        }
                        "Key" => {
                            if found.starts_with(&search_prefix)
                                && found.trim_end_matches('/') != exact_key
                            {
                                return Ok(true);
                            }
                        }
                        _ => {}
                    }
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

#[derive(Serialize)]
struct S3AclGrant {
    principal: String,
    permission: String,
}

#[derive(Serialize)]
struct S3Acl {
    owner: Option<String>,
    grants: Vec<S3AclGrant>,
}

#[derive(Deserialize)]
struct S3AclInput {
    owner: String,
    grants: Vec<S3AclGrantInput>,
}

#[derive(Deserialize)]
struct S3AclGrantInput {
    principal: String,
    permission: String,
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn parse_s3_acl(body: &[u8]) -> S3Acl {
    let mut reader = Reader::from_reader(body);
    reader.config_mut().trim_text(true);
    let mut buffer = Vec::with_capacity(256);
    let mut owner = None;
    let mut grants = Vec::new();
    let mut current_grant: Option<(Option<String>, Option<String>)> = None;
    let mut grantee_kind = "canonical-user".to_string();
    let mut in_owner = false;
    let mut in_grantee = false;
    let mut field: Option<String> = None;

    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(element)) => {
                let element_name = element.name();
                let name = local_name(element_name.as_ref());
                match name {
                    "Owner" => in_owner = true,
                    "Grant" => current_grant = Some((None, None)),
                    "Grantee" => {
                        in_grantee = true;
                        grantee_kind = "canonical-user".to_string();
                        for attribute in element.attributes().flatten() {
                            if attribute.key.as_ref().ends_with(b"type") {
                                if let Ok(value) = attribute.decoded_and_normalized_value(
                                    XmlVersion::Implicit1_0,
                                    reader.decoder(),
                                ) {
                                    grantee_kind = value.into_owned().to_ascii_lowercase();
                                }
                            }
                        }
                    }
                    "ID" if in_owner || in_grantee => field = Some("ID".to_string()),
                    "URI" if in_grantee => field = Some("URI".to_string()),
                    "EmailAddress" if in_grantee => field = Some("EmailAddress".to_string()),
                    "Permission" if current_grant.is_some() => field = Some("Permission".to_string()),
                    _ => {}
                }
            }
            Ok(Event::Text(text)) => {
                let Some(field_name) = field.as_deref() else {
                    buffer.clear();
                    continue;
                };
                let Ok(value) = text.decode() else {
                    buffer.clear();
                    continue;
                };
                if in_owner && field_name == "ID" {
                    owner = Some(value.into_owned());
                } else if let Some((principal, permission)) = current_grant.as_mut() {
                    match field_name {
                        "Permission" => *permission = Some(value.into_owned()),
                        "ID" | "URI" | "EmailAddress" if in_grantee => {
                            *principal = Some(format!("{}:{}", grantee_kind, value));
                        }
                        _ => {}
                    }
                }
            }
            Ok(Event::End(element)) => {
                match local_name(element.name().as_ref()) {
                    "Owner" => in_owner = false,
                    "Grantee" => in_grantee = false,
                    "Grant" => {
                        if let Some((Some(principal), Some(permission))) = current_grant.take() {
                            grants.push(S3AclGrant { principal, permission });
                        }
                    }
                    "ID" | "URI" | "EmailAddress" | "Permission" => field = None,
                    _ => {}
                }
            }
            Ok(Event::Eof) | Err(_) => break,
            _ => {}
        }
        buffer.clear();
    }
    S3Acl { owner, grants }
}

async fn do_s3_get_acl(creds: &S3Creds, key: &str) -> Result<String, PyErr> {
    let path = format!("/{key}");
    let query = [("acl".to_string(), String::new())];
    let (status, body, _) = do_s3_request(creds, "GET", &path, &query, None, false, &[])
        .await
        .map_err(PyRuntimeError::new_err)?;
    if !status.is_success() {
        return Err(status_to_pyerr(status, &creds.error_path(key), &body));
    }
    sonic_rs::to_string(&parse_s3_acl(&body))
        .map_err(|error| PyRuntimeError::new_err(format!("could not serialize S3 ACL: {error}")))
}

async fn do_s3_put_acl(creds: &S3Creds, key: &str, acl_json: &str) -> Result<(), PyErr> {
    let acl: S3AclInput = sonic_rs::from_str(acl_json)
        .map_err(|error| PyValueError::new_err(format!("invalid S3 ACL: {error}")))?;
    let mut xml = format!(
        "<AccessControlPolicy xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Owner><ID>{}</ID></Owner><AccessControlList>",
        xml_escape(&acl.owner)
    );
    for grant in acl.grants {
        let (kind, identifier) = grant.principal.split_once(':').ok_or_else(|| {
            PyValueError::new_err("S3 ACL grant principal must have a kind prefix")
        })?;
        let (type_name, element_name) = match kind {
            "canonical-user" | "canonicaluser" => ("CanonicalUser", "ID"),
            "email" | "emailaddress" => ("AmazonCustomerByEmail", "EmailAddress"),
            "uri" | "group" => ("Group", "URI"),
            _ => return Err(PyValueError::new_err(format!("unsupported S3 ACL principal: {kind}"))),
        };
        xml.push_str(&format!(
            "<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"{type_name}\"><{element_name}>{}</{element_name}></Grantee><Permission>{}</Permission></Grant>",
            xml_escape(identifier),
            xml_escape(&grant.permission),
        ));
    }
    xml.push_str("</AccessControlList></AccessControlPolicy>");
    let path = format!("/{key}");
    let query = [("acl".to_string(), String::new())];
    let headers = [("content-type".to_string(), "application/xml".to_string())];
    let (status, body, _) = do_s3_request(
        creds,
        "PUT",
        &path,
        &query,
        Some(Bytes::from(xml)),
        false,
        &headers,
    )
    .await
    .map_err(PyRuntimeError::new_err)?;
    if !status.is_success() {
        return Err(status_to_pyerr(status, &creds.error_path(key), &body));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Python-exposed single-op async functions
// ---------------------------------------------------------------------------

/// Check if a prefix has any children (is a "directory").
#[pyfunction]
pub fn s3_is_dir<'py>(
    py: Python<'py>,
    endpoint: String,
    bucket: String,
    prefix: String,
    region: String,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = s3_creds!(
            endpoint,
            bucket,
            region,
            access_key,
            secret_key,
            session_token
        );
        do_s3_is_dir(&creds, &prefix, false).await
    })
}

/// Fetch an object's S3 ACL as JSON with its owner and grant list.
#[pyfunction]
pub fn s3_get_acl<'py>(
    py: Python<'py>,
    endpoint: String,
    bucket: String,
    key: String,
    region: String,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = s3_creds!(
            endpoint,
            bucket,
            region,
            access_key,
            secret_key,
            session_token
        );
        do_s3_get_acl(&creds, &key).await
    })
}

/// Replace an object's complete S3 ACL from a JSON policy document.
#[pyfunction]
pub fn s3_put_acl<'py>(
    py: Python<'py>,
    endpoint: String,
    bucket: String,
    key: String,
    acl_json: String,
    region: String,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = s3_creds!(
            endpoint,
            bucket,
            region,
            access_key,
            secret_key,
            session_token
        );
        do_s3_put_acl(&creds, &key, &acl_json).await
    })
}

// ---------------------------------------------------------------------------
// Python-exposed batch async functions
// ---------------------------------------------------------------------------

/// Get multiple objects concurrently. Returns list of byte vectors.
#[pyfunction]
pub fn s3_get_batch<'py>(
    py: Python<'py>,
    endpoint: String,
    bucket: String,
    keys: Vec<String>,
    region: String,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = s3_creds!(
            endpoint,
            bucket,
            region,
            access_key,
            secret_key,
            session_token
        );
        batch_collect!(creds, keys, h2, get)
    })
}

/// HEAD multiple objects concurrently. Returns list of header dicts.
#[pyfunction]
pub fn s3_head_batch<'py>(
    py: Python<'py>,
    endpoint: String,
    bucket: String,
    keys: Vec<String>,
    region: String,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = s3_creds!(
            endpoint,
            bucket,
            region,
            access_key,
            secret_key,
            session_token
        );
        batch_collect!(creds, keys, h2, head)
    })
}

/// PUT multiple objects concurrently.
#[pyfunction]
pub fn s3_put_batch<'py>(
    py: Python<'py>,
    endpoint: String,
    bucket: String,
    items: Vec<(String, Vec<u8>, String)>,
    region: String,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = s3_creds!(
            endpoint,
            bucket,
            region,
            access_key,
            secret_key,
            session_token
        );
        batch_put!(creds, items, h2)
    })
}

/// Copy multiple objects concurrently (server-side). Pairs are `(src_key, dst_key)`.
#[pyfunction]
pub fn s3_copy_batch<'py>(
    py: Python<'py>,
    endpoint: String,
    bucket: String,
    pairs: Vec<(String, String)>,
    region: String,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
    options_json: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = s3_creds!(
            endpoint,
            bucket,
            region,
            access_key,
            secret_key,
            session_token
        );
        batch_copy!(creds, pairs, options_json.unwrap_or_else(|| "{}".to_string()), h2)
    })
}

/// DELETE multiple objects concurrently.
#[pyfunction]
pub fn s3_delete_batch<'py>(
    py: Python<'py>,
    endpoint: String,
    bucket: String,
    keys: Vec<String>,
    region: String,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = s3_creds!(
            endpoint,
            bucket,
            region,
            access_key,
            secret_key,
            session_token
        );
        batch_fire!(creds, keys, h2, delete)
    })
}

/// Check existence of multiple objects concurrently. Returns list of bools.
#[pyfunction]
pub fn s3_exists_batch<'py>(
    py: Python<'py>,
    endpoint: String,
    bucket: String,
    keys: Vec<String>,
    region: String,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = s3_creds!(
            endpoint,
            bucket,
            region,
            access_key,
            secret_key,
            session_token
        );
        batch_collect!(creds, keys, h2, exists)
    })
}

/// Check is_dir for multiple prefixes concurrently. Returns list of bools.
#[pyfunction]
pub fn s3_is_dir_batch<'py>(
    py: Python<'py>,
    endpoint: String,
    bucket: String,
    prefixes: Vec<String>,
    region: String,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = s3_creds!(
            endpoint,
            bucket,
            region,
            access_key,
            secret_key,
            session_token
        );
        let futs: Vec<_> = prefixes
            .iter()
            .map(|p| do_s3_is_dir(&creds, p, h2))
            .collect();
        let results = join_all(futs).await;
        let mut out = Vec::with_capacity(results.len());
        for r in results {
            out.push(r?);
        }
        Ok(out)
    })
}

/// List multiple prefixes concurrently. Returns list of URI lists.
#[pyfunction]
pub fn s3_list_batch<'py>(
    py: Python<'py>,
    endpoint: String,
    bucket: String,
    prefixes: Vec<String>,
    region: String,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = s3_creds!(
            endpoint,
            bucket,
            region,
            access_key,
            secret_key,
            session_token
        );
        let futs: Vec<_> = prefixes.iter().map(|p| do_s3_list(&creds, p, h2)).collect();
        let results = join_all(futs).await;
        let mut out = Vec::with_capacity(results.len());
        for r in results {
            out.push(r?);
        }
        Ok(out)
    })
}

/// List all buckets (service-level). Returns bare bucket names.
#[pyfunction]
pub fn s3_list_buckets<'py>(
    py: Python<'py>,
    endpoint: String,
    region: String,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
    use_h2: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    let h2 = use_h2.unwrap_or(false);
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let creds = s3_creds!(
            endpoint,
            String::new(),
            region,
            access_key,
            secret_key,
            session_token
        );
        do_s3_list_buckets(&creds, h2).await
    })
}

#[cfg(test)]
mod tests {
    use super::parse_s3_acl;

    #[test]
    fn parses_owner_and_grants() {
        let acl = parse_s3_acl(
            br#"<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Owner><ID>owner-id</ID></Owner>
                <AccessControlList>
                    <Grant><Grantee xsi:type="CanonicalUser" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><ID>reader-id</ID></Grantee><Permission>READ</Permission></Grant>
                    <Grant><Grantee xsi:type="Group" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee><Permission>READ_ACP</Permission></Grant>
                </AccessControlList>
            </AccessControlPolicy>"#,
        );

        assert_eq!(acl.owner.as_deref(), Some("owner-id"));
        assert_eq!(acl.grants.len(), 2);
        assert_eq!(acl.grants[0].principal, "canonicaluser:reader-id");
        assert_eq!(acl.grants[0].permission, "READ");
        assert_eq!(acl.grants[1].principal, "group:http://acs.amazonaws.com/groups/global/AllUsers");
        assert_eq!(acl.grants[1].permission, "READ_ACP");
    }
}

// ---------------------------------------------------------------------------
// Presigned URL generation
// ---------------------------------------------------------------------------

/// Generate a presigned URL for an S3 object using SigV4 query-string signing.
#[pyfunction]
pub fn s3_presign(
    endpoint: String,
    bucket: String,
    key: String,
    region: String,
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
    method: Option<String>,
    expires: Option<u64>,
) -> PyResult<String> {
    use aws_sigv4::http_request::SignatureLocation;
    use std::time::Duration;

    let method_str = method.as_deref().unwrap_or("GET");
    let expires_secs = expires.unwrap_or(3600);

    let path = if key.is_empty() {
        format!("/{}", bucket)
    } else {
        format!("/{}/{}", bucket, key)
    };
    let url = format!("{}{}", endpoint, path);

    let credentials = Credentials::new(
        &access_key,
        &secret_key,
        session_token.clone(),
        None,
        "asanypath",
    );
    let identity = credentials.into();

    let mut settings = SigningSettings::default();
    settings.signature_location = SignatureLocation::QueryParams;
    settings.expires_in = Some(Duration::from_secs(expires_secs));

    let signing_params = v4::SigningParams::builder()
        .identity(&identity)
        .region(&region)
        .name("s3")
        .time(SystemTime::now())
        .settings(settings)
        .build()
        .map_err(|e| PyRuntimeError::new_err(format!("signing params: {e}")))?;

    let signing_params = SigningParams::V4(signing_params);

    let host = extract_host(&endpoint);
    let http_req = http::Request::builder()
        .method(method_str)
        .uri(&url)
        .header("host", &host)
        .body("")
        .map_err(|e| PyRuntimeError::new_err(format!("build request: {e}")))?;

    let signable = SignableRequest::new(
        http_req.method().as_str(),
        http_req.uri().to_string(),
        http_req
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str(), v.to_str().unwrap_or(""))),
        SignableBody::UnsignedPayload,
    )
    .map_err(|e| PyRuntimeError::new_err(format!("signable request: {e}")))?;

    let (signing_instructions, _signature) = sign(signable, &signing_params)
        .map_err(|e| PyRuntimeError::new_err(format!("signing: {e}")))?
        .into_parts();

    // Apply signing to get the final URL with query params
    let mut apply_req = http::Request::builder()
        .method(method_str)
        .uri(&url)
        .header("host", &host)
        .body("")
        .unwrap();
    signing_instructions.apply_to_request_http1x(&mut apply_req);

    Ok(apply_req.uri().to_string())
}
