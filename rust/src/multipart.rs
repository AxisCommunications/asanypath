//! `multipart/mixed` batch requests (GCS JSON batch, Azure Blob Batch).

/**
 * Copyright (C) 2026 Axis Communications AB, Lund, Sweden
 *
 * Use of this source code is governed by an MIT-style
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
use pyo3::PyErr;
use pyo3::exceptions::PyRuntimeError;
use reqwest::StatusCode;

use crate::http::status_to_pyerr;

/// A random boundary; it cannot occur in a part by chance.
pub fn boundary() -> String {
    let mut bytes = [0u8; 16];
    ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut bytes)
        .expect("system random source");
    let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!("batch_{hex}")
}

/// Path and query of `url`, as a sub-request line names its target.
pub fn path_of(url: &str) -> &str {
    let rest = url.split_once("://").map_or(url, |(_, rest)| rest);
    rest.find('/').map_or("/", |start| &rest[start..])
}

/// Batch body for `requests` (request line and headers of each sub-request,
/// without a body); the i-th part is tagged `Content-ID: i`. The CRLF before
/// a boundary belongs to the boundary, so each request ends with two more.
pub fn body(boundary: &str, requests: &[String]) -> String {
    let mut out = String::new();
    for (i, request) in requests.iter().enumerate() {
        out.push_str(&format!(
            "--{boundary}\r\nContent-Type: application/http\r\n\
             Content-Transfer-Encoding: binary\r\nContent-ID: {i}\r\n\r\n{request}\r\n\r\n\r\n"
        ));
    }
    out.push_str(&format!("--{boundary}--\r\n"));
    out
}

/// `(content-id, status)` of each part of a batch response. The content id is
/// the number the request was tagged with (`0`, `<response-0>`, ...).
pub fn statuses(body: &[u8]) -> Vec<(Option<usize>, u16)> {
    let text = String::from_utf8_lossy(body);
    let Some(delimiter) = text
        .lines()
        .map(str::trim_end)
        .find(|l| l.starts_with("--"))
    else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for part in text.split(delimiter).skip(1) {
        let mut id = None;
        let mut status = None;
        for line in part.lines().map(str::trim) {
            if let Some((name, value)) = line.split_once(':') {
                if name.eq_ignore_ascii_case("content-id") && status.is_none() {
                    let digits: String = value
                        .trim_end_matches('>')
                        .chars()
                        .rev()
                        .take_while(char::is_ascii_digit)
                        .collect();
                    id = digits.chars().rev().collect::<String>().parse().ok();
                }
            }
            if line.starts_with("HTTP/") {
                status = line.split_whitespace().nth(1).and_then(|s| s.parse().ok());
                break;
            }
        }
        if let Some(status) = status {
            out.push((id, status));
        }
    }
    out
}

/// Index and status (`None`: no response) of the first key whose sub-request
/// failed; a key that was already gone (404) counts as deleted.
pub fn first_failed(
    statuses: Vec<(Option<usize>, u16)>,
    count: usize,
) -> Option<(usize, Option<u16>)> {
    let mut by_key = vec![None; count];
    for (position, (id, status)) in statuses.into_iter().enumerate() {
        if let Some(slot) = by_key.get_mut(id.unwrap_or(position)) {
            *slot = Some(status);
        }
    }
    by_key
        .into_iter()
        .enumerate()
        .find_map(|(i, status)| match status {
            Some(s) if (200..300).contains(&s) || s == 404 => None,
            other => Some((i, other)),
        })
}

/// Raise for the first key whose sub-request failed.
pub fn check_deletes(
    statuses: Vec<(Option<usize>, u16)>,
    keys: &[String],
    error_path: impl Fn(&str) -> String,
) -> Result<(), PyErr> {
    match first_failed(statuses, keys.len()) {
        None => Ok(()),
        Some((i, Some(s))) => {
            let status = StatusCode::from_u16(s).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            Err(status_to_pyerr(status, &error_path(&keys[i]), &[]))
        }
        Some((i, None)) => Err(PyRuntimeError::new_err(format!(
            "no batch response for '{}'",
            error_path(&keys[i])
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_tags_each_request() {
        let body = body(
            "B",
            &["DELETE /a HTTP/1.1".into(), "DELETE /b HTTP/1.1".into()],
        );
        assert_eq!(
            body,
            "--B\r\nContent-Type: application/http\r\nContent-Transfer-Encoding: binary\r\n\
             Content-ID: 0\r\n\r\nDELETE /a HTTP/1.1\r\n\r\n\r\n\
             --B\r\nContent-Type: application/http\r\nContent-Transfer-Encoding: binary\r\n\
             Content-ID: 1\r\n\r\nDELETE /b HTTP/1.1\r\n\r\n\r\n--B--\r\n"
        );
        assert!(boundary().starts_with("batch_") && boundary() != boundary());
    }

    #[test]
    fn parses_gcs_and_azure_responses() {
        let gcs = b"--r1\r\nContent-Type: application/http\r\nContent-ID: <response-1>\r\n\r\n\
HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\n\r\n{}\r\n\
--r1\r\nContent-Type: application/http\r\nContent-ID: <response-0>\r\n\r\n\
HTTP/1.1 204 No Content\r\n\r\n\r\n--r1--\r\n";
        assert_eq!(statuses(gcs), vec![(Some(1), 404), (Some(0), 204)]);
        let azure =
            b"--batchresponse_x \r\nContent-Type: application/http \r\nContent-ID: 0 \r\n\r\n\
HTTP/1.1 202 Accepted \r\nx-ms-version: 2018-11-09 \r\n\r\n\
--batchresponse_x \r\nContent-Type: application/http \r\nContent-ID: 1 \r\n\r\n\
HTTP/1.1 403 This request is not authorized \r\n\r\n--batchresponse_x-- \r\n";
        assert_eq!(statuses(azure), vec![(Some(0), 202), (Some(1), 403)]);
    }

    #[test]
    fn first_failed_maps_statuses_to_keys() {
        assert_eq!(first_failed(vec![(Some(1), 404), (Some(0), 204)], 2), None);
        assert_eq!(
            first_failed(vec![(Some(0), 204), (Some(1), 403)], 2),
            Some((1, Some(403)))
        );
        assert_eq!(
            first_failed(vec![(None, 202), (None, 500)], 2),
            Some((1, Some(500)))
        );
        assert_eq!(first_failed(vec![(Some(0), 204)], 2), Some((1, None)));
    }
}
