//! Native cloud acceleration for asanypath.
//!
//! Shared infrastructure lives in `http` (HTTP client) and `xml` (XML parsing).
//! Each cloud backend has its own module exporting `#[pyfunction]`s.

/**
 * Copyright (C) 2026 Axis Communications AB, Lund, Sweden
 *
 * Use of this source code is governed by an MIT-style
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
use pyo3::prelude::*;

#[macro_use]
pub mod backend;
pub mod http;
pub mod xml;

pub mod artifactory;
pub mod azure;
pub mod gcs;
pub mod s3;

// ---------------------------------------------------------------------------
// Python module
// ---------------------------------------------------------------------------

#[pymodule]
fn asanypath_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // S3
    m.add_function(wrap_pyfunction!(s3::s3_is_dir, m)?)?;
    m.add_function(wrap_pyfunction!(s3::s3_get_acl, m)?)?;
    m.add_function(wrap_pyfunction!(s3::s3_put_acl, m)?)?;
    m.add_function(wrap_pyfunction!(s3::s3_presign, m)?)?;
    // S3 batch
    m.add_function(wrap_pyfunction!(s3::s3_list_batch, m)?)?;
    m.add_function(wrap_pyfunction!(s3::s3_get_batch, m)?)?;
    m.add_function(wrap_pyfunction!(s3::s3_head_batch, m)?)?;
    m.add_function(wrap_pyfunction!(s3::s3_put_batch, m)?)?;
    m.add_function(wrap_pyfunction!(s3::s3_delete_batch, m)?)?;
    m.add_function(wrap_pyfunction!(s3::s3_exists_batch, m)?)?;
    m.add_function(wrap_pyfunction!(s3::s3_is_dir_batch, m)?)?;
    m.add_function(wrap_pyfunction!(s3::s3_copy_batch, m)?)?;
    m.add_function(wrap_pyfunction!(s3::s3_list_buckets, m)?)?;
    // GCS
    m.add_function(wrap_pyfunction!(gcs::gcs_head, m)?)?;
    m.add_function(wrap_pyfunction!(gcs::gcs_get_acl, m)?)?;
    m.add_function(wrap_pyfunction!(gcs::gcs_put_acl, m)?)?;
    m.add_function(wrap_pyfunction!(gcs::gcs_is_dir, m)?)?;
    m.add_function(wrap_pyfunction!(gcs::gcs_presign, m)?)?;
    // GCS batch
    m.add_function(wrap_pyfunction!(gcs::gcs_list_batch, m)?)?;
    m.add_function(wrap_pyfunction!(gcs::gcs_get_batch, m)?)?;
    m.add_function(wrap_pyfunction!(gcs::gcs_head_batch, m)?)?;
    m.add_function(wrap_pyfunction!(gcs::gcs_put_batch, m)?)?;
    m.add_function(wrap_pyfunction!(gcs::gcs_delete_batch, m)?)?;
    m.add_function(wrap_pyfunction!(gcs::gcs_exists_batch, m)?)?;
    m.add_function(wrap_pyfunction!(gcs::gcs_is_dir_batch, m)?)?;
    m.add_function(wrap_pyfunction!(gcs::gcs_copy_batch, m)?)?;
    m.add_function(wrap_pyfunction!(gcs::gcs_list_buckets, m)?)?;
    // Azure
    m.add_function(wrap_pyfunction!(azure::az_is_dir, m)?)?;
    m.add_function(wrap_pyfunction!(azure::az_get_container_acl, m)?)?;
    m.add_function(wrap_pyfunction!(azure::az_put_container_acl, m)?)?;
    m.add_function(wrap_pyfunction!(azure::az_presign, m)?)?;
    // Azure batch
    m.add_function(wrap_pyfunction!(azure::az_list_batch, m)?)?;
    m.add_function(wrap_pyfunction!(azure::az_get_batch, m)?)?;
    m.add_function(wrap_pyfunction!(azure::az_head_batch, m)?)?;
    m.add_function(wrap_pyfunction!(azure::az_put_batch, m)?)?;
    m.add_function(wrap_pyfunction!(azure::az_delete_batch, m)?)?;
    m.add_function(wrap_pyfunction!(azure::az_exists_batch, m)?)?;
    m.add_function(wrap_pyfunction!(azure::az_is_dir_batch, m)?)?;
    m.add_function(wrap_pyfunction!(azure::az_copy_batch, m)?)?;
    m.add_function(wrap_pyfunction!(azure::az_list_containers, m)?)?;
    // Artifactory
    m.add_function(wrap_pyfunction!(artifactory::art_storage_info, m)?)?;
    m.add_function(wrap_pyfunction!(artifactory::art_get_permission_target, m)?)?;
    m.add_function(wrap_pyfunction!(artifactory::art_put_permission_target, m)?)?;
    // Artifactory batch
    m.add_function(wrap_pyfunction!(artifactory::art_get_batch, m)?)?;
    m.add_function(wrap_pyfunction!(artifactory::art_put_batch, m)?)?;
    m.add_function(wrap_pyfunction!(artifactory::art_delete_batch, m)?)?;
    m.add_function(wrap_pyfunction!(artifactory::art_exists_batch, m)?)?;
    m.add_function(wrap_pyfunction!(artifactory::art_list_batch, m)?)?;
    m.add_function(wrap_pyfunction!(artifactory::art_copy_batch, m)?)?;
    m.add_function(wrap_pyfunction!(artifactory::art_list_repos, m)?)?;
    // Generic HTTP (unsigned)
    m.add_function(wrap_pyfunction!(http::http_get, m)?)?;
    m.add_function(wrap_pyfunction!(http::http_head, m)?)?;
    m.add_function(wrap_pyfunction!(http::http_put, m)?)?;
    m.add_function(wrap_pyfunction!(http::http_post, m)?)?;
    m.add_function(wrap_pyfunction!(http::http_delete, m)?)?;
    m.add_function(wrap_pyfunction!(http::http_patch, m)?)?;
    m.add_function(wrap_pyfunction!(http::http_options, m)?)?;
    m.add_function(wrap_pyfunction!(http::http_exists, m)?)?;
    m.add_function(wrap_pyfunction!(http::http_request, m)?)?;
    m.add_function(wrap_pyfunction!(http::http_scrape_links, m)?)?;
    // Unified
    m.add_function(wrap_pyfunction!(backend::range_read, m)?)?;
    Ok(())
}
