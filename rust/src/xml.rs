//! Shared XML parsing helpers for backends using XML APIs (S3, Azure).

/**
 * Copyright (C) 2026 Axis Communications AB, Lund, Sweden
 *
 * Use of this source code is governed by an MIT-style
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

/// Strip XML namespace prefix, returning the local element name.
///
/// E.g. `"{http://s3.amazonaws.com/doc/2006-03-01/}Key"` → `"Key"`.
pub fn local_name(full: &[u8]) -> &str {
    let s = std::str::from_utf8(full).unwrap_or("");
    if let Some(pos) = s.rfind('}') {
        &s[pos + 1..]
    } else {
        s
    }
}
