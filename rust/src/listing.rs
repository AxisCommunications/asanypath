//! Shared one-level listing builder for prefix-delimited object stores.

/**
 * Copyright (C) 2026 Axis Communications AB, Lund, Sweden
 *
 * Use of this source code is governed by an MIT-style
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
use std::collections::HashMap;

/// `(uri, is_dir, size, modified)` for one direct child; `modified` is the
/// backend's raw timestamp text, parsed on the Python side.
pub type ListEntry = (String, bool, Option<u64>, Option<String>);

/// Collects the direct children of `prefix`, deduplicated by name.
pub struct Listing {
    base: String,
    current_key: String,
    search_prefix: String,
    index: HashMap<String, usize>,
    entries: Vec<ListEntry>,
}

impl Listing {
    /// `base` is the URI root such as `s3://bucket`.
    pub fn new(base: String, prefix: &str) -> Self {
        let current_key = prefix.trim_matches('/').to_string();
        let search_prefix = if current_key.is_empty() {
            String::new()
        } else {
            format!("{current_key}/")
        };
        Self {
            base,
            current_key,
            search_prefix,
            index: HashMap::new(),
            entries: Vec::new(),
        }
    }

    pub fn search_prefix(&self) -> &str {
        &self.search_prefix
    }

    /// Add a delimiter-rolled-up common prefix (a directory).
    pub fn add_prefix(&mut self, prefix: &str) {
        self.insert(prefix.trim_end_matches('/'), true, None, None);
    }

    /// Add an object key; keys below a deeper `/` collapse to their directory.
    pub fn add_object(&mut self, key: &str, size: Option<u64>, modified: Option<String>) {
        let relative = key
            .strip_prefix(self.search_prefix.as_str())
            .unwrap_or(key)
            .trim_start_matches('/');
        match relative.split_once('/') {
            Some((first, _)) => {
                let name = format!("{}{first}", self.search_prefix);
                self.insert(&name, true, None, None);
            }
            None => self.insert(key.trim_end_matches('/'), false, size, modified),
        }
    }

    pub fn finish(self) -> Vec<ListEntry> {
        self.entries
    }

    fn insert(&mut self, name: &str, is_dir: bool, size: Option<u64>, modified: Option<String>) {
        if name.is_empty() || name == self.current_key {
            return;
        }
        if let Some(&position) = self.index.get(name) {
            // A name that is both an object and a prefix lists as a directory.
            if is_dir {
                self.entries[position] = (self.entries[position].0.clone(), true, None, None);
            }
            return;
        }
        self.index.insert(name.to_string(), self.entries.len());
        self.entries
            .push((format!("{}/{name}", self.base), is_dir, size, modified));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_children_and_skips_self() {
        let mut listing = Listing::new("s3://b".into(), "/root/");
        listing.add_object("root/", Some(0), None);
        listing.add_object(
            "root/a.txt",
            Some(3),
            Some("2024-01-02T03:04:05.000Z".into()),
        );
        listing.add_prefix("root/sub/");
        listing.add_object("root/marker/", Some(0), None);
        listing.add_object("root/deep/x/y", Some(1), None);
        assert_eq!(
            listing.finish(),
            vec![
                (
                    "s3://b/root/a.txt".into(),
                    false,
                    Some(3),
                    Some("2024-01-02T03:04:05.000Z".into())
                ),
                ("s3://b/root/sub".into(), true, None, None),
                ("s3://b/root/marker".into(), true, None, None),
                ("s3://b/root/deep".into(), true, None, None),
            ]
        );
    }

    #[test]
    fn prefix_wins_over_same_named_object() {
        let mut listing = Listing::new("az://c".into(), "");
        listing.add_object("a", Some(1), None);
        listing.add_prefix("a/");
        listing.add_prefix("a/");
        assert_eq!(
            listing.finish(),
            vec![("az://c/a".into(), true, None, None)]
        );
    }
}
