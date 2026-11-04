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

/// Receives the entries of one listing page.
pub trait ListSink {
    /// A delimiter-rolled-up common prefix (a directory).
    fn add_prefix(&mut self, prefix: &str);
    /// An object key.
    fn add_object(&mut self, key: &str, size: Option<u64>, modified: Option<String>);
}

/// Every object key of a listing made without a delimiter.
#[derive(Default)]
pub struct Keys(pub Vec<String>);

impl ListSink for Keys {
    fn add_prefix(&mut self, _prefix: &str) {}

    fn add_object(&mut self, key: &str, _size: Option<u64>, _modified: Option<String>) {
        self.0.push(key.to_string());
    }
}

/// The key prefix that selects everything below `prefix` (`""` for the root).
pub fn search_prefix(prefix: &str) -> String {
    let key = prefix.trim_matches('/');
    if key.is_empty() {
        String::new()
    } else {
        format!("{key}/")
    }
}

/// Whether `key` lies under `search_prefix` and can be sent as a request path
/// as is: URL parsing would resolve `.`/`..` segments, treat `\` as `/`,
/// decode `%`, split at `?`/`#` and drop tab, CR and LF.
pub fn plain_key(key: &str, search_prefix: &str) -> bool {
    key.strip_prefix(search_prefix).is_some_and(|rel| {
        !rel.contains(['\\', '%', '?', '#', '\t', '\r', '\n'])
            && !rel
                .split('/')
                .any(|segment| segment == "." || segment == "..")
    })
}

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
        Self {
            base,
            current_key: prefix.trim_matches('/').to_string(),
            search_prefix: search_prefix(prefix),
            index: HashMap::new(),
            entries: Vec::new(),
        }
    }

    pub fn search_prefix(&self) -> &str {
        &self.search_prefix
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

impl ListSink for Listing {
    fn add_prefix(&mut self, prefix: &str) {
        self.insert(prefix.trim_end_matches('/'), true, None, None);
    }

    /// Keys below a deeper `/` collapse to their directory.
    fn add_object(&mut self, key: &str, size: Option<u64>, modified: Option<String>) {
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
    fn keys_sink_keeps_every_key() {
        let mut keys = Keys::default();
        keys.add_prefix("ignored/");
        keys.add_object("dir/", Some(0), None);
        keys.add_object("dir/a/b.txt", Some(1), None);
        assert_eq!(keys.0, vec!["dir/", "dir/a/b.txt"]);
    }

    #[test]
    fn search_prefix_selects_below() {
        assert_eq!(search_prefix("/dir/sub/"), "dir/sub/");
        assert_eq!(search_prefix(""), "");
    }

    #[test]
    fn plain_key_rejects_keys_a_url_would_reinterpret() {
        for key in [
            "dir/",
            "dir/a",
            "dir/a/b.txt",
            "dir/x/",
            "dir/a..b",
            "dir/.hidden",
        ] {
            assert!(plain_key(key, "dir/"), "{key}");
        }
        for key in [
            "other/a",
            "dir/../outside.txt",
            "dir/a/./b",
            "dir/a/..",
            "dir/a\\..\\b",
            "dir/a%2Fb",
            "dir/a?b",
            "dir/a#b",
            "dir/a\tb",
            "dir/a\nb",
        ] {
            assert!(!plain_key(key, "dir/"), "{key:?}");
        }
        assert!(plain_key("anything/at/root", ""));
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
