//! Helpers for turning an incoming HTTP request into a shard-routing key.
//!
//! The default extractor resolves the database name from the request path.

use crate::proxy::DbShardExtractor;

/// Extracts shard routing keys from a path prefix.
///
/// The extractor removes [`Self::prefix`] from the request path and treats the
/// next path segment as the database name. Client-supplied shard headers are
/// ignored; shard IDs are selected from server-side routing metadata only.
pub struct PrefixExtractor {
    /// Path prefix that precedes the database name.
    ///
    /// For example, a prefix of `/db/` extracts `tenant_a` from
    /// `/db/tenant_a/query`. Use [`PrefixExtractor::new`] to normalize the
    /// prefix; a raw prefix without a trailing slash (e.g. `/db`) would also
    /// match `/dbfoo/x` and extract the wrong name.
    pub prefix: String,
}

impl PrefixExtractor {
    /// Creates an extractor, normalizing the prefix to start and end with
    /// `/` so `/db` cannot match `/dbfoo/x`.
    pub fn new(prefix: impl Into<String>) -> Self {
        let mut prefix = prefix.into();
        if !prefix.starts_with('/') {
            prefix.insert(0, '/');
        }
        if !prefix.ends_with('/') {
            prefix.push('/');
        }
        Self { prefix }
    }
}

/// Mirrors the backend's database-name rules (`[a-z0-9_]{1,64}`, see
/// `anda_db_schema::validate_field_name`) so invalid names are rejected
/// before they reach the routing store (PostgreSQL).
fn is_valid_db_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'_'))
}

impl DbShardExtractor for PrefixExtractor {
    fn extract(
        &self,
        uri: &axum::http::Uri,
        _headers: &axum::http::HeaderMap,
    ) -> (Option<u32>, Option<String>) {
        // Extract from path: prefix{db_name}/...
        if let Some(path) = uri.path().strip_prefix(&self.prefix)
            && let Some(db_name) = path.split('/').next()
            && is_valid_db_name(db_name)
        {
            return (None, Some(db_name.to_string()));
        }

        (None, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderValue, Uri};

    #[test]
    fn prefix_extractor_extracts_db_from_root_prefix() {
        let extractor = PrefixExtractor {
            prefix: "/".to_string(),
        };

        let uri: Uri = "/mydb/some/path".parse().unwrap();
        let headers = HeaderMap::new();
        assert_eq!(
            extractor.extract(&uri, &headers),
            (None, Some("mydb".into()))
        );

        let uri: Uri = "/mydb".parse().unwrap();
        assert_eq!(
            extractor.extract(&uri, &headers),
            (None, Some("mydb".into()))
        );
    }

    #[test]
    fn prefix_extractor_extracts_db_from_custom_prefix() {
        let extractor = PrefixExtractor {
            prefix: "/db/".to_string(),
        };

        let uri: Uri = "/db/mydb/some/path".parse().unwrap();
        let headers = HeaderMap::new();
        assert_eq!(
            extractor.extract(&uri, &headers),
            (None, Some("mydb".into()))
        );

        let uri: Uri = "/db/mydb".parse().unwrap();
        assert_eq!(
            extractor.extract(&uri, &headers),
            (None, Some("mydb".into()))
        );
    }

    #[test]
    fn prefix_extractor_new_normalizes_prefix() {
        assert_eq!(PrefixExtractor::new("/db").prefix, "/db/");
        assert_eq!(PrefixExtractor::new("db/").prefix, "/db/");
        assert_eq!(PrefixExtractor::new("/").prefix, "/");

        // Without normalization, `/db` would extract `foo` from `/dbfoo/x`.
        let extractor = PrefixExtractor::new("/db");
        let uri: Uri = "/dbfoo/x".parse().unwrap();
        let headers = HeaderMap::new();
        assert_eq!(extractor.extract(&uri, &headers), (None, None));

        let uri: Uri = "/db/mydb/x".parse().unwrap();
        assert_eq!(
            extractor.extract(&uri, &headers),
            (None, Some("mydb".into()))
        );
    }

    #[test]
    fn prefix_extractor_rejects_invalid_db_names() {
        let extractor = PrefixExtractor::new("/");
        let headers = HeaderMap::new();

        // Double slash yields an empty first segment.
        let uri: Uri = "//mydb".parse().unwrap();
        assert_eq!(extractor.extract(&uri, &headers), (None, None));

        // Characters outside [a-z0-9_] never reach the routing store.
        for path in ["/My-Db/x", "/db%20name/x", "/db.name/x"] {
            let uri: Uri = path.parse().unwrap();
            assert_eq!(
                extractor.extract(&uri, &headers),
                (None, None),
                "path: {path}"
            );
        }

        // Over-long names are rejected as well.
        let uri: Uri = format!("/{}/x", "a".repeat(65)).parse().unwrap();
        assert_eq!(extractor.extract(&uri, &headers), (None, None));
    }

    #[test]
    fn prefix_extractor_ignores_shard_header_when_path_missing() {
        let extractor = PrefixExtractor {
            prefix: "/db/".to_string(),
        };

        let uri: Uri = "/other-path".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("Shard-ID", HeaderValue::from_static("42"));
        assert_eq!(extractor.extract(&uri, &headers), (None, None));
    }

    #[test]
    fn prefix_extractor_returns_none_when_path_and_headers_missing() {
        let extractor = PrefixExtractor {
            prefix: "/db/".to_string(),
        };

        let uri: Uri = "/".parse().unwrap();
        let headers = HeaderMap::new();
        assert_eq!(extractor.extract(&uri, &headers), (None, None));
    }

    #[test]
    fn prefix_extractor_ignores_x_shard_header() {
        let extractor = PrefixExtractor {
            prefix: "/db/".to_string(),
        };

        let uri: Uri = "/db/".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("X-Shard", HeaderValue::from_static("88"));
        assert_eq!(extractor.extract(&uri, &headers), (None, None));
    }

    #[test]
    fn prefix_extractor_prefers_path_over_untrusted_header() {
        let extractor = PrefixExtractor {
            prefix: "/db/".to_string(),
        };

        let uri: Uri = "/db/mydb/query".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("Shard-ID", HeaderValue::from_static("7"));
        assert_eq!(
            extractor.extract(&uri, &headers),
            (None, Some("mydb".into()))
        );
    }
}
