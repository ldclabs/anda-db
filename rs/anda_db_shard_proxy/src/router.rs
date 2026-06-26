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
    /// For example, a prefix of `/db/` extracts `tenant-a` from
    /// `/db/tenant-a/query`.
    pub prefix: String,
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
            && !db_name.is_empty()
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
