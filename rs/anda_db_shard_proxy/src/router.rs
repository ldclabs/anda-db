//! Helpers for turning an incoming HTTP request into a shard-routing key.
//!
//! The default extractor first tries shard identifiers supplied through
//! request headers, then falls back to a database name encoded in the request
//! path.

use crate::proxy::DbShardExtractor;

pub struct PrefixExtractor {
    pub prefix: String,
}

impl DbShardExtractor for PrefixExtractor {
    fn extract(
        &self,
        uri: &axum::http::Uri,
        headers: &axum::http::HeaderMap,
    ) -> (Option<u32>, Option<String>) {
        // Prefer shard-id headers when present.
        if let Some(v) = headers.get("Shard-ID").or_else(|| headers.get("X-Shard"))
            && let Ok(shard_id) = v.to_str()
            && let Ok(id) = shard_id.parse::<u32>()
        {
            return (Some(id), None);
        }

        // Fall back to extracting from path: prefix{db_name}/...
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
    fn prefix_extractor_falls_back_to_shard_header_when_path_missing() {
        let extractor = PrefixExtractor {
            prefix: "/db/".to_string(),
        };

        let uri: Uri = "/other-path".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("Shard-ID", HeaderValue::from_static("42"));
        assert_eq!(extractor.extract(&uri, &headers), (Some(42), None));
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
    fn prefix_extractor_falls_back_to_x_shard_header() {
        let extractor = PrefixExtractor {
            prefix: "/db/".to_string(),
        };

        let uri: Uri = "/db/".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("X-Shard", HeaderValue::from_static("88"));
        assert_eq!(extractor.extract(&uri, &headers), (Some(88), None));
    }

    #[test]
    fn prefix_extractor_prefers_header_over_path() {
        let extractor = PrefixExtractor {
            prefix: "/db/".to_string(),
        };

        let uri: Uri = "/db/mydb/query".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("Shard-ID", HeaderValue::from_static("7"));
        assert_eq!(extractor.extract(&uri, &headers), (Some(7), None));
    }
}
