//! Helpers for turning an incoming HTTP request into a shard-routing key.
//!
//! The default extractor prefers a database name encoded in the request path.
//! If no database name is present, it falls back to shard identifiers supplied
//! through request headers.

/// Extracts the database name or shard ID from the request URL path or headers.
///
/// Resolution order:
/// 1. Path formats:
///    - `/{db_name}/...` → `db_name`
///    - `/v1/{db_name}/...` → `db_name`
/// 2. `Shard-ID` or `X-Shard` header value
pub fn extract_db_name(
    uri: &axum::http::Uri,
    headers: &axum::http::HeaderMap,
) -> (Option<String>, Option<u32>) {
    // Try extracting from the URL path: /{db_name}/... or /v1/{db_name}/...
    let mut segments = uri.path().split('/').filter(|segment| !segment.is_empty());
    let db_name = match (segments.next(), segments.next()) {
        (Some("v1"), Some(db_name)) => Some(db_name),
        (Some("v1"), None) => None,
        (Some(db_name), _) => Some(db_name),
        (None, _) => None,
    };

    if let Some(db_name) = db_name {
        return (Some(db_name.to_string()), None);
    }

    if let Some(v) = headers.get("Shard-ID").or_else(|| headers.get("X-Shard"))
        && let Ok(shard_id) = v.to_str()
            && let Ok(id) = shard_id.parse::<u32>() {
                return (None, Some(id));
            }

    (None, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderValue, Uri};

    #[test]
    fn extract_from_path() {
        let uri: Uri = "/mydb/some/path".parse().unwrap();
        let headers = HeaderMap::new();
        assert_eq!(extract_db_name(&uri, &headers), (Some("mydb".into()), None));

        let uri: Uri = "/mydb".parse().unwrap();
        let headers = HeaderMap::new();
        assert_eq!(extract_db_name(&uri, &headers), (Some("mydb".into()), None));
    }

    #[test]
    fn extract_from_v1_path() {
        let uri: Uri = "/v1/mydb/some/path".parse().unwrap();
        let headers = HeaderMap::new();
        assert_eq!(extract_db_name(&uri, &headers), (Some("mydb".into()), None));

        let uri: Uri = "/v1/mydb".parse().unwrap();
        let headers = HeaderMap::new();
        assert_eq!(extract_db_name(&uri, &headers), (Some("mydb".into()), None));

        let uri: Uri = "/v1".parse().unwrap();
        let headers = HeaderMap::new();
        assert_eq!(extract_db_name(&uri, &headers), (None, None));
    }

    #[test]
    fn extract_from_header() {
        let uri: Uri = "/".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("Shard-ID", HeaderValue::from_static("42"));
        assert_eq!(extract_db_name(&uri, &headers), (None, Some(42)));
    }

    #[test]
    fn extract_none_when_missing() {
        let uri: Uri = "/".parse().unwrap();
        let headers = HeaderMap::new();
        assert_eq!(extract_db_name(&uri, &headers), (None, None));
    }

    #[test]
    fn v1_without_db_falls_back_to_header() {
        let uri: Uri = "/v1/".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("X-Shard", HeaderValue::from_static("88"));
        assert_eq!(extract_db_name(&uri, &headers), (None, Some(88)));
    }
}
