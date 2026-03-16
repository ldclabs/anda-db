//! Reverse-proxy state and request forwarding helpers.
//!
//! This module resolves a request to a shard backend, rewrites the target URI,
//! strips hop-by-hop headers, and forwards the request with a shared Hyper
//! client.

use axum::{
    body::Body,
    extract::State,
    http::{
        HeaderMap, Request, Response, StatusCode, Uri,
        header::{self, HeaderName},
    },
    response::IntoResponse,
};
use hyper_util::client::legacy::Client;
use std::sync::Arc;
use tokio::time::{Duration, timeout};

use crate::store::ShardStore;

const KEEP_ALIVE_HEADER: HeaderName = HeaderName::from_static("keep-alive");
const SHARD_ID_HEADER: HeaderName = HeaderName::from_static("shard-id");

/// A function that extracts the database name or shard ID from the incoming request.
///
/// Takes the request URI and headers, returns `(Option<db_name>, Option<shard_id>)` on success.
/// A default implementation is provided by [`router::extract_db_name`].
pub type DbNameExtractor =
    Arc<dyn Fn(&Uri, &HeaderMap) -> (Option<String>, Option<u32>) + Send + Sync>;

/// Shared application state passed to all handlers.
#[derive(Clone)]
pub struct AppState {
    /// Shared routing metadata store.
    pub store: ShardStore,
    /// Hyper client used to forward requests to shard backends.
    pub client: Arc<Client<hyper_util::client::legacy::connect::HttpConnector, Body>>,
    /// Optional bearer token required for management endpoints.
    pub api_key: Arc<Option<String>>,
    /// Custom function to extract the database name or shard ID from a request.
    /// Defaults to [`router::extract_db_name`].
    pub db_name_extractor: DbNameExtractor,
    /// Upper bound for a proxied backend request.
    pub proxy_request_timeout: Duration,
}

/// The catch-all reverse proxy handler.
///
/// 1. Extracts the database name or shard ID from the incoming request.
/// 2. Resolves which backend shard to forward to.
/// 3. Rewrites the request URI and forwards it to the backend.
pub async fn proxy_handler(
    State(state): State<AppState>,
    mut req: Request<Body>,
) -> Result<Response<Body>, impl IntoResponse> {
    let original_uri = req.uri().clone();
    let route = match (state.db_name_extractor)(req.uri(), req.headers()) {
        (Some(name), _) => state.store.resolve(&name).await,
        (_, id) => state.store.resolve_by_shard(id.unwrap_or(0)).await,
    };

    let route = route.ok_or({
        (
            StatusCode::NOT_FOUND,
            "no shard mapping found for database name or shard ID",
        )
    })?;

    *req.uri_mut() = build_target_uri(&route.backend_addr, &original_uri)
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "invalid backend URI"))?;

    remove_hop_by_hop_headers(req.headers_mut());
    // add the shard ID header so backends can know which shard the request is for (required)
    req.headers_mut()
        .insert(SHARD_ID_HEADER, route.shard_id.into());

    let mut resp = timeout(state.proxy_request_timeout, state.client.request(req))
        .await
        .map_err(|_| (StatusCode::GATEWAY_TIMEOUT, "backend request timed out"))?
        .map_err(|_| (StatusCode::BAD_GATEWAY, "backend request failed"))?;

    remove_hop_by_hop_headers(resp.headers_mut());
    // add the shard ID header to the response so clients can know which shard they hit (optional but useful for debugging)
    resp.headers_mut()
        .insert(SHARD_ID_HEADER, route.shard_id.into());

    Ok::<_, (StatusCode, &str)>(resp.map(Body::new))
}

/// Build the backend URI by preserving the original path and query string.
fn build_target_uri(backend_addr: &str, request_uri: &Uri) -> Result<Uri, ()> {
    let path_and_query = request_uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");

    format!("{}{}", backend_addr.trim_end_matches('/'), path_and_query)
        .parse::<Uri>()
        .map_err(|_| ())
}

/// Remove headers named inside the `Connection` header, as required by RFC 9110.
fn remove_connection_listed_headers(headers: &mut HeaderMap) {
    let names: Vec<HeaderName> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect();

    for name in names {
        headers.remove(name);
    }
}

/// Remove hop-by-hop headers that must not be forwarded by proxies.
fn remove_hop_by_hop_headers(headers: &mut HeaderMap) {
    remove_connection_listed_headers(headers);

    for name in [
        header::CONNECTION,
        header::HOST,
        KEEP_ALIVE_HEADER,
        header::PROXY_AUTHENTICATE,
        header::PROXY_AUTHORIZATION,
        header::TE,
        header::TRAILER,
        header::TRANSFER_ENCODING,
        header::UPGRADE,
    ] {
        headers.remove(name);
    }

    headers.remove("proxy-connection");
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::header::HeaderValue;

    #[test]
    fn build_target_uri_preserves_path_and_query() {
        let request_uri: Uri = "/v1/mydb/query?a=1&b=2".parse().unwrap();

        let target = build_target_uri("http://127.0.0.1:8080/", &request_uri).unwrap();

        assert_eq!(target, "http://127.0.0.1:8080/v1/mydb/query?a=1&b=2");
    }

    #[test]
    fn remove_hop_by_hop_headers_preserves_forwarded_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("proxy.example.com"));
        headers.insert(
            HeaderName::from_static("x-forwarded-host"),
            HeaderValue::from_static("lb.example.com"),
        );
        headers.insert(
            HeaderName::from_static("x-forwarded-proto"),
            HeaderValue::from_static("https"),
        );

        remove_hop_by_hop_headers(&mut headers);

        assert_eq!(
            headers
                .get("x-forwarded-host")
                .and_then(|v| v.to_str().ok()),
            Some("lb.example.com")
        );
        assert_eq!(
            headers
                .get("x-forwarded-proto")
                .and_then(|v| v.to_str().ok()),
            Some("https")
        );
    }

    #[test]
    fn remove_hop_by_hop_headers_removes_standard_and_connection_listed_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("proxy.example.com"));
        headers.insert(
            header::CONNECTION,
            HeaderValue::from_static("keep-alive, x-remove-me"),
        );
        headers.insert(KEEP_ALIVE_HEADER, HeaderValue::from_static("timeout=5"));
        headers.insert(
            HeaderName::from_static("x-remove-me"),
            HeaderValue::from_static("1"),
        );
        headers.insert(
            HeaderName::from_static("x-keep-me"),
            HeaderValue::from_static("ok"),
        );

        remove_hop_by_hop_headers(&mut headers);

        assert!(!headers.contains_key(header::HOST));
        assert!(!headers.contains_key(header::CONNECTION));
        assert!(!headers.contains_key(KEEP_ALIVE_HEADER));
        assert!(!headers.contains_key("x-remove-me"));
        assert_eq!(
            headers.get("x-keep-me").and_then(|v| v.to_str().ok()),
            Some("ok")
        );
    }
}
