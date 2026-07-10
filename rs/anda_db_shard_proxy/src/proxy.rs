//! Reverse-proxy state and request forwarding helpers.
//!
//! This module resolves a request to a shard backend, rewrites the target URI,
//! strips hop-by-hop headers, and forwards the request with a shared Hyper
//! client.

use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::{
        HeaderMap, HeaderValue, Request, Response, StatusCode, Uri,
        header::{self, HeaderName},
    },
    response::IntoResponse,
};
use hyper_util::client::legacy::Client;
use std::{net::SocketAddr, sync::Arc};
use tokio::time::{Duration, timeout};

use crate::store::{ResolvedRoute, ShardStore};

const KEEP_ALIVE_HEADER: HeaderName = HeaderName::from_static("keep-alive");
const SHARD_ID_HEADER: HeaderName = HeaderName::from_static("shard-id");
const X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");
const X_FORWARDED_HOST: HeaderName = HeaderName::from_static("x-forwarded-host");
const X_FORWARDED_PROTO: HeaderName = HeaderName::from_static("x-forwarded-proto");

/// A pluggable extractor that resolves either database name or shard id
/// from an incoming request.
///
/// A default implementation is provided by [`crate::router::PrefixExtractor`].
pub trait DbShardExtractor: Send + Sync {
    /// Extracts either a shard id or database name from a request.
    ///
    /// The first tuple member is a concrete shard id. The second member is a
    /// database name that still needs to be resolved through the shard store.
    /// Implementations should return at most one populated value.
    fn extract(&self, uri: &Uri, headers: &HeaderMap) -> (Option<u32>, Option<String>);
}

/// Shared application state passed to all handlers.
#[derive(Clone)]
pub struct AppState {
    /// Shared routing metadata store.
    pub store: ShardStore,
    /// Hyper client used to forward requests to shard backends.
    pub client: Arc<Client<hyper_util::client::legacy::connect::HttpConnector, Body>>,
    /// Optional bearer token required for management endpoints.
    pub api_key: Arc<Option<String>>,
    /// Custom extractor to read the database name or shard ID from requests.
    /// Defaults to [`crate::router::PrefixExtractor`].
    pub db_name_extractor: Arc<dyn DbShardExtractor>,
    /// Upper bound for a proxied backend request **up to the response
    /// headers**: it covers connecting, sending the request, and waiting for
    /// the backend to start responding. Streaming the response body is not
    /// bounded by this timeout.
    pub proxy_request_timeout: Duration,
    /// Default backend address to use if no shard mapping is found.
    pub default_backend: Option<ResolvedRoute>,
}

/// Validates a shard backend address: an absolute `http://` URI with a host.
///
/// The proxy's HTTP client is built with `build_http()` (plain HTTP), so an
/// `https://` backend would always fail with `BAD_GATEWAY`; rejecting bad
/// addresses at write time avoids taking a shard down with a typo.
pub fn validate_backend_addr(addr: &str) -> Result<(), String> {
    let uri: Uri = addr
        .parse()
        .map_err(|err| format!("invalid backend_addr {addr:?}: {err}"))?;
    if uri.scheme_str() != Some("http") {
        return Err(format!(
            "backend_addr {addr:?} must use the http scheme (the proxy forwards plain HTTP)"
        ));
    }
    if uri.authority().is_none() {
        return Err(format!("backend_addr {addr:?} must include a host"));
    }
    Ok(())
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
    let route = match state.db_name_extractor.extract(req.uri(), req.headers()) {
        (Some(id), _) => state.store.resolve_by_shard(id).await,
        (_, Some(name)) => match state.store.resolve(&name).await {
            Ok(route) => route,
            // A routing-store failure is not "database not found": answer 503
            // so clients retry instead of assuming the database is gone.
            Err(err) => {
                log::error!("failed to resolve route for {name:?}: {err}");
                return Err((StatusCode::SERVICE_UNAVAILABLE, "routing store unavailable"));
            }
        },
        _ => state.default_backend.clone(),
    };

    let route = route.ok_or((StatusCode::NOT_FOUND, "No backend found"))?;
    *req.uri_mut() = build_target_uri(&route.backend_addr, &original_uri)
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "invalid backend URI"))?;

    // Capture forwarding metadata before the hop-by-hop cleanup drops Host.
    let client_ip = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0.ip());
    let original_host = req.headers().get(header::HOST).cloned();

    remove_hop_by_hop_headers(req.headers_mut());
    // add the shard ID header so backends can know which shard the request is for (required)
    req.headers_mut()
        .insert(SHARD_ID_HEADER, route.shard_id.into());
    add_forwarded_headers(req.headers_mut(), client_ip, original_host);

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

/// Append the standard `X-Forwarded-*` headers so backends keep the client
/// IP, original Host, and scheme for auditing and rate limiting. Existing
/// values from an upstream load balancer are preserved: the client IP is
/// appended to `X-Forwarded-For`, while `X-Forwarded-Host`/`-Proto` are only
/// set when absent.
fn add_forwarded_headers(
    headers: &mut HeaderMap,
    client_ip: Option<std::net::IpAddr>,
    original_host: Option<HeaderValue>,
) {
    if let Some(ip) = client_ip {
        let xff = match headers.get(X_FORWARDED_FOR).and_then(|v| v.to_str().ok()) {
            Some(existing) => format!("{existing}, {ip}"),
            None => ip.to_string(),
        };
        if let Ok(value) = HeaderValue::from_str(&xff) {
            headers.insert(X_FORWARDED_FOR, value);
        }
    }
    if !headers.contains_key(X_FORWARDED_HOST)
        && let Some(host) = original_host
    {
        headers.insert(X_FORWARDED_HOST, host);
    }
    if !headers.contains_key(X_FORWARDED_PROTO) {
        // The proxy itself only terminates plain HTTP.
        headers.insert(X_FORWARDED_PROTO, HeaderValue::from_static("http"));
    }
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
    fn add_forwarded_headers_appends_client_ip_and_sets_missing_headers() {
        let mut headers = HeaderMap::new();
        add_forwarded_headers(
            &mut headers,
            Some("10.1.2.3".parse().unwrap()),
            Some(HeaderValue::from_static("proxy.example.com")),
        );
        assert_eq!(
            headers.get(X_FORWARDED_FOR).and_then(|v| v.to_str().ok()),
            Some("10.1.2.3")
        );
        assert_eq!(
            headers.get(X_FORWARDED_HOST).and_then(|v| v.to_str().ok()),
            Some("proxy.example.com")
        );
        assert_eq!(
            headers.get(X_FORWARDED_PROTO).and_then(|v| v.to_str().ok()),
            Some("http")
        );
    }

    #[test]
    fn add_forwarded_headers_preserves_upstream_values() {
        let mut headers = HeaderMap::new();
        headers.insert(X_FORWARDED_FOR, HeaderValue::from_static("192.0.2.9"));
        headers.insert(X_FORWARDED_HOST, HeaderValue::from_static("lb.example.com"));
        headers.insert(X_FORWARDED_PROTO, HeaderValue::from_static("https"));

        add_forwarded_headers(
            &mut headers,
            Some("10.1.2.3".parse().unwrap()),
            Some(HeaderValue::from_static("proxy.example.com")),
        );

        assert_eq!(
            headers.get(X_FORWARDED_FOR).and_then(|v| v.to_str().ok()),
            Some("192.0.2.9, 10.1.2.3")
        );
        assert_eq!(
            headers.get(X_FORWARDED_HOST).and_then(|v| v.to_str().ok()),
            Some("lb.example.com")
        );
        assert_eq!(
            headers.get(X_FORWARDED_PROTO).and_then(|v| v.to_str().ok()),
            Some("https")
        );
    }

    #[test]
    fn validate_backend_addr_accepts_http_and_rejects_others() {
        assert!(validate_backend_addr("http://10.0.0.12:8080").is_ok());
        assert!(validate_backend_addr("http://db.internal").is_ok());
        assert!(validate_backend_addr("https://db.internal").is_err());
        assert!(validate_backend_addr("db.internal:8080").is_err());
        assert!(validate_backend_addr("not a uri").is_err());
        assert!(validate_backend_addr("/relative/path").is_err());
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
