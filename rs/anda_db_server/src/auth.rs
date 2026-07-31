//! Two-tier API-key authorization.
//!
//! The server distinguishes two kinds of credential:
//!
//! - The **admin key** — the single process-global key configured with
//!   `--api-key` / `API_KEY`. It is the only credential that reaches the root
//!   scope (`POST /`: `info`, `db.list`, `db.create`, `db.open`,
//!   `db.connect`, `db.close`, `db.set_api_key`, `db.remove_api_key`) and it
//!   also authorizes every database scope, since it can rotate any
//!   per-database key anyway.
//! - A **per-database key** — bound to exactly one database and accepted only
//!   on `POST /{that_db}`. Its SHA3-256 hash is persisted in the primary
//!   database's extension metadata (see [`crate::state::DB_API_KEYS_KEY`]);
//!   the key itself is never stored.
//!
//! ## Precedence
//!
//! 1. **No admin key configured** — the instance is unauthenticated and every
//!    request is treated as [`Principal::Admin`]. This is the pre-existing
//!    loopback/`--insecure-no-api-key` development mode, preserved verbatim.
//!    Per-database keys cannot be provisioned in this mode (see
//!    [`crate::state::AppState::set_db_api_key`]), so "no admin key" always
//!    implies "no per-database keys" and the two tiers can never disagree.
//! 2. **Admin key presented** — [`Principal::Admin`] in any scope.
//! 3. **Database scope with a key bound to that database** — the presented
//!    token must match that key, giving [`Principal::Database`].
//! 4. **Anything else** — `401`. In particular a database with no bound key
//!    falls back to the admin key, which is exactly how the server behaved
//!    before per-database keys existed.
//!
//! Rule 4 deliberately returns the same `401` whether the named database
//! exists, exists under a different key, or does not exist at all: an
//! unauthorized caller must not be able to probe the database namespace. Only
//! an admin ever sees `404 not_found` for a missing database.

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use sha3::Digest;
use std::fmt;

use crate::{api::constant_time_eq, error::ApiError};

/// Length in bytes of a server-generated API key before hex encoding.
const GENERATED_KEY_BYTES: usize = 32;

/// The scope an RPC request is dispatched into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope<'a> {
    /// `POST /` — server-level methods. Admin key only.
    Root,
    /// `POST /{db_name}` — methods scoped to a single database.
    Database(&'a str),
}

/// The authenticated identity of a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Principal {
    /// Holder of the admin key — or any caller when the instance runs
    /// without an admin key at all.
    Admin,
    /// Holder of the key bound to the database named in the request path.
    /// Entitled to that database only, and never to server-level state.
    Database,
}

impl Principal {
    /// Whether this principal may see and change server-level state.
    pub fn is_admin(&self) -> bool {
        matches!(self, Principal::Admin)
    }
}

/// SHA3-256 digest of an API key.
///
/// Only the digest is ever persisted or held in the registry, so a leak of
/// the primary database's metadata does not hand out working credentials.
/// Serialized as a lowercase hex string so the persisted map stays readable
/// and identical in CBOR and JSON.
#[derive(Clone)]
pub struct ApiKeyHash([u8; 32]);

impl ApiKeyHash {
    /// Hashes an API key.
    pub fn from_key(key: &str) -> Self {
        let mut hasher = sha3::Sha3_256::new();
        hasher.update(key.as_bytes());
        Self(hasher.finalize().into())
    }

    /// Constant-time check of a presented key against this digest.
    ///
    /// The presented key is hashed first, so the comparison always runs over
    /// two 32-byte digests and cannot leak the key length either.
    pub fn verify(&self, presented: &str) -> bool {
        constant_time_eq(&Self::from_key(presented).0, &self.0)
    }
}

impl PartialEq for ApiKeyHash {
    fn eq(&self, other: &Self) -> bool {
        constant_time_eq(&self.0, &other.0)
    }
}

impl Eq for ApiKeyHash {}

impl fmt::Debug for ApiKeyHash {
    /// Never renders the digest: a weak key would be recoverable from it by
    /// brute force, and this type ends up inside logged state structures.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ApiKeyHash(<redacted>)")
    }
}

impl Serialize for ApiKeyHash {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&hex::encode(self.0))
    }
}

impl<'de> Deserialize<'de> for ApiKeyHash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let encoded = String::deserialize(deserializer)?;
        let bytes = hex::decode(&encoded).map_err(de::Error::custom)?;
        let bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| de::Error::custom("expected a 32-byte SHA3-256 API key hash"))?;
        Ok(Self(bytes))
    }
}

/// Generates a new API key from the OS-seeded CSPRNG (`rand::rng`, a ChaCha
/// generator periodically reseeded from OS entropy), hex-encoded.
pub fn generate_api_key() -> String {
    use rand::Rng;

    let mut bytes = [0u8; GENERATED_KEY_BYTES];
    rand::rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// Applies the precedence rules documented at the module level.
///
/// `admin` is the hash of the configured admin key (`None` on an
/// unauthenticated instance) and `bound` is the hash bound to the database
/// named by `scope`. `bound` is never consulted for [`Scope::Root`]: a
/// per-database key must not reach server-level methods.
pub fn authorize(
    admin: Option<&ApiKeyHash>,
    bound: Option<&ApiKeyHash>,
    scope: Scope<'_>,
    presented: Option<&str>,
) -> Result<Principal, ApiError> {
    // Rule 1: no admin key means the whole instance is unauthenticated. The
    // startup checks (`check_startup_api_key`, `AppState::connect`) confine
    // that mode to loopback or an explicit `--insecure-no-api-key` opt-in,
    // and forbid per-database keys from existing at all.
    let Some(admin) = admin else {
        return Ok(Principal::Admin);
    };

    // Rule 2: the admin key authorizes everything.
    if let Some(presented) = presented
        && admin.verify(presented)
    {
        return Ok(Principal::Admin);
    }

    match scope {
        // Rule 4 for the root scope: no fallback exists — server-level
        // methods are admin-only.
        Scope::Root => Err(ApiError::unauthorized()),
        Scope::Database(_) => match (bound, presented) {
            // Rule 3.
            (Some(bound), Some(presented)) if bound.verify(presented) => Ok(Principal::Database),
            // Rule 4: a database with no bound key falls back to the admin
            // key, which was already rejected above. The response is
            // identical to the wrong-key and unknown-database cases.
            _ => Err(ApiError::unauthorized()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    fn hash(key: &str) -> ApiKeyHash {
        ApiKeyHash::from_key(key)
    }

    fn assert_unauthorized(result: Result<Principal, ApiError>) {
        let err = result.expect_err("expected an authorization failure");
        assert_eq!(err.status, StatusCode::UNAUTHORIZED);
        assert_eq!(err.code, "unauthorized");
        // The message must never depend on which check failed.
        assert_eq!(err.message, "invalid or missing API key");
    }

    #[test]
    fn hashing_is_stable_and_verification_is_exact() {
        let h = hash("s3cret");
        assert!(h.verify("s3cret"));
        assert!(!h.verify("s3cre"));
        assert!(!h.verify("s3crett"));
        assert!(!h.verify(""));
        assert_eq!(h, hash("s3cret"));
        assert_ne!(h, hash("other"));
        // The digest must not appear in debug output.
        assert_eq!(format!("{h:?}"), "ApiKeyHash(<redacted>)");
    }

    #[test]
    fn hash_round_trips_through_json_and_cbor() {
        let h = hash("s3cret");
        let json = serde_json::to_string(&h).unwrap();
        assert_eq!(json.len(), 66); // 64 hex chars plus quotes
        assert_eq!(serde_json::from_str::<ApiKeyHash>(&json).unwrap(), h);

        let mut buf = Vec::new();
        cbor2::ser::to_writer(&h, &mut buf).unwrap();
        assert_eq!(
            cbor2::de::from_reader::<ApiKeyHash, _>(&buf[..]).unwrap(),
            h
        );

        // A truncated digest is rejected rather than silently padded.
        assert!(serde_json::from_str::<ApiKeyHash>("\"00\"").is_err());
    }

    #[test]
    fn generated_keys_are_unique_and_high_entropy() {
        let a = generate_api_key();
        let b = generate_api_key();
        assert_eq!(a.len(), GENERATED_KEY_BYTES * 2);
        assert_ne!(a, b);
    }

    #[test]
    fn without_an_admin_key_every_scope_is_open() {
        for scope in [Scope::Root, Scope::Database("tenant_a")] {
            assert_eq!(
                authorize(None, None, scope, None).unwrap(),
                Principal::Admin
            );
            assert_eq!(
                authorize(None, None, scope, Some("anything")).unwrap(),
                Principal::Admin
            );
        }
    }

    #[test]
    fn admin_key_authorizes_root_and_every_database() {
        let admin = hash("admin-key");
        let bound = hash("tenant-key");
        assert_eq!(
            authorize(Some(&admin), None, Scope::Root, Some("admin-key")).unwrap(),
            Principal::Admin
        );
        assert_eq!(
            authorize(
                Some(&admin),
                Some(&bound),
                Scope::Database("tenant_a"),
                Some("admin-key")
            )
            .unwrap(),
            Principal::Admin
        );
    }

    #[test]
    fn database_key_is_confined_to_its_own_database() {
        let admin = hash("admin-key");
        let a = hash("key-a");
        let b = hash("key-b");

        assert_eq!(
            authorize(Some(&admin), Some(&a), Scope::Database("a"), Some("key-a")).unwrap(),
            Principal::Database
        );
        // Database b's binding does not accept database a's key ...
        assert_unauthorized(authorize(
            Some(&admin),
            Some(&b),
            Scope::Database("b"),
            Some("key-a"),
        ));
        // ... nor does an unbound database, which falls back to the admin key.
        assert_unauthorized(authorize(
            Some(&admin),
            None,
            Scope::Database("c"),
            Some("key-a"),
        ));
        // ... and the root scope has no fallback at all.
        assert_unauthorized(authorize(Some(&admin), None, Scope::Root, Some("key-a")));
    }

    #[test]
    fn missing_and_wrong_credentials_are_indistinguishable() {
        let admin = hash("admin-key");
        let bound = hash("key-a");
        let rejected = [
            // No token at all.
            authorize(Some(&admin), Some(&bound), Scope::Database("a"), None),
            // Wrong token on a bound database.
            authorize(
                Some(&admin),
                Some(&bound),
                Scope::Database("a"),
                Some("nope"),
            ),
            // Right token, but a different (or non-existent) database: the
            // caller cannot tell those two apart, which is the point.
            authorize(
                Some(&admin),
                None,
                Scope::Database("does_not_exist"),
                Some("key-a"),
            ),
        ];
        for result in rejected {
            assert_unauthorized(result);
        }
    }
}
