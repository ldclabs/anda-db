//! `#[cbor(key = N)]` on a top-level document field would make the stored
//! document unreadable (documents use text field names); reject at compile
//! time.
use anda_db_derive::AndaDBSchema;

#[derive(AndaDBSchema)]
struct Claims {
    _id: u64,
    #[cbor(key = 1)]
    issuer: Option<String>,
}

fn main() {}
