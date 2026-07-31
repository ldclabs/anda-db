//! `_id` is injected by the schema builder as a *required* entry, so a struct
//! that never serializes an `"_id"` key builds a schema it can never satisfy:
//! `Document::try_from` fails at runtime with `field "_id" is required`.
//! Both the missing field and a skipped one must be rejected at compile time.
use anda_db_derive::AndaDBSchema;

#[derive(AndaDBSchema)]
struct NoId {
    name: String,
}

#[derive(AndaDBSchema)]
struct SkippedId {
    #[serde(skip)]
    _id: u64,
    name: String,
}

fn main() {}
