//! `#[unique]` is a field-level constraint; on the container it used to be
//! silently ignored.
use anda_db_derive::AndaDBSchema;

#[derive(AndaDBSchema)]
#[unique]
struct Doc {
    _id: u64,
    name: String,
}

fn main() {}
