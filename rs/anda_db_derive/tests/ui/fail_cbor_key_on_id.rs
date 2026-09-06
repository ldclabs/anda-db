use anda_db_derive::AndaDBSchema;

#[derive(AndaDBSchema)]
struct Doc {
    #[cbor(key = 0)]
    _id: u64,
}

fn main() {}
