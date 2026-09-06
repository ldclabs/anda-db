use anda_db_derive::FieldTyped;

#[derive(FieldTyped)]
struct Node {
    next: Option<Box<Node>>,
}

#[derive(FieldTyped)]
struct SelfNode {
    next: Vec<Self>,
}

fn main() {}
