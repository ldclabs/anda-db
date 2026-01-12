# Anda DB Utils

`anda_db_utils` is a utility Rust library for Anda DB, providing helpful extensions and data structures.

## Features

- **`Pipe` Trait**: Enables functional-style method chaining.
- **`UniqueVec<T>`**: A `Vec` that ensures all its elements are unique.
- **`CountingWriter`**: A writer that counts bytes, useful for determining the size of serialized data without allocation.

## Usage

### Pipe Trait

The `Pipe` trait allows you to chain functions in a fluent, readable way.

```rust
use anda_db_utils::Pipe;

let result = 5.pipe(|x| x * 2).pipe(|x| x + 1);
assert_eq!(result, 11);
```

### UniqueVec<T>

`UniqueVec<T>` is a vector that automatically handles uniqueness of its elements, backed by a `HashSet` for efficiency.

```rust
use anda_db_utils::UniqueVec;

let mut unique_vec = UniqueVec::from(vec![1, 2, 3]);

// Pushing an existing item does nothing
unique_vec.push(2);
assert_eq!(unique_vec.as_ref(), &[1, 2, 3]);

// Pushing a new item adds it to the vector
unique_vec.push(4);
assert_eq!(unique_vec.as_ref(), &[1, 2, 3, 4]);

// Extend with a mix of new and existing items
unique_vec.extend(vec![3, 5, 6]);
assert_eq!(unique_vec.as_ref(), &[1, 2, 3, 4, 5, 6]);
```

### CountingWriter

`CountingWriter` can be used to calculate the size of data when serialized, for example with CBOR, without actually writing to memory.

```rust
use anda_db_utils::CountingWriter;
use serde::Serialize;

#[derive(Serialize)]
struct MyData {
    id: u32,
    name: String,
}

let data = MyData { id: 1, name: "test".to_string() };
let size = estimate_cbor_size(&data);

println!("Serialized size: {} bytes", size);
```

## License

Copyright © 2026 [LDC Labs](https://github.com/ldclabs).

`ldclabs/anda-db` is licensed under the MIT License. See [LICENSE](../../LICENSE) for the full license text.
