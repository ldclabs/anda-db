# Anda-DB BM25 Full-Text Search Library

[![Crates.io](https://img.shields.io/crates/v/anda_db_tfs)](https://crates.io/crates/anda_db_tfs)
[![Documentation](https://docs.rs/anda_db_tfs/badge.svg)](https://docs.rs/anda_db_tfs)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Build Status](https://github.com/ldclabs/anda-db/actions/workflows/test.yml/badge.svg)](https://github.com/ldclabs/anda-db/actions)

`anda_db_tfs` is a full-text search library implementing the BM25 ranking algorithm in Rust. BM25 (Best Matching 25) is a ranking function used by search engines to estimate the relevance of documents to a given search query. It's an extension of the TF-IDF model.

## Features

- **High Performance**: Optimized for speed with parallel processing using Rayon.
- **Customizable Tokenization**: Support for various tokenizers including Chinese text via jieba.
- **BM25 Ranking**: Industry-standard relevance scoring algorithm.
- **Serialization**: Save and load indices in CBOR format with optional compression.
- **Incremental Persistent**: Support incremental index updates persistent (insertions and deletions)
- **Thread-safe concurrent access**: Safely use the index from multiple threads

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
anda_db_tfs = "0.4"
```

For full features including tantivy tokenizers and jieba support:

```toml
[dependencies]
anda_db_tfs = { version = "0.4", features = ["full"] }
```

## Quick Start

```rust
use anda_db_tfs::{BM25Index, SimpleTokenizer};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

// Create a new index with a simple tokenizer
let index = BM25Index::new("my_bm25_index".to_string(), SimpleTokenizer::default(), None);

// Add documents to the index
index.insert(1, "The quick brown fox jumps over the lazy dog", now_ms).unwrap();
index.insert(2, "A fast brown fox runs past the lazy dog", now_ms).unwrap();
index.insert(3, "The lazy dog sleeps all day", now_ms).unwrap();

// Search for documents containing "fox"
let results = index.search("fox", 10);
for (doc_id, score) in results {
    println!("Document {}: score {}", doc_id, score);
}

// Remove a document
index.remove(3, "The lazy dog sleeps all day", now_ms);

// Store the index
{
    let metadata = std::fs::File::create("tfs_demo/metadata.cbor")?;
    index
        .flush(
            metadata,
            0,
            async |id, data| {
                let mut bucket = std::fs::File::create(format!("tfs_demo/b_{id}.cbor"))?;
                bucket.write_all(data)?;
                Ok(true)
            },
        )
        .await?;
}

// Load the index from a file
let metadata = std::fs::File::open("debug/hnsw_demo/metadata.cbor")?;
let loaded_index = BM25Index::load_all(
    jieba_tokenizer(),
    metadata,
    async |id| {
        let mut bucket = std::fs::File::open(format!("tfs_demo/b_{id}.cbor"))?;
        let mut buf = Vec::new();
        bucket.read_to_end(&mut buf)?;
        Ok(Some(buf))
    },
)
.await?;
println!("Loaded index with {} documents", loaded_index.len());
```

## Chinese Text Support

With the `tantivy-jieba` feature enabled, you can use the jieba tokenizer for Chinese text:

```rust
use anda_db_tfs::{BM25Index, jieba_tokenizer};

// Create an index with jieba tokenizer
let index = BM25Index::new("my_bm25_index".to_string(), jieba_tokenizer(), None);

// Add documents with Chinese text
index.insert(1, "Rust 是一种系统编程语言", now_ms).unwrap();
index.insert(2, "Rust 快速且内存高效，安全、并发、实用", now_ms).unwrap();

// Search for documents
let results = index.search("安全", 10);
```

## Advanced Usage

### Custom Tokenizer and BM25 Parameters

```rust
use anda_db_tfs::{BM25Index, BM25Config};
use tantivy::tokenizer::{LowerCaser, RemoveLongFilter, SimpleTokenizer, Stemmer};

// Create an index with custom BM25 parameters
let params = BM25Config { k1: 1.5, b: 0.75 };
let index_name = "my_custom_index".to_string();
let tokenizer = TokenizerChain::builder(SimpleTokenizer::default())
  .filter(RemoveLongFilter::limit(32))
  .filter(LowerCaser)
  .filter(Stemmer::default())
  .build();
let index = BM25Index::new(index_name, tokenizer, Some(params));
```

## API Documentation

👉 https://docs.rs/anda_db_tfs

### BM25Config

Parameters for the BM25 ranking algorithm.

```rust
pub struct BM25Config {
    // Controls term frequency saturation
    pub k1: f32,
    // Controls document length normalization
    pub b: f32,
}
```

Default values: `k1 = 1.2, b = 0.75`

## Error Handling

The library uses a custom error type `BM25Error` for various error conditions:

- `BM25Error::Generic`: Index-related errors.
- `BM25Error::Serialization`: CBOR serialization/deserialization errors.
- `BM25Error::NotFound`: Error when a token is not found.
- `BM25Error::AlreadyExists`: When trying to add a document with an ID that already exists.
- `BM25Error::TokenizeFailed`: When tokenization produces no tokens for a document.

## Performance Considerations

- For large documents, the library automatically uses parallel processing for tokenization.
- The search function uses parallel processing for query terms.
- For best performance with large indices, consider using SSD storage for serialized indices.
- Memory usage scales with the number of documents and unique terms.

## License
Copyright © 2026 [LDC Labs](https://github.com/ldclabs).

`ldclabs/anda-db` is licensed under the MIT License. See [LICENSE](../../LICENSE) for the full license text.