//! # Anda-DB B-tree Index Library
//!
//! This module provides a B-tree based index implementation for Anda-DB.
//! It supports indexing fields of various types including u64, i64, String, and binary data.

//!
//! ```
//! use anda_db_btree::{BTreeIndex, RangeQuery};
//! let index = BTreeIndex::<u64, String>::new("tags".into(), None);
//! index.insert(1, "rust".into(), 1)?;
//! assert!(!index.insert(1, "rust".into(), 2)?);
//! let keys = index.try_range_query_with(
//!     RangeQuery::Ge("r".into()), |key, _| (false, vec![key.clone()]),
//! )?;
//! assert_eq!(keys, vec!["rust"]);
//! # Ok::<(), anda_db_btree::BTreeError>(())
//! ```
//!
//! ```
//! # async fn round_trip() -> Result<(), anda_db_btree::BTreeError> {
//! use anda_db_btree::{BTreeIndex, BucketObject};
//! use std::collections::BTreeMap;
//! let index = BTreeIndex::<u64, String>::new("durable".into(), None);
//! index.insert(1, "rust".into(), 1)?;
//! let mut metadata = Vec::new();
//! let mut buckets = BTreeMap::<BucketObject, Vec<u8>>::new();
//! index.flush_owned_with(2, |data| {
//!     metadata = data; std::future::ready(Ok(()))
//! }, |object, data| {
//!     buckets.insert(object, data); std::future::ready(Ok(()))
//! }).await?;
//! let loaded = BTreeIndex::<u64, String>::load_all(&metadata[..],
//!     async |object| Ok(buckets.get(&object).cloned()),
//! ).await?;
//! assert_eq!(loaded.query_with(&"rust".into(), |ids| Some(ids.clone())), Some(vec![1]));
//! # Ok(())
//! # }
//! # futures::executor::block_on(round_trip()).unwrap();
//! ```

mod btree;
mod error;

pub use btree::*;
pub use error::*;
