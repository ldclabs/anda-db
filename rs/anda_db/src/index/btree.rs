use anda_db_btree::{BTreeIndex, BucketObject};
use bytes::Bytes;
use cbor2::{from_reader, to_canonical_vec};
use ic_auth_types::ByteBufB64;
use parking_lot::RwLock;
use serde::{Serialize, de::DeserializeOwned};
use std::{fmt::Debug, hash::Hash, str::FromStr, sync::Arc};

pub use anda_db_btree::{BTreeConfig, BTreeMetadata, BTreeStats, RangeQuery};

use super::from_virtual_field_name;
use crate::{
    error::DBError,
    schema::{BoxError, DocumentId, Fe, Ft, Fv, as_wildcard_map},
    storage::{ObjectVersion, PutMode, Storage},
    unix_ms,
};

/// Collection-level typed B-tree index wrapper.
///
/// AndaDB supports B-tree indexes over scalar `u64`, `i64`, text, and byte
/// values. Array and map fields are indexed by their elements or map keys when
/// the underlying scalar type is supported.
pub enum BTree {
    /// B-tree over unsigned integer keys.
    U64(InnerBTree<u64>),
    /// B-tree over signed integer keys.
    I64(InnerBTree<i64>),
    /// B-tree over UTF-8 text keys.
    String(InnerBTree<String>),
    /// B-tree over byte-array keys.
    Bytes(InnerBTree<Vec<u8>>),
}

impl Debug for BTree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BTree::I64(btree) => write!(f, "BTreeIndex<I64>({})", btree.name),
            BTree::U64(btree) => write!(f, "BTreeIndex<U64>({})", btree.name),
            BTree::String(btree) => write!(f, "BTreeIndex<String>({})", btree.name),
            BTree::Bytes(btree) => write!(f, "BTreeIndex<Bytes>({})", btree.name),
        }
    }
}

impl PartialEq for &BTree {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (BTree::I64(a), BTree::I64(b)) => a.name == b.name,
            (BTree::U64(a), BTree::U64(b)) => a.name == b.name,
            (BTree::String(a), BTree::String(b)) => a.name == b.name,
            (BTree::Bytes(a), BTree::Bytes(b)) => a.name == b.name,
            _ => false,
        }
    }
}

impl Eq for &BTree {}
impl Hash for &BTree {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            BTree::I64(btree) => btree.name.hash(state),
            BTree::U64(btree) => btree.name.hash(state),
            BTree::String(btree) => btree.name.hash(state),
            BTree::Bytes(btree) => btree.name.hash(state),
        }
    }
}

/// Resolves the key type a B-tree index uses for a declared field type:
/// at most one `Option` layer is unwrapped, then at most one homogeneous
/// container layer — an `Array` of a single element type (indexed by its
/// elements) or a *wildcard* `Map` (indexed by its keys).
///
/// Wildcard detection must come from the schema itself
/// ([`as_wildcard_map`]), which requires one of the sentinel keys `"*"` /
/// `b"*"` / `i64::MIN`. A `Map` that declares its keys explicitly — what
/// `#[derive(FieldTyped)]` emits for *every* nested struct — is returned
/// unchanged, so [`BTree::inner_new`] rejects it as an unsupported field type.
/// (Regression: treating any one-entry `Map` as a wildcard resolved a nested
/// one-field struct such as `struct One { a: String }` to a `String` index,
/// where [`BTree::insert`] then indexed every document under the constant
/// field *name* `"a"`: equality queries returned the whole collection and a
/// `#[unique]` field failed the second insert with a spurious conflict.)
///
/// Anything else is returned as-is and validated by the caller.
fn key_type_of(ft: &Ft) -> Ft {
    let ft = match ft {
        Ft::Option(inner) => inner.as_ref(),
        other => other,
    };
    match ft {
        Ft::Array(v) if v.len() == 1 => v[0].clone(),
        Ft::Map(v) => match as_wildcard_map(v) {
            Some((key, _)) => key.field_type(),
            None => ft.clone(),
        },
        other => other.clone(),
    }
}

/// Concrete B-tree wrapper for one supported field value type.
///
/// The type parameter is the native key type stored in the lower-level index.
pub struct InnerBTree<FV>
where
    FV: Eq + Ord + Hash + Debug + Clone + Serialize + DeserializeOwned,
{
    name: String,
    fields: Vec<String>,
    index: BTreeIndex<u64, FV>,
    storage: Storage, // 与 Collection 共享同一个 Storage 实例
    /// CAS token of the last observed metadata object: the remaining defense
    /// against a second writer. A `Precondition` conflict is never
    /// reconciled in place — the error propagates, the collection poisons
    /// its handle and recovery happens on reopen.
    metadata_version: Arc<RwLock<ObjectVersion>>,
    flush_gate: Arc<tokio::sync::Mutex<()>>,
}

impl BTree {
    pub(crate) fn dir_path(name: &str) -> String {
        format!("btree_indexes/{name}/")
    }

    fn metadata_path(name: &str) -> String {
        format!("btree_indexes/{name}/meta.cbor")
    }

    /// Object path for a bucket generation. Generation `0` is the legacy
    /// (pre-manifest) un-suffixed object and is only ever read, never
    /// written; the manifest protocol writes generation-suffixed objects.
    fn bucket_path(name: &str, object: BucketObject) -> String {
        if object.generation == 0 {
            format!("btree_indexes/{name}/b_{}.cbor", object.bucket_id)
        } else {
            format!(
                "btree_indexes/{name}/b_{}_{}.cbor",
                object.bucket_id, object.generation
            )
        }
    }

    /// Decodes an optional pagination cursor from base64url deterministic CBOR.
    pub fn from_cursor<T>(cursor: &Option<String>) -> Result<Option<T>, DBError>
    where
        T: DeserializeOwned,
    {
        cursor
            .as_ref()
            .map(|c| ByteBufB64::from_str(c))
            .transpose()
            .map_err(|err| DBError::Serialization {
                name: "from_cursor".to_string(),
                source: err.into(),
            })?
            .map(|v| from_reader(&v[..]))
            .transpose()
            .map_err(|err| DBError::Serialization {
                name: "from_cursor".to_string(),
                source: err.into(),
            })
    }

    /// Encodes a pagination cursor as base64url deterministic CBOR.
    pub fn to_cursor<T>(cursor: &T) -> Option<String>
    where
        T: Serialize,
    {
        to_canonical_vec(cursor)
            .map(|v| ByteBufB64(v).to_string())
            .ok()
    }

    /// Creates a new persisted single-field B-tree index.
    pub async fn new(field: Fe, storage: Storage, now_ms: u64) -> Result<Self, DBError> {
        let config = BTreeConfig {
            bucket_overload_size: storage.bucket_overload_size(),
            allow_duplicates: !field.unique(),
        };
        let field_name = field.name().to_string();
        BTree::inner_new(
            vec![field_name],
            &key_type_of(field.r#type()),
            config,
            storage,
            now_ms,
        )
        .await
    }

    /// Creates a persisted multi-field B-tree index.
    ///
    /// Multi-field indexes store a deterministic byte key composed from each
    /// configured field value.
    pub async fn with_virtual_field(
        fields: Vec<String>,
        storage: Storage,
        now_ms: u64,
    ) -> Result<Self, DBError> {
        if fields.len() < 2 {
            return Err(DBError::Index {
                name: fields.join("-"),
                source: "BTree::with_virtual_field: at least two fields are required".into(),
            });
        }
        let config = BTreeConfig {
            bucket_overload_size: storage.bucket_overload_size(),
            allow_duplicates: false,
        };
        BTree::inner_new(fields, &Ft::Bytes, config, storage, now_ms).await
    }

    /// Loads an existing B-tree index from persisted metadata and bucket objects.
    ///
    /// The type resolution here must mirror [`BTree::new`] exactly: a field
    /// type accepted at index creation must resolve to the same key type on
    /// reload, otherwise a collection with such an index can never be
    /// reopened. (Regression: `Map` fields were accepted by `new` but missing
    /// here, bricking collections on restart.) Both now share [`key_type_of`],
    /// plus one load-only widening: 0.10's `new` accepted any one-entry
    /// non-wildcard `Map` (the `{"a": Text}` shape of a one-field nested
    /// struct) and resolved it to the first key's own type, so persisted 0.10
    /// metadata can carry such an index. `new` now rejects the shape, but
    /// `bootstrap` must still load it — refusing here would make the
    /// collection unopenable, and removing the index requires an open
    /// collection.
    pub async fn bootstrap(name: String, ft: &Ft, storage: Storage) -> Result<Self, DBError> {
        let mut key_type = key_type_of(ft);
        if let Ft::Map(v) = &key_type
            && v.len() == 1
        {
            let legacy = v.keys().next().expect("length checked above").field_type();
            log::warn!(
                "BTree index {name:?}: legacy non-wildcard map field type resolved to \
                 {legacy:?} for load only; this index mis-indexes by field name — \
                 remove and recreate it on a supported field type"
            );
            key_type = legacy;
        }
        BTree::inner_bootstrap(name, &key_type, storage).await
    }

    async fn inner_new(
        fields: Vec<String>,
        ft: &Ft,
        config: BTreeConfig,
        storage: Storage,
        now_ms: u64,
    ) -> Result<Self, DBError> {
        let btree = match ft {
            Ft::U64 => BTree::U64(InnerBTree::new(fields, config, storage, now_ms).await?),
            Ft::I64 => BTree::I64(InnerBTree::new(fields, config, storage, now_ms).await?),
            Ft::Text => BTree::String(InnerBTree::new(fields, config, storage, now_ms).await?),
            Ft::Bytes => BTree::Bytes(InnerBTree::new(fields, config, storage, now_ms).await?),
            _ => {
                return Err(DBError::Index {
                    name: fields.join("-"),
                    source: format!("BTree: unsupported field type: {ft:?}").into(),
                });
            }
        };

        Ok(btree)
    }

    async fn inner_bootstrap(name: String, ft: &Ft, storage: Storage) -> Result<Self, DBError> {
        match ft {
            Ft::U64 => {
                let btree = InnerBTree::<u64>::bootstrap(name, storage).await?;
                Ok(BTree::U64(btree))
            }
            Ft::I64 => {
                let btree = InnerBTree::<i64>::bootstrap(name, storage).await?;
                Ok(BTree::I64(btree))
            }
            Ft::Text => {
                let btree = InnerBTree::<String>::bootstrap(name, storage).await?;
                Ok(BTree::String(btree))
            }
            Ft::Bytes => {
                let btree = InnerBTree::<Vec<u8>>::bootstrap(name, storage).await?;
                Ok(BTree::Bytes(btree))
            }
            _ => Err(DBError::Index {
                name,
                source: format!("BTree: unsupported field type: {ft:?}").into(),
            }),
        }
    }

    /// Returns the stable index name.
    pub fn name(&self) -> &str {
        match self {
            BTree::I64(btree) => &btree.name,
            BTree::U64(btree) => &btree.name,
            BTree::String(btree) => &btree.name,
            BTree::Bytes(btree) => &btree.name,
        }
    }

    /// Returns the physical fields represented by this index.
    pub fn virtual_field(&self) -> &[String] {
        match self {
            BTree::I64(btree) => &btree.fields,
            BTree::U64(btree) => &btree.fields,
            BTree::String(btree) => &btree.fields,
            BTree::Bytes(btree) => &btree.fields,
        }
    }

    /// Returns whether multiple documents may share the same indexed key.
    pub fn allow_duplicates(&self) -> bool {
        match self {
            BTree::I64(btree) => btree.index.allow_duplicates(),
            BTree::U64(btree) => btree.index.allow_duplicates(),
            BTree::String(btree) => btree.index.allow_duplicates(),
            BTree::Bytes(btree) => btree.index.allow_duplicates(),
        }
    }

    /// Returns a snapshot of B-tree runtime statistics.
    pub fn stats(&self) -> BTreeStats {
        match self {
            BTree::I64(btree) => btree.index.stats(),
            BTree::U64(btree) => btree.index.stats(),
            BTree::String(btree) => btree.index.stats(),
            BTree::Bytes(btree) => btree.index.stats(),
        }
    }

    /// Returns a snapshot of B-tree metadata.
    pub fn metadata(&self) -> BTreeMetadata {
        match self {
            BTree::I64(btree) => btree.index.metadata(),
            BTree::U64(btree) => btree.index.metadata(),
            BTree::String(btree) => btree.index.metadata(),
            BTree::Bytes(btree) => btree.index.metadata(),
        }
    }

    fn convert_array_values<FV, I>(&self, field_values: I) -> Result<Vec<FV>, DBError>
    where
        I: IntoIterator<Item = Fv>,
        FV: TryFrom<Fv, Error = BoxError>,
    {
        let name = self.name().to_string();
        field_values
            .into_iter()
            .map(|val| {
                FV::try_from(val).map_err(|source| DBError::Index {
                    name: name.clone(),
                    source,
                })
            })
            .collect()
    }

    /// Inserts an indexed value for `doc_id`.
    ///
    /// `Null` values are ignored. Array values are expanded into multiple keys,
    /// and map values index their keys.
    pub fn insert(
        &self,
        doc_id: DocumentId,
        field_value: &Fv,
        now_ms: u64,
    ) -> Result<bool, DBError> {
        if field_value == &Fv::Null {
            return Ok(false);
        }

        if let Fv::Array(vals) = field_value {
            return self
                .insert_array(doc_id, vals.clone(), now_ms)
                .map(|n| n > 0);
        } else if let Fv::Map(vals) = field_value {
            return self
                .insert_array(
                    doc_id,
                    vals.keys().map(|k| Fv::from(k.clone())).collect(),
                    now_ms,
                )
                .map(|n| n > 0);
        }

        match (&self, field_value) {
            (BTree::I64(btree), Fv::I64(val)) => btree
                .index
                .insert(doc_id, *val, now_ms)
                .map_err(DBError::from),
            // Defense in depth: a non-negative I64 field value is observed
            // as U64 when read back through generic CBOR (documents are
            // normalized at materialization, but hooks or older data may
            // still surface the read-back shape). Silently mismatching here
            // would corrupt the index (see the same arm in `remove`).
            (BTree::I64(btree), Fv::U64(val)) if *val <= i64::MAX as u64 => btree
                .index
                .insert(doc_id, *val as i64, now_ms)
                .map_err(DBError::from),
            (BTree::U64(btree), Fv::U64(val)) => btree
                .index
                .insert(doc_id, *val, now_ms)
                .map_err(DBError::from),
            (BTree::String(btree), Fv::Text(val)) => btree
                .index
                .insert(doc_id, val.clone(), now_ms)
                .map_err(DBError::from),
            (BTree::Bytes(btree), Fv::Bytes(val)) => btree
                .index
                .insert(doc_id, val.clone(), now_ms)
                .map_err(DBError::from),
            (_, v) => Err(DBError::Index {
                name: self.name().to_string(),
                source: format!("{:?}: field value type mismatch: {:?}", self, v).into(),
            }),
        }
    }

    fn insert_array(
        &self,
        doc_id: DocumentId,
        field_values: Vec<Fv>,
        now_ms: u64,
    ) -> Result<usize, DBError> {
        match &self {
            BTree::I64(btree) => {
                let values = self.convert_array_values::<i64, _>(field_values)?;
                btree
                    .index
                    .insert_array(doc_id, values, now_ms)
                    .map_err(DBError::from)
            }
            BTree::U64(btree) => {
                let values = self.convert_array_values::<u64, _>(field_values)?;
                btree
                    .index
                    .insert_array(doc_id, values, now_ms)
                    .map_err(DBError::from)
            }
            BTree::String(btree) => {
                let values = self.convert_array_values::<String, _>(field_values)?;
                btree
                    .index
                    .insert_array(doc_id, values, now_ms)
                    .map_err(DBError::from)
            }
            BTree::Bytes(btree) => {
                let values = self.convert_array_values::<Vec<u8>, _>(field_values)?;
                btree
                    .index
                    .insert_array(doc_id, values, now_ms)
                    .map_err(DBError::from)
            }
        }
    }

    /// Removes an indexed value for `doc_id`.
    pub fn remove(&self, doc_id: DocumentId, field_value: &Fv, now_ms: u64) -> bool {
        if field_value == &Fv::Null {
            return false;
        }

        if let Fv::Array(vals) = field_value {
            return self
                .remove_array(doc_id, vals.clone(), now_ms)
                .map(|n| n > 0)
                .unwrap_or_default();
        } else if let Fv::Map(vals) = field_value {
            return self
                .remove_array(
                    doc_id,
                    vals.keys().map(|k| Fv::from(k.clone())).collect(),
                    now_ms,
                )
                .map(|n| n > 0)
                .unwrap_or_default();
        }

        match (&self, field_value) {
            (BTree::I64(btree), Fv::I64(val)) => btree.index.remove(doc_id, *val, now_ms),
            // Tolerate the U64 read-back shape of a non-negative I64 value:
            // falling through to the `_ => false` arm would silently leak
            // the index entry (phantom matches after update/remove).
            (BTree::I64(btree), Fv::U64(val)) if *val <= i64::MAX as u64 => {
                btree.index.remove(doc_id, *val as i64, now_ms)
            }
            (BTree::U64(btree), Fv::U64(val)) => btree.index.remove(doc_id, *val, now_ms),
            (BTree::String(btree), Fv::Text(val)) => {
                btree.index.remove(doc_id, val.clone(), now_ms)
            }
            (BTree::Bytes(btree), Fv::Bytes(val)) => {
                btree.index.remove(doc_id, val.clone(), now_ms)
            }
            _ => false,
        }
    }

    /// Updates the indexed value for `doc_id`.
    ///
    /// Returns `true` if the index changed.
    pub fn update(
        &self,
        doc_id: DocumentId,
        old_value: &Fv,
        new_value: &Fv,
        now_ms: u64,
    ) -> Result<bool, DBError> {
        if self.values_equal(old_value, new_value) {
            return Ok(false);
        }

        if old_value == &Fv::Null {
            return self.insert(doc_id, new_value, now_ms);
        }

        if new_value == &Fv::Null {
            return Ok(self.remove(doc_id, old_value, now_ms));
        }

        if let Fv::Array(new_value) = new_value
            && let Fv::Array(old_value) = old_value
        {
            return self
                .batch_update(doc_id, old_value, new_value, now_ms)
                .map(|(r, i)| i > 0 || r > 0);
        } else if let Fv::Map(new_value) = new_value
            && let Fv::Map(old_value) = old_value
        {
            return self
                .batch_update(
                    doc_id,
                    &old_value
                        .keys()
                        .map(|k| Fv::from(k.clone()))
                        .collect::<Vec<_>>(),
                    &new_value
                        .keys()
                        .map(|k| Fv::from(k.clone()))
                        .collect::<Vec<_>>(),
                    now_ms,
                )
                .map(|(r, i)| i > 0 || r > 0);
        }

        let rt1 = self.insert(doc_id, new_value, now_ms)?;
        let rt2 = self.remove(doc_id, old_value, now_ms);
        Ok(rt1 || rt2)
    }

    /// Compares values after applying the scalar key canonicalization used by
    /// this index. In particular, generic CBOR read-back represents a
    /// non-negative I64 as U64, even though both variants address the same
    /// underlying `i64` posting.
    fn values_equal(&self, left: &Fv, right: &Fv) -> bool {
        if left == right {
            return true;
        }

        matches!(
            (self, left, right),
            (BTree::I64(_), Fv::I64(i), Fv::U64(u))
                | (BTree::I64(_), Fv::U64(u), Fv::I64(i))
                if *i >= 0 && *i as u64 == *u
        )
    }

    fn remove_array(
        &self,
        doc_id: DocumentId,
        field_values: Vec<Fv>,
        now_ms: u64,
    ) -> Result<usize, DBError> {
        match &self {
            BTree::I64(btree) => {
                let values = self.convert_array_values::<i64, _>(field_values)?;
                Ok(btree.index.remove_array(doc_id, values, now_ms))
            }
            BTree::U64(btree) => {
                let values = self.convert_array_values::<u64, _>(field_values)?;
                Ok(btree.index.remove_array(doc_id, values, now_ms))
            }
            BTree::String(btree) => {
                let values = self.convert_array_values::<String, _>(field_values)?;
                Ok(btree.index.remove_array(doc_id, values, now_ms))
            }
            BTree::Bytes(btree) => {
                let values = self.convert_array_values::<Vec<u8>, _>(field_values)?;
                Ok(btree.index.remove_array(doc_id, values, now_ms))
            }
        }
    }

    /// Applies an array-style batch update and returns `(removed, inserted)`.
    pub fn batch_update(
        &self,
        doc_id: DocumentId,
        old_field_values: &[Fv],
        new_field_values: &[Fv],
        now_ms: u64,
    ) -> Result<(usize, usize), DBError> {
        match &self {
            BTree::I64(btree) => {
                let old_field_values =
                    self.convert_array_values::<i64, _>(old_field_values.iter().cloned())?;
                let new_field_values =
                    self.convert_array_values::<i64, _>(new_field_values.iter().cloned())?;
                Ok(btree
                    .index
                    .batch_update(doc_id, old_field_values, new_field_values, now_ms)?)
            }
            BTree::U64(btree) => {
                let old_field_values =
                    self.convert_array_values::<u64, _>(old_field_values.iter().cloned())?;
                let new_field_values =
                    self.convert_array_values::<u64, _>(new_field_values.iter().cloned())?;
                Ok(btree
                    .index
                    .batch_update(doc_id, old_field_values, new_field_values, now_ms)?)
            }
            BTree::String(btree) => {
                let old_field_values =
                    self.convert_array_values::<String, _>(old_field_values.iter().cloned())?;
                let new_field_values =
                    self.convert_array_values::<String, _>(new_field_values.iter().cloned())?;
                Ok(btree
                    .index
                    .batch_update(doc_id, old_field_values, new_field_values, now_ms)?)
            }
            BTree::Bytes(btree) => {
                let old_field_values =
                    self.convert_array_values::<Vec<u8>, _>(old_field_values.iter().cloned())?;
                let new_field_values =
                    self.convert_array_values::<Vec<u8>, _>(new_field_values.iter().cloned())?;
                Ok(btree
                    .index
                    .batch_update(doc_id, old_field_values, new_field_values, now_ms)?)
            }
        }
    }

    /// Executes `f` with the document ids matching an exact key.
    pub fn query_with<F, R>(&self, field_value: &Fv, f: F) -> Option<R>
    where
        F: FnOnce(&Vec<DocumentId>) -> Option<R>,
    {
        match (self, field_value) {
            (BTree::I64(btree), Fv::I64(val)) => btree.index.query_with(val, f),
            // Tolerate the U64 read-back shape of a non-negative I64 value,
            // mirroring `insert` / `remove` (range queries already tolerate
            // it through `TryFrom<Fv> for i64`).
            (BTree::I64(btree), Fv::U64(val)) if *val <= i64::MAX as u64 => {
                btree.index.query_with(&(*val as i64), f)
            }
            (BTree::U64(btree), Fv::U64(val)) => btree.index.query_with(val, f),
            (BTree::String(btree), Fv::Text(val)) => btree.index.query_with(val, f),
            (BTree::Bytes(btree), Fv::Bytes(val)) => btree.index.query_with(val, f),
            _ => None,
        }
    }

    /// Runs a range query and feeds each matching id list to `f`.
    ///
    /// `f` returns `false` to stop the scan early. Unlike
    /// [`BTree::range_query_with`], this variant:
    ///
    /// - returns an error when the query value type does not match the index
    ///   key type (instead of silently returning an empty result), so a
    ///   mistyped filter is distinguishable from "no match";
    /// - does not materialize an owned key per hit (Text/Bytes keys are not
    ///   cloned), which matters on large range scans.
    ///
    /// `descending` picks which end of the matching range a scan that stops
    /// early keeps: `false` walks up from the smallest matching key, `true`
    /// walks down from the largest. Ids are delivered in ascending key order
    /// either way. The caller owns this choice — it cannot be inferred from
    /// the query shape without making the same predicate mean opposite things
    /// in different filter positions.
    pub fn try_range_query_ids<F>(
        &self,
        query: RangeQuery<Fv>,
        descending: bool,
        mut f: F,
    ) -> Result<(), DBError>
    where
        F: FnMut(&[DocumentId]) -> bool,
    {
        let type_error = |source: BoxError| DBError::Index {
            name: self.name().to_string(),
            source,
        };
        macro_rules! scan {
            ($index:expr, $q:expr) => {{
                let cb = |_: &_, pks: &Vec<DocumentId>| (f(pks), Vec::<()>::new());
                if descending {
                    $index.range_query_rev_with($q, cb);
                } else {
                    $index.range_query_with($q, cb);
                }
            }};
        }
        match self {
            BTree::I64(btree) => {
                let q = RangeQuery::<i64>::try_convert_from(query).map_err(type_error)?;
                scan!(btree.index, q);
            }
            BTree::U64(btree) => {
                let q = RangeQuery::<u64>::try_convert_from(query).map_err(type_error)?;
                scan!(btree.index, q);
            }
            BTree::String(btree) => {
                let q = RangeQuery::<String>::try_convert_from(query).map_err(type_error)?;
                scan!(btree.index, q);
            }
            BTree::Bytes(btree) => {
                let q = RangeQuery::<Vec<u8>>::try_convert_from(query).map_err(type_error)?;
                scan!(btree.index, q);
            }
        }
        Ok(())
    }

    /// Runs a range query and maps each matching key/id-list pair through `f`.
    ///
    /// A query value type that does not match the index key type yields an
    /// empty result; use [`BTree::try_range_query_ids`] to surface the
    /// mismatch as an error.
    pub fn range_query_with<F, R>(&self, query: RangeQuery<Fv>, mut f: F) -> Vec<R>
    where
        F: FnMut(Fv, &Vec<DocumentId>) -> (bool, Vec<R>),
    {
        match self {
            BTree::I64(btree) => match RangeQuery::<i64>::try_convert_from(query) {
                Ok(q) => btree
                    .index
                    .range_query_with(q, |fv, pks| f(Fv::I64(*fv), pks)),
                Err(_) => {
                    vec![]
                }
            },
            BTree::U64(btree) => match RangeQuery::<u64>::try_convert_from(query) {
                Ok(q) => btree
                    .index
                    .range_query_with(q, |fv, pks| f(Fv::U64(*fv), pks)),
                Err(_) => {
                    vec![]
                }
            },
            BTree::String(btree) => match RangeQuery::<String>::try_convert_from(query) {
                Ok(q) => btree
                    .index
                    .range_query_with(q, |fv, pks| f(Fv::Text(fv.to_owned()), pks)),
                Err(_) => {
                    vec![]
                }
            },
            BTree::Bytes(btree) => match RangeQuery::<Vec<u8>>::try_convert_from(query) {
                Ok(q) => btree
                    .index
                    .range_query_with(q, |fv, pks| f(Fv::Bytes(fv.clone()), pks)),
                Err(_) => {
                    vec![]
                }
            },
        }
    }

    /// Returns index keys after `cursor`, limited by `limit` when provided.
    pub fn keys(&self, cursor: Option<String>, limit: Option<usize>) -> Vec<Fv> {
        match self {
            BTree::I64(btree) => match Self::from_cursor(&cursor) {
                Err(_) => vec![],
                Ok(cursor) => btree
                    .index
                    .keys(cursor, limit)
                    .into_iter()
                    .map(Fv::I64)
                    .collect(),
            },
            BTree::U64(btree) => match Self::from_cursor(&cursor) {
                Err(_) => vec![],
                Ok(cursor) => btree
                    .index
                    .keys(cursor, limit)
                    .into_iter()
                    .map(Fv::U64)
                    .collect(),
            },
            BTree::String(btree) => match Self::from_cursor(&cursor) {
                Err(_) => vec![],
                Ok(cursor) => btree
                    .index
                    .keys(cursor, limit)
                    .into_iter()
                    .map(Fv::Text)
                    .collect(),
            },
            BTree::Bytes(btree) => match Self::from_cursor(&cursor) {
                Err(_) => vec![],
                Ok(cursor) => btree
                    .index
                    .keys(cursor, limit)
                    .into_iter()
                    .map(Fv::Bytes)
                    .collect(),
            },
        }
    }

    /// Compacts bucket layout and persists the new layout if the bucket count
    /// shrinks.
    ///
    /// Under the manifest protocol compaction needs no special write
    /// ordering: the repacked layout becomes visible atomically with the
    /// manifest commit of the following flush, which also retires every
    /// pre-compaction bucket object best-effort.
    pub async fn compact_index(&self) -> Result<(), DBError> {
        match self {
            BTree::I64(btree) => btree.compact().await,
            BTree::U64(btree) => btree.compact().await,
            BTree::String(btree) => btree.compact().await,
            BTree::Bytes(btree) => btree.compact().await,
        }
    }

    /// Persists dirty metadata and buckets.
    ///
    /// Returns `true` when any object was written.
    pub async fn flush(&self, now_ms: u64) -> Result<bool, DBError> {
        match self {
            BTree::I64(btree) => btree.flush(now_ms).await,
            BTree::U64(btree) => btree.flush(now_ms).await,
            BTree::String(btree) => btree.flush(now_ms).await,
            BTree::Bytes(btree) => btree.flush(now_ms).await,
        }
    }

    /// Returns whether metadata or buckets have in-memory changes to flush.
    pub fn has_pending_flush(&self) -> bool {
        match self {
            BTree::I64(btree) => btree.has_pending_flush(),
            BTree::U64(btree) => btree.has_pending_flush(),
            BTree::String(btree) => btree.has_pending_flush(),
            BTree::Bytes(btree) => btree.has_pending_flush(),
        }
    }

    pub(crate) async fn drop_data(&self) {
        let rt = match self {
            BTree::I64(btree) => btree.drop_data().await,
            BTree::U64(btree) => btree.drop_data().await,
            BTree::String(btree) => btree.drop_data().await,
            BTree::Bytes(btree) => btree.drop_data().await,
        };

        if let Err(err) = rt {
            log::warn!(
                action = "BTree::drop_data",
                index = self.name();
                "Failed to drop BTree index data: {err:?}",
            );
        }
    }
}

impl<FV> InnerBTree<FV>
where
    FV: Eq + Ord + Hash + Debug + Clone + Serialize + DeserializeOwned,
{
    async fn new(
        fields: Vec<String>,
        config: BTreeConfig,
        storage: Storage,
        now_ms: u64,
    ) -> Result<Self, DBError> {
        let name = fields.join("-");
        let index = BTreeIndex::new(name.clone(), Some(config));
        let mut data = Vec::new();
        index
            .flush(&mut data, now_ms, |_, _| std::future::ready(Ok(())))
            .await?;
        // The collection metadata is the source of truth for which indexes
        // exist, so overwrite any leftover files from a crashed creation or a
        // previously removed index instead of failing with AlreadyExists.
        let ver = storage
            .put_bytes(
                &BTree::metadata_path(&name),
                data.into(),
                PutMode::Overwrite,
            )
            .await?;
        Ok(InnerBTree {
            name,
            fields,
            index,
            storage,
            metadata_version: Arc::new(RwLock::new(ver)),
            flush_gate: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    async fn drop_data(&self) -> Result<(), DBError> {
        // Delete the metadata and all bucket objects under the index directory.
        self.storage.drop_prefix(&BTree::dir_path(&self.name)).await
    }

    async fn bootstrap(name: String, storage: Storage) -> Result<Self, DBError> {
        let fields: Vec<String> = from_virtual_field_name(&name);
        let path = BTree::metadata_path(&name);
        let (metadata, ver) = storage.fetch_bytes(&path).await?;
        let n = Arc::new(name.clone());
        let s = Arc::new(storage.clone());
        let index = BTreeIndex::<DocumentId, FV>::load_all(&metadata[..], async move |object| {
            let path = BTree::bucket_path(n.clone().as_str(), object);
            match s.clone().fetch_bytes(&path).await {
                Ok((data, _)) => Ok(Some(data.into())),
                Err(DBError::NotFound { .. }) => Ok(None),
                Err(e) => Err(e.into()),
            }
        })
        .await?;

        Ok(Self {
            name,
            fields,
            index,
            storage,
            metadata_version: Arc::new(RwLock::new(ver)),
            flush_gate: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// Delegates the complete persistence transaction to the low-level index,
    /// which writes every dirty bucket to a fresh generation-suffixed object
    /// and then commits the manifest-bearing metadata with a single
    /// conditional PUT. Objects the new manifest no longer references are
    /// deleted best-effort afterwards.
    async fn flush(&self, now_ms: u64) -> Result<bool, DBError> {
        let _flush_guard = self.flush_gate.clone().lock_owned().await;
        self.flush_inner(now_ms).await
    }

    /// The sole production persistence path. Callers must hold `flush_gate`.
    async fn flush_inner(&self, now_ms: u64) -> Result<bool, DBError> {
        let metadata_path = BTree::metadata_path(&self.name);
        let metadata_storage = self.storage.clone();
        let metadata_version = self.metadata_version.clone();
        let bucket_storage = self.storage.clone();
        let bucket_name = self.name.clone();
        let outcome = self
            .index
            .flush_owned_with(
                now_ms,
                move |data: Vec<u8>| async move {
                    // The manifest commit. A single conditional PUT is the
                    // remaining second-writer defense; a `Precondition`
                    // conflict propagates and the collection poisons its
                    // handle (recovery is a reopen).
                    let expected = { metadata_version.read().clone() };
                    let version = metadata_storage
                        .put_bytes(
                            &metadata_path,
                            Bytes::from(data),
                            PutMode::Update(expected.into()),
                        )
                        .await
                        .map_err(BoxError::from)?;
                    *metadata_version.write() = version;
                    Ok(())
                },
                move |object, data: Vec<u8>| {
                    let storage = bucket_storage.clone();
                    let name = bucket_name.clone();
                    async move {
                        let path = BTree::bucket_path(&name, object);
                        let _ = storage
                            .put_bytes(&path, Bytes::from(data), PutMode::Overwrite)
                            .await?;
                        Ok(())
                    }
                },
            )
            .await?;

        // Best-effort retirement of bucket objects the committed manifest no
        // longer references. A failed deletion only leaks storage space and
        // never affects loads: the manifest is the loader's single source of
        // truth.
        for object in &outcome.obsolete {
            let path = BTree::bucket_path(&self.name, *object);
            match self.storage.delete(&path).await {
                Ok(()) | Err(DBError::NotFound { .. }) => {}
                Err(err) => {
                    log::warn!(
                        action = "BTree::flush",
                        index = self.name,
                        bucket = object.bucket_id,
                        generation = object.generation;
                        "Failed to delete obsolete bucket object: {err:?}",
                    );
                }
            }
        }

        Ok(outcome.saved)
    }

    /// See [`BTree::compact_index`] for the persistence rationale.
    async fn compact(&self) -> Result<(), DBError> {
        let _flush_guard = self.flush_gate.clone().lock_owned().await;
        let (old_bucket_count, new_bucket_count) = self.index.compact_buckets();
        if new_bucket_count >= old_bucket_count {
            return Ok(());
        }

        log::warn!(
            "Compacted BTree index '{}': {} -> {} buckets",
            self.name,
            old_bucket_count,
            new_bucket_count
        );

        // Delegate to the same coordinated persistence path used by normal
        // production flushes; it commits the manifest and deletes the
        // replaced objects.
        self.flush_inner(unix_ms()).await?;

        Ok(())
    }

    fn has_pending_flush(&self) -> bool {
        if self.index.has_dirty_buckets() {
            return true;
        }

        self.index.has_pending_metadata_flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageConfig;
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
        memory::InMemory, path::Path,
    };
    use parking_lot::Mutex as ParkingMutex;
    use std::{
        collections::BTreeMap,
        fmt,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    /// Delegating in-memory store with a deterministic bucket-PUT failpoint.
    /// Metadata writes are recorded too, so tests can assert the exact
    /// production-wrapper ordering around the injected crash boundary.
    #[derive(Debug)]
    struct FailNthBucketPutStore {
        inner: Arc<InMemory>,
        armed: AtomicBool,
        bucket_puts: AtomicUsize,
        fail_at: usize,
        events: ParkingMutex<Vec<String>>,
    }

    impl FailNthBucketPutStore {
        fn new(fail_at: usize) -> Self {
            Self {
                inner: Arc::new(InMemory::new()),
                armed: AtomicBool::new(false),
                bucket_puts: AtomicUsize::new(0),
                fail_at,
                events: ParkingMutex::new(Vec::new()),
            }
        }

        fn arm(&self) {
            self.bucket_puts.store(0, Ordering::Release);
            self.events.lock().clear();
            self.armed.store(true, Ordering::Release);
        }

        fn events(&self) -> Vec<String> {
            self.events.lock().clone()
        }
    }

    impl fmt::Display for FailNthBucketPutStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("FailNthBucketPutStore")
        }
    }

    #[async_trait]
    impl ObjectStore for FailNthBucketPutStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            let path = location.to_string();
            if self.armed.load(Ordering::Acquire) && path.contains("btree_indexes/fault_tree/") {
                self.events.lock().push(path.clone());
                if path.contains("/b_")
                    && self.bucket_puts.fetch_add(1, Ordering::AcqRel) + 1 == self.fail_at
                {
                    return Err(object_store::Error::Generic {
                        store: "fail_nth_btree_bucket_put",
                        source: "injected bucket PUT failure".into(),
                    });
                }
            }
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> ObjectStoreResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, ObjectStoreResult<Path>>,
        ) -> BoxStream<'static, ObjectStoreResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    async fn test_storage() -> Storage {
        Storage::connect(
            "btree_wrapper_tests".to_string(),
            Arc::new(InMemory::new()),
            StorageConfig::default(),
        )
        .await
        .unwrap()
    }

    fn field(name: &str, ft: Ft) -> Fe {
        Fe::new(name.to_string(), ft).unwrap()
    }

    #[tokio::test]
    async fn debug_eq_and_type_branches_are_covered() {
        let storage = test_storage().await;
        let now = unix_ms();

        let i64_tree = BTree::new(field("i64_field", Ft::I64), storage.clone(), now)
            .await
            .unwrap();
        let u64_tree = BTree::new(field("u64_field", Ft::U64), storage.clone(), now)
            .await
            .unwrap();
        let text_tree = BTree::new(field("text_field", Ft::Text), storage.clone(), now)
            .await
            .unwrap();
        let bytes_tree = BTree::new(field("bytes_field", Ft::Bytes), storage.clone(), now)
            .await
            .unwrap();

        assert_eq!(format!("{i64_tree:?}"), "BTreeIndex<I64>(i64_field)");
        assert_eq!(format!("{u64_tree:?}"), "BTreeIndex<U64>(u64_field)");
        assert_eq!(format!("{text_tree:?}"), "BTreeIndex<String>(text_field)");
        assert_eq!(format!("{bytes_tree:?}"), "BTreeIndex<Bytes>(bytes_field)");

        assert_eq!(&i64_tree, &i64_tree);
        assert_eq!(&u64_tree, &u64_tree);
        assert_eq!(&text_tree, &text_tree);
        assert_eq!(&bytes_tree, &bytes_tree);
        assert_ne!(&i64_tree, &u64_tree);

        assert!(i64_tree.insert(1, &Fv::I64(-7), now).unwrap());
        assert!(u64_tree.insert(1, &Fv::U64(7), now).unwrap());
        assert!(text_tree.insert(1, &Fv::Text("alpha".into()), now).unwrap());
        assert!(bytes_tree.insert(1, &Fv::Bytes(vec![1, 2]), now).unwrap());

        assert_eq!(
            i64_tree.query_with(&Fv::I64(-7), |ids| Some(ids.clone())),
            Some(vec![1])
        );
        assert_eq!(
            u64_tree.query_with(&Fv::U64(7), |ids| Some(ids.clone())),
            Some(vec![1])
        );
        assert_eq!(
            text_tree.query_with(&Fv::Text("alpha".into()), |ids| Some(ids.clone())),
            Some(vec![1])
        );
        assert_eq!(
            bytes_tree.query_with(&Fv::Bytes(vec![1, 2]), |ids| Some(ids.clone())),
            Some(vec![1])
        );

        assert_eq!(
            i64_tree.query_with(&Fv::Text("bad".into()), |_| Some(())),
            None
        );
        assert!(i64_tree.insert(2, &Fv::Text("bad".into()), now).is_err());
        assert!(!i64_tree.remove(2, &Fv::Text("bad".into()), now));

        assert!(i64_tree.flush(now + 1).await.unwrap());
        assert!(!i64_tree.has_pending_flush());
        let reloaded = BTree::bootstrap("i64_field".into(), &Ft::I64, storage.clone())
            .await
            .unwrap();
        assert_eq!(
            reloaded.query_with(&Fv::I64(-7), |ids| Some(ids.clone())),
            Some(vec![1])
        );
    }

    #[tokio::test]
    async fn option_array_map_and_error_branches_are_covered() {
        let storage = test_storage().await;
        let now = unix_ms();

        let option_array = BTree::new(
            field(
                "option_array",
                Ft::Option(Box::new(Ft::Array(vec![Ft::U64]))),
            ),
            storage.clone(),
            now,
        )
        .await
        .unwrap();
        assert!(matches!(option_array, BTree::U64(_)));

        // Only *wildcard* maps are indexable; `{"k": Text}` used to resolve
        // here too (see `non_wildcard_maps_are_rejected_not_indexed_by_name`).
        let option_map = BTree::new(
            field(
                "option_map",
                Ft::Option(Box::new(Ft::Map(BTreeMap::from([("*".into(), Ft::Text)])))),
            ),
            storage.clone(),
            now,
        )
        .await
        .unwrap();
        assert!(matches!(option_map, BTree::String(_)));

        let map_tree = BTree::new(
            field(
                "map_field",
                Ft::Map(BTreeMap::from([(b"*".to_vec().into(), Ft::Bytes)])),
            ),
            storage.clone(),
            now,
        )
        .await
        .unwrap();
        assert!(matches!(map_tree, BTree::Bytes(_)));

        let option_plain = BTree::new(
            field("option_plain", Ft::Option(Box::new(Ft::I64))),
            storage.clone(),
            now,
        )
        .await
        .unwrap();
        assert!(matches!(option_plain, BTree::I64(_)));

        let unsupported = BTree::new(field("unsupported", Ft::Bool), storage.clone(), now).await;
        assert!(matches!(unsupported, Err(DBError::Index { .. })));

        let bad_virtual = BTree::with_virtual_field(vec!["only_one".into()], storage.clone(), now)
            .await
            .unwrap_err();
        assert!(matches!(bad_virtual, DBError::Index { .. }));

        assert!(BTree::from_cursor::<u64>(&Some("not-base64".into())).is_err());
        let invalid_cbor = ByteBufB64(vec![0xff]).to_string();
        assert!(BTree::from_cursor::<u64>(&Some(invalid_cbor)).is_err());

        let cursor = BTree::to_cursor(&123_u64).unwrap();
        assert_eq!(BTree::from_cursor::<u64>(&Some(cursor)).unwrap(), Some(123));
        assert_eq!(BTree::from_cursor::<u64>(&None).unwrap(), None);
    }

    #[tokio::test]
    async fn array_and_map_updates_cover_all_value_variants() {
        let storage = test_storage().await;
        let now = unix_ms();

        let i64_tree = BTree::new(
            field("i64_array", Ft::Array(vec![Ft::I64])),
            storage.clone(),
            now,
        )
        .await
        .unwrap();
        let bytes_tree = BTree::new(
            field("bytes_array", Ft::Array(vec![Ft::Bytes])),
            storage.clone(),
            now,
        )
        .await
        .unwrap();

        assert!(
            i64_tree
                .insert(1, &Fv::Array(vec![Fv::I64(-1), Fv::I64(2)]), now)
                .unwrap()
        );
        assert!(
            bytes_tree
                .insert(
                    1,
                    &Fv::Array(vec![Fv::Bytes(vec![1]), Fv::Bytes(vec![2])]),
                    now,
                )
                .unwrap()
        );

        assert!(
            i64_tree
                .update(
                    1,
                    &Fv::Array(vec![Fv::I64(-1), Fv::I64(2)]),
                    &Fv::Array(vec![Fv::I64(3)]),
                    now + 1,
                )
                .unwrap()
        );
        assert!(
            bytes_tree
                .batch_update(
                    1,
                    &[Fv::Bytes(vec![1]), Fv::Bytes(vec![2])],
                    &[Fv::Bytes(vec![3])],
                    now + 1,
                )
                .unwrap()
                .0
                > 0
        );

        assert!(i64_tree.remove(1, &Fv::Array(vec![Fv::I64(3)]), now + 2));
        assert!(bytes_tree.remove(1, &Fv::Array(vec![Fv::Bytes(vec![3])]), now + 2));

        assert!(
            i64_tree
                .insert(2, &Fv::Array(vec![Fv::Text("bad".into())]), now)
                .is_err()
        );
        assert!(
            bytes_tree
                .batch_update(2, &[Fv::Bytes(vec![1])], &[Fv::Text("bad".into())], now,)
                .is_err()
        );
    }

    #[tokio::test]
    async fn all_variants_expose_metadata_queries_flush_and_drop() {
        let storage = Storage::connect(
            "btree_all_variants".to_string(),
            Arc::new(InMemory::new()),
            StorageConfig {
                bucket_overload_size: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let now = unix_ms();

        let i64_tree = BTree::new(field("i64_all", Ft::I64), storage.clone(), now)
            .await
            .unwrap();
        let u64_tree = BTree::new(field("u64_all", Ft::U64), storage.clone(), now)
            .await
            .unwrap();
        let text_tree = BTree::new(field("text_all", Ft::Text), storage.clone(), now)
            .await
            .unwrap();
        let bytes_tree = BTree::new(field("bytes_all", Ft::Bytes), storage.clone(), now)
            .await
            .unwrap();

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        std::hash::Hash::hash(&&i64_tree, &mut hasher);
        std::hash::Hash::hash(&&u64_tree, &mut hasher);
        std::hash::Hash::hash(&&text_tree, &mut hasher);
        std::hash::Hash::hash(&&bytes_tree, &mut hasher);

        assert!(i64_tree.allow_duplicates());
        assert!(u64_tree.allow_duplicates());
        assert!(text_tree.allow_duplicates());
        assert!(bytes_tree.allow_duplicates());
        assert_eq!(i64_tree.virtual_field(), &["i64_all".to_string()]);
        assert_eq!(u64_tree.virtual_field(), &["u64_all".to_string()]);
        assert_eq!(text_tree.virtual_field(), &["text_all".to_string()]);
        assert_eq!(bytes_tree.virtual_field(), &["bytes_all".to_string()]);

        for id in 1..=8 {
            assert!(i64_tree.insert(id, &Fv::I64(-(id as i64)), now).unwrap());
            assert!(u64_tree.insert(id, &Fv::U64(id), now).unwrap());
            assert!(
                text_tree
                    .insert(id, &Fv::Text(format!("k{id}")), now)
                    .unwrap()
            );
            assert!(
                bytes_tree
                    .insert(id, &Fv::Bytes(vec![id as u8]), now)
                    .unwrap()
            );
        }

        assert!(
            u64_tree
                .insert(30, &Fv::Array(vec![Fv::U64(30), Fv::U64(31)]), now)
                .unwrap()
        );
        assert!(
            text_tree
                .insert(
                    31,
                    &Fv::Array(vec![Fv::Text("array-a".into()), Fv::Text("array-b".into())]),
                    now,
                )
                .unwrap()
        );
        assert!(u64_tree.remove(30, &Fv::Array(vec![Fv::U64(30)]), now));
        assert!(text_tree.remove(31, &Fv::Array(vec![Fv::Text("array-a".into())]), now));
        assert!(!text_tree.remove(31, &Fv::Null, now));
        assert!(bytes_tree.remove(
            8,
            &Fv::Map(BTreeMap::from([(vec![8_u8].into(), Fv::U64(1))])),
            now,
        ));
        assert_eq!(
            u64_tree
                .batch_update(40, &[Fv::U64(1)], &[Fv::U64(2), Fv::U64(3)], now)
                .unwrap(),
            (0, 2)
        );

        assert!(!i64_tree.insert(99, &Fv::Null, now).unwrap());
        assert!(!i64_tree.update(1, &Fv::I64(-1), &Fv::I64(-1), now).unwrap());
        assert!(i64_tree.update(20, &Fv::Null, &Fv::I64(-20), now).unwrap());
        assert!(i64_tree.update(20, &Fv::I64(-20), &Fv::Null, now).unwrap());
        assert!(
            i64_tree
                .update(1, &Fv::I64(-1), &Fv::I64(-10), now)
                .unwrap()
        );

        assert_eq!(i64_tree.stats().num_elements, 8);
        assert!(u64_tree.stats().num_elements >= 8);
        assert!(text_tree.stats().num_elements >= 8);
        assert!(bytes_tree.stats().num_elements >= 7);
        assert_eq!(i64_tree.metadata().name, "i64_all");
        assert_eq!(u64_tree.metadata().name, "u64_all");
        assert_eq!(text_tree.metadata().name, "text_all");
        assert_eq!(bytes_tree.metadata().name, "bytes_all");

        let i64_hits = i64_tree.range_query_with(RangeQuery::Le(Fv::I64(-3)), |key, ids| {
            (true, vec![(key, ids.clone())])
        });
        assert!(!i64_hits.is_empty());
        let u64_hits = u64_tree
            .range_query_with(RangeQuery::Between(Fv::U64(3), Fv::U64(5)), |key, ids| {
                (true, vec![(key, ids.clone())])
            });
        assert_eq!(u64_hits.len(), 3);
        let text_hits = text_tree
            .range_query_with(RangeQuery::Ge(Fv::Text("k6".to_string())), |key, ids| {
                (true, vec![(key, ids.clone())])
            });
        assert!(!text_hits.is_empty());
        let bytes_hits = bytes_tree
            .range_query_with(RangeQuery::Gt(Fv::Bytes(vec![4])), |key, ids| {
                (ids[0] < 7, vec![(key, ids.clone())])
            });
        assert!(!bytes_hits.is_empty());

        assert!(
            i64_tree
                .range_query_with(RangeQuery::Eq(Fv::Text("bad".into())), |_, _| {
                    (true, Vec::<()>::new())
                })
                .is_empty()
        );
        assert!(
            u64_tree
                .range_query_with(RangeQuery::Eq(Fv::Text("bad".into())), |_, _| {
                    (true, Vec::<()>::new())
                })
                .is_empty()
        );
        assert!(
            text_tree
                .range_query_with(RangeQuery::Eq(Fv::U64(1)), |_, _| {
                    (true, Vec::<()>::new())
                })
                .is_empty()
        );
        assert!(
            bytes_tree
                .range_query_with(RangeQuery::Eq(Fv::U64(1)), |_, _| {
                    (true, Vec::<()>::new())
                })
                .is_empty()
        );

        assert_eq!(i64_tree.keys(None, Some(2)).len(), 2);
        assert_eq!(u64_tree.keys(None, Some(2)).len(), 2);
        assert_eq!(text_tree.keys(None, Some(2)).len(), 2);
        assert_eq!(bytes_tree.keys(None, Some(2)).len(), 2);
        assert!(
            i64_tree
                .keys(Some("bad-cursor".to_string()), Some(2))
                .is_empty()
        );
        assert!(
            text_tree
                .keys(Some("bad-cursor".to_string()), Some(2))
                .is_empty()
        );
        assert!(
            bytes_tree
                .keys(Some("bad-cursor".to_string()), Some(2))
                .is_empty()
        );

        assert!(i64_tree.has_pending_flush());
        assert!(u64_tree.has_pending_flush());
        assert!(text_tree.has_pending_flush());
        assert!(bytes_tree.has_pending_flush());
        assert!(i64_tree.flush(now + 1).await.unwrap());
        assert!(u64_tree.flush(now + 1).await.unwrap());
        assert!(text_tree.flush(now + 1).await.unwrap());
        assert!(bytes_tree.flush(now + 1).await.unwrap());
        assert!(!i64_tree.flush(now + 1).await.unwrap());

        assert!(
            BTree::bootstrap(
                "option_boot".to_string(),
                &Ft::Option(Box::new(Ft::Bool)),
                storage.clone()
            )
            .await
            .is_err()
        );
        let _ = BTree::bootstrap(
            "u64_all".to_string(),
            &Ft::Option(Box::new(Ft::Array(vec![Ft::U64]))),
            storage.clone(),
        )
        .await
        .unwrap();
        let _ = BTree::bootstrap("i64_all".to_string(), &Ft::I64, storage.clone())
            .await
            .unwrap();
        let _ = BTree::bootstrap("u64_all".to_string(), &Ft::U64, storage.clone())
            .await
            .unwrap();
        let _ = BTree::bootstrap("text_all".to_string(), &Ft::Text, storage.clone())
            .await
            .unwrap();
        let _ = BTree::bootstrap("bytes_all".to_string(), &Ft::Bytes, storage.clone())
            .await
            .unwrap();

        i64_tree.compact_index().await.unwrap();
        u64_tree.compact_index().await.unwrap();
        text_tree.compact_index().await.unwrap();
        bytes_tree.compact_index().await.unwrap();
        i64_tree.drop_data().await;
        u64_tree.drop_data().await;
        text_tree.drop_data().await;
        bytes_tree.drop_data().await;
    }

    /// Regression (#1/#9): a non-negative I64 field value is observed as U64
    /// when read back through generic CBOR. The scalar insert/remove/query
    /// paths of an I64 index must tolerate that shape instead of erroring
    /// (insert) or silently doing nothing (remove), which leaked stale index
    /// entries on update/remove and broke backfill of existing data.
    #[tokio::test]
    async fn i64_index_tolerates_u64_read_back_shape() {
        let storage = test_storage().await;
        let now = unix_ms();
        let tree = BTree::new(field("i64_rb", Ft::I64), storage.clone(), now)
            .await
            .unwrap();

        // Insert with the read-back shape, query with the canonical one.
        assert!(tree.insert(1, &Fv::U64(5), now).unwrap());
        assert_eq!(
            tree.query_with(&Fv::I64(5), |ids| Some(ids.clone())),
            Some(vec![1])
        );
        // Query with the read-back shape too.
        assert_eq!(
            tree.query_with(&Fv::U64(5), |ids| Some(ids.clone())),
            Some(vec![1])
        );

        // Update from the read-back shape to a new value must retire the old
        // entry.
        assert!(tree.update(1, &Fv::U64(5), &Fv::I64(7), now + 1).unwrap());
        assert_eq!(tree.query_with(&Fv::I64(5), |ids| Some(ids.clone())), None);
        assert_eq!(
            tree.query_with(&Fv::I64(7), |ids| Some(ids.clone())),
            Some(vec![1])
        );

        // Remove with the read-back shape.
        assert!(tree.remove(1, &Fv::U64(7), now + 2));
        assert_eq!(tree.query_with(&Fv::I64(7), |ids| Some(ids.clone())), None);
        assert_eq!(tree.stats().num_elements, 0);

        // Out-of-range U64 is still a type mismatch, not a silent wrap.
        assert!(tree.insert(2, &Fv::U64(i64::MAX as u64 + 1), now).is_err());
        assert!(!tree.remove(2, &Fv::U64(i64::MAX as u64 + 1), now));
    }

    #[tokio::test]
    async fn i64_update_same_numeric_key_across_variants_is_a_noop() {
        let storage = test_storage().await;
        let now = unix_ms();
        let tree = BTree::new(field("i64_same", Ft::I64), storage, now)
            .await
            .unwrap();

        assert!(tree.insert(1, &Fv::I64(5), now).unwrap());
        assert!(!tree.update(1, &Fv::U64(5), &Fv::I64(5), now + 1).unwrap());
        assert_eq!(
            tree.query_with(&Fv::I64(5), |ids| Some(ids.clone())),
            Some(vec![1])
        );

        assert!(tree.insert(2, &Fv::U64(6), now + 2).unwrap());
        assert!(!tree.update(2, &Fv::I64(6), &Fv::U64(6), now + 3).unwrap());
        assert_eq!(
            tree.query_with(&Fv::I64(6), |ids| Some(ids.clone())),
            Some(vec![2])
        );
        assert_eq!(tree.stats().num_elements, 2);
    }

    /// Crash-window test for the manifest protocol: a bucket PUT failure in
    /// the middle of a flush (some new-generation objects durable, some not)
    /// must prevent the manifest commit entirely, so a reopen sees the
    /// previous complete snapshot; a retry then converges.
    #[tokio::test]
    async fn wrapper_fault_mid_bucket_puts_keeps_previous_snapshot() {
        let object_store = Arc::new(FailNthBucketPutStore::new(2));
        let storage = Storage::connect(
            "btree_wrapper_fault".to_string(),
            object_store.clone(),
            StorageConfig {
                compress_level: 0,
                bucket_overload_size: 80,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let now = unix_ms();
        let tree = BTree::new(field("fault_tree", Ft::Text), storage, now)
            .await
            .unwrap();

        tree.insert(1, &Fv::Text("apple".into()), now).unwrap();
        tree.flush(now + 1).await.unwrap();

        let mut next_id = 2;
        while tree.stats().max_bucket_id == 0 {
            tree.insert(next_id, &Fv::Text("apple".into()), now + 2)
                .unwrap();
            next_id += 1;
        }

        // Fail the second bucket PUT: one new-generation object becomes a
        // durable orphan, the other is never written, the manifest commit
        // must not happen.
        object_store.arm();
        assert!(tree.flush(now + 3).await.is_err());

        let events = object_store.events();
        assert!(
            events.iter().filter(|path| path.contains("/b_")).count() >= 2,
            "expected at least two bucket PUT attempts: {events:?}"
        );
        assert!(
            !events.iter().any(|path| path.ends_with("meta.cbor")),
            "the manifest must not be committed when a bucket write fails: {events:?}"
        );

        // Simulate process loss: reconnect with a fresh Storage/cache. The
        // old manifest still references only the committed snapshot; the
        // orphaned new-generation object is invisible.
        let reopened_storage = Storage::connect(
            "btree_wrapper_fault".to_string(),
            object_store.clone(),
            StorageConfig::default(),
        )
        .await
        .unwrap();
        let reloaded = BTree::bootstrap(
            "fault_tree".to_string(),
            &Ft::Text,
            reopened_storage.clone(),
        )
        .await
        .unwrap();
        let ids = reloaded
            .query_with(&Fv::Text("apple".into()), |ids| Some(ids.clone()))
            .unwrap();
        assert_eq!(
            ids,
            vec![1],
            "only the committed snapshot may be visible after the crash"
        );
        drop(reloaded);

        // The retried flush (fail point exhausted) commits one complete new
        // snapshot; a reopen sees every posting.
        tree.flush(now + 4).await.unwrap();
        let recovered_storage = Storage::connect(
            "btree_wrapper_fault".to_string(),
            object_store,
            StorageConfig::default(),
        )
        .await
        .unwrap();
        let recovered = BTree::bootstrap("fault_tree".to_string(), &Ft::Text, recovered_storage)
            .await
            .unwrap();
        let ids = recovered
            .query_with(&Fv::Text("apple".into()), |ids| Some(ids.clone()))
            .unwrap();
        assert_eq!(ids.len(), (next_id - 1) as usize);
        for id in 1..next_id {
            assert!(ids.contains(&id));
        }
    }

    /// The wrapper's metadata CAS token is the last defense against a second
    /// writer: after a foreign overwrite the next flush must fail instead of
    /// being silently reconciled in place. Recovery is a reopen, which reads
    /// the durable objects and their fresh versions.
    #[tokio::test]
    async fn foreign_metadata_writer_fails_flush() {
        let storage = test_storage().await;
        let now = unix_ms();
        let tree = BTree::new(field("conflict_tree", Ft::Text), storage.clone(), now)
            .await
            .unwrap();
        tree.insert(1, &Fv::Text("apple".into()), now + 1).unwrap();
        assert!(tree.flush(now + 2).await.unwrap());

        // Simulate a second writer replacing the durable metadata object.
        let path = BTree::metadata_path("conflict_tree");
        let (data, _) = storage.fetch_bytes(&path).await.unwrap();
        storage
            .put_bytes(&path, data, PutMode::Overwrite)
            .await
            .unwrap();

        tree.insert(2, &Fv::Text("banana".into()), now + 3).unwrap();
        assert!(
            tree.flush(now + 4).await.is_err(),
            "stale CAS token must remain a conflict",
        );

        // Reopening loads the durable "apple" generation; the unflushed
        // banana insert is recovered by the collection's WAL replay, not by
        // this wrapper.
        let reloaded = BTree::bootstrap("conflict_tree".to_string(), &Ft::Text, storage)
            .await
            .unwrap();
        assert_eq!(
            reloaded.query_with(&Fv::Text("apple".into()), |ids| Some(ids.clone())),
            Some(vec![1])
        );
    }

    /// Regression: `BTree::bootstrap` must accept every field type that
    /// `BTree::new` accepts. `Map` (and `Option<Map>`) fields were accepted
    /// at creation but rejected on reload, which made collections with such
    /// an index impossible to reopen.
    #[tokio::test]
    async fn map_indexes_survive_bootstrap() {
        let storage = test_storage().await;
        let now = unix_ms();

        let map_tree = BTree::new(
            field("map_boot", Ft::Map(BTreeMap::from([("*".into(), Ft::U64)]))),
            storage.clone(),
            now,
        )
        .await
        .unwrap();
        assert!(matches!(map_tree, BTree::String(_)));
        assert!(
            map_tree
                .insert(7, &Fv::Map(BTreeMap::from([("k".into(), Fv::U64(1))])), now,)
                .unwrap()
        );
        assert!(map_tree.flush(now + 1).await.unwrap());

        let reloaded = BTree::bootstrap(
            "map_boot".to_string(),
            &Ft::Map(BTreeMap::from([("*".into(), Ft::U64)])),
            storage.clone(),
        )
        .await
        .unwrap();
        assert!(matches!(reloaded, BTree::String(_)));
        assert_eq!(
            reloaded.query_with(&Fv::Text("k".into()), |ids| Some(ids.clone())),
            Some(vec![7])
        );

        let option_map_tree = BTree::new(
            field(
                "option_map_boot",
                Ft::Option(Box::new(Ft::Map(BTreeMap::from([(
                    b"*".to_vec().into(),
                    Ft::Text,
                )])))),
            ),
            storage.clone(),
            now,
        )
        .await
        .unwrap();
        assert!(matches!(option_map_tree, BTree::Bytes(_)));
        // No mutations yet: flush is a no-op, the metadata object written at
        // creation time is what bootstrap loads below.
        assert!(!option_map_tree.flush(now + 1).await.unwrap());

        let reloaded = BTree::bootstrap(
            "option_map_boot".to_string(),
            &Ft::Option(Box::new(Ft::Map(BTreeMap::from([(
                b"*".to_vec().into(),
                Ft::Text,
            )])))),
            storage,
        )
        .await
        .unwrap();
        assert!(matches!(reloaded, BTree::Bytes(_)));
    }

    /// Regression: the B-tree layer treated *any* one-entry `Map` as a
    /// wildcard map, while the schema (`as_wildcard_map`) requires one of the
    /// sentinel keys. `#[derive(FieldTyped)]` emits `Map({Text("a"): Text})`
    /// for `struct One { a: String }`, which therefore resolved to a `String`
    /// index whose keys came from `Fv::Map::keys()` — the constant field
    /// *name* `"a"` for every document. Equality queries returned the whole
    /// collection and a `#[unique]` field rejected the second insert as a
    /// duplicate.
    #[tokio::test]
    async fn non_wildcard_maps_are_rejected_not_indexed_by_name() {
        let storage = test_storage().await;
        let now = unix_ms();

        // The shape `#[derive(FieldTyped)]` produces for a one-field struct.
        let nested_one = Ft::Map(BTreeMap::from([("a".into(), Ft::Text)]));

        let err = BTree::new(field("nested", nested_one.clone()), storage.clone(), now)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, DBError::Index { source, .. } if source.to_string().contains("unsupported field type")),
            "{err}"
        );

        // The same through an `Option` layer, and for a multi-field struct.
        let err = BTree::new(
            field("nested_opt", Ft::Option(Box::new(nested_one.clone()))),
            storage.clone(),
            now,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, DBError::Index { .. }), "{err}");

        let err = BTree::new(
            field(
                "nested_two",
                Ft::Map(BTreeMap::from([
                    ("a".into(), Ft::Text),
                    ("b".into(), Ft::U64),
                ])),
            ),
            storage.clone(),
            now,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, DBError::Index { .. }), "{err}");

        // `new` rejects the shape going forward, but 0.10 accepted it and
        // persisted such indexes (resolved to the first key's own type), so
        // `bootstrap` must still load them — refusing would make the
        // collection unopenable with no way to remove the index. Simulate the
        // 0.10 artifact: a String-keyed index persisted under this name.
        let legacy = BTree::new(field("nested_legacy", Ft::Text), storage.clone(), now)
            .await
            .unwrap();
        assert!(matches!(legacy, BTree::String(_)));
        let reloaded = BTree::bootstrap("nested_legacy".to_string(), &nested_one, storage.clone())
            .await
            .unwrap();
        assert!(matches!(reloaded, BTree::String(_)));

        // Multi-entry non-wildcard maps were never accepted by any release:
        // `bootstrap` keeps rejecting them.
        let err = BTree::bootstrap(
            "nested_two".to_string(),
            &Ft::Map(BTreeMap::from([
                ("a".into(), Ft::Text),
                ("b".into(), Ft::U64),
            ])),
            storage.clone(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, DBError::Index { .. }), "{err}");

        // All three wildcard sentinels remain indexable, each resolving to
        // the B-tree matching its key variant.
        let text_map = BTree::new(
            field(
                "wild_text",
                Ft::Map(BTreeMap::from([("*".into(), Ft::U64)])),
            ),
            storage.clone(),
            now,
        )
        .await
        .unwrap();
        assert!(matches!(text_map, BTree::String(_)));

        let i64_map = BTree::new(
            field(
                "wild_i64",
                Ft::Map(BTreeMap::from([(i64::MIN.into(), Ft::U64)])),
            ),
            storage.clone(),
            now,
        )
        .await
        .unwrap();
        assert!(matches!(i64_map, BTree::I64(_)));

        let bytes_map = BTree::new(
            field(
                "wild_bytes",
                Ft::Map(BTreeMap::from([(b"*".to_vec().into(), Ft::U64)])),
            ),
            storage,
            now,
        )
        .await
        .unwrap();
        assert!(matches!(bytes_map, BTree::Bytes(_)));
    }
}
