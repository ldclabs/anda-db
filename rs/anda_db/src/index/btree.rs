use anda_db_btree::BTreeIndex;
use bytes::Bytes;
use cbor2::from_reader;
use ic_auth_types::{ByteBufB64, deterministic_cbor_into_vec};
use parking_lot::RwLock;
use serde::{Serialize, de::DeserializeOwned};
use std::{fmt::Debug, hash::Hash, str::FromStr, sync::Arc};

pub use anda_db_btree::{BTreeConfig, BTreeMetadata, BTreeStats, RangeQuery};

use super::from_virtual_field_name;
use crate::{
    error::DBError,
    schema::{BoxError, DocumentId, Fe, Ft, Fv},
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
    metadata_version: RwLock<ObjectVersion>,
}

impl BTree {
    pub(crate) fn dir_path(name: &str) -> String {
        format!("btree_indexes/{name}/")
    }

    fn metadata_path(name: &str) -> String {
        format!("btree_indexes/{name}/meta.cbor")
    }

    fn bucket_path(name: &str, bucket: u32) -> String {
        format!("btree_indexes/{name}/b_{bucket}.cbor")
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
        deterministic_cbor_into_vec(cursor)
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
        match field.r#type() {
            Ft::Option(ft) => match ft.as_ref() {
                Ft::Array(v) if v.len() == 1 => {
                    BTree::inner_new(vec![field_name], &v[0], config, storage, now_ms).await
                }
                Ft::Map(v) if v.len() == 1 => {
                    BTree::inner_new(
                        vec![field_name],
                        &v.keys().next().unwrap().field_type(),
                        config,
                        storage,
                        now_ms,
                    )
                    .await
                }
                v => BTree::inner_new(vec![field_name], v, config, storage, now_ms).await,
            },
            Ft::Array(v) if v.len() == 1 => {
                BTree::inner_new(vec![field_name], &v[0], config, storage, now_ms).await
            }
            Ft::Map(v) if v.len() == 1 => {
                BTree::inner_new(
                    vec![field_name],
                    &v.keys().next().unwrap().field_type(),
                    config,
                    storage,
                    now_ms,
                )
                .await
            }
            v => BTree::inner_new(vec![field_name], v, config, storage, now_ms).await,
        }
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
    pub async fn bootstrap(name: String, ft: &Ft, storage: Storage) -> Result<Self, DBError> {
        match ft {
            Ft::Option(ft) => match ft.as_ref() {
                Ft::Array(v) if v.len() == 1 => BTree::inner_bootstrap(name, &v[0], storage).await,
                v => BTree::inner_bootstrap(name, v, storage).await,
            },
            Ft::Array(v) if v.len() == 1 => BTree::inner_bootstrap(name, &v[0], storage).await,
            v => BTree::inner_bootstrap(name, v, storage).await,
        }
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
        if old_value == new_value {
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
            (BTree::U64(btree), Fv::U64(val)) => btree.index.query_with(val, f),
            (BTree::String(btree), Fv::Text(val)) => btree.index.query_with(val, f),
            (BTree::Bytes(btree), Fv::Bytes(val)) => btree.index.query_with(val, f),
            _ => None,
        }
    }

    /// Runs a range query and maps each matching key/id-list pair through `f`.
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

    /// Compacts bucket layout and flushes if bucket count shrinks.
    pub async fn compact_index(&self) -> Result<(), DBError> {
        match self {
            BTree::I64(btree) => {
                let (old_bucket_count, new_bucket_count) = btree.index.compact_buckets();
                if new_bucket_count < old_bucket_count {
                    log::warn!(
                        "Compacted BTree index '{}': {} -> {} buckets",
                        btree.name,
                        old_bucket_count,
                        new_bucket_count
                    );
                    btree.flush(unix_ms()).await?;
                }
            }
            BTree::U64(btree) => {
                let (old_bucket_count, new_bucket_count) = btree.index.compact_buckets();
                if new_bucket_count < old_bucket_count {
                    log::warn!(
                        "Compacted BTree index '{}': {} -> {} buckets",
                        btree.name,
                        old_bucket_count,
                        new_bucket_count
                    );
                    btree.flush(unix_ms()).await?;
                }
            }
            BTree::String(btree) => {
                let (old_bucket_count, new_bucket_count) = btree.index.compact_buckets();
                if new_bucket_count < old_bucket_count {
                    log::warn!(
                        "Compacted BTree index '{}': {} -> {} buckets",
                        btree.name,
                        old_bucket_count,
                        new_bucket_count
                    );
                    btree.flush(unix_ms()).await?;
                }
            }
            BTree::Bytes(btree) => {
                let (old_bucket_count, new_bucket_count) = btree.index.compact_buckets();
                if new_bucket_count < old_bucket_count {
                    log::warn!(
                        "Compacted BTree index '{}': {} -> {} buckets",
                        btree.name,
                        old_bucket_count,
                        new_bucket_count
                    );
                    btree.flush(unix_ms()).await?;
                }
            }
        }
        Ok(())
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
            .flush(&mut data, now_ms, async |_, _| Ok(true))
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
            metadata_version: RwLock::new(ver),
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
        let index = BTreeIndex::<DocumentId, FV>::load_all(&metadata[..], async move |id: u32| {
            let path = BTree::bucket_path(n.clone().as_str(), id);
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
            metadata_version: RwLock::new(ver),
        })
    }

    async fn flush(&self, now_ms: u64) -> Result<bool, DBError> {
        let mut buf = Vec::with_capacity(256);
        let meta_saved = self.index.store_metadata(&mut buf, now_ms)?;
        let had_dirty = self.index.has_dirty_buckets();

        if !meta_saved && !had_dirty {
            return Ok(false);
        }

        if meta_saved {
            let path = BTree::metadata_path(&self.name);
            let ver = { self.metadata_version.read().clone() };
            let ver = self
                .storage
                .put_bytes(&path, buf.into(), PutMode::Update(ver.into()))
                .await?;
            {
                *self.metadata_version.write() = ver;
            }
        }

        let n = Arc::new(self.name.clone());
        let s = Arc::new(self.storage.clone());
        self.index
            .store_dirty_buckets(async move |id, data| {
                let path = BTree::bucket_path(n.clone().as_str(), id);
                let _ = s
                    .clone()
                    .put_bytes(&path, Bytes::copy_from_slice(data), PutMode::Overwrite)
                    .await?;
                Ok(true)
            })
            .await?;

        Ok(meta_saved || had_dirty)
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
    use object_store::memory::InMemory;
    use std::collections::BTreeMap;

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

        let option_map = BTree::new(
            field(
                "option_map",
                Ft::Option(Box::new(Ft::Map(BTreeMap::from([("k".into(), Ft::Text)])))),
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
                Ft::Map(BTreeMap::from([(vec![1_u8].into(), Ft::Bytes)])),
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
}
