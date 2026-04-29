use anda_db_btree::BTreeIndex;
use bytes::Bytes;
use ciborium::from_reader;
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

pub enum BTree {
    U64(InnerBTree<u64>),
    I64(InnerBTree<i64>),
    String(InnerBTree<String>),
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
    fn metadata_path(name: &str) -> String {
        format!("btree_indexes/{name}/meta.cbor")
    }

    fn bucket_path(name: &str, bucket: u32) -> String {
        format!("btree_indexes/{name}/b_{bucket}.cbor")
    }

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

    pub fn to_cursor<T>(cursor: &T) -> Option<String>
    where
        T: Serialize,
    {
        deterministic_cbor_into_vec(cursor)
            .map(|v| ByteBufB64(v).to_string())
            .ok()
    }

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

    pub fn name(&self) -> &str {
        match self {
            BTree::I64(btree) => &btree.name,
            BTree::U64(btree) => &btree.name,
            BTree::String(btree) => &btree.name,
            BTree::Bytes(btree) => &btree.name,
        }
    }

    pub fn virtual_field(&self) -> &[String] {
        match self {
            BTree::I64(btree) => &btree.fields,
            BTree::U64(btree) => &btree.fields,
            BTree::String(btree) => &btree.fields,
            BTree::Bytes(btree) => &btree.fields,
        }
    }

    pub fn allow_duplicates(&self) -> bool {
        match self {
            BTree::I64(btree) => btree.index.allow_duplicates(),
            BTree::U64(btree) => btree.index.allow_duplicates(),
            BTree::String(btree) => btree.index.allow_duplicates(),
            BTree::Bytes(btree) => btree.index.allow_duplicates(),
        }
    }

    pub fn stats(&self) -> BTreeStats {
        match self {
            BTree::I64(btree) => btree.index.stats(),
            BTree::U64(btree) => btree.index.stats(),
            BTree::String(btree) => btree.index.stats(),
            BTree::Bytes(btree) => btree.index.stats(),
        }
    }

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

    pub async fn flush(&self, now_ms: u64) -> Result<bool, DBError> {
        match self {
            BTree::I64(btree) => btree.flush(now_ms).await,
            BTree::U64(btree) => btree.flush(now_ms).await,
            BTree::String(btree) => btree.flush(now_ms).await,
            BTree::Bytes(btree) => btree.flush(now_ms).await,
        }
    }

    pub fn has_pending_flush(&self) -> bool {
        match self {
            BTree::I64(btree) => btree.has_pending_flush(),
            BTree::U64(btree) => btree.has_pending_flush(),
            BTree::String(btree) => btree.has_pending_flush(),
            BTree::Bytes(btree) => btree.has_pending_flush(),
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
        let ver = storage
            .put_bytes(&BTree::metadata_path(&name), data.into(), PutMode::Create)
            .await?;
        Ok(InnerBTree {
            name,
            fields,
            index,
            storage,
            metadata_version: RwLock::new(ver),
        })
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
