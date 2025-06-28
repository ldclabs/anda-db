use anda_db_btree::BTreeIndex;
use bytes::Bytes;
use parking_lot::RwLock;
use serde::{Serialize, de::DeserializeOwned};
use std::{fmt::Debug, hash::Hash};

pub use anda_db_btree::{BTreeConfig, BTreeMetadata, BTreeStats, RangeQuery};

use crate::{
    error::DBError,
    schema::{DocumentId, Fe, Ft, Fv},
    storage::{ObjectVersion, PutMode, Storage},
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
    virtual_field: Vec<String>,
    index: BTreeIndex<u64, FV>,
    storage: Storage, // 与 Collection 共享同一个 Storage 实例
    metadata_version: RwLock<ObjectVersion>,
}

impl BTree {
    fn metadata_path(name: &str) -> String {
        format!("btree_indexes/{name}/meta.cbor")
    }

    fn posting_path(name: &str, bucket: u32) -> String {
        format!("btree_indexes/{name}/p_{bucket}.cbor")
    }

    pub fn virtual_field_name(fields: &[&str]) -> String {
        fields.join("-")
    }

    pub async fn new(field: Fe, storage: Storage, now_ms: u64) -> Result<Self, DBError> {
        let config = BTreeConfig {
            bucket_overload_size: storage.object_chunk_size() as u32 * 2,
            allow_duplicates: !field.unique(),
        };
        let field_name = field.name().to_string();
        match field.r#type() {
            Ft::Option(ft) => match ft.as_ref() {
                Ft::Array(v) if v.len() == 1 => {
                    BTree::inner_new(vec![field_name], &v[0], config, storage, now_ms).await
                }
                v => BTree::inner_new(vec![field_name], v, config, storage, now_ms).await,
            },
            Ft::Array(v) if v.len() == 1 => {
                BTree::inner_new(vec![field_name], &v[0], config, storage, now_ms).await
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
            bucket_overload_size: storage.object_chunk_size() as u32 * 2,
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
            BTree::I64(btree) => &btree.virtual_field,
            BTree::U64(btree) => &btree.virtual_field,
            BTree::String(btree) => &btree.virtual_field,
            BTree::Bytes(btree) => &btree.virtual_field,
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

    pub fn insert(
        &self,
        doc_id: DocumentId,
        field_value: Fv,
        now_ms: u64,
    ) -> Result<bool, DBError> {
        if let Fv::Array(vals) = field_value {
            return self.insert_array(doc_id, vals, now_ms).map(|n| n > 0);
        }

        match (&self, field_value) {
            (BTree::I64(btree), Fv::I64(val)) => btree
                .index
                .insert(doc_id, val, now_ms)
                .map_err(DBError::from),
            (BTree::U64(btree), Fv::U64(val)) => btree
                .index
                .insert(doc_id, val, now_ms)
                .map_err(DBError::from),
            (BTree::String(btree), Fv::Text(val)) => btree
                .index
                .insert(doc_id, val, now_ms)
                .map_err(DBError::from),
            (BTree::Bytes(btree), Fv::Bytes(val)) => btree
                .index
                .insert(doc_id, val, now_ms)
                .map_err(DBError::from),
            (_, v) => Err(DBError::Index {
                name: self.name().to_string(),
                source: format!("{:?}: field value type mismatch: {:?}", self, v).into(),
            }),
        }
    }

    pub fn insert_array(
        &self,
        doc_id: DocumentId,
        field_values: Vec<Fv>,
        now_ms: u64,
    ) -> Result<usize, DBError> {
        match &self {
            BTree::I64(btree) => {
                let values: Vec<i64> = field_values
                    .into_iter()
                    .filter_map(|val| val.try_into().ok())
                    .collect();
                btree
                    .index
                    .insert_array(doc_id, values, now_ms)
                    .map_err(DBError::from)
            }
            BTree::U64(btree) => {
                let values: Vec<u64> = field_values
                    .into_iter()
                    .filter_map(|val| val.try_into().ok())
                    .collect();
                btree
                    .index
                    .insert_array(doc_id, values, now_ms)
                    .map_err(DBError::from)
            }
            BTree::String(btree) => {
                let values: Vec<String> = field_values
                    .into_iter()
                    .filter_map(|val| val.try_into().ok())
                    .collect();
                btree
                    .index
                    .insert_array(doc_id, values, now_ms)
                    .map_err(DBError::from)
            }
            BTree::Bytes(btree) => {
                let values: Vec<Vec<u8>> = field_values
                    .into_iter()
                    .filter_map(|val| val.try_into().ok())
                    .collect();
                btree
                    .index
                    .insert_array(doc_id, values, now_ms)
                    .map_err(DBError::from)
            }
        }
    }

    pub fn remove(&self, doc_id: DocumentId, field_value: &Fv, now_ms: u64) -> bool {
        if let Fv::Array(vals) = field_value {
            return self
                .remove_array(doc_id, vals, now_ms)
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

    pub fn remove_array(
        &self,
        doc_id: DocumentId,
        field_values: &[Fv],
        now_ms: u64,
    ) -> Result<usize, DBError> {
        match &self {
            BTree::I64(btree) => {
                let values: Vec<i64> = field_values
                    .iter()
                    .filter_map(|val| val.try_into().ok())
                    .collect();
                Ok(btree.index.remove_array(doc_id, values, now_ms))
            }
            BTree::U64(btree) => {
                let values: Vec<u64> = field_values
                    .iter()
                    .filter_map(|val| val.try_into().ok())
                    .collect();
                Ok(btree.index.remove_array(doc_id, values, now_ms))
            }
            BTree::String(btree) => {
                let values: Vec<String> = field_values
                    .iter()
                    .filter_map(|val| val.clone().try_into().ok())
                    .collect();
                Ok(btree.index.remove_array(doc_id, values, now_ms))
            }
            BTree::Bytes(btree) => {
                let values: Vec<Vec<u8>> = field_values
                    .iter()
                    .filter_map(|val| val.clone().try_into().ok())
                    .collect();
                Ok(btree.index.remove_array(doc_id, values, now_ms))
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

    pub async fn flush(&self, now_ms: u64) -> Result<bool, DBError> {
        match self {
            BTree::I64(btree) => btree.flush(now_ms).await,
            BTree::U64(btree) => btree.flush(now_ms).await,
            BTree::String(btree) => btree.flush(now_ms).await,
            BTree::Bytes(btree) => btree.flush(now_ms).await,
        }
    }
}

impl<FV> InnerBTree<FV>
where
    FV: Eq + Ord + Hash + Debug + Clone + Serialize + DeserializeOwned,
{
    async fn new(
        virtual_field: Vec<String>,
        config: BTreeConfig,
        storage: Storage,
        now_ms: u64,
    ) -> Result<Self, DBError> {
        let name = virtual_field.join("-");
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
            virtual_field,
            index,
            storage,
            metadata_version: RwLock::new(ver),
        })
    }

    async fn bootstrap(name: String, storage: Storage) -> Result<Self, DBError> {
        let virtual_field: Vec<String> = name.split('-').map(String::from).collect();
        let path = BTree::metadata_path(&name);
        let (metadata, ver) = storage.fetch_bytes(&path).await?;
        let index = BTreeIndex::<DocumentId, FV>::load_all(&metadata[..], async |id: u32| {
            let path = BTree::posting_path(&name, id);
            match storage.fetch_bytes(&path).await {
                Ok((data, _)) => Ok(Some(data.into())),
                Err(DBError::NotFound { .. }) => Ok(None),
                Err(e) => Err(e.into()),
            }
        })
        .await?;

        Ok(Self {
            name,
            virtual_field,
            index,
            storage,
            metadata_version: RwLock::new(ver),
        })
    }

    async fn flush(&self, now_ms: u64) -> Result<bool, DBError> {
        let mut data = Vec::new();
        if !self.index.store_metadata(&mut data, now_ms)? {
            return Ok(false);
        }

        let path = BTree::metadata_path(&self.name);
        let ver = { self.metadata_version.read().clone() };
        let ver = self
            .storage
            .put_bytes(&path, data.into(), PutMode::Update(ver.into()))
            .await?;
        {
            *self.metadata_version.write() = ver;
        }

        self.index
            .store_dirty_postings(async |id, data| {
                let path = BTree::posting_path(&self.name, id);
                let _ = self
                    .storage
                    .put_bytes(&path, Bytes::copy_from_slice(data), PutMode::Overwrite)
                    .await?;
                Ok(true)
            })
            .await?;

        Ok(true)
    }
}
