//! Collection index ops implementation.
use super::*;

impl Collection {
    /// Loads all indexes from storage.
    ///
    /// # Returns
    /// Ok(()) if successful, or an error if loading fails
    pub(super) async fn load_indexes(&mut self) -> Result<(), DBError> {
        let (btree_meta, bm25_names, hnsw_names) = {
            let meta = self.metadata.read();
            (
                meta.btree_indexes.clone(),
                meta.bm25_indexes.keys().cloned().collect::<Vec<_>>(),
                meta.hnsw_indexes.keys().cloned().collect::<Vec<_>>(),
            )
        };
        let cleanup = !self.is_read_only();
        let (mut btree_indexes, bm25_indexes, hnsw_indexes) = try_join_await!(
            try_join_all(btree_meta.iter().map(|(name, field)| {
                BTree::bootstrap(name.clone(), field.r#type(), self.storage.clone())
            })),
            try_join_all(bm25_names.into_iter().map(|name| {
                BM25::bootstrap(name, self.tokenizer.clone(), self.storage.clone())
            })),
            try_join_all(
                hnsw_names
                    .into_iter()
                    .map(|name| Hnsw::bootstrap_with_cleanup(name, self.storage.clone(), cleanup))
            ),
        )?;
        // Unique indexes first, so a conflicting write fails before any
        // non-unique index changes (the same order index creation keeps).
        btree_indexes.sort_by_key(BTree::allow_duplicates);

        self.btree_indexes = btree_indexes;
        self.bm25_indexes = bm25_indexes;
        self.hnsw_indexes = hnsw_indexes;
        Ok(())
    }

    /// Streams every existing document through `f`, one at a time.
    ///
    /// Documents are fetched with `Storage::fetch` (bypassing the cache) so a
    /// full backfill scan does not evict the hot working set, and are never
    /// collected into memory as a whole — large collections would otherwise
    /// risk OOM during index creation.
    pub(super) async fn for_each_existing_document<F>(&self, mut f: F) -> Result<(), DBError>
    where
        F: FnMut(DocumentId, Document) -> Result<(), DBError>,
    {
        let schema = self.schema();
        let mut stream = self.fetch_documents(self.ids());
        while let Some((id, result)) = stream.next().await {
            match result {
                Ok(doc) => {
                    f(id, Document::try_from_doc(schema.clone(), doc)?)?;
                }
                Err(DBError::NotFound { .. }) => {}
                Err(err) => return Err(err),
            }
        }

        Ok(())
    }

    pub(super) async fn backfill_btree_index(
        &self,
        index: &BTree,
        now_ms: u64,
    ) -> Result<(), DBError> {
        if self.is_empty() {
            return Ok(());
        }

        self.for_each_existing_document(|id, doc| {
            if let Some(fv) = self.index_hooks.btree_index_value(index, &doc) {
                if fv.as_ref() == &FieldValue::Null {
                    return Ok(());
                }
                index.insert(id, &fv, now_ms)?;
            }
            Ok(())
        })
        .await
    }

    pub(super) async fn backfill_bm25_index(
        &self,
        index: &BM25,
        now_ms: u64,
    ) -> Result<(), DBError> {
        if self.is_empty() {
            return Ok(());
        }

        self.for_each_existing_document(|id, doc| {
            if let Some(text) = self.index_hooks.bm25_index_value(index, &doc) {
                index.insert(id, &text, now_ms)?;
            }
            Ok(())
        })
        .await
    }

    pub(super) async fn backfill_hnsw_index(
        &self,
        index: &Hnsw,
        now_ms: u64,
    ) -> Result<(), DBError> {
        if self.is_empty() {
            return Ok(());
        }

        self.for_each_existing_document(|id, doc| {
            if let Some(vector) = self.index_hooks.hnsw_index_value(index, &doc) {
                index.insert(id, vector.into_owned(), now_ms)?;
            }
            Ok(())
        })
        .await
    }

    /// Tokenizes the given text using the collection's tokenizer.
    pub fn tokenize(&self, text: &str) -> Vec<String> {
        BM25::collect_tokens(&self.tokenizer, text)
    }

    /// Creates a BTree index on the specified field.
    ///
    /// # Uniqueness semantics
    ///
    /// - A **single-field** index enforces uniqueness only when the field is
    ///   declared `unique` in the schema.
    /// - A **multi-field** index (two or more fields) always acts as a
    ///   composite **unique** index: inserting a second document with the
    ///   same combination of field values is rejected. Existing documents
    ///   are backfilled at creation time and can make creation fail if they
    ///   already violate the constraint.
    ///
    /// # Arguments
    /// * `fields` - Fields to index
    ///
    /// # Returns
    /// Ok(()) if successful, or an error if creation fails
    pub async fn create_btree_index(&mut self, fields: &[&str]) -> Result<(), DBError> {
        self.ensure_recovered().await?;
        self.ensure_mutable()?;
        if fields.is_empty() {
            return Err(DBError::Schema {
                name: self.name.clone(),
                source: "BTree index requires at least one field".into(),
            });
        }

        let now_ms = unix_ms();
        let name = virtual_field_name(fields);

        // The primary key is served directly from the always-present id
        // bitmap: `filter_by_field_with` dispatches `_id` to `filter_by_id`
        // before consulting the B-tree registry, so an index registered under
        // this exact name could never answer a query — it would only be
        // backfilled, persisted and flushed forever. Rejecting it here is
        // preferable to routing `_id` through the registry, which would make
        // `_id` filters fail on every collection that has no such index (all
        // of them today) and would keep a second, redundant copy of the id
        // set in sync for no gain.
        if name == Schema::ID_KEY {
            return Err(DBError::Schema {
                name: self.name.clone(),
                source: format!(
                    "BTree index on {:?} is not supported: the primary key is always queryable through the collection id index",
                    Schema::ID_KEY
                )
                .into(),
            });
        }

        {
            if self.metadata.read().btree_indexes.contains_key(&name) {
                return Err(DBError::AlreadyExists {
                    name: name.to_string(),
                    path: self.name.clone(),
                    source: "BTree index already exists".into(),
                    _id: 0,
                });
            }
        }

        if fields.len() == 1 {
            let field = self.schema.get_field_or_err(fields[0])?;

            let index = BTree::new(field.clone(), self.storage.clone(), now_ms).await?;
            if let Err(err) = self.backfill_btree_index(&index, now_ms).await {
                index.drop_data().await;
                return Err(err);
            }
            if field.unique() {
                self.btree_indexes.insert(0, index);
            } else {
                self.btree_indexes.push(index);
            }
            let mut meta = self.metadata.write();
            meta.btree_indexes.insert(name.to_string(), field.clone());
            meta.stats.version += 1;
        } else {
            for field in fields {
                self.schema.get_field_or_err(field)?;
            }
            let field = FieldEntry::new("_virtual_field_".to_string(), FieldType::Bytes)?
                .with_unique()
                .with_description(name.clone());
            let index = BTree::with_virtual_field(
                fields.iter().map(|s| s.to_string()).collect(),
                self.storage.clone(),
                now_ms,
            )
            .await?;

            if let Err(err) = self.backfill_btree_index(&index, now_ms).await {
                index.drop_data().await;
                return Err(err);
            }
            self.btree_indexes.insert(0, index);
            let mut meta = self.metadata.write();
            meta.btree_indexes.insert(name, field);
            meta.stats.version += 1;
        }

        Ok(())
    }

    /// Creates a BTree index if it doesn't already exist.
    ///
    /// A B-tree index takes no configuration beyond its field list (which is
    /// its identity) and the schema entry those fields resolve to, so an
    /// existing index can never disagree with the request — unlike
    /// [`Collection::create_hnsw_index_nx`].
    ///
    /// # Arguments
    /// * `fields` - Fields to index
    ///
    /// # Returns
    /// Ok(()) if successful, or an error if creation fails
    pub async fn create_btree_index_nx(&mut self, fields: &[&str]) -> Result<(), DBError> {
        self.ensure_mutable()?;
        if self.find_btree_index(fields).is_ok() {
            return Ok(());
        }
        self.create_btree_index(fields).await
    }

    /// Creates a BM25 text search index.
    ///
    /// # Arguments
    /// * `field` - Name of the field to index
    ///
    /// # Returns
    /// Ok(()) if successful, or an error if creation fails
    pub async fn create_bm25_index(&mut self, fields: &[&str]) -> Result<(), DBError> {
        self.ensure_recovered().await?;
        self.ensure_mutable()?;
        if fields.is_empty() {
            return Err(DBError::Schema {
                name: self.name.clone(),
                source: "BM25 index requires at least one field".into(),
            });
        }

        let now_ms = unix_ms();
        let name = virtual_field_name(fields);

        {
            if self.metadata.read().bm25_indexes.contains_key(&name) {
                return Err(DBError::AlreadyExists {
                    name: name.clone(),
                    path: self.name.clone(),
                    source: "BM25 index already exists".into(),
                    _id: 0,
                });
            }
        }

        for field in fields {
            self.schema.get_field_or_err(field)?;
        }

        let index = BM25::new(
            fields.iter().map(|s| s.to_string()).collect(),
            self.tokenizer.clone(),
            self.storage.clone(),
            now_ms,
        )
        .await?;

        if let Err(err) = self.backfill_bm25_index(&index, now_ms).await {
            index.drop_data().await;
            return Err(err);
        }

        {
            let mut meta = self.metadata.write();
            meta.stats.version += 1;
            let field = FieldEntry::new("_virtual_field_".to_string(), FieldType::Text)?
                .with_description(name.clone());
            meta.bm25_indexes.insert(name, field);
        }

        self.bm25_indexes.push(index);
        Ok(())
    }

    /// Creates a BM25 index if it doesn't already exist.
    ///
    /// Like [`Collection::create_btree_index_nx`], a BM25 index takes no
    /// configuration beyond its field list, so an existing index cannot
    /// disagree with the request.
    ///
    /// # Arguments
    /// * `fields` - Fields to index
    ///
    /// # Returns
    /// Ok(()) if successful, or an error if creation fails
    pub async fn create_bm25_index_nx(&mut self, fields: &[&str]) -> Result<(), DBError> {
        match self.create_bm25_index(fields).await {
            Ok(_) => Ok(()),
            Err(DBError::AlreadyExists { .. }) => {
                // Ignore the error if the index already exists
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    /// Creates a HNSW (vector) search index.
    ///
    /// # Arguments
    /// * `field` - Name of the field to index
    /// * `config` - HNSW index configuration
    ///
    /// # Returns
    /// Ok(()) if successful, or an error if creation fails
    pub async fn create_hnsw_index(
        &mut self,
        field: &str,
        config: HnswConfig,
    ) -> Result<(), DBError> {
        self.ensure_recovered().await?;
        self.ensure_mutable()?;
        validate_field_name(field)?;

        let name = field.to_string();
        let now_ms = unix_ms();

        {
            if self.metadata.read().hnsw_indexes.contains_key(&name) {
                return Err(DBError::AlreadyExists {
                    name: name.clone(),
                    path: self.name.clone(),
                    source: "HNSW index already exists".into(),
                    _id: 0,
                });
            }
        }

        // A missing field is a `Schema` error, as for the B-tree and BM25
        // constructors, so callers can classify all three the same way.
        let field = self.schema.get_field_or_err(field)?;
        let value_type = match field.r#type() {
            FieldType::Option(inner) => inner.as_ref(),
            value_type => value_type,
        };
        if value_type != &FieldType::Vector {
            return Err(DBError::Schema {
                name: self.name.clone(),
                source: "The type of field for HNSW index should be Vector or Option<Vector>"
                    .into(),
            });
        }

        let index = Hnsw::new(field, config, self.storage.clone(), now_ms).await?;
        if let Err(err) = self.backfill_hnsw_index(&index, now_ms).await {
            index.drop_data().await;
            return Err(err);
        }

        {
            let mut meta = self.metadata.write();
            meta.stats.version += 1;
            meta.hnsw_indexes.insert(name, field.clone());
        }

        self.hnsw_indexes.push(index);
        Ok(())
    }

    /// Creates a HNSW index if it doesn't already exist.
    ///
    /// # Configuration conflicts
    ///
    /// "If it doesn't already exist" means exactly that: an existing index is
    /// never reconfigured in place. When the field already carries an index
    /// whose **persisted** configuration differs from `config`, this returns
    /// an error instead of silently keeping the old one — a discarded
    /// `dimension` change used to surface much later as a `DimensionMismatch`
    /// on every insert. Remove the index and recreate it to change its
    /// configuration.
    ///
    /// # Arguments
    /// * `field` - Name of the field to index
    /// * `config` - HNSW index configuration
    ///
    /// # Returns
    /// Ok(()) if successful, or an error if creation fails
    pub async fn create_hnsw_index_nx(
        &mut self,
        field: &str,
        config: HnswConfig,
    ) -> Result<(), DBError> {
        match self.create_hnsw_index(field, config.clone()).await {
            Ok(_) => Ok(()),
            Err(DBError::AlreadyExists {
                name,
                path,
                source,
                _id,
            }) => match self.hnsw_indexes.iter().find(|i| i.field_name() == field) {
                Some(index) => {
                    let persisted = index.metadata().config;
                    if persisted == config {
                        return Ok(());
                    }
                    Err(DBError::Index {
                        name: field.to_string(),
                        source: format!(
                            "HNSW index already exists with a different configuration; \
                             remove it before recreating it. persisted={persisted:?}, requested={config:?}"
                        )
                        .into(),
                    })
                }
                // The name is registered in metadata but no index is loaded,
                // or the duplicate came from somewhere else entirely: there is
                // no configuration to compare, so surface the original error.
                None => Err(DBError::AlreadyExists {
                    name,
                    path,
                    source,
                    _id,
                }),
            },
            Err(err) => Err(err),
        }
    }

    /// Removes a B-tree index and its persisted files.
    ///
    /// Returns `true` when either metadata or an in-memory index entry was
    /// removed. Returns `false` if the requested index did not exist.
    pub async fn remove_btree_index(&mut self, fields: &[&str]) -> Result<bool, DBError> {
        self.ensure_recovered().await?;
        self.ensure_mutable()?;
        if fields.is_empty() {
            return Err(DBError::Schema {
                name: self.name.clone(),
                source: "BTree index requires at least one field".into(),
            });
        }

        let name = virtual_field_name(fields);
        let removed_index = self
            .btree_indexes
            .iter()
            .position(|index| index.name() == name)
            .map(|position| self.btree_indexes.remove(position))
            .is_some();

        let removed_metadata = {
            let mut meta = self.metadata.write();
            let removed = meta.btree_indexes.remove(&name).is_some();
            if removed {
                meta.stats.version += 1;
            }
            removed
        };

        let removed = removed_index || removed_metadata;
        if removed {
            self.cleanup_removed_index(&BTree::dir_path(&name)).await?;
        }

        Ok(removed)
    }

    /// Persists the metadata change of a removed index, then best-effort deletes
    /// its storage files.
    ///
    /// The order matters for crash safety: metadata must stop referencing the
    /// index before its files disappear, otherwise reopening the collection
    /// would fail to bootstrap the index. Leftover files from a failed deletion
    /// are harmless and will be overwritten if the index is re-created.
    ///
    /// Like every other durable-write path, this holds an `operation_gate`
    /// lease (required by [`Self::store_metadata_unclaimed`], which relies on
    /// it to be serialized against flush) and arms a [`CancelGuard`].
    pub(super) async fn cleanup_removed_index(&self, dir_path: &str) -> Result<(), DBError> {
        let _operation_lease = self.mutation_lease().await?;
        self.guarded("Collection::cleanup_removed_index", async {
            self.store_metadata_unclaimed().await?;
            if let Err(err) = self.storage.drop_prefix(dir_path).await {
                log::warn!(
                    action = "Collection::cleanup_removed_index",
                    collection = self.name,
                    index_dir = dir_path;
                    "Failed to drop index data: {err:?}",
                );
            }
            Ok(())
        })
        .await
    }

    /// Removes a BM25 full-text index and its persisted files.
    ///
    /// Returns `true` when either metadata or an in-memory index entry was
    /// removed. Returns `false` if the requested index did not exist.
    pub async fn remove_bm25_index(&mut self, fields: &[&str]) -> Result<bool, DBError> {
        self.ensure_recovered().await?;
        self.ensure_mutable()?;
        if fields.is_empty() {
            return Err(DBError::Schema {
                name: self.name.clone(),
                source: "BM25 index requires at least one field".into(),
            });
        }

        let name = virtual_field_name(fields);
        let removed_index = self
            .bm25_indexes
            .iter()
            .position(|index| index.name() == name)
            .map(|position| self.bm25_indexes.remove(position))
            .is_some();

        let removed_metadata = {
            let mut meta = self.metadata.write();
            let removed = meta.bm25_indexes.remove(&name).is_some();
            if removed {
                meta.stats.version += 1;
            }
            removed
        };

        let removed = removed_index || removed_metadata;
        if removed {
            self.cleanup_removed_index(&BM25::dir_path(&name)).await?;
        }

        Ok(removed)
    }

    /// Removes an HNSW vector index and its persisted files.
    ///
    /// Returns `true` when either metadata or an in-memory index entry was
    /// removed. Returns `false` if the requested field has no HNSW index.
    pub async fn remove_hnsw_index(&mut self, field: &str) -> Result<bool, DBError> {
        self.ensure_recovered().await?;
        self.ensure_mutable()?;
        if field.is_empty() {
            return Err(DBError::Schema {
                name: self.name.clone(),
                source: "HNSW index requires a non-empty field name".into(),
            });
        }

        validate_field_name(field)?;

        let removed_index = self
            .hnsw_indexes
            .iter()
            .position(|index| index.field_name() == field)
            .map(|position| self.hnsw_indexes.remove(position))
            .is_some();

        let removed_metadata = {
            let mut meta = self.metadata.write();
            let removed = meta.hnsw_indexes.remove(field).is_some();
            if removed {
                meta.stats.version += 1;
            }
            removed
        };

        let removed = removed_index || removed_metadata;
        if removed {
            self.cleanup_removed_index(&Hnsw::dir_path(field)).await?;
        }

        Ok(removed)
    }

    /// Returns the B-tree index over `fields`.
    ///
    /// Multi-field indexes are addressed by the same virtual field name used
    /// during index creation.
    pub fn get_btree_index(&self, fields: &[&str]) -> Result<BTreeIndexView<'_>, DBError> {
        self.find_btree_index(fields)
            .map(|inner| BTreeIndexView { inner })
    }

    pub(super) fn find_btree_index(&self, fields: &[&str]) -> Result<&BTree, DBError> {
        let name = virtual_field_name(fields);
        if let Some(index) = self.btree_indexes.iter().find(|i| i.name() == name) {
            return Ok(index);
        }

        Err(DBError::Index {
            name,
            source: "BTree index not found".into(),
        })
    }

    /// Returns the BM25 full-text index over `fields`.
    ///
    /// Multi-field indexes are addressed by the same virtual field name used
    /// during index creation.
    pub fn get_bm25_index(&self, fields: &[&str]) -> Result<BM25IndexView<'_>, DBError> {
        self.find_bm25_index(fields)
            .map(|inner| BM25IndexView { inner })
    }

    pub(super) fn find_bm25_index(&self, fields: &[&str]) -> Result<&BM25, DBError> {
        let name = virtual_field_name(fields);
        if let Some(index) = self.bm25_indexes.iter().find(|i| i.name() == name) {
            return Ok(index);
        }

        Err(DBError::Index {
            name,
            source: "BM25 index not found".into(),
        })
    }

    /// Returns the HNSW vector index for `field`.
    pub fn get_hnsw_index(&self, field: &str) -> Result<HnswIndexView<'_>, DBError> {
        self.find_hnsw_index(field)
            .map(|inner| HnswIndexView { inner })
    }

    pub(super) fn find_hnsw_index(&self, field: &str) -> Result<&Hnsw, DBError> {
        if let Some(index) = self.hnsw_indexes.iter().find(|i| i.field_name() == field) {
            return Ok(index);
        }

        Err(DBError::Index {
            name: field.to_string(),
            source: "HNSW index not found".into(),
        })
    }

    /// Compacts the specified BM25 index to optimize storage and performance.
    ///
    /// Takes the **exclusive** operation gate, like [`Collection::flush`] and
    /// not like the shared lease `add`/`update`/`remove` hold: compaction ends
    /// in a persistence pass over the index, and a checkpoint must never
    /// interleave with document mutations. The index crate's own gate is the
    /// first lock a mutator takes and is always released before it returns, so
    /// acquiring the collection gate first cannot deadlock against it.
    pub async fn compact_bm25_index(&self, fields: &[&str]) -> Result<(), DBError> {
        self.ensure_recovered().await?;
        let _operation_guard = self.operation_gate.clone().write_owned().await;
        self.ensure_mutable()?;
        let index = self.find_bm25_index(fields)?;
        let result = self
            .guarded("Collection::compact_bm25_index", index.compact_index())
            .await;
        if result.is_err() {
            self.poison("Collection::compact_bm25_index");
        }
        result
    }

    /// Compacts the specified BTree index to optimize storage and performance.
    ///
    /// See [`Collection::compact_bm25_index`] for why this takes the exclusive
    /// operation gate.
    pub async fn compact_btree_index(&self, fields: &[&str]) -> Result<(), DBError> {
        self.ensure_recovered().await?;
        let _operation_guard = self.operation_gate.clone().write_owned().await;
        self.ensure_mutable()?;
        let index = self.find_btree_index(fields)?;
        let result = self
            .guarded("Collection::compact_btree_index", index.compact_index())
            .await;
        if result.is_err() {
            self.poison("Collection::compact_btree_index");
        }
        result
    }
}
