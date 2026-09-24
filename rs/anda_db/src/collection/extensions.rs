//! Collection extensions implementation.
use super::*;

impl Collection {
    /// Gets the value of a user-defined extension key.
    pub fn get_extension(&self, key: &str) -> Option<FieldValue> {
        self.metadata.read().extensions.get(key).cloned()
    }

    /// Gets the value of a user-defined extension key and deserializes it into the specified type.
    pub fn get_extension_as<T>(&self, key: &str) -> Option<T>
    where
        T: DeserializeOwned,
    {
        self.get_extension(key).and_then(|v| v.deserialized().ok())
    }

    /// Inserts an extension only if the persisted metadata snapshot still
    /// fits the storage object budget, and bumps the version so the next
    /// flush persists it. An oversized value left in memory would fail that
    /// flush and poison the handle.
    fn insert_extension(
        &self,
        meta: &mut CollectionMetadata,
        key: String,
        value: FieldValue,
    ) -> Result<Option<FieldValue>, DBError> {
        // Flush overlays live counters (ids, hit counts, save time) on the
        // snapshot; each CBOR u64 can grow by at most 8 bytes.
        const LIVE_STATS_SLACK: u64 = 64;
        value.validate_complexity()?;
        let limit = self.storage.max_small_object_size();
        let old = meta.extensions.insert(key.clone(), value);
        let size = cbor2::serialized_size(&*meta).map_err(|source| DBError::Serialization {
            name: self.name.clone(),
            source: source.into(),
        })?;
        if size.saturating_add(LIVE_STATS_SLACK) > limit as u64 {
            match old {
                Some(old) => meta.extensions.insert(key, old),
                None => meta.extensions.remove(&key),
            };
            return Err(DBError::PayloadTooLarge {
                path: self.storage.full_path(Self::METADATA_PATH).to_string(),
                size: usize::try_from(size).unwrap_or(usize::MAX),
                limit,
            });
        }
        meta.stats.version += 1;
        Ok(old)
    }

    /// Sets a user-defined extension key-value pair.
    /// The change is persisted on the next `flush()`.
    /// The extensions should not be large, as they are stored in the same object as collection metadata which size is expected to be small (<= 1MB) and loaded frequently.
    /// Values that fail [`FieldValue::validate_complexity`] or would push the
    /// metadata past the storage object budget are dropped with a warning.
    pub fn set_extension(&self, key: String, value: FieldValue) {
        let mut meta = self.metadata.write();
        let result = self
            .ensure_mutable()
            .and_then(|()| self.insert_extension(&mut meta, key, value));
        if let Err(err) = result {
            log::warn!(
                action = "Collection::set_extension",
                collection = self.name;
                "Dropping extension value: {err:?}",
            );
        }
    }

    /// Sets a user-defined extension key-value pair with a serializable value.
    /// Values that fail to serialize are dropped with a warning, matching
    /// [`Collection::set_extension`]'s handling of over-complex values.
    pub fn set_extension_from<T>(&self, key: String, value: T)
    where
        T: Serialize,
    {
        match FieldValue::serialized(&value, None) {
            Ok(value) => self.set_extension(key, value),
            Err(err) => {
                log::warn!(
                    action = "Collection::set_extension_from",
                    collection = self.name,
                    key = key;
                    "Dropping extension value that failed to serialize: {err:?}",
                );
            }
        }
    }

    /// Updates a user-defined extension using a functional approach.
    ///
    /// This method retrieves the current value for the given key (if any) and computes
    /// a new value using the provided function. If the function returns `None`,
    /// no change is made to the extensions.
    ///
    /// # Arguments
    /// * `key` - The name of the extension key to update.
    /// * `f` - An update function that takes `Option<&FieldValue>` and returns `Option<FieldValue>`.
    ///
    /// # Returns
    /// Returns the previous value `Option<FieldValue>` if a change was made.
    ///
    /// # Notes
    /// The change is persisted to storage on the next `flush()` call.
    /// Values rejected like in [`Collection::set_extension`] are dropped with a warning.
    pub fn set_extension_with<F>(&self, key: String, f: F) -> Option<FieldValue>
    where
        F: FnOnce(Option<&FieldValue>) -> Option<FieldValue>,
    {
        let mut meta = self.metadata.write();
        if self.ensure_mutable().is_err() {
            return None;
        }
        let value = f(meta.extensions.get(&key))?;
        match self.insert_extension(&mut meta, key, value) {
            Ok(old) => old,
            Err(err) => {
                log::warn!(
                    action = "Collection::set_extension_with",
                    collection = self.name;
                    "Dropping extension value: {err:?}",
                );
                None
            }
        }
    }

    /// Updates a user-defined extension with a serializable value using a functional approach.
    pub fn set_extension_from_with<F, T>(&self, key: String, f: F) -> Option<T>
    where
        F: FnOnce(Option<T>) -> Option<T>,
        T: Serialize + DeserializeOwned,
    {
        let mut meta = self.metadata.write();
        if self.ensure_mutable().is_err() {
            return None;
        }
        let old_value = meta.extensions.get(&key);
        let value = f(old_value.and_then(|v| v.clone().deserialized().ok()))?;
        let inserted = FieldValue::serialized(&value, None)
            .map_err(DBError::from)
            .and_then(|value| self.insert_extension(&mut meta, key, value));
        match inserted {
            Ok(old) => old.and_then(|v| v.deserialized().ok()),
            Err(err) => {
                log::warn!(
                    action = "Collection::set_extension_from_with",
                    collection = self.name;
                    "Dropping extension value: {err:?}",
                );
                None
            }
        }
    }

    /// Sets a user-defined extension key-value pair and immediately persists the change.
    /// The extensions should not be large, as they are stored in the same object as collection metadata which size is expected to be small (<= 1MB) and loaded frequently.
    /// Returns [`DBError::PayloadTooLarge`] without changing anything when
    /// the value would push the metadata past the storage object budget.
    pub async fn save_extension(&self, key: String, value: FieldValue) -> Result<(), DBError> {
        let _operation_lease = self.mutation_lease().await?;
        self.guarded("Collection::save_extension", async {
            self.update_metadata(|meta| self.insert_extension(meta, key, value))?;
            // Persist the metadata object directly (a single small put)
            // instead of running a full flush: extensions live only in the
            // metadata object, and the full flush caused write amplification
            // plus an unpersisted window — a concurrent flusher could claim
            // the version first, making this call take the fast path and
            // return Ok while the winner's snapshot (possibly without this
            // extension) was still in flight or failed. The unclaimed write
            // keeps the "returning Ok means persisted" contract and does not
            // advance `last_saved_version`, so the next full flush still
            // persists the ids bitmap alongside the metadata.
            self.store_metadata_unclaimed().await
        })
        .await
    }

    /// Sets a user-defined extension key-value pair with a serializable value and immediately persists the change.
    pub async fn save_extension_from<T>(&self, key: String, value: &T) -> Result<(), DBError>
    where
        T: Serialize,
    {
        let field_value = FieldValue::serialized(value, None)?;
        self.save_extension(key, field_value).await
    }

    /// Removes a user-defined extension key and immediately persists the change.
    /// Returns the previous value if the key existed.
    pub async fn remove_extension(&self, key: &str) -> Result<Option<FieldValue>, DBError> {
        let _operation_lease = self.mutation_lease().await?;

        self.guarded("Collection::remove_extension", async {
            let old = self.update_metadata(|meta| {
                let old = meta.extensions.remove(key);
                if old.is_some() {
                    meta.stats.version += 1;
                }
                old
            });
            if old.is_some() {
                // See `save_extension` for why this is a direct, unclaimed
                // metadata write instead of a full flush.
                self.store_metadata_unclaimed().await?;
            }
            Ok(old)
        })
        .await
    }

    /// Provides access to the entire extensions map for advanced use cases.
    pub fn extensions_with<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&BTreeMap<String, FieldValue>) -> R,
    {
        f(&self.metadata.read().extensions)
    }
}
