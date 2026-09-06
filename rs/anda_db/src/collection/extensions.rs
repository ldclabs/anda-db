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

    /// Sets a user-defined extension key-value pair.
    /// The change is persisted on the next `flush()`.
    /// The extensions should not be large, as they are stored in the same object as collection metadata which size is expected to be small (<= 1MB) and loaded frequently.
    /// Values that fail [`FieldValue::validate_complexity`] are dropped with a warning.
    pub fn set_extension(&self, key: String, value: FieldValue) {
        if let Err(err) = value.validate_complexity() {
            log::warn!(
                action = "Collection::set_extension",
                collection = self.name,
                key = key;
                "Dropping extension value that exceeds complexity limits: {err:?}",
            );
            return;
        }
        let mut meta = self.metadata.write();
        if let Err(err) = self.ensure_mutable() {
            log::warn!(
                action = "Collection::set_extension",
                collection = self.name;
                "Ignoring extension mutation on inactive handle: {err:?}",
            );
            return;
        }
        meta.extensions.insert(key, value);
        // Bump the version so the next `flush()` persists the change;
        // `store_metadata` skips the write when the version is unchanged.
        meta.stats.version += 1;
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
    /// Values that fail [`FieldValue::validate_complexity`] are dropped with a warning.
    pub fn set_extension_with<F>(&self, key: String, f: F) -> Option<FieldValue>
    where
        F: FnOnce(Option<&FieldValue>) -> Option<FieldValue>,
    {
        let mut meta = self.metadata.write();
        if self.ensure_mutable().is_err() {
            return None;
        }
        let old_value = meta.extensions.get(&key);
        let new_value = f(old_value);
        if let Some(value) = new_value {
            if let Err(err) = value.validate_complexity() {
                log::warn!(
                    action = "Collection::set_extension_with",
                    collection = self.name,
                    key = key;
                    "Dropping extension value that exceeds complexity limits: {err:?}",
                );
                return None;
            }
            meta.stats.version += 1;
            meta.extensions.insert(key, value)
        } else {
            None
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
        let value = match FieldValue::serialized(&value, None) {
            Ok(value) => value,
            Err(err) => {
                log::warn!(
                    action = "Collection::set_extension_from_with",
                    collection = self.name,
                    key = key;
                    "Dropping extension value that failed to serialize: {err:?}",
                );
                return None;
            }
        };
        if let Err(err) = value.validate_complexity() {
            log::warn!(
                action = "Collection::set_extension_from_with",
                collection = self.name,
                key = key;
                "Dropping extension value that exceeds complexity limits: {err:?}",
            );
            return None;
        }
        meta.stats.version += 1;
        meta.extensions
            .insert(key, value)
            .and_then(|v| v.deserialized().ok())
    }

    /// Sets a user-defined extension key-value pair and immediately persists the change.
    /// The extensions should not be large, as they are stored in the same object as collection metadata which size is expected to be small (<= 1MB) and loaded frequently.
    pub async fn save_extension(&self, key: String, value: FieldValue) -> Result<(), DBError> {
        let _operation_lease = self.mutation_lease().await?;
        value.validate_complexity()?;

        self.guarded("Collection::save_extension", async {
            self.update_metadata(|meta| {
                meta.extensions.insert(key, value);
                meta.stats.version += 1;
            });
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
