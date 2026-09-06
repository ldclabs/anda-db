//! Persistent tombstones for nested fields, and recovery of pre-history schemas.
use super::Schema;
use crate::{
    DocumentOwned, FieldKey, FieldType as Ft, FieldValue as Fv, MAX_CONVERSION_DEPTH, SchemaError,
    as_wildcard_map,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(super) enum Step {
    /// A fixed map key, or the key-type sentinel of a wildcard map.
    Key(FieldKey),
    /// Zero for a homogeneous array, the position for a tuple-like array.
    Item(usize),
}

type Paths = BTreeSet<Vec<Step>>;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(super) struct UpgradeHistory {
    #[serde(default)]
    pub complete: bool,
    #[serde(
        default,
        skip_serializing_if = "BTreeMap::is_empty",
        deserialize_with = "crate::value_serde::unique_map"
    )]
    pub retired: BTreeMap<usize, Paths>,
}

impl UpgradeHistory {
    pub fn known() -> Self {
        Self {
            complete: true,
            retired: BTreeMap::new(),
        }
    }

    pub fn validate(&self) -> Result<(), SchemaError> {
        for (idx, paths) in &self.retired {
            if *idx > u16::MAX as usize
                || paths
                    .iter()
                    .any(|p| p.is_empty() || p.len() > MAX_CONVERSION_DEPTH)
            {
                return Err(SchemaError::Schema(
                    "invalid nested-field upgrade history".into(),
                ));
            }
        }
        Ok(())
    }

    /// Called only after the ordinary structural compatibility check succeeds.
    pub fn upgrade(&mut self, idx: usize, new: &Ft, old: &Ft) -> Result<(), SchemaError> {
        let paths = self.retired.entry(idx).or_default();
        record_changes(new, old, &mut Vec::new(), paths, self.complete)?;
        if paths.is_empty() {
            self.retired.remove(&idx);
        }
        Ok(())
    }
}

fn inner(ft: &Ft) -> &Ft {
    if let Ft::Option(ft) = ft { ft } else { ft }
}

fn record_changes(
    new: &Ft,
    old: &Ft,
    path: &mut Vec<Step>,
    retired: &mut Paths,
    known: bool,
) -> Result<(), SchemaError> {
    match (inner(new), inner(old)) {
        (Ft::Array(new), Ft::Array(old)) => {
            for (i, (new, old)) in new.iter().zip(old).enumerate() {
                path.push(Step::Item(i));
                record_changes(new, old, path, retired, known)?;
                path.pop();
            }
        }
        (Ft::Map(new), Ft::Map(old)) => {
            for (key, old_type) in old {
                path.push(Step::Key(key.clone()));
                if let Some(new_type) = new.get(key) {
                    record_changes(new_type, old_type, path, retired, known)?;
                } else {
                    retired.insert(path.clone());
                }
                path.pop();
            }
            for key in new.keys().filter(|k| !old.contains_key(*k)) {
                path.push(Step::Key(key.clone()));
                if !known {
                    return Err(SchemaError::Schema(format!(
                        "nested-field history is unknown at {path:?}; recover schema history from all stored documents before adding keys"
                    )));
                }
                if retired.contains(path) {
                    return Err(SchemaError::Schema(format!(
                        "nested field {path:?} was retired and cannot be reused without migrating the data"
                    )));
                }
                path.pop();
            }
        }
        _ => {}
    }
    Ok(())
}

/// Recovers schema history from a complete, consistent scan of stored data.
///
/// Feed **every** raw document, including unregistered objects and every
/// previous/proposed image in pending recovery records, into [`Self::observe`].
/// Do not prune or normalize fields before observing them. Writers must be
/// excluded for the duration of the scan and the following upgrade. Calling
/// [`Self::finish`] certifies that the scan is complete; a partial scan cannot
/// establish that a field index or nested key is safe to allocate.
///
/// The core database performs this scan automatically on schema upgrade when
/// history is missing. Custom storage integrations can use the same API.
pub struct SchemaHistoryRecovery {
    schema: Schema,
}

impl SchemaHistoryRecovery {
    pub(super) fn new(schema: &Schema) -> Self {
        Self {
            schema: schema.clone(),
        }
    }

    /// Includes a raw persisted document or recovery image in the scan.
    pub fn observe(&mut self, doc: &DocumentOwned) -> Result<(), SchemaError> {
        if let Some(idx) = doc.fields.keys().next_back() {
            if *idx > u16::MAX as usize {
                return Err(SchemaError::Schema(format!(
                    "stored field index {idx} exceeds u16::MAX"
                )));
            }
            self.schema.next_idx = self.schema.allocated_idx_end().max(idx + 1);
        }
        for field in self.schema.fields.values() {
            if let Some(value) = doc.fields.get(&field.idx()) {
                let paths = self.schema.history.retired.entry(field.idx()).or_default();
                observe_nested(field.r#type(), value, &mut Vec::new(), paths)?;
            }
        }
        Ok(())
    }

    /// Completes a successful full scan and returns the recovered schema.
    pub fn finish(mut self) -> Schema {
        self.schema
            .history
            .retired
            .retain(|_, paths| !paths.is_empty());
        self.schema.history.complete = true;
        self.schema.next_idx = self.schema.allocated_idx_end();
        self.schema.legacy_lineage = false;
        self.schema
    }
}

fn observe_nested(
    ft: &Ft,
    value: &Fv,
    path: &mut Vec<Step>,
    retired: &mut Paths,
) -> Result<(), SchemaError> {
    if path.len() > MAX_CONVERSION_DEPTH {
        return Err(SchemaError::Schema(
            "nested-field history exceeds maximum depth".into(),
        ));
    }
    // DocumentOwned is public; callers may also supply a canonical Json
    // container whose stored, untyped CBOR form is a Map/Array.
    if let Fv::Json(json) = value
        && matches!(inner(ft), Ft::Map(_) | Ft::Array(_))
    {
        let value = Fv::serialized(json, None)?;
        return observe_nested(ft, &value, path, retired);
    }
    match (inner(ft), value) {
        (Ft::Array(types), Fv::Array(values)) => {
            for (i, value) in values.iter().enumerate() {
                let position = if types.len() == 1 { 0 } else { i };
                if let Some(ft) = types.get(position) {
                    path.push(Step::Item(position));
                    observe_nested(ft, value, path, retired)?;
                    path.pop();
                }
            }
        }
        (Ft::Map(types), Fv::Map(values)) if !types.is_empty() => {
            if let Some((key, ft)) = as_wildcard_map(types) {
                path.push(Step::Key(key.clone()));
                for value in values.values() {
                    observe_nested(ft, value, path, retired)?;
                }
                path.pop();
            } else {
                for (key, value) in values {
                    path.push(Step::Key(key.clone()));
                    if let Some(ft) = types.get(key) {
                        observe_nested(ft, value, path, retired)?;
                    } else {
                        retired.insert(path.clone());
                    }
                    path.pop();
                }
            }
        }
        _ => {}
    }
    Ok(())
}
