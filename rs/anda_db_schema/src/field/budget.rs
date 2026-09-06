//! Structural limits and allocation-free scalar checks.
use super::*;
/// Structural complexity budget for runtime field values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldValueBudget {
    /// Maximum nesting depth across [`FieldValue`] and JSON containers.
    pub max_depth: usize,
    /// Maximum total [`FieldValue`] and JSON nodes traversed.
    pub max_nodes: usize,
    /// Maximum elements accepted in one array.
    pub max_array_len: usize,
    /// Maximum entries accepted in one map or JSON object.
    pub max_map_entries: usize,
}

impl Default for FieldValueBudget {
    fn default() -> Self {
        Self {
            max_depth: 64,
            max_nodes: 16_384,
            max_array_len: 4_096,
            max_map_entries: 4_096,
        }
    }
}

/// Maximum container nesting depth accepted by the recursive
/// `Cbor` ⇄ [`FieldValue`] conversion routines ([`FieldType::extract`],
/// [`FieldValue::try_from`], [`FieldValue::array_from`],
/// [`FieldValue::map_from`]).
///
/// This is a stack-safety bound, not a semantic budget: it must stay well
/// below the recursion depth that would exhaust the stack, while remaining
/// larger than [`FieldValueBudget::max_depth`] (the default per-value budget
/// enforced by validation) so that it never rejects a value that validation
/// would accept. The serde deserialization entry is bounded separately by
/// the data format itself (`cbor2` caps recursion at 256, `serde_json` at
/// 128).
pub const MAX_CONVERSION_DEPTH: usize = 128;

/// Returns an error when `depth` exceeds [`MAX_CONVERSION_DEPTH`].
pub(super) fn check_conversion_depth(depth: usize) -> Result<(), SchemaError> {
    if depth > MAX_CONVERSION_DEPTH {
        return Err(SchemaError::FieldValue(format!(
            "value exceeds maximum nesting depth {MAX_CONVERSION_DEPTH}"
        )));
    }
    Ok(())
}

impl FieldValue {
    /// Validates this value against the default structural complexity budget.
    ///
    /// The check is iterative and covers nested [`FieldValue::Array`],
    /// [`FieldValue::Map`], and [`FieldValue::Json`] containers.
    pub fn validate_complexity(&self) -> Result<(), SchemaError> {
        self.validate_complexity_with(FieldValueBudget::default())
    }

    /// Validates this value against an explicit structural complexity budget.
    pub fn validate_complexity_with(&self, budget: FieldValueBudget) -> Result<(), SchemaError> {
        if !matches!(
            self,
            FieldValue::Array(_) | FieldValue::Map(_) | FieldValue::Json(_)
        ) {
            return if budget.max_nodes == 0 {
                Err(SchemaError::FieldValue(
                    "FieldValue exceeds maximum node count 0".into(),
                ))
            } else {
                Ok(())
            };
        }
        enum Item<'a> {
            Field(&'a FieldValue, usize),
            Json(&'a Json, usize),
        }

        let mut nodes = 0usize;
        let mut stack = vec![Item::Field(self, 0)];

        while let Some(item) = stack.pop() {
            nodes = nodes.saturating_add(1);
            if nodes > budget.max_nodes {
                return Err(SchemaError::FieldValue(format!(
                    "FieldValue exceeds maximum node count {}",
                    budget.max_nodes
                )));
            }

            let depth = match &item {
                Item::Field(_, depth) | Item::Json(_, depth) => *depth,
            };
            if depth > budget.max_depth {
                return Err(SchemaError::FieldValue(format!(
                    "FieldValue exceeds maximum depth {}",
                    budget.max_depth
                )));
            }

            match item {
                Item::Field(FieldValue::Array(values), depth) => {
                    if values.len() > budget.max_array_len {
                        return Err(SchemaError::FieldValue(format!(
                            "FieldValue array length {} exceeds maximum {}",
                            values.len(),
                            budget.max_array_len
                        )));
                    }
                    stack.extend(values.iter().map(|value| Item::Field(value, depth + 1)));
                }
                Item::Field(FieldValue::Map(values), depth) => {
                    if values.len() > budget.max_map_entries {
                        return Err(SchemaError::FieldValue(format!(
                            "FieldValue map entries {} exceed maximum {}",
                            values.len(),
                            budget.max_map_entries
                        )));
                    }
                    stack.extend(values.values().map(|value| Item::Field(value, depth + 1)));
                }
                Item::Field(FieldValue::Json(value), depth) => {
                    stack.push(Item::Json(value, depth + 1));
                }
                Item::Field(_, _) => {}
                Item::Json(Json::Array(values), depth) => {
                    if values.len() > budget.max_array_len {
                        return Err(SchemaError::FieldValue(format!(
                            "JSON array length {} exceeds maximum {}",
                            values.len(),
                            budget.max_array_len
                        )));
                    }
                    stack.extend(values.iter().map(|value| Item::Json(value, depth + 1)));
                }
                Item::Json(Json::Object(values), depth) => {
                    if values.len() > budget.max_map_entries {
                        return Err(SchemaError::FieldValue(format!(
                            "JSON object entries {} exceed maximum {}",
                            values.len(),
                            budget.max_map_entries
                        )));
                    }
                    stack.extend(values.values().map(|value| Item::Json(value, depth + 1)));
                }
                Item::Json(_, _) => {}
            }
        }

        Ok(())
    }
}
