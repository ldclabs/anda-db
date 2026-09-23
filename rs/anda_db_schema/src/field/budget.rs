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
        enum Item<'a> {
            Field(&'a FieldValue),
            Json(&'a Json),
        }
        enum Children<'a> {
            Array(std::slice::Iter<'a, FieldValue>),
            Map(std::collections::btree_map::Values<'a, FieldKey, FieldValue>),
            JsonArray(std::slice::Iter<'a, Json>),
            JsonMap(serde_json::map::Values<'a>),
        }
        impl<'a> Children<'a> {
            fn next(&mut self) -> Option<Item<'a>> {
                // Preserve the previous stack's reverse visitation order.
                match self {
                    Self::Array(values) => values.next_back().map(Item::Field),
                    Self::Map(values) => values.next_back().map(Item::Field),
                    Self::JsonArray(values) => values.next_back().map(Item::Json),
                    Self::JsonMap(values) => values.next_back().map(Item::Json),
                }
            }
        }

        let mut nodes = 0usize;
        // Keep only the unfinished container at each depth, not all siblings.
        // Scalars (including JSON scalars) never allocate a traversal stack.
        let mut stack: Vec<(Children<'_>, usize)> = Vec::new();
        let mut next = Some((Item::Field(self), 0usize));

        while let Some((item, depth)) = next.take() {
            nodes = nodes.saturating_add(1);
            if nodes > budget.max_nodes {
                return Err(SchemaError::FieldValue(format!(
                    "FieldValue exceeds maximum node count {}",
                    budget.max_nodes
                )));
            }

            if depth > budget.max_depth {
                return Err(SchemaError::FieldValue(format!(
                    "FieldValue exceeds maximum depth {}",
                    budget.max_depth
                )));
            }

            let children = match item {
                Item::Field(FieldValue::Array(values)) => {
                    if values.len() > budget.max_array_len {
                        return Err(SchemaError::FieldValue(format!(
                            "FieldValue array length {} exceeds maximum {}",
                            values.len(),
                            budget.max_array_len
                        )));
                    }
                    Some(Children::Array(values.iter()))
                }
                Item::Field(FieldValue::Map(values)) => {
                    if values.len() > budget.max_map_entries {
                        return Err(SchemaError::FieldValue(format!(
                            "FieldValue map entries {} exceed maximum {}",
                            values.len(),
                            budget.max_map_entries
                        )));
                    }
                    Some(Children::Map(values.values()))
                }
                Item::Field(FieldValue::Json(value)) => {
                    // Json's wrapper remains a separate node and depth level.
                    next = Some((Item::Json(value), depth + 1));
                    continue;
                }
                Item::Json(Json::Array(values)) => {
                    if values.len() > budget.max_array_len {
                        return Err(SchemaError::FieldValue(format!(
                            "JSON array length {} exceeds maximum {}",
                            values.len(),
                            budget.max_array_len
                        )));
                    }
                    Some(Children::JsonArray(values.iter()))
                }
                Item::Json(Json::Object(values)) => {
                    if values.len() > budget.max_map_entries {
                        return Err(SchemaError::FieldValue(format!(
                            "JSON object entries {} exceed maximum {}",
                            values.len(),
                            budget.max_map_entries
                        )));
                    }
                    Some(Children::JsonMap(values.values()))
                }
                _ => None,
            };

            if let Some(mut children) = children
                && let Some(first) = children.next()
            {
                stack.push((children, depth + 1));
                next = Some((first, depth + 1));
                continue;
            }

            next = loop {
                let Some((children, depth)) = stack.last_mut() else {
                    break None;
                };
                if let Some(item) = children.next() {
                    break Some((item, *depth));
                }
                stack.pop();
            };
        }

        Ok(())
    }
}
