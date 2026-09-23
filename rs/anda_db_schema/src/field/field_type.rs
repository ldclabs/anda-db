//! Type declarations, compatibility and typed value preparation.
use super::*;
use serde::{Deserialize, Serialize};
/// The type of a field declared in a [`Schema`](crate::Schema).
///
/// `FieldType` is the closed enum of every type supported by Anda DB.
/// It is purely descriptive: a value of this enum is metadata, never
/// payload. The matching payload type is [`FieldValue`].
///
/// Composite variants:
/// - [`FieldType::Array`] holds either zero, one, or several inner types.
///   With one inner type the array is *homogeneous* (every element must
///   match it). With several inner types it is a fixed-size *tuple-like*
///   array.
/// - [`FieldType::Map`] declares per-key types. A wildcard map
///   (`{ "*": T }`, `{ i64::MIN: T }`, or `{ b"*": T }`) matches any key with
///   values of type `T`. See [`TEXT_WILDCARD_KEY`] / [`BYTES_WILDCARD_KEY`] /
///   [`I64_WILDCARD_KEY`], [`FieldKey::is_wildcard`] and [`as_wildcard_map`].
/// - [`FieldType::Option`] makes a field nullable.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldType {
    /// Boolean value
    Bool,
    /// Signed 64-bit integer
    I64,
    /// Unsigned 64-bit integer
    U64,
    /// 64-bit floating point number
    F64,
    /// 32-bit floating point number
    F32,
    /// Binary data
    Bytes,
    /// UTF-8 encoded text
    Text,
    /// JSON value
    Json,
    /// `Vec<bf16>`, bf16: 16-bit floating point type implementing the bfloat16 format.
    /// Detail: <https://docs.rs/half/latest/half/struct.bf16.html>
    Vector,
    /// Array of field types
    Array(Vec<FieldType>),
    /// Map with typed field keys and field type values
    Map(
        #[serde(deserialize_with = "crate::value_serde::unique_map")] BTreeMap<FieldKey, FieldType>,
    ),
    /// Optional field type
    Option(Box<FieldType>),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ValueMode {
    Write,
    Read,
}

impl FieldType {
    /// Shared owned traversal: writes coerce input; reads restore only valid
    /// persisted shapes and discard retired nested keys. No intermediate CBOR
    /// tree is needed for canonical values or typed containers.
    pub(super) fn prepare(
        &self,
        mut value: FieldValue,
        depth: usize,
        mode: ValueMode,
    ) -> Result<FieldValue, SchemaError> {
        check_conversion_depth(depth)?;
        let canonical = match (self, &value) {
            (FieldType::Bool, FieldValue::Bool(_))
            | (FieldType::I64, FieldValue::I64(_))
            | (FieldType::U64, FieldValue::U64(_))
            | (FieldType::Text, FieldValue::Text(_))
            | (FieldType::Bytes, FieldValue::Bytes(_))
            | (FieldType::Vector, FieldValue::Vector(_)) => true,
            (FieldType::F32, FieldValue::F32(f)) => !f.is_nan(),
            (FieldType::F64, FieldValue::F64(f)) => !f.is_nan(),
            _ => false,
        };
        if canonical {
            return Ok(value);
        }
        match self {
            FieldType::Option(inner) => {
                if matches!(value, FieldValue::Null | FieldValue::Json(Json::Null)) {
                    return Ok(FieldValue::Null);
                }
                return inner.prepare(value, depth, mode);
            }
            FieldType::Json => return Ok(FieldValue::Json(field_value_into_json(value, depth)?)),
            FieldType::Bytes if mode == ValueMode::Write => {
                if let FieldValue::Array(values) = &value {
                    let mut bytes = Vec::with_capacity(values.len());
                    for value in values {
                        let byte = match value {
                            FieldValue::U64(byte) if *byte <= u8::MAX as u64 => *byte as u8,
                            FieldValue::I64(byte) if (0..=i64::from(u8::MAX)).contains(byte) => {
                                *byte as u8
                            }
                            // Preserve the CBOR adapter for other accepted
                            // shapes, including JSON integer elements.
                            _ => break,
                        };
                        bytes.push(byte);
                    }
                    if bytes.len() == values.len() {
                        return Ok(FieldValue::Bytes(bytes));
                    }
                }
            }
            FieldType::Vector if mode == ValueMode::Write => {
                if let FieldValue::Array(values) = &value {
                    let mut vector = Vec::with_capacity(values.len());
                    for value in values {
                        let bits = match value {
                            FieldValue::U64(bits) if *bits <= u16::MAX as u64 => *bits as u16,
                            FieldValue::I64(bits) if (0..=i64::from(u16::MAX)).contains(bits) => {
                                *bits as u16
                            }
                            // This path is only an allocation-saving shortcut
                            // for the canonical integer representation. Other
                            // inputs must retain the established CBOR coercion
                            // below (for example `Json(Number(1))`).
                            _ => break,
                        };
                        vector.push(bf16::from_bits(bits));
                    }
                    if vector.len() == values.len() {
                        return Ok(FieldValue::Vector(vector));
                    }
                }
            }
            FieldType::Array(types) if !types.is_empty() => {
                if let FieldValue::Array(values) = &mut value {
                    if types.len() > 1 && types.len() != values.len() {
                        return Err(SchemaError::FieldValue(format!(
                            "invalid array length, expected {}, got {}",
                            types.len(),
                            values.len()
                        )));
                    }
                    for (i, value) in values.iter_mut().enumerate() {
                        let ft = &types[if types.len() == 1 { 0 } else { i }];
                        *value = ft.prepare(
                            std::mem::replace(value, FieldValue::Null),
                            depth + 1,
                            mode,
                        )?;
                    }
                    return Ok(value);
                }
            }
            FieldType::Map(types) if !types.is_empty() => {
                if let FieldValue::Map(values) = &mut value {
                    let wildcard = as_wildcard_map(types);
                    if mode == ValueMode::Read && wildcard.is_none() {
                        values.retain(|key, _| types.contains_key(key));
                    }
                    for (key, value) in values.iter_mut() {
                        let ft = if let Some((sentinel, ft)) = wildcard {
                            check_wildcard_key(key, sentinel)?;
                            ft
                        } else {
                            types.get(key).ok_or_else(|| {
                                SchemaError::FieldValue(format!("invalid map key {key:?}"))
                            })?
                        };
                        *value = ft.prepare(
                            std::mem::replace(value, FieldValue::Null),
                            depth + 1,
                            mode,
                        )?;
                    }
                    if wildcard.is_none() {
                        for (key, ft) in types {
                            if !ft.allows_null() && !values.contains_key(key) {
                                return Err(SchemaError::FieldValue(format!(
                                    "required map key {key:?} is missing"
                                )));
                            }
                        }
                    }
                    return Ok(value);
                }
            }
            _ => {}
        }
        if mode == ValueMode::Read {
            self.normalize_read_back(&mut value);
            self.validate_inner(&value)?;
            return Ok(value);
        }
        // Noncanonical inputs retain the established CBOR coercion rules,
        // including byte arrays, numeric conversions and untyped containers.
        self.extract_at(field_value_to_cbor(value, depth, true)?, depth)
    }
    /// Returns `true` if this type accepts [`FieldValue::Null`], i.e. it is an
    /// [`Option`](FieldType::Option) variant.
    pub fn allows_null(&self) -> bool {
        matches!(self, FieldType::Option(_))
    }

    /// Checks that this type is a well-formed *declaration*.
    ///
    /// The shapes rejected here either cannot be told apart from another
    /// declaration once serialized, or make the wildcard rule ambiguous:
    ///
    /// - `Option<Option<T>>`: serde writes `Some(None)` and `None` both as
    ///   `null`, so the inner level can never be observed;
    /// - a `Map` that mixes a wildcard key (`"*"`, `b"*"`, `i64::MIN`; see
    ///   [`FieldKey::is_wildcard`]) with other keys: a wildcard map has
    ///   exactly one entry, so the extra keys would silently turn the
    ///   sentinel into an ordinary key;
    /// - container nesting deeper than [`MAX_CONVERSION_DEPTH`].
    ///
    /// An empty `Map` is allowed and means "any map" — the shape
    /// `#[derive(FieldTyped)]` emits for a struct without serialized fields.
    ///
    /// [`FieldEntry::new`] and `Schema` deserialization run this check, so
    /// every type that reaches a [`Schema`](crate::Schema) is well-formed.
    ///
    /// # Errors
    /// Returns [`SchemaError::FieldType`] describing the first violation.
    pub fn validate_declaration(&self) -> Result<(), SchemaError> {
        self.validate_declaration_at(0)
    }

    /// Collapses every `Option<Option<T>>` chain into a single `Option<T>`,
    /// recursing into `Array` elements and `Map` values.
    ///
    /// The two shapes are indistinguishable once serialized — serde writes
    /// `Some(None)` and `None` both as `null` — so this is lossless. It
    /// exists for schemas persisted before [`FieldType::validate_declaration`]
    /// did: the derive used to infer `Option<Option<T>>`, and rejecting such
    /// a schema on load would make every document in its collection
    /// unreachable. Declarations coming from code still fail in
    /// [`FieldEntry::new`], where the nesting is a bug worth reporting.
    pub(crate) fn flatten_nested_options(&mut self) {
        match self {
            FieldType::Array(types) => {
                types.iter_mut().for_each(FieldType::flatten_nested_options);
            }
            FieldType::Map(types) => {
                types
                    .values_mut()
                    .for_each(FieldType::flatten_nested_options);
            }
            FieldType::Option(inner) => {
                inner.flatten_nested_options();
                // The recursive call leaves at most one `Option` level
                // inside, so a single unwrap collapses the whole chain.
                if matches!(**inner, FieldType::Option(_)) {
                    let taken = std::mem::replace(inner, Box::new(FieldType::Bool));
                    if let FieldType::Option(nested) = *taken {
                        *inner = nested;
                    }
                }
            }
            _ => {}
        }
    }

    fn validate_declaration_at(&self, depth: usize) -> Result<(), SchemaError> {
        if depth > MAX_CONVERSION_DEPTH {
            return Err(SchemaError::FieldType(format!(
                "type exceeds maximum nesting depth {MAX_CONVERSION_DEPTH}"
            )));
        }

        match self {
            FieldType::Array(types) => types
                .iter()
                .try_for_each(|ft| ft.validate_declaration_at(depth + 1)),
            FieldType::Map(types) => {
                if types.len() > 1
                    && let Some(key) = types.keys().find(|k| k.is_wildcard())
                {
                    return Err(SchemaError::FieldType(format!(
                        "wildcard key {key:?} must be the only key of a Map, found {} keys",
                        types.len()
                    )));
                }
                types
                    .values()
                    .try_for_each(|ft| ft.validate_declaration_at(depth + 1))
            }
            FieldType::Option(inner) => {
                if inner.allows_null() {
                    return Err(SchemaError::FieldType(
                        "Option<Option<T>> is not allowed: serde serializes Some(None) and None identically"
                            .to_string(),
                    ));
                }
                // `Option` wrapping is type-level nesting only.
                inner.validate_declaration_at(depth)
            }
            _ => Ok(()),
        }
    }

    /// Coerce a CBOR value into a [`FieldValue`] that conforms to this type.
    ///
    /// This is more strict than [`FieldValue::try_from`]: instead of inferring
    /// a value from the CBOR shape, `extract` requires the CBOR to match
    /// `self`. For [`Option`](FieldType::Option), CBOR `null` produces
    /// [`FieldValue::Null`].
    ///
    /// # Arguments
    /// * `value` - The CBOR value to convert.
    ///
    /// # Errors
    /// Returns [`SchemaError::FieldValue`] when the CBOR shape does not
    /// match `self` (e.g. extracting a `Text` from CBOR `Bytes`), or when
    /// the value is nested deeper than [`MAX_CONVERSION_DEPTH`].
    pub fn extract(&self, value: Cbor) -> Result<FieldValue, SchemaError> {
        self.extract_at(value, 0)
    }

    /// Depth-tracked body of [`FieldType::extract`]: `depth` counts the
    /// container nesting level of `value` and is bounded by
    /// [`MAX_CONVERSION_DEPTH`] so that deeply nested payloads fail with an
    /// error instead of exhausting the stack.
    pub(super) fn extract_at(&self, value: Cbor, depth: usize) -> Result<FieldValue, SchemaError> {
        check_conversion_depth(depth)?;

        match &self {
            FieldType::Bool => FieldValue::bool_from(value),
            FieldType::I64 => FieldValue::i64_from(value),
            FieldType::U64 => FieldValue::u64_from(value),
            FieldType::F64 => FieldValue::f64_from(value),
            FieldType::F32 => FieldValue::f32_from(value),
            FieldType::Bytes => FieldValue::bytes_from(value),
            FieldType::Text => FieldValue::text_from(value),
            FieldType::Json => Ok(FieldValue::Json(cbor_into_json(value, depth)?)),
            FieldType::Vector => FieldValue::vector_from(value),
            FieldType::Array(types) => FieldValue::array_from_at(value, types, depth),
            FieldType::Map(types) => FieldValue::map_from_at(value, types, depth),
            FieldType::Option(ft) => {
                if value == Cbor::Null {
                    return Ok(FieldValue::Null);
                }
                // `Option` wrapping is type-level nesting only; the CBOR
                // value itself is not a container level.
                ft.extract_at(value, depth)
            }
        }
    }

    /// Validate that `value` is acceptable for this type.
    ///
    /// Some declared types accept a *read-back* shape in addition to their
    /// canonical variant, because generic deserialization (without type
    /// information) cannot restore the original variant:
    ///
    /// - `Vector` accepts an [`Array`](FieldValue::Array) of `U64 <= u16::MAX`
    ///   (bf16 bit patterns),
    /// - `I64` accepts a non-negative [`U64`](FieldValue::U64) within `i64`
    ///   range,
    /// - `F32` accepts an [`F64`](FieldValue::F64) that a stored `f32` can
    ///   produce when read back through CBOR (exact widening) or JSON
    ///   (shortest-decimal round trip); see `is_f32_read_back`,
    /// - `F64` accepts any integer ([`I64`](FieldValue::I64) /
    ///   [`U64`](FieldValue::U64)) and `F32` accepts the integers an `f32`
    ///   holds exactly: JSON has a single number type, so `1.0` reaches a
    ///   float field as `1`.
    ///
    /// Reading a document ([`Document::try_from_doc`](crate::Document::try_from_doc))
    /// folds accepted read-back shapes into the canonical variant.
    ///
    /// `Json` accepts its canonical variant and JSON-compatible read-back
    /// shapes. Bytes and maps with non-text keys are rejected.
    ///
    /// `Option(T)` accepts [`FieldValue::Null`]. `Json` also accepts it as the
    /// read-back representation of a JSON null payload; other types reject it.
    ///
    /// # Errors
    /// Returns [`SchemaError::FieldValue`] describing the first mismatch.
    pub fn validate(&self, value: &FieldValue) -> Result<(), SchemaError> {
        // The complexity budget covers the whole tree in one iterative pass,
        // so it only needs to run once at the top level; the recursive
        // structural checks below use `validate_inner`.
        value.validate_complexity()?;
        self.validate_inner(value)
    }

    /// Structural validation without the complexity-budget pass.
    /// See [`FieldType::validate`] for the accepted shapes.
    pub(super) fn validate_inner(&self, value: &FieldValue) -> Result<(), SchemaError> {
        match (self, value) {
            (FieldType::Bool, FieldValue::Bool(_)) => Ok(()),
            (FieldType::I64, FieldValue::I64(_)) => Ok(()),
            // Mirror of the Vector ↔ Array(U64) rule below: a non-negative
            // I64 value is observed as U64 when read back through generic
            // CBOR without type information.
            (FieldType::I64, FieldValue::U64(v)) if *v <= i64::MAX as u64 => Ok(()),
            (FieldType::U64, FieldValue::U64(_)) => Ok(()),
            (FieldType::F64, FieldValue::F64(v)) if !v.is_nan() => Ok(()),
            (FieldType::F64, FieldValue::F64(v)) => Err(SchemaError::FieldValue(format!(
                "expected non-NaN F64, got {v:?}"
            ))),
            (FieldType::F32, FieldValue::F32(v)) if !v.is_nan() => Ok(()),
            (FieldType::F32, FieldValue::F32(v)) => Err(SchemaError::FieldValue(format!(
                "expected non-NaN F32, got {v:?}"
            ))),
            // An F32 value is observed as F64 when read back through generic
            // CBOR (exact widening) or JSON (shortest-decimal round trip)
            // without type information; only the values such read-backs can
            // produce are accepted (see `is_f32_read_back`).
            (FieldType::F32, FieldValue::F64(v)) if is_f32_read_back(*v) => Ok(()),
            // JSON has a single number type: `1.0` reaches a float field as
            // the integer `1`. `F64` takes any integer, converting the way
            // serde does (`as f64`: exact to 2^53, rounded beyond), because
            // it accepts every non-NaN `F64` anyway. `F32` takes only the
            // integers it stores exactly, mirroring `is_f32_read_back` —
            // otherwise `16777217` would be silently rounded to `16777216`
            // while the same value written as `16777217.0` is rejected.
            (FieldType::F64, FieldValue::I64(_) | FieldValue::U64(_)) => Ok(()),
            (FieldType::F32, FieldValue::I64(v))
                if exact_f32_from_integer(*v as i128).is_some() =>
            {
                Ok(())
            }
            (FieldType::F32, FieldValue::U64(v))
                if exact_f32_from_integer(*v as i128).is_some() =>
            {
                Ok(())
            }
            (FieldType::Bytes, FieldValue::Bytes(_)) => Ok(()),
            (FieldType::Text, FieldValue::Text(_)) => Ok(()),
            (FieldType::Json, _) => validate_json_shape(value, 0),
            (FieldType::Vector, FieldValue::Vector(_)) => Ok(()),
            (FieldType::Vector, FieldValue::Array(values)) => {
                // Each element must be a bf16 bit pattern, i.e. fit in u16,
                // so that the value can also be extracted as a Vector.
                if values
                    .iter()
                    .all(|v| matches!(v, FieldValue::U64(u) if *u <= u16::MAX as u64))
                {
                    return Ok(());
                }
                Err(SchemaError::FieldValue(format!(
                    "expected Vector, got {}",
                    Brief(values)
                )))
            }
            (FieldType::Array(types), FieldValue::Array(values)) => match types.len() {
                0 => Ok(()),
                1 => {
                    let ft = types.first().unwrap();
                    for fv in values.iter() {
                        ft.validate_inner(fv)?;
                    }
                    Ok(())
                }
                _ => {
                    if values.len() != types.len() {
                        return Err(SchemaError::FieldValue(format!(
                            "invalid array length, expected {:?}, got {:?}",
                            types.len(),
                            values.len()
                        )));
                    }

                    for (ft, fv) in types.iter().zip(values) {
                        ft.validate_inner(fv)?;
                    }
                    Ok(())
                }
            },
            (FieldType::Map(types), FieldValue::Map(values)) => validate_map_fields(types, values),
            (FieldType::Option(ft), val) => {
                if val == &FieldValue::Null {
                    return Ok(());
                }
                ft.validate_inner(val)
            }
            _ => Err(SchemaError::FieldValue(format!(
                "expected type {self:?}, got value {}",
                Brief(value)
            ))),
        }
    }

    /// Folds a generic *read-back* shape of a scalar or `Vector` value into
    /// the declared variant (see [`FieldType::validate`] for the accepted
    /// shapes). Composite and `Json` values never get here: `prepare`
    /// recurses into them itself. Anything else is left unchanged for the
    /// following validation to report.
    fn normalize_read_back(&self, value: &mut FieldValue) {
        match self {
            FieldType::I64 => {
                if let FieldValue::U64(v) = value
                    && *v <= i64::MAX as u64
                {
                    *value = FieldValue::I64(*v as i64);
                }
            }
            FieldType::F64 => match value {
                FieldValue::I64(i) => *value = FieldValue::F64(*i as f64),
                FieldValue::U64(u) => *value = FieldValue::F64(*u as f64),
                _ => {}
            },
            FieldType::F32 => match value {
                FieldValue::F64(v) if is_f32_read_back(*v) => {
                    *value = FieldValue::F32(*v as f32);
                }
                FieldValue::I64(i) => {
                    if let Some(f) = exact_f32_from_integer(*i as i128) {
                        *value = FieldValue::F32(f);
                    }
                }
                FieldValue::U64(u) => {
                    if let Some(f) = exact_f32_from_integer(*u as i128) {
                        *value = FieldValue::F32(f);
                    }
                }
                _ => {}
            },
            FieldType::Vector => {
                if let FieldValue::Array(values) = value {
                    // This iterator pipeline benchmarks better with the
                    // workspace's size-optimized release profile. Reads
                    // validate after normalization, avoiding a second scan.
                    let vector = values
                        .iter()
                        .map(|value| match value {
                            FieldValue::U64(bits) if *bits <= u16::MAX as u64 => {
                                Some(bf16::from_bits(*bits as u16))
                            }
                            _ => None,
                        })
                        .collect::<Option<Vec<_>>>();
                    if let Some(vector) = vector {
                        *value = FieldValue::Vector(vector);
                    }
                }
            }
            _ => {}
        }
    }

    /// Returns `true` when a field previously declared as `old` may be
    /// re-declared as `self` without rewriting the documents already stored.
    ///
    /// Types must match exactly, with two exceptions that keep every stored
    /// value readable:
    ///
    /// - a required type may become optional (`T` → `Option<T>`), at the top
    ///   level or anywhere inside a composite: stored values are non-null and
    ///   still match `T`. The reverse (`Option<T>` → `T`) stays incompatible,
    ///   because stored nulls would fail validation;
    /// - mirroring the *top-level* evolution rule enforced by
    ///   [`Schema::upgrade_with`](crate::Schema::upgrade_with) (a new field
    ///   must be optional, a removed field is tolerated on read), a
    ///   non-wildcard [`FieldType::Map`] — the shape `#[derive(FieldTyped)]`
    ///   emits for a nested struct — may **gain** a key, provided the new key
    ///   is optional, so documents written before the upgrade (which lack it)
    ///   still validate; and **lose** a key: stored values keep the stale
    ///   entry, which document reads drop.
    ///
    /// A key whose type changed otherwise, a new *required* key, and any
    /// change of the wildcard-ness or key variant of a map remain
    /// incompatible, as do all other type changes.
    /// Changing between an empty (open) and a nonempty map also requires a
    /// migration. [`Schema::upgrade_with`](crate::Schema::upgrade_with)
    /// additionally checks persistent history to prevent retired-key reuse.
    pub fn is_compatible_upgrade_of(&self, old: &FieldType) -> bool {
        match (self, old) {
            (FieldType::Array(new_types), FieldType::Array(old_types)) => {
                // Array arity is part of the shape: a tuple-like array that
                // gains or loses an element changes every stored value.
                new_types.len() == old_types.len()
                    && new_types
                        .iter()
                        .zip(old_types)
                        .all(|(new, old)| new.is_compatible_upgrade_of(old))
            }
            (FieldType::Map(new_types), FieldType::Map(old_types)) => {
                // Empty declarations are open maps, not empty fixed structs.
                // Narrowing can invalidate values; opening a formerly fixed
                // map can expose keys retired by an earlier schema version.
                if old_types.is_empty() != new_types.is_empty() {
                    return false;
                }
                match (as_wildcard_map(new_types), as_wildcard_map(old_types)) {
                    (Some((new_key, new_ft)), Some((old_key, old_ft))) => {
                        new_key == old_key && new_ft.is_compatible_upgrade_of(old_ft)
                    }
                    // A wildcard map and an explicitly keyed one describe
                    // different shapes; neither can become the other.
                    (Some(_), None) | (None, Some(_)) => false,
                    (None, None) => new_types.iter().all(|(k, new_ft)| match old_types.get(k) {
                        Some(old_ft) => new_ft.is_compatible_upgrade_of(old_ft),
                        // Keys only in `old` were removed: tolerated on read.
                        None => new_ft.allows_null(),
                    }),
                }
            }
            (FieldType::Option(new_ft), FieldType::Option(old_ft)) => {
                new_ft.is_compatible_upgrade_of(old_ft)
            }
            // Making a type optional is read-safe: every stored value is
            // non-null and still matches the inner type. The reverse is not,
            // so it falls through to the exact-match arm and fails.
            (FieldType::Option(new_ft), old) => new_ft.is_compatible_upgrade_of(old),
            (new, old) => new == old,
        }
    }
}

/// Returns `true` when `v` is a possible read-back shape of a stored `f32`
/// observed as `f64` through generic (schema-less) deserialization:
///
/// - **CBOR**: an `f32` is widened exactly, so `v` equals `f64::from(f)` for
///   some finite or infinite `f32` `f`;
/// - **JSON**: an `f32` is serialized as its shortest round-trip decimal
///   (e.g. `2.71`), which parses back into an `f64` that is generally *not*
///   the exact widening. `v` is accepted when it equals the `f64` parse of
///   the shortest decimal form of `v as f32`.
///
/// Any other `f64` — NaN, finite values beyond the `f32` range, or values
/// with precision no `f32` read-back can produce (e.g. `2.7100000000001`) —
/// is rejected.
pub(super) fn is_f32_read_back(v: f64) -> bool {
    if v.is_nan() {
        return false;
    }
    let f = v as f32;
    if f.is_infinite() && v.is_finite() {
        // Finite f64 beyond the f32 range: not a possible read-back.
        return false;
    }
    // CBOR read-back: exact widening of the f32.
    if f64::from(f) == v {
        return true;
    }
    // Use the actual JSON formatter AND parser. Rust Display can choose a
    // different shortest decimal at ties, and serde_json's default parser
    // can differ from str::parse by one f64 ULP. A stack buffer avoids a
    // temporary String on this read-back path.
    let mut buffer = [0u8; 64];
    let mut cursor = std::io::Cursor::new(buffer.as_mut_slice());
    if serde_json::to_writer(&mut cursor, &f).is_err() {
        return false;
    }
    let len = cursor.position() as usize;
    let json = &buffer[..len];
    if matches!(serde_json::from_slice::<f64>(json), Ok(parsed) if parsed == v) {
        return true;
    }
    // Accept correctly rounded JSON readers too, independently of the
    // application's unified serde_json/float_roundtrip feature setting.
    std::str::from_utf8(json)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        == Some(v)
}

pub(super) fn validate_map_fields(
    types: &BTreeMap<FieldKey, FieldType>,
    values: &BTreeMap<FieldKey, FieldValue>,
) -> Result<(), SchemaError> {
    if types.is_empty() {
        return Ok(());
    }

    if let Some((wildcard, ft)) = as_wildcard_map(types) {
        for (k, fv) in values {
            check_wildcard_key(k, wildcard)?;
            ft.validate_inner(fv)?;
        }
        return Ok(());
    }

    if let Some(k) = values.keys().find(|k| !types.contains_key(*k)) {
        return Err(SchemaError::FieldValue(format!("invalid map key {k:?}")));
    }

    for (k, ft) in types {
        let rt = match values.get(k) {
            None if ft.allows_null() => Ok(()),
            None => Err(SchemaError::FieldValue(format!(
                "required map key {k:?} is missing"
            ))),
            Some(v) => ft.validate_inner(v),
        };

        rt.map_err(|err| {
            SchemaError::FieldValue(format!(
                "invalid map value at key {k:?}, error: {}",
                err.detail()
            ))
        })?;
    }
    Ok(())
}
