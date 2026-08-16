//! # References and Literals
//!
//! A Proposition endpoint is either a reference to a Cognitive Element or a
//! Literal (Spec §6, §9). Both have to reduce to a deterministic key, because
//! Proposition identity is defined by the tuple and a Space keeps one canonical
//! Proposition per semantic tuple (§12.5, §93.6) — which is an equality
//! question the storage layer has to answer, not a matter of taste.
//!
//! The two rules that make it deterministic:
//!
//! - a number is equal by *normalized finite value*, so `1`, `1.0` and `1e0`
//!   are one Literal and not three Propositions (§9.4);
//! - a language tag is part of Literal identity, so `"苹果"@zh-Hans` and a
//!   bare `"苹果"` are different Literals (§9.5).

use anda_kip::{Json, KipError, Map, Number};
use std::fmt::Write as _;

use crate::id::ElementId;

/// The Core datatype of a string Literal.
pub const DT_STRING: &str = "kip:string";
/// The Core datatype of a numeric Literal.
pub const DT_NUMBER: &str = "kip:number";
/// The Core datatype of a boolean Literal.
pub const DT_BOOLEAN: &str = "kip:boolean";
/// The Core datatype of the `null` Literal.
pub const DT_NULL: &str = "kip:null";

/// A Core Literal (Spec §9.2).
///
/// The payload is restricted to JSON scalar semantics; an array or object is
/// not a Core Literal, and a structured value that needs semantic identity
/// belongs in a Concept.
#[derive(Clone, Debug, PartialEq)]
pub struct Literal {
    /// The scalar payload.
    pub value: Json,
    /// The datatype symbol; one of the `DT_*` constants, or a Schema-defined
    /// refinement such as `kip:datetime`.
    pub datatype: String,
    /// The language tag, when language is semantically relevant.
    pub language: Option<String>,
}

impl Literal {
    /// Builds a Literal from a bare JSON scalar, inferring the Core datatype.
    ///
    /// This is the "primitive shorthand" of §9.3: the model-facing syntax
    /// writes `"+08:00"` or `3`, and the canonical internal model still
    /// distinguishes the datatype.
    pub fn from_scalar(value: Json) -> Result<Self, KipError> {
        let datatype = match &value {
            Json::String(_) => DT_STRING,
            Json::Number(n) => {
                if n.as_f64().is_some_and(|f| !f.is_finite()) {
                    return Err(KipError::type_mismatch(
                        "NaN and Infinity are not valid Core JSON numbers",
                    ));
                }
                DT_NUMBER
            }
            Json::Bool(_) => DT_BOOLEAN,
            Json::Null => DT_NULL,
            Json::Array(_) | Json::Object(_) => {
                return Err(KipError::type_mismatch(
                    "arrays and objects are not Core Literals; a structured value with its own \
                     semantic identity belongs in a Concept",
                ));
            }
        };
        Ok(Self {
            value,
            datatype: datatype.to_string(),
            language: None,
        })
    }

    /// Reads the explicit `{value, datatype, language}` form.
    pub fn from_object(map: &Map<String, Json>) -> Result<Self, KipError> {
        let value = map.get("value").cloned().unwrap_or(Json::Null);
        let mut literal = Self::from_scalar(value)?;
        if let Some(datatype) = map.get("datatype") {
            match datatype {
                Json::Null => {}
                Json::String(s) => literal.datatype = s.clone(),
                _ => {
                    return Err(KipError::type_mismatch(
                        "a Literal datatype must be a symbol string",
                    ));
                }
            }
        }
        match map.get("language") {
            None | Some(Json::Null) => {}
            Some(Json::String(tag)) => literal.language = Some(tag.clone()),
            Some(_) => {
                return Err(KipError::type_mismatch(
                    "a Literal language must be a language tag string",
                ));
            }
        }
        Ok(literal)
    }

    /// The persisted form: always the explicit object, never the shorthand.
    ///
    /// Storing the shorthand would throw away the datatype the moment a Schema
    /// refined it — `kip:datetime` would read back as `kip:string`.
    pub fn to_json(&self) -> Json {
        let mut map = Map::new();
        map.insert("value".into(), self.value.clone());
        map.insert("datatype".into(), Json::String(self.datatype.clone()));
        if let Some(language) = &self.language {
            map.insert("language".into(), Json::String(language.clone()));
        }
        Json::Object(map)
    }
}

/// Canonicalizes a finite JSON number to its normalized value form.
///
/// `1`, `1.0` and `1e0` all reduce to `1`, so they cannot become three
/// distinct Propositions (§9.4).
fn canonical_number(n: &Number) -> String {
    if let Some(i) = n.as_i64() {
        return i.to_string();
    }
    if let Some(u) = n.as_u64() {
        return u.to_string();
    }
    let f = n.as_f64().unwrap_or(f64::NAN);
    // A float that is exactly an integer must agree with the integer spelling,
    // or `1` and `1.0` would key differently after all.
    if f.is_finite() && f.fract() == 0.0 && f.abs() < 9.007_199_254_740_992e15 {
        return format!("{}", f as i64);
    }
    // Rust's `{}` for f64 is the shortest representation that round-trips.
    format!("{f}")
}

/// One endpoint of a Proposition tuple.
#[derive(Clone, Debug, PartialEq)]
pub enum Endpoint {
    /// A same-Space reference to a Cognitive Element (§6.1).
    Local(ElementId),
    /// A canonical external identity, used when no local Concept exists (§6.2).
    Canonical(String),
    /// An explicit cross-Space reference (§6.3).
    Foreign {
        /// The Space the element lives in.
        space_id: String,
        /// The element's id inside that Space.
        element_id: String,
    },
    /// A Literal value (§9).
    Literal(Literal),
}

/// The field separator inside a composite key.
///
/// A unit separator cannot occur in an element id and is vanishingly unlikely
/// in a datatype symbol, so no two different endpoints can collide by writing
/// each other's separator.
const SEP: char = '\u{1f}';

impl Endpoint {
    /// Reads an endpoint from its persisted JSON form.
    ///
    /// A bare scalar is the Literal shorthand; an object is a reference or the
    /// explicit Literal form, told apart by which key it carries.
    pub fn from_json(value: &Json) -> Result<Self, KipError> {
        let Json::Object(map) = value else {
            return Ok(Endpoint::Literal(Literal::from_scalar(value.clone())?));
        };

        if let Some(id) = map.get("id") {
            let Json::String(id) = id else {
                return Err(KipError::structural_reference_invalid(
                    "an element reference's `id` must be a string",
                ));
            };
            return Ok(Endpoint::Local(id.parse()?));
        }
        if let Some(canonical) = map.get("canonical_id") {
            let Json::String(canonical) = canonical else {
                return Err(KipError::structural_reference_invalid(
                    "a canonical identity reference's `canonical_id` must be a string",
                ));
            };
            return Ok(Endpoint::Canonical(canonical.clone()));
        }
        if let (Some(space_id), Some(element_id)) = (map.get("space_id"), map.get("element_id")) {
            let (Json::String(space_id), Json::String(element_id)) = (space_id, element_id) else {
                return Err(KipError::structural_reference_invalid(
                    "a foreign Space reference needs string `space_id` and `element_id`",
                ));
            };
            return Ok(Endpoint::Foreign {
                space_id: space_id.clone(),
                element_id: element_id.clone(),
            });
        }
        if map.contains_key("value") {
            return Ok(Endpoint::Literal(Literal::from_object(map)?));
        }
        Err(KipError::structural_reference_invalid(
            "an endpoint object must carry `id`, `canonical_id`, `space_id`+`element_id`, or a \
             Literal `value`",
        ))
    }

    /// The persisted JSON form.
    pub fn to_json(&self) -> Json {
        match self {
            Endpoint::Local(id) => {
                let mut map = Map::new();
                map.insert("id".into(), Json::String(id.to_string()));
                Json::Object(map)
            }
            Endpoint::Canonical(canonical_id) => {
                let mut map = Map::new();
                map.insert("canonical_id".into(), Json::String(canonical_id.clone()));
                Json::Object(map)
            }
            Endpoint::Foreign {
                space_id,
                element_id,
            } => {
                let mut map = Map::new();
                map.insert("space_id".into(), Json::String(space_id.clone()));
                map.insert("element_id".into(), Json::String(element_id.clone()));
                Json::Object(map)
            }
            Endpoint::Literal(literal) => literal.to_json(),
        }
    }

    /// The deterministic equality key.
    ///
    /// Two endpoints are the same endpoint exactly when their keys are equal,
    /// which is what makes a B-Tree index over this column answer the identity
    /// question the tuple asks.
    pub fn key(&self) -> String {
        match self {
            Endpoint::Local(id) => format!("id{SEP}{id}"),
            Endpoint::Canonical(canonical_id) => format!("cid{SEP}{canonical_id}"),
            Endpoint::Foreign {
                space_id,
                element_id,
            } => format!("fs{SEP}{space_id}{SEP}{element_id}"),
            Endpoint::Literal(literal) => {
                let mut key = String::from("lit");
                let _ = write!(key, "{SEP}{}", literal.datatype);
                let _ = write!(key, "{SEP}{}", literal.language.as_deref().unwrap_or(""));
                let _ = match &literal.value {
                    Json::String(s) => write!(key, "{SEP}s{s}"),
                    Json::Number(n) => write!(key, "{SEP}n{}", canonical_number(n)),
                    Json::Bool(b) => write!(key, "{SEP}b{b}"),
                    // The datatype segment already separates `null` from the
                    // empty string, so the payload segment can be empty.
                    _ => write!(key, "{SEP}z"),
                };
                key
            }
        }
    }

    /// The element this endpoint resolves to inside this Space, if any.
    ///
    /// Same-Space closure is checked against this: a Literal has nothing to
    /// close over, and a canonical or foreign reference is deliberately outside
    /// the rule (§7).
    pub fn local(&self) -> Option<ElementId> {
        match self {
            Endpoint::Local(id) => Some(*id),
            _ => None,
        }
    }
}

/// The structural identity of a Proposition tuple within its Space (§12.5).
///
/// Digested rather than concatenated because the raw key of a Literal endpoint
/// is unbounded — a Proposition object can be a paragraph — while a B-Tree
/// index key should not be. The digest is over the same separated encoding the
/// individual key columns use, so two tuples collide exactly when their
/// endpoints and predicate are equal.
pub fn tuple_key(
    space: &str,
    subject: &Endpoint,
    predicate_ref: &str,
    object: &Endpoint,
) -> String {
    use sha3::{Digest, Sha3_256};

    let mut hasher = Sha3_256::new();
    for part in [space, &subject.key(), predicate_ref, &object.key()] {
        // Length-prefixing keeps `("ab", "c")` from digesting like `("a", "bc")`.
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part.as_bytes());
    }
    hex::encode(hasher.finalize())
}

/// Reads a structural reference that must resolve to a local element.
///
/// Core structural references are same-Space by definition; a canonical or
/// foreign identity in one of these slots is a malformed record rather than an
/// unresolved lookup (§8.2, §93.3).
pub fn local_ref(value: &Json, field: &str) -> Result<ElementId, KipError> {
    match Endpoint::from_json(value)? {
        Endpoint::Local(id) => Ok(id),
        _ => Err(KipError::structural_reference_invalid(format!(
            "`{field}` must reference a local element by id"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anda_kip::ElementKind;
    use serde_json::json;

    fn key_of(value: Json) -> String {
        Endpoint::from_json(&value).unwrap().key()
    }

    #[test]
    fn one_and_one_point_zero_are_one_literal() {
        // Spec §9.4: three lexical forms of the same finite value must not
        // become three semantic Propositions.
        let one = key_of(json!(1));
        assert_eq!(key_of(json!(1.0)), one);
        assert_eq!(key_of(serde_json::from_str::<Json>("1e0").unwrap()), one);
        assert_ne!(key_of(json!(1.5)), one);
        assert_ne!(key_of(json!("1")), one);
    }

    #[test]
    fn a_language_tag_changes_literal_identity() {
        // Spec §9.5.
        let bare = key_of(json!("苹果"));
        let tagged = key_of(json!({"value": "苹果", "language": "zh-Hans"}));
        assert_ne!(bare, tagged);
        assert_eq!(
            tagged,
            key_of(json!({"value": "苹果", "datatype": "kip:string", "language": "zh-Hans"}))
        );
    }

    #[test]
    fn null_is_equal_only_to_null() {
        let null = key_of(json!(null));
        assert_ne!(null, key_of(json!("")));
        assert_ne!(null, key_of(json!(false)));
        assert_eq!(null, key_of(json!({"value": null})));
    }

    #[test]
    fn a_refined_datatype_survives_a_round_trip() {
        let value = json!({"value": "2026-08-13T10:00:00Z", "datatype": "kip:datetime"});
        let endpoint = Endpoint::from_json(&value).unwrap();
        assert_eq!(endpoint.to_json(), value);
        // A datetime and a plain string with the same text are different
        // Literals, so they cannot silently share a Proposition.
        assert_ne!(endpoint.key(), key_of(json!("2026-08-13T10:00:00Z")));
    }

    #[test]
    fn reference_kinds_stay_distinguishable() {
        let local = Endpoint::Local(ElementId::new(ElementKind::Concept, 1));
        assert_eq!(Endpoint::from_json(&local.to_json()).unwrap(), local);
        assert_eq!(local.local(), Some(ElementId::new(ElementKind::Concept, 1)));

        let canonical = key_of(json!({"canonical_id": "did:example:123"}));
        let foreign = key_of(json!({"space_id": "public://research", "element_id": "C-1"}));
        assert_ne!(canonical, foreign);
        assert_ne!(canonical, local.key());
        // A foreign reference is not a local one even when the ids agree.
        assert_ne!(foreign, local.key());
    }

    #[test]
    fn a_structured_value_is_not_a_core_literal() {
        // Spec §9.2.
        assert!(Endpoint::from_json(&json!([1, 2])).is_err());
        assert!(Literal::from_scalar(json!({"a": 1})).is_err());
    }

    #[test]
    fn a_tuple_key_separates_what_a_concatenation_would_merge() {
        let alice = Endpoint::Local(ElementId::new(ElementKind::Concept, 1));
        let bob = Endpoint::Local(ElementId::new(ElementKind::Concept, 2));
        let dark = Endpoint::Literal(Literal::from_scalar(json!("dark")).unwrap());

        let base = tuple_key("s1", &alice, "prefers", &dark);
        assert_eq!(base, tuple_key("s1", &alice, "prefers", &dark));
        // Every coordinate of the tuple participates in its identity.
        assert_ne!(base, tuple_key("s2", &alice, "prefers", &dark));
        assert_ne!(base, tuple_key("s1", &bob, "prefers", &dark));
        assert_ne!(base, tuple_key("s1", &alice, "likes", &dark));
        assert_ne!(base, tuple_key("s1", &alice, "prefers", &bob));

        // Length-prefixing: a boundary shifted between two parts must not
        // produce the same key.
        assert_ne!(
            tuple_key("s", &alice, "ab", &dark),
            tuple_key("s", &alice, "a", &dark)
        );
    }

    #[test]
    fn a_structural_slot_refuses_a_non_local_reference() {
        assert!(local_ref(&json!({"id": "P-1"}), "proposition_id").is_ok());
        let err =
            local_ref(&json!({"canonical_id": "did:example:1"}), "proposition_id").unwrap_err();
        assert_eq!(err.name(), "StructuralReferenceInvalid");
    }
}
