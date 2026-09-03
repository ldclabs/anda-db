//! # References and Literals
//!
//! A Proposition endpoint is either a reference to a Cognitive Element or a
//! Literal (Spec §6, §9). Both have to reduce to a deterministic key, because
//! Proposition identity is defined by the tuple and a Space keeps one canonical
//! Proposition per semantic tuple (§12.5, §93.6) — which is an equality
//! question the storage layer has to answer, not a matter of taste.
//!
//! The rules that make it deterministic are §9.6's canonical form, applied on
//! write:
//!
//! - a string is compared by its Unicode scalar values after NFC
//!   normalization — no trimming, no case folding — so NFC and NFD spellings
//!   of one word are one Literal and two strings that differ by a trailing
//!   space are two;
//! - a number is equal by *mathematical value*, so `1`, `1.0` and `1e0` are
//!   one Literal and not three Propositions, and `-0` is `0` (§9.3, §9.6);
//! - there is no language tag (§9.4): a `language` member is refused as
//!   `TypeMismatch` rather than accepted and dropped, or accepted and made
//!   part of identity.

use anda_kip::{Json, KipError, Map, Number};
use std::fmt::Write as _;
use unicode_normalization::UnicodeNormalization;

use crate::id::ElementId;

/// The Core datatype of a string Literal (§9.2).
pub const DT_STRING: &str = "string";
/// The Core datatype of a numeric Literal.
pub const DT_NUMBER: &str = "number";
/// The Core datatype of a boolean Literal.
pub const DT_BOOLEAN: &str = "boolean";
/// The Core datatype of the `null` Literal.
pub const DT_NULL: &str = "null";

/// A Core Literal (Spec §9.2).
///
/// The payload is restricted to JSON scalar semantics; an array or object is
/// not a Core Literal, and a structured value that needs semantic identity
/// belongs in a Concept. The `datatype` is one of the four baseline names;
/// a finer value shape — a timestamp, a URI — is a `format` the Predicate
/// declares (§20.15), validated on write and never part of identity.
#[derive(Clone, Debug, PartialEq)]
pub struct Literal {
    /// The scalar payload, in canonical form (§9.6).
    pub value: Json,
    /// The datatype: one of the `DT_*` constants.
    pub datatype: String,
}

impl Literal {
    /// Builds a Literal from a bare JSON scalar, inferring the Core datatype
    /// and canonicalizing the value (§9.6).
    ///
    /// This is the "primitive shorthand" of §9.1: the model-facing syntax
    /// writes `"+08:00"` or `3`, and the canonical internal model still
    /// distinguishes the datatype.
    pub fn from_scalar(value: Json) -> Result<Self, KipError> {
        let (value, datatype) = match value {
            Json::String(text) => (Json::String(text.nfc().collect()), DT_STRING),
            Json::Number(n) => {
                if n.as_f64().is_some_and(|f| !f.is_finite()) {
                    return Err(KipError::type_mismatch(
                        "NaN and Infinity are not valid Core JSON numbers",
                    ));
                }
                (canonical_number_json(&n), DT_NUMBER)
            }
            Json::Bool(flag) => (Json::Bool(flag), DT_BOOLEAN),
            Json::Null => (Json::Null, DT_NULL),
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
        })
    }

    /// Reads the explicit `{value, datatype}` form.
    ///
    /// A `language` member is refused (§9.4): the baseline Literal carries no
    /// tag, and accepting one only to drop it would let a caller believe two
    /// tagged strings were kept apart. A `datatype` is accepted in the
    /// baseline spelling or with the `kip:` prefix earlier drafts used, and
    /// must agree with the value it labels.
    pub fn from_object(map: &Map<String, Json>) -> Result<Self, KipError> {
        if let Some(language) = map.get("language")
            && !language.is_null()
        {
            return Err(KipError::type_mismatch(
                "a Literal carries no language tag (§9.4); multilingual text belongs on a \
                 Concept with per-language attributes or in a schema-defined value object",
            ));
        }
        let value = map.get("value").cloned().unwrap_or(Json::Null);
        let literal = Self::from_scalar(value)?;
        if let Some(datatype) = map.get("datatype") {
            match datatype {
                Json::Null => {}
                Json::String(name) => {
                    let declared = normalize_datatype(name);
                    if declared != literal.datatype {
                        return Err(KipError::type_mismatch(format!(
                            "a Literal of datatype {declared:?} cannot carry {}; the baseline \
                             datatypes are string, number, boolean and null (§9.2)",
                            literal.value
                        )));
                    }
                }
                _ => {
                    return Err(KipError::type_mismatch(
                        "a Literal datatype must be a symbol string",
                    ));
                }
            }
        }
        Ok(literal)
    }

    /// The persisted form: always the explicit object, never the shorthand.
    ///
    /// Storing the object keeps a stored endpoint self-describing: a reader
    /// that meets `{value, datatype}` knows it holds a Literal and never
    /// mistakes a string that happens to spell an element id for a reference.
    pub fn to_json(&self) -> Json {
        let mut map = Map::new();
        map.insert("value".into(), self.value.clone());
        map.insert("datatype".into(), Json::String(self.datatype.clone()));
        Json::Object(map)
    }
}

/// One datatype name in the baseline spelling (§9.2).
///
/// Earlier drafts spelled the four names `kip:string` and so on; a package or
/// a caller still using that spelling means the same datatype.
pub fn normalize_datatype(name: &str) -> String {
    name.strip_prefix("kip:").unwrap_or(name).to_string()
}

/// Canonicalizes a finite JSON number to its mathematical value (§9.6).
///
/// `1`, `1.0` and `1e0` all become the integer `1`, and `-0` becomes `0`, so
/// none of them can become a second Proposition.
fn canonical_number_json(n: &Number) -> Json {
    if let Some(i) = n.as_i64() {
        return Json::from(i);
    }
    if let Some(u) = n.as_u64() {
        return Json::from(u);
    }
    let f = n.as_f64().unwrap_or(0.0);
    if f.fract() == 0.0 && f.abs() < 9.007_199_254_740_992e15 {
        // `-0.0 as i64` is 0, which is the point.
        return Json::from(f as i64);
    }
    Number::from_f64(f).map(Json::Number).unwrap_or(Json::Null)
}

/// Canonicalizes a finite JSON number to its normalized value form, for the
/// equality key.
fn canonical_number(n: &Number) -> String {
    match canonical_number_json(n) {
        Json::Number(n) => n.to_string(),
        other => other.to_string(),
    }
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

/// The endpoint an inline `{...}` matcher names, when it names one (§8.1, §8.2).
///
/// A tuple endpoint may be written as an object pattern — `term` admits
/// `object_pattern` in the grammar — but only two spellings of one *name*
/// something already: `{id: ...}` is a Local Element Reference and
/// `{canonical_id: ...}` is a Canonical Identity Reference. Every other matcher
/// describes a search, and resolving one would mean picking a winner among the
/// Concepts a description is allowed to match — the arbitrary choice §7.2
/// forbids for names.
///
/// The refusal is `IdentitySelectorRequired` and not `UnsupportedCapability`:
/// no engine should ever resolve a description to one endpoint, so this is not
/// a gap that a later version closes.
///
/// `resolve` reads a `:parameter`, because the two callers bind parameters from
/// different places (a query's request/operation maps, a mutation's bindings)
/// and neither should have to know about the other.
pub fn matcher_endpoint(
    matcher: &anda_kip::ObjectMatcher,
    what: &str,
    mut resolve: impl FnMut(&str) -> Result<Json, KipError>,
) -> Result<Endpoint, KipError> {
    for field in ["id", "canonical_id"] {
        let Some(value) = matcher.get(field) else {
            continue;
        };
        // An identity resolves the endpoint; it does not also filter it. A
        // matcher carrying more than the identity asked for something this
        // position cannot do, and answering it by dropping the rest would let
        // `{id: "C-1", name: "Zed"}` match C-1 whatever C-1 is called — the
        // silent wrong answer an unconstrained endpoint gives, one member in.
        if matcher.len() > 1 {
            let extra: Vec<&str> = matcher
                .keys()
                .filter(|key| key.as_str() != field)
                .map(String::as_str)
                .collect();
            return Err(KipError::identity_selector_required(format!(
                "{what} names `{field}`, so it is resolved by identity and not matched by \
                 description; {} would be silently ignored. Drop {} or bind the element with its \
                 own pattern",
                extra.join(", "),
                if extra.len() == 1 { "it" } else { "them" }
            )));
        }
        let resolved = match value {
            anda_kip::MatchValue::Literal(literal) => Json::from(literal.clone()),
            anda_kip::MatchValue::Param(name) => resolve(name)?,
            _ => {
                return Err(KipError::identity_selector_required(format!(
                    "`{field}` in {what} must be a literal identity or a parameter, not a pattern"
                )));
            }
        };
        let Json::String(id) = resolved else {
            return Err(KipError::identity_selector_required(format!(
                "`{field}` in {what} must be a string, got {resolved}"
            )));
        };
        let mut map = Map::new();
        map.insert(field.to_string(), Json::String(id));
        return Endpoint::from_json(&Json::Object(map));
    }

    Err(KipError::identity_selector_required(format!(
        "{what} written as an object must name a stable identity: {{id: \"…\"}} or \
         {{canonical_id: \"…\"}}; matching one by description would pick a winner among the \
         Concepts a description is allowed to share"
    )))
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
    fn a_language_tag_is_refused_rather_than_dropped() {
        // Spec §9.4, KIP2-CORE-024: accepted and dropped, two tagged strings
        // would silently collapse; accepted and kept, they would split a
        // Literal the baseline says is one.
        let err =
            Endpoint::from_json(&json!({"value": "苹果", "language": "zh-Hans"})).unwrap_err();
        assert_eq!(err.name(), "TypeMismatch");
        // A `kip:`-prefixed datatype is the same datatype.
        assert_eq!(
            key_of(json!({"value": "苹果", "datatype": "kip:string"})),
            key_of(json!("苹果"))
        );
    }

    #[test]
    fn nfc_and_nfd_spellings_are_one_literal_and_whitespace_is_not() {
        // Spec §9.6, KIP2-CORE-023: canonical form normalizes and never trims.
        let composed = key_of(json!("caf\u{e9}"));
        let decomposed = key_of(json!("cafe\u{301}"));
        assert_eq!(composed, decomposed);
        assert_ne!(composed, key_of(json!("caf\u{e9} ")));
        assert_ne!(composed, key_of(json!("CAF\u{c9}")));
        // The stored value is the canonical form.
        let literal = Literal::from_scalar(json!("cafe\u{301}")).unwrap();
        assert_eq!(literal.value, json!("caf\u{e9}"));
    }

    #[test]
    fn negative_zero_is_zero() {
        assert_eq!(key_of(json!(-0.0)), key_of(json!(0)));
        assert_eq!(Literal::from_scalar(json!(1.0)).unwrap().value, json!(1));
    }

    #[test]
    fn null_is_equal_only_to_null() {
        let null = key_of(json!(null));
        assert_ne!(null, key_of(json!("")));
        assert_ne!(null, key_of(json!(false)));
        assert_eq!(null, key_of(json!({"value": null})));
    }

    #[test]
    fn a_datatype_must_agree_with_its_value() {
        // §9.2: the four baseline names, and nothing finer — a timestamp is a
        // `format` the Predicate declares, never a datatype of its own.
        let value = json!({"value": "2026-08-13T10:00:00Z", "datatype": "string"});
        let endpoint = Endpoint::from_json(&value).unwrap();
        assert_eq!(endpoint.to_json(), value);
        assert_eq!(endpoint.key(), key_of(json!("2026-08-13T10:00:00Z")));
        let err = Endpoint::from_json(&json!({"value": 3, "datatype": "string"})).unwrap_err();
        assert_eq!(err.name(), "TypeMismatch");
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
