//! # Element identity
//!
//! Every durable Cognitive Element carries an immutable, Nexus-local `id` that
//! is opaque to clients and never reused (Spec §7.1). The engine mints one from
//! two things it already has: the element's Core kind and the row id `anda_db`
//! allocated for it.
//!
//! Encoding the kind in the id is not decoration. A KIP reference such as
//! `{"id": "A-42"}` arrives with no other type information, and every read of
//! it has to know which collection to look in before it can look anywhere. The
//! alternative — a global id → collection map — would be a second index to keep
//! consistent with the rows it describes, for no gain.
//!
//! ```text
//! C-<n>   Concept
//! P-<n>   Proposition
//! A-<n>   Assertion
//! E-<n>   Evidence
//! X-<n>   Activity
//! ```
//!
//! `anda_db` allocates row ids monotonically per collection, so `(kind, seq)`
//! is unique for the lifetime of the Nexus across every MemorySpace it holds —
//! Spaces are a field on the row, not a separate collection (§29).

use anda_kip::{ElementKind, KipError};
use std::{fmt, str::FromStr};

/// The Nexus-local identity of one Cognitive Element.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ElementId {
    /// Which Core kind the element is.
    pub kind: ElementKind,
    /// The `anda_db` row id inside that kind's collection.
    pub seq: u64,
}

/// Ordered by tag then sequence, so a sorted set of ids groups by kind.
///
/// [`ElementKind`] itself is deliberately unordered — no Core kind outranks
/// another — so the order comes from the wire tag, which is stable.
impl Ord for ElementId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        ElementId::tag(self.kind)
            .cmp(&ElementId::tag(other.kind))
            .then(self.seq.cmp(&other.seq))
    }
}

impl PartialOrd for ElementId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl ElementId {
    /// Builds an id for a freshly allocated row.
    pub const fn new(kind: ElementKind, seq: u64) -> Self {
        Self { kind, seq }
    }

    /// The single-character kind tag used by the wire form.
    pub const fn tag(kind: ElementKind) -> char {
        match kind {
            ElementKind::Concept => 'C',
            ElementKind::Proposition => 'P',
            ElementKind::Assertion => 'A',
            ElementKind::Evidence => 'E',
            // `A` is taken by Assertion, and Activity is the rarer term in a
            // reference, so it takes the arbitrary letter.
            ElementKind::Activity => 'X',
        }
    }

    /// The kind a tag denotes, if it denotes one.
    pub const fn kind_of(tag: char) -> Option<ElementKind> {
        match tag {
            'C' => Some(ElementKind::Concept),
            'P' => Some(ElementKind::Proposition),
            'A' => Some(ElementKind::Assertion),
            'E' => Some(ElementKind::Evidence),
            'X' => Some(ElementKind::Activity),
            _ => None,
        }
    }

    /// Parses an id, requiring it to name the expected kind.
    ///
    /// A reference that resolves to the wrong kind is a structural reference
    /// error, not a lookup miss: `Assertion.proposition_id` pointing at an
    /// Evidence record is malformed input, and reporting it as "not found"
    /// would send a caller looking for a row that was never the right row
    /// (Spec §17.2).
    pub fn parse_kind(s: &str, expected: ElementKind) -> Result<Self, KipError> {
        let id: ElementId = s.parse()?;
        if id.kind != expected {
            return Err(KipError::structural_reference_invalid(format!(
                "{s} names a {} where a {expected} was required",
                id.kind
            )));
        }
        Ok(id)
    }
}

impl fmt::Display for ElementId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", Self::tag(self.kind), self.seq)
    }
}

impl FromStr for ElementId {
    type Err = KipError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let invalid = || {
            KipError::invalid_identifier(format!(
                "{s:?} is not a Nexus element id; the form is a kind tag, a hyphen and a decimal \
                 sequence, e.g. \"C-42\""
            ))
        };
        let (tag, seq) = s.split_once('-').ok_or_else(invalid)?;
        let mut tag = tag.chars();
        let (Some(tag), None) = (tag.next(), tag.next()) else {
            return Err(invalid());
        };
        let kind = Self::kind_of(tag).ok_or_else(invalid)?;
        // `str::parse` would accept a leading `+`, and both it and a leading
        // zero would let one element answer to two spellings of its own id —
        // which is how two references to the same row compare unequal.
        if seq.is_empty()
            || !seq.bytes().all(|b| b.is_ascii_digit())
            || (seq.len() > 1 && seq.starts_with('0'))
        {
            return Err(invalid());
        }
        let seq: u64 = seq.parse().map_err(|_| invalid())?;
        Ok(Self { kind, seq })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_id_round_trips_through_its_wire_form() {
        for kind in [
            ElementKind::Concept,
            ElementKind::Proposition,
            ElementKind::Assertion,
            ElementKind::Evidence,
            ElementKind::Activity,
        ] {
            let id = ElementId::new(kind, 42);
            let text = id.to_string();
            assert_eq!(text.parse::<ElementId>().unwrap(), id, "for {text}");
        }
    }

    #[test]
    fn every_kind_gets_its_own_tag() {
        let mut tags: Vec<char> = [
            ElementKind::Concept,
            ElementKind::Proposition,
            ElementKind::Assertion,
            ElementKind::Evidence,
            ElementKind::Activity,
        ]
        .into_iter()
        .map(ElementId::tag)
        .collect();
        tags.sort_unstable();
        tags.dedup();
        assert_eq!(tags.len(), 5);
    }

    #[test]
    fn one_element_answers_to_exactly_one_spelling() {
        // Anything that would give a second spelling of the same id lets two
        // references disagree about equality while naming the same row.
        for bad in [
            "C-+1", "C-01", "C- 1", "C-1 ", "c-1", "CC-1", "C-", "-1", "C1", "C-1.0", "C-1-2", "",
        ] {
            assert!(
                bad.parse::<ElementId>().is_err(),
                "{bad:?} should not parse"
            );
        }
    }

    #[test]
    fn a_reference_to_the_wrong_kind_is_structural_not_missing() {
        let err = ElementId::parse_kind("E-1", ElementKind::Proposition).unwrap_err();
        assert_eq!(err.name(), "StructuralReferenceInvalid");
        assert!(ElementId::parse_kind("P-1", ElementKind::Proposition).is_ok());
    }
}
