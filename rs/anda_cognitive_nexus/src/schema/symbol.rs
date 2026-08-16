//! # Schema symbol identity
//!
//! A symbol means nothing outside its package and version lineage (Spec §16):
//!
//! ```text
//! kip://<package-path>@<exact-version>[/<symbol>]
//! ```
//!
//! The rule the rest of the engine leans on is §13: **every persisted schema
//! reference resolves to an exact version.** `Person@latest` stored on an
//! element would mean the element's meaning changes when someone else
//! publishes a package — the stored data would say one thing today and another
//! thing tomorrow, with no transaction in between. So a range parses in a
//! *resolution input*, and never in a `schema_ref`.

use anda_kip::KipError;
use std::{cmp::Ordering, fmt, str::FromStr};

/// The URI scheme every package reference carries.
pub const SCHEME: &str = "kip://";

/// A SemVer-like version triplet with an optional pre-release tag.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Version {
    /// Breaking-change coordinate.
    pub major: u64,
    /// Additive-change coordinate.
    pub minor: u64,
    /// Fix-level coordinate.
    pub patch: u64,
    /// The pre-release tag, empty for a release.
    pub pre: String,
}

impl Version {
    /// Builds a release version.
    pub const fn new(major: u64, minor: u64, patch: u64) -> Self {
        Self {
            major,
            minor,
            patch,
            pre: String::new(),
        }
    }

    /// Whether this version carries a pre-release tag.
    pub fn is_prerelease(&self) -> bool {
        !self.pre.is_empty()
    }
}

/// Ordered by triplet, with a pre-release sorting *below* its own release.
///
/// `2.0.0-rc1 < 2.0.0`, per SemVer. Getting this backwards would make a
/// release candidate look like an upgrade from the release it preceded.
impl Ord for Version {
    fn cmp(&self, other: &Self) -> Ordering {
        self.major
            .cmp(&other.major)
            .then(self.minor.cmp(&other.minor))
            .then(self.patch.cmp(&other.patch))
            .then(match (self.pre.is_empty(), other.pre.is_empty()) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                (false, false) => self.pre.cmp(&other.pre),
            })
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)?;
        if !self.pre.is_empty() {
            write!(f, "-{}", self.pre)?;
        }
        Ok(())
    }
}

impl FromStr for Version {
    type Err = KipError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let invalid = |why: &str| {
            KipError::invalid_identifier(format!(
                "{s:?} is not an exact schema version ({why}); a persisted schema reference must \
                 name one immutable package version, never a range such as \"2.x\" or \"latest\""
            ))
        };
        let (triplet, pre) = match s.split_once('-') {
            Some((triplet, pre)) if !pre.is_empty() => (triplet, pre.to_string()),
            Some(_) => return Err(invalid("empty pre-release tag")),
            None => (s, String::new()),
        };
        let mut parts = triplet.split('.');
        let mut next = |what: &str| -> Result<u64, KipError> {
            let part = parts.next().ok_or_else(|| invalid(what))?;
            // `str::parse` accepts `+2`, which would give one version two
            // spellings and break exact-reference equality.
            if part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()) {
                return Err(invalid(what));
            }
            part.parse().map_err(|_| invalid(what))
        };
        let major = next("missing major")?;
        let minor = next("missing minor")?;
        let patch = next("missing patch")?;
        if parts.next().is_some() {
            return Err(invalid("more than three numeric components"));
        }
        Ok(Self {
            major,
            minor,
            patch,
            pre,
        })
    }
}

/// One immutable package version: `kip://profiles/cognitive-memory@2.0.0`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PackageRef {
    /// The stable namespace-qualified name, including the `kip://` scheme.
    pub package_id: String,
    /// The exact version.
    pub version: Version,
}

impl fmt::Display for PackageRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.package_id, self.version)
    }
}

impl FromStr for PackageRef {
    type Err = KipError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let invalid = |why: &str| {
            KipError::invalid_identifier(format!(
                "{s:?} is not a package reference ({why}); the form is \
                 kip://<path>@<major.minor.patch>"
            ))
        };
        if !s.starts_with(SCHEME) {
            return Err(invalid("missing the kip:// scheme"));
        }
        // Split on the last `@`: a package path may not contain one, but
        // splitting from the right is what makes that a rule rather than an
        // assumption.
        let (package_id, version) = s.rsplit_once('@').ok_or_else(|| invalid("no @version"))?;
        let path = &package_id[SCHEME.len()..];
        if path.is_empty() || path.starts_with('/') || path.ends_with('/') {
            return Err(invalid("empty or malformed package path"));
        }
        Ok(Self {
            package_id: package_id.to_string(),
            version: version.parse()?,
        })
    }
}

/// One canonical symbol: `kip://profiles/cognitive-memory@2.0.0/has_step`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SymbolRef {
    /// The package version the symbol belongs to.
    pub package: PackageRef,
    /// The local symbol name inside that package.
    pub name: String,
}

impl fmt::Display for SymbolRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.package, self.name)
    }
}

impl FromStr for SymbolRef {
    type Err = KipError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let invalid = |why: &str| {
            KipError::invalid_identifier(format!(
                "{s:?} is not a canonical schema symbol ({why}); the form is \
                 kip://<path>@<version>/<Symbol>"
            ))
        };
        // The symbol name follows the version, so the split point is the first
        // `/` *after* the `@` — the package path has slashes of its own.
        let at = s.rfind('@').ok_or_else(|| invalid("no @version"))?;
        let slash = s[at..]
            .find('/')
            .map(|offset| at + offset)
            .ok_or_else(|| invalid("no /symbol"))?;
        let name = &s[slash + 1..];
        if name.is_empty() || name.contains('/') {
            return Err(invalid("empty or nested symbol name"));
        }
        Ok(Self {
            package: s[..slash].parse()?,
            name: name.to_string(),
        })
    }
}

/// Whether a string is already a canonical, fully-qualified reference.
///
/// This is the test that separates "the caller named an exact symbol" from
/// "the caller wrote a local name for the environment to resolve" (§19).
pub fn is_qualified(name: &str) -> bool {
    name.starts_with(SCHEME)
}

/// The kinds of symbol a Schema Package defines (Spec §17).
///
/// Core element kinds are not in this list: a package cannot redefine what an
/// Assertion is (§240.22).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SymbolKind {
    /// A Concept type, referenced by `schema_ref`.
    ConceptType,
    /// A predicate type, referenced by `predicate_ref`.
    PredicateType,
    /// A Facet definition.
    Facet,
    /// A structural field definition.
    StructuralField,
    /// A named value set.
    Enum,
}

impl SymbolKind {
    /// The key this kind occupies in a package artifact's `definitions`.
    pub fn section(&self) -> &'static str {
        match self {
            SymbolKind::ConceptType => "concept_types",
            SymbolKind::PredicateType => "predicates",
            SymbolKind::Facet => "facets",
            SymbolKind::StructuralField => "structural_fields",
            SymbolKind::Enum => "enums",
        }
    }
}

impl fmt::Display for SymbolKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            SymbolKind::ConceptType => "Concept type",
            SymbolKind::PredicateType => "predicate",
            SymbolKind::Facet => "Facet",
            SymbolKind::StructuralField => "structural field",
            SymbolKind::Enum => "enum",
        };
        f.write_str(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_symbol_round_trips_through_its_canonical_form() {
        let text = "kip://profiles/cognitive-memory@2.0.0/has_step";
        let symbol: SymbolRef = text.parse().unwrap();
        assert_eq!(symbol.to_string(), text);
        assert_eq!(symbol.name, "has_step");
        assert_eq!(symbol.package.package_id, "kip://profiles/cognitive-memory");
        assert_eq!(symbol.package.version, Version::new(2, 0, 0));
        // The package path's own slashes must not be mistaken for the symbol
        // separator.
        assert_eq!(
            symbol.package.to_string(),
            "kip://profiles/cognitive-memory@2.0.0"
        );
    }

    #[test]
    fn a_version_range_is_never_an_exact_reference() {
        // Spec §13: an element bound to `@latest` would change meaning when
        // someone else publishes, with no transaction in between.
        for bad in [
            "2", "2.x", "^2.0", "latest", "2.0", "2.0.0.0", "+2.0.0", "2.0.0-",
        ] {
            assert!(bad.parse::<Version>().is_err(), "{bad:?} should not parse");
        }
        for bad in [
            "kip://core",
            "core@2.0.0",
            "kip://@2.0.0",
            "kip://core@latest",
        ] {
            assert!(
                bad.parse::<PackageRef>().is_err(),
                "{bad:?} should not parse"
            );
        }
    }

    #[test]
    fn a_prerelease_sorts_below_its_own_release() {
        let rc: Version = "2.0.0-rc1".parse().unwrap();
        let release = Version::new(2, 0, 0);
        assert!(rc < release, "a release candidate precedes its release");
        assert!(rc.is_prerelease());
        assert_eq!(rc.to_string(), "2.0.0-rc1");
        assert!(Version::new(1, 9, 9) < rc);
        assert!(release < Version::new(2, 0, 1));
        assert!(Version::new(2, 0, 1) < Version::new(2, 1, 0));
    }

    #[test]
    fn a_local_name_is_told_apart_from_a_canonical_one() {
        // Spec §19: local names are model-facing sugar and must be resolved
        // before execution; a canonical reference is already resolved.
        assert!(is_qualified("kip://core@2.0.0/Assertion"));
        assert!(!is_qualified("Person"));
        assert!(!is_qualified("has_step"));
    }

    #[test]
    fn a_symbol_needs_both_a_version_and_a_name() {
        assert!("kip://core@2.0.0".parse::<SymbolRef>().is_err());
        assert!("kip://core@2.0.0/".parse::<SymbolRef>().is_err());
        assert!("kip://core@2.0.0/a/b".parse::<SymbolRef>().is_err());
        assert!("kip://core/Assertion".parse::<SymbolRef>().is_err());
    }
}
