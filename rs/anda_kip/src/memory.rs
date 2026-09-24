//! Optional Agent-to-Brain interface artifacts. A raw Nexus does not acquire
//! a Brain binding merely by installing the Cognitive Memory vocabulary.
use crate::{Json, KipError};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;

pub mod binding;
pub use binding::{MemorySession, MemorySessionSnapshot};

pub const MEMORY_INTERFACE: &str = include_str!("../Memory-Interface.md");
/// The former Cognitive Consistency companion, now an informative redirect:
/// its contracts moved into the Specification (§11.5–§11.6, §21.11–§21.13,
/// §25.2–§25.5, §57.6–§57.8), [`BRAIN_RUNTIME`] and [`VALIDATED_LEARNING`].
/// Kept so existing hosts still resolve the old anchors.
pub const COGNITIVE_CONSISTENCY: &str = include_str!("../Cognitive-Consistency.md");
/// The Brain Runtime companion: durable attention, leases, dispatch and
/// receiver fencing, outside the memory protocol.
pub const BRAIN_RUNTIME: &str = include_str!("../brain/Brain-Runtime.md");
/// The Validated Learning companion: trials, attempts, evaluations, prospective
/// enrollment and comparable learning.
pub const VALIDATED_LEARNING: &str = include_str!("../brain/Validated-Learning.md");
/// Shared `$defs` (`TimeBound`, `TimePoint`, ...) the other schemas reference.
pub const COMMON_SCHEMA: &str = include_str!("../schemas/kip-common.schema.json");
pub const MEMORY_SCHEMA: &str = include_str!("../schemas/kip-memory.schema.json");
/// The request envelope schema; the Memory Interface reuses its `SpaceSelector`.
pub const REQUEST_SCHEMA: &str = include_str!("../schemas/kip-request.schema.json");
/// The response envelope schema; the Memory Interface reuses its `Error`.
pub const RESPONSE_SCHEMA: &str = include_str!("../schemas/kip-response.schema.json");
pub const PROJECTION_SCHEMA: &str = include_str!("../schemas/kip-projection.schema.json");
pub const COGNITIVE_RECORDS_SCHEMA: &str =
    include_str!("../schemas/kip-cognitive-records.schema.json");
pub const ELEMENT_SCHEMA: &str = include_str!("../schemas/kip-element.schema.json");
pub const CAPSULE_SCHEMA: &str = include_str!("../schemas/kip-capsule.schema.json");
pub const SCHEMA_PACKAGE_SCHEMA: &str = include_str!("../schemas/kip-schema-package.schema.json");
/// The pinned Change Envelope schema, bundled for downstream contract validation.
pub const CHANGE_ENVELOPE_SCHEMA: &str = include_str!("../schemas/kip-change-envelope.schema.json");
pub const MEMORY_BUNDLES: &str = include_str!("../profiles/memory-bundles.json");
pub const MEMORY_AGENT_CARD: &str = include_str!("../brain/MemoryInterface.md");
pub const KIP_RECALL_CARD: &str = include_str!("../brain/KIPRecall.md");
pub const KIP_FORMATION_CARD: &str = include_str!("../brain/KIPFormation.md");
pub const KIP_MAINTENANCE_CARD: &str = include_str!("../brain/KIPMaintenance.md");

/// Every vendored JSON Schema, keyed by its `$id` and parsed once.
///
/// This is the closed resource set a validator resolves `$ref` against: no
/// network, file or ambient cache may satisfy a reference these do not.
pub fn vendored_schemas() -> &'static BTreeMap<String, Json> {
    static CATALOG: LazyLock<BTreeMap<String, Json>> = LazyLock::new(|| {
        [
            COMMON_SCHEMA,
            MEMORY_SCHEMA,
            REQUEST_SCHEMA,
            RESPONSE_SCHEMA,
            PROJECTION_SCHEMA,
            COGNITIVE_RECORDS_SCHEMA,
            ELEMENT_SCHEMA,
            CAPSULE_SCHEMA,
            SCHEMA_PACKAGE_SCHEMA,
            CHANGE_ENVELOPE_SCHEMA,
        ]
        .into_iter()
        .map(|text| {
            let schema = crate::parse_canonical_json(text).expect("vendored strict schema");
            let id = schema["$id"]
                .as_str()
                .expect("vendored schema $id")
                .to_string();
            (id, schema)
        })
        .collect()
    });
    &CATALOG
}

/// Compiles a JSON Schema (draft 2020-12) against a closed resource set,
/// validating `date-time` and `timestamp` as canonical KIP timestamps (§6.5).
///
/// The one place the validator is configured, so the Memory Interface, an
/// engine's Schema Package contracts and the tests cannot drift into different
/// notions of a valid timestamp or of which resources may be fetched.
#[cfg(feature = "schema-validation")]
pub fn schema_validator(
    schema: &Json,
    resources: BTreeMap<String, Json>,
) -> Result<jsonschema::Validator, String> {
    struct Pinned(BTreeMap<String, Json>);
    impl jsonschema::Retrieve for Pinned {
        fn retrieve(
            &self,
            uri: &jsonschema::Uri<String>,
        ) -> Result<Json, Box<dyn std::error::Error + Send + Sync>> {
            self.0
                .get(uri.as_str())
                .cloned()
                .ok_or_else(|| format!("unpinned schema resource {uri}").into())
        }
    }
    fn timestamp(value: &str) -> bool {
        crate::timestamp::parse(value, "timestamp").is_ok()
    }
    jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .should_validate_formats(true)
        .with_format("date-time", timestamp)
        .with_format("timestamp", timestamp)
        .with_retriever(Pinned(resources))
        .build(schema)
        .map_err(|error| error.to_string())
}

/// One Memory Interface level (companion §2).
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct MemoryBundle {
    pub requires: Vec<String>,
    /// The Nexus conformance level the binding runs on (§89).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nexus_level: Option<String>,
    pub capabilities: Vec<String>,
    pub guarantees: Vec<String>,
    pub contract: String,
    pub validated_standing: bool,
    #[serde(default)]
    pub required_kml: Vec<String>,
}

/// The vendored Memory Interface levels, keyed by name; parsed once.
pub fn memory_bundles() -> &'static BTreeMap<String, MemoryBundle> {
    #[derive(Deserialize)]
    struct Registry {
        bundles: BTreeMap<String, MemoryBundle>,
    }
    static BUNDLES: LazyLock<BTreeMap<String, MemoryBundle>> = LazyLock::new(|| {
        serde_json::from_str::<Registry>(MEMORY_BUNDLES)
            .expect("vendored memory bundles")
            .bundles
    });
    &BUNDLES
}

/// Validate a host's declaration without inferring a binding from type names.
///
/// `declared` names Memory Interface levels; `memory_interface` itself is the
/// registry capability of the binding (§67.4), which `binding_available`
/// answers, so it adds no dependency of its own.
pub fn validate_memory_bundles(
    declared: &BTreeSet<String>,
    binding_available: bool,
    mut capability_available: impl FnMut(&str) -> bool,
) -> Result<(), KipError> {
    if declared.is_empty() {
        return Ok(());
    }
    if !binding_available {
        return Err(KipError::unsupported_capability(
            "memory bundles require an available Brain binding",
        ));
    }
    let registry = memory_bundles();
    for name in declared {
        if name == "memory_interface" {
            continue;
        }
        let bundle = registry.get(name).ok_or_else(|| {
            KipError::unsupported_capability(format!("unknown memory bundle {name}"))
        })?;
        for capability in &bundle.capabilities {
            if !capability_available(capability) {
                return Err(KipError::unsupported_capability(format!(
                    "{name} requires {capability}"
                )));
            }
        }
        for dependency in &bundle.requires {
            if !declared.contains(dependency) {
                return Err(KipError::unsupported_capability(format!(
                    "{name} must advertise dependency {dependency}"
                )));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn memory_declarations_require_a_binding_and_transitive_dependencies() {
        let basic = BTreeSet::from(["memory_interface".into(), "memory_basic".into()]);
        assert!(validate_memory_bundles(&basic, false, |_| true).is_err());
        assert!(validate_memory_bundles(&basic, true, |_| false).is_ok());
        let learning = BTreeSet::from([
            "memory_basic".into(),
            "memory_experience".into(),
            "memory_learning".into(),
        ]);
        assert!(validate_memory_bundles(&learning, true, |_| false).is_err());
        assert!(validate_memory_bundles(&learning, true, |_| true).is_ok());
        let incomplete = BTreeSet::from(["memory_learning".into(), "memory_experience".into()]);
        assert!(validate_memory_bundles(&incomplete, true, |_| true).is_err());
        // Durable workers and Capsule exchange are capabilities, not levels.
        let retired = BTreeSet::from(["memory_durable".into()]);
        assert!(validate_memory_bundles(&retired, true, |_| true).is_err());
        let bundles = memory_bundles();
        assert_eq!(bundles.len(), 3);
        assert_eq!(
            bundles["memory_basic"].nexus_level.as_deref(),
            Some("KIP-Core")
        );
    }
}
