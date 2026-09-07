//! Optional Agent-to-Brain interface artifacts. A raw Nexus does not acquire
//! a Brain binding merely by installing the Cognitive Memory vocabulary.
use crate::KipError;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const MEMORY_INTERFACE: &str = include_str!("../Memory-Interface.md");
pub const COGNITIVE_CONSISTENCY: &str = include_str!("../Cognitive-Consistency.md");
pub const MEMORY_SCHEMA: &str = include_str!("../schemas/kip-memory.schema.json");
pub const PROJECTION_SCHEMA: &str = include_str!("../schemas/kip-projection.schema.json");
pub const COGNITIVE_RECORDS_SCHEMA: &str =
    include_str!("../schemas/kip-cognitive-records.schema.json");
pub const ELEMENT_SCHEMA: &str = include_str!("../schemas/kip-element.schema.json");
pub const CAPSULE_SCHEMA: &str = include_str!("../schemas/kip-capsule.schema.json");
pub const SCHEMA_PACKAGE_SCHEMA: &str = include_str!("../schemas/kip-schema-package.schema.json");
pub const MEMORY_BUNDLES: &str = include_str!("../profiles/memory-bundles.json");
pub const MEMORY_AGENT_CARD: &str = include_str!("../brain/MemoryInterface.md");
pub const KIP_RECALL_CARD: &str = include_str!("../brain/KIPRecall.md");
pub const KIP_FORMATION_CARD: &str = include_str!("../brain/KIPFormation.md");
pub const KIP_MAINTENANCE_CARD: &str = include_str!("../brain/KIPMaintenance.md");

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct MemoryBundle {
    pub requires: Vec<String>,
    pub capabilities: Vec<String>,
    pub guarantees: Vec<String>,
    pub contract: String,
    pub validated_standing: bool,
    #[serde(default)]
    pub required_kml: Vec<String>,
}

pub fn memory_bundles() -> BTreeMap<String, MemoryBundle> {
    #[derive(Deserialize)]
    struct Registry {
        bundles: BTreeMap<String, MemoryBundle>,
    }
    serde_json::from_str::<Registry>(MEMORY_BUNDLES)
        .expect("vendored memory bundles")
        .bundles
}

/// Validate a host's declaration without inferring a binding from type names.
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
        let required = if name == "memory_interface" {
            vec!["memory_basic".to_string()]
        } else {
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
            bundle.requires.clone()
        };
        for dependency in required {
            if !declared.contains(&dependency) {
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
        let basic = BTreeSet::from(["memory_basic".into()]);
        assert!(validate_memory_bundles(&basic, false, |_| true).is_err());
        assert!(validate_memory_bundles(&basic, true, |_| false).is_err());
        assert!(validate_memory_bundles(&basic, true, |_| true).is_ok());
        let incomplete = BTreeSet::from(["memory_learning".into(), "memory_experience".into()]);
        assert!(validate_memory_bundles(&incomplete, true, |_| true).is_err());
    }
}
