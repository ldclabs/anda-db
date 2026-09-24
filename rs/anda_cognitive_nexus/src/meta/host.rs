//! # Capabilities a host answers for
//!
//! Three §67.4 registry names describe what sits *around* a Nexus rather than
//! the Nexus itself: `memory_interface` is a Brain binding (Memory Interface
//! §2), `durable_brain_runtime` and `receiver_fencing` are the Brain Runtime
//! companion's workers and receivers. This engine implements none of them, so a
//! raw Nexus answers `false` for each — a raw Nexus MUST NOT advertise a
//! binding or level its connected Brain cannot serve (§2).
//!
//! A host that does serve them says so here, once, before it shares the
//! Nexus. `DESCRIBE CAPABILITIES`, `DESCRIBE PRIMER` and every request's
//! `requires` block then answer with the host's declaration, which is checked
//! against what this engine can actually carry: a level whose Nexus level this
//! engine does not claim, or whose capabilities it lacks, is refused rather than
//! advertised.

use anda_kip::memory::binding::{Bundle, Descriptor};
use anda_kip::{Json, KipError};

/// The registry names a host, not this engine, answers for.
pub const HOST_NAMES: &[&str] = &[
    "memory_interface",
    "durable_brain_runtime",
    "receiver_fencing",
];

/// A host's declaration of the capabilities around this Nexus.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HostCapabilities {
    /// The Memory Interface descriptor of the connected Brain binding.
    pub memory_interface: Option<Descriptor>,
    /// Restart-safe attention, leases and dispatch (Brain Runtime §2–§4).
    pub durable_brain_runtime: bool,
    /// End-to-end fencing at the effect-owning receiver (Brain Runtime §4).
    pub receiver_fencing: bool,
}

impl HostCapabilities {
    /// Checks the declaration against this engine.
    ///
    /// Each advertised level must run on a conformance level this engine
    /// claims and every capability the level adds must be one this engine
    /// supports; `receiver_fencing` is a further capability on top of
    /// `durable_brain_runtime` (Brain Runtime §1).
    pub fn validate(&self) -> Result<(), KipError> {
        if let Some(descriptor) = &self.memory_interface {
            descriptor.validate()?;
            let registry = anda_kip::memory_bundles();
            for bundle in &descriptor.bundles {
                let entry = registry.get(bundle.as_str()).ok_or_else(|| {
                    KipError::unsupported_capability(format!(
                        "{} is not a level of the vendored bundle registry",
                        bundle.as_str()
                    ))
                })?;
                if let Some(level) = &entry.nexus_level
                    && !claims_level(level)
                {
                    return Err(KipError::unsupported_capability(format!(
                        "{} runs on {level}, which this engine does not claim",
                        bundle.as_str()
                    )));
                }
                for capability in &entry.capabilities {
                    if super::capability_state(&Self::default(), capability) != Some(true) {
                        return Err(KipError::unsupported_capability(format!(
                            "{} requires {capability}, which this engine does not support",
                            bundle.as_str()
                        )));
                    }
                }
            }
        }
        if self.receiver_fencing && !self.durable_brain_runtime {
            return Err(KipError::unsupported_capability(
                "receiver_fencing is a further capability on top of durable_brain_runtime",
            ));
        }
        Ok(())
    }

    /// The host's answer for one of [`HOST_NAMES`]; `None` for any other name.
    pub fn state(&self, name: &str) -> Option<bool> {
        match name {
            "memory_interface" => Some(self.memory_interface.is_some()),
            "durable_brain_runtime" => Some(self.durable_brain_runtime),
            "receiver_fencing" => Some(self.receiver_fencing),
            _ => None,
        }
    }

    /// The registry value `DESCRIBE CAPABILITIES` reports for a host name: the
    /// descriptor itself for an advertised binding, otherwise the boolean.
    pub fn registry_value(&self, name: &str) -> Option<Json> {
        match (name, &self.memory_interface) {
            ("memory_interface", Some(descriptor)) => serde_json::to_value(descriptor).ok(),
            _ => self.state(name).map(Json::Bool),
        }
    }

    /// Whether a Memory Interface level is advertised.
    pub fn advertises(&self, bundle: Bundle) -> bool {
        self.memory_interface
            .as_ref()
            .is_some_and(|descriptor| descriptor.advertises(bundle))
    }
}

fn claims_level(level: &str) -> bool {
    super::CONFORMANCE_PROFILES
        .iter()
        .any(|profile| profile.as_str() == level)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anda_kip::memory::binding::Budget;

    fn basic() -> Descriptor {
        Descriptor {
            kip_memory: "2.0".into(),
            bundles: vec![Bundle::MemoryBasic],
            default_scope: None,
            default_budget: Budget {
                max_output_tokens: Some(4096),
                deadline_ms: Some(30_000),
                tokenizer: None,
            },
            tokenizer: "o200k_base".into(),
            minimum_response_tokens: 256,
            default_space: None,
        }
    }

    #[test]
    fn a_host_declares_only_what_this_engine_can_carry() {
        assert_eq!(
            HostCapabilities::default().state("memory_interface"),
            Some(false)
        );
        let host = HostCapabilities {
            memory_interface: Some(basic()),
            ..Default::default()
        };
        host.validate().unwrap();
        assert_eq!(host.state("memory_interface"), Some(true));
        assert_eq!(
            host.registry_value("memory_interface").unwrap()["bundles"][0],
            "memory_basic"
        );
        assert!(host.advertises(Bundle::MemoryBasic));
        assert_eq!(host.state("kql"), None);

        let fencing = HostCapabilities {
            receiver_fencing: true,
            ..Default::default()
        };
        assert!(fencing.validate().is_err());
        let durable = HostCapabilities {
            durable_brain_runtime: true,
            receiver_fencing: true,
            ..Default::default()
        };
        durable.validate().unwrap();

        let mut undeclared_dependency = basic();
        undeclared_dependency.bundles = vec![Bundle::MemoryExperience];
        assert!(
            HostCapabilities {
                memory_interface: Some(undeclared_dependency),
                ..Default::default()
            }
            .validate()
            .is_err()
        );
    }

    /// A level runs on a Nexus level; one this engine does not claim cannot
    /// be advertised over it (Memory Interface §2).
    #[test]
    fn a_level_needs_the_nexus_level_it_runs_on() {
        let mut descriptor = basic();
        descriptor.bundles = vec![Bundle::MemoryBasic, Bundle::MemoryExperience];
        let host = HostCapabilities {
            memory_interface: Some(descriptor),
            ..Default::default()
        };
        let claimed = claims_level("KIP-CognitiveMemory");
        assert_eq!(host.validate().is_ok(), claimed);
    }
}
