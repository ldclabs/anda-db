use super::*;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Debug)]
pub struct WakeResumeInput {
    pub wake: WakeRecord,
    pub condition: Json,
}

/// Trusted read-only host observation. This callback is awaited without the
/// Nexus write lock; no serialized boolean can substitute for its execution.
#[async_trait::async_trait]
pub trait WakeResumeVerifier: Send + Sync {
    async fn verify(&self, input: WakeResumeInput) -> Result<bool, KipError>;
}

#[derive(Clone)]
pub(crate) struct Binding {
    pub pin: RuntimePin,
    pub condition: Json,
    pub verifier: Arc<dyn WakeResumeVerifier>,
}

#[derive(Default)]
pub(crate) struct ResumeVerifiers(parking_lot::RwLock<BTreeMap<String, Binding>>);
impl ResumeVerifiers {
    pub(crate) fn get(&self, key: &str) -> Option<Binding> {
        self.0.read().get(key).cloned()
    }
}

impl crate::CognitiveNexus {
    /// Register executable host code once per live Nexus, including after a
    /// restart. Registration is not a KML operation and writes no graph data.
    pub fn register_wake_resume_verifier(
        &self,
        condition: Json,
        pin: RuntimePin,
        verifier: Arc<dyn WakeResumeVerifier>,
    ) -> Result<String, KipError> {
        if !bounded(&pin.id)
            || !valid_digest(&pin.digest)
            || serde_json::to_vec(&condition)
                .map_err(|e| invalid(&e.to_string()))?
                .len()
                > 65_536
        {
            return Err(invalid("invalid bounded resume verifier"));
        }
        let key = digest(&condition)?;
        let mut bindings = self.resume_verifiers.0.write();
        if bindings.contains_key(&key) {
            return Err(invalid("resume verifier already registered"));
        }
        bindings.insert(
            key.clone(),
            Binding {
                pin,
                condition,
                verifier,
            },
        );
        Ok(key)
    }
}

impl Session {
    pub(super) async fn verified_resume(
        &self,
        space: &str,
        reference: &str,
        expected: u64,
        fence: u64,
    ) -> Result<Option<(String, RuntimePin)>, KipError> {
        let wake = self.read_wake(space, reference).await?;
        let WakeState::Blocked {
            retry:
                WakeRetry {
                    resume: WakeResume::OnChange { condition_digest },
                    ..
                },
        } = &wake.state
        else {
            return Ok(None);
        };
        if wake.version != expected || wake.fence != fence {
            return Err(conflict("version_or_fence_conflict"));
        }
        let binding = self
            .nexus
            .resume_verifiers
            .get(condition_digest)
            .ok_or_else(|| {
                KipError::unsupported_capability(
                    "no registered verifier for this wake resume condition",
                )
            })?;
        let digest = condition_digest.clone();
        if !binding
            .verifier
            .verify(WakeResumeInput {
                wake,
                condition: binding.condition,
            })
            .await?
        {
            return Err(conflict("not_ready"));
        }
        Ok(Some((digest, binding.pin)))
    }
}
