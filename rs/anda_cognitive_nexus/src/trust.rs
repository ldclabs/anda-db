//! Explicit contextual trust and atomic provenance. Calibration algorithms and
//! automatic application policies remain with the trusted Brain host.
use crate::{
    governance::{EffectiveAuthority, Permission, ResourceContext},
    nexus::Session,
    schema::{Intent, SymbolKind, contracts::digest},
    store::Element,
    tx::Transaction,
};
use anda_kip::{Json, KipError, cognitive::ArtifactPin};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ContextualTrustRule {
    pub id: String,
    pub actor_ref: String,
    pub predicate_ref: Option<String>,
    pub context_ref: Option<String>,
    pub weight: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TrustConfiguration {
    pub weights: BTreeMap<String, f64>,
    pub default_weight: f64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<ContextualTrustRule>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TrustCalibrationProposal {
    pub format: String,
    pub space_id: String,
    pub expected_version: u64,
    pub configuration: TrustConfiguration,
    pub method: ArtifactPin,
    pub evidence_refs: Vec<String>,
    pub uncertainty: Json,
}

fn invalid(message: &str) -> KipError {
    KipError::constraint_violation(message)
}

impl TrustConfiguration {
    pub fn validate(&self) -> Result<(), KipError> {
        if self.weights.len() > 1024
            || self.rules.len() > 128
            || std::iter::once(&self.default_weight)
                .chain(self.weights.values())
                .chain(self.rules.iter().map(|r| &r.weight))
                .any(|v| !v.is_finite() || !(0.0..=1.0).contains(v))
        {
            return Err(invalid("trust weights must be bounded in [0,1]"));
        }
        let mut ids = BTreeSet::new();
        let mut selectors = BTreeSet::new();
        for rule in &self.rules {
            if rule.id.is_empty()
                || rule.id.len() > 128
                || !ids.insert(&rule.id)
                || (rule.predicate_ref.is_none() && rule.context_ref.is_none())
                || !selectors.insert((&rule.actor_ref, &rule.predicate_ref, &rule.context_ref))
            {
                return Err(invalid(
                    "contextual trust needs unique IDs/selectors and an explicit context or predicate",
                ));
            }
        }
        Ok(())
    }

    /// More specific rules win. Conflicting equal-specificity matches fail
    /// closed; rule ordering never chooses an implicit winner.
    pub fn weight(
        &self,
        actor: &str,
        predicate: &str,
        contexts: &[String],
    ) -> Result<f64, KipError> {
        weight(
            &self.rules,
            self.weights
                .get(actor)
                .copied()
                .unwrap_or(self.default_weight),
            actor,
            predicate,
            contexts,
        )
    }
}

pub(crate) fn weight(
    rules: &[ContextualTrustRule],
    fallback: f64,
    actor: &str,
    predicate: &str,
    contexts: &[String],
) -> Result<f64, KipError> {
    let mut selected: Option<(usize, f64)> = None;
    let mut ambiguous = false;
    for rule in rules {
        if rule.actor_ref != actor
            || rule.predicate_ref.as_ref().is_some_and(|p| p != predicate)
            || rule
                .context_ref
                .as_ref()
                .is_some_and(|c| !contexts.contains(c))
        {
            continue;
        }
        let specificity =
            usize::from(rule.predicate_ref.is_some()) + usize::from(rule.context_ref.is_some());
        match selected {
            Some((old, value)) if specificity == old => {
                ambiguous |= value != rule.weight;
            }
            Some((old, _)) if old > specificity => {}
            _ => {
                selected = Some((specificity, rule.weight));
                ambiguous = false;
            }
        }
    }
    if ambiguous {
        return Err(invalid(
            "ambiguous contextual trust; use an explicit combined scope",
        ));
    }
    Ok(selected.map(|(_, v)| v).unwrap_or(fallback))
}

async fn validate_references(
    session: &Session,
    authority: &EffectiveAuthority,
    space: &str,
    configuration: &TrustConfiguration,
) -> Result<(), KipError> {
    configuration.validate()?;
    let env = session.nexus.store.schema_environment(space).await?;
    for rule in &configuration.rules {
        for reference in std::iter::once(&rule.actor_ref).chain(rule.context_ref.iter()) {
            let id = crate::ElementId::parse_kind(reference, anda_kip::ElementKind::Concept)?;
            let row = session.nexus.store.get_element(id).await?;
            if row.space() != space || id.to_string() != *reference {
                return Err(KipError::not_found_or_not_visible(
                    "trust context unavailable",
                ));
            }
            authority
                .authorize(
                    Permission::Read,
                    &ResourceContext::of_element(&row),
                    &session.auth,
                )
                .into_result()?;
        }
        if let Some(predicate) = &rule.predicate_ref
            && env
                .resolve_symbol(SymbolKind::PredicateType, predicate, Intent::Read)?
                .to_string()
                != *predicate
        {
            return Err(invalid(
                "trust predicates must use their exact schema references",
            ));
        }
    }
    Ok(())
}

impl Session {
    /// Explicit replacement of global and contextual settings; no algorithm is
    /// run and no cognitive text is interpreted as a control-plane instruction.
    pub async fn set_contextual_trust(
        &self,
        space: &str,
        expected: u64,
        configuration: TrustConfiguration,
    ) -> Result<Json, KipError> {
        self.governed(space, Permission::ManageTrust, async || {
            let authority = self.effective_authority(space).await?;
            validate_references(self, &authority, space, &configuration).await?;
            let row = self
                .nexus
                .store
                .publish_control(
                    space,
                    "trust",
                    "trust",
                    expected,
                    json!(configuration),
                    json!({"principal_id":self.auth.principal_id}),
                )
                .await?;
            Ok(json!({"version":row.version,"trust":row.value}))
        })
        .await
    }

    /// Commit the exact proposed configuration, provenance and Governance audit
    /// in one redo plan. ManageTrust remains necessary; no Create permission is
    /// implied or needed merely to record the native Governance audit.
    pub async fn apply_trust_calibration(
        &self,
        space: &str,
        expected: u64,
        proposal: ArtifactPin,
        operation_key: &str,
    ) -> Result<Json, KipError> {
        if operation_key.is_empty() || operation_key.len() > 256 {
            return Err(invalid("bounded trust operation key required"));
        }
        self.governed(space, Permission::ManageTrust, async || {
            let store = &self.nexus.store;
            let authority = self.effective_authority(space).await?;
            let (content, sources) = store
                .authorized_artifact(space, &proposal.artifact_ref, &authority, &self.auth)
                .await?;
            if digest(&content)? != proposal.content_digest {
                return Err(invalid("proposal digest mismatch"));
            }
            let proposed: TrustCalibrationProposal =
                serde_json::from_value(content).map_err(|_| invalid("invalid trust proposal"))?;
            if proposed.format != "nexus:trust-calibration-v1"
                || proposed.space_id != space
                || proposed.expected_version != expected
                || proposed.evidence_refs.is_empty()
                || proposed.evidence_refs.len() > 128
                || proposed
                    .uncertainty
                    .as_object()
                    .is_none_or(|v| v.is_empty())
            {
                return Err(invalid(
                    "trust calibration needs scope, evidence and explicit uncertainty",
                ));
            }
            validate_references(self, &authority, space, &proposed.configuration).await?;
            let (method, _) = store
                .authorized_artifact(space, &proposed.method.artifact_ref, &authority, &self.auth)
                .await?;
            if digest(&method)? != proposed.method.content_digest {
                return Err(invalid("calibration method digest mismatch"));
            }
            let mut evidence = BTreeSet::new();
            for reference in &proposed.evidence_refs {
                if !evidence.insert(reference) || !sources.contains(reference) {
                    return Err(invalid(
                        "proposal must inherit every independent evidence material reference",
                    ));
                }
                let row = store.get_element(reference.parse()?).await?;
                let Element::Evidence(record) = &row else {
                    return Err(invalid("calibration evidence must be Evidence"));
                };
                if row.space() != space
                    || record.status == "corrected"
                    || row.state() != crate::store::rows::state::ACTIVE
                {
                    return Err(invalid("calibration evidence is no longer eligible"));
                }
                authority
                    .authorize(
                        Permission::Read,
                        &ResourceContext::of_element(&row),
                        &self.auth,
                    )
                    .into_result()?;
            }
            let identity = json!({"operation":"apply_trust_calibration","key":operation_key});
            let key = crate::attention::request_key(&self.auth.principal_id, &identity)?;
            let request_digest = digest(&json!({"expected":expected,"proposal":proposal}))?;
            if let Some(result) = crate::attention::replay(store, space, &key, &request_digest).await? {
                return Ok(result);
            }
            let mut tx = Transaction::begin(
                store,
                space,
                json!({"principal_id":self.auth.principal_id}),
                false,
                authority,
                (*self.auth).clone(),
            )
            .await?;
            let mut value = json!(proposed.configuration);
            value["calibration"] = json!({"proposal":proposal,"method":proposed.method,"evidence_refs":proposed.evidence_refs});
            crate::attention::stage_control(store, &mut tx, "trust", expected, "trust", value).await?;
            tx.defer_governance_audit(crate::governance::store::MutationEntry {
                        at:tx.cx.at.clone(),
                        space_id:space.into(),principal_id:self.auth.principal_id.clone(),operation:"apply_trust_calibration",resource:"trust".into(),
                        record:json!({"proposal":proposal,"method":proposed.method,"before_version":expected,"after_version":expected+1}),
                    });
            crate::attention::commit(store,tx,key,request_digest,json!({"version":expected+1,"proposal":proposal,"audit_operation":"apply_trust_calibration"})).await
        })
        .await
    }
}
