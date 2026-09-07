//! Trusted rule bindings. A digest has exactly one evaluator for a Nexus
//! lifetime; loading a new implementation requires a new artifact/version.
use anda_kip::{
    Json, KipError,
    cognitive::{EvaluationInput, EvaluationRule, evaluate_binary_rule},
};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone)]
pub struct EvaluationRules(Arc<parking_lot::RwLock<RuleMap>>);
type RuleMap = BTreeMap<String, Arc<dyn EvaluationRule>>;

impl std::fmt::Debug for EvaluationRules {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EvaluationRules")
            .field("digests", &self.0.read().keys().collect::<Vec<_>>())
            .finish()
    }
}

impl Default for EvaluationRules {
    fn default() -> Self {
        let registry = Self(Arc::new(parking_lot::RwLock::new(BTreeMap::new())));
        registry.register(&serde_json::json!({"engine":"kip:binary-stratified-v1"}),Arc::new(|input:&EvaluationInput| {
            let count:usize=input.samples.treatment.values().map(Vec::len).sum();
            let quota=input.trial["quota"].as_u64().unwrap_or(2).max(input.minimum_independent_attempts);
            if (count as u64)<quota {
                return Ok(serde_json::json!({"status":"insufficient","effect":null,"uncertainty":{"method":"hoeffding","alpha":input.parameters["alpha"]}}));
            }
            evaluate_binary_rule(&input.rule,&input.parameters,&input.trial["comparability"],&input.samples)
        })).expect("builtin rule");
        registry
    }
}

impl EvaluationRules {
    pub fn supports(&self, digest: &str) -> bool {
        self.0.read().contains_key(digest)
    }

    pub fn register(
        &self,
        artifact: &Json,
        rule: Arc<dyn EvaluationRule>,
    ) -> Result<String, KipError> {
        let digest = crate::schema::contracts::digest(artifact)?;
        let mut rules = self.0.write();
        if rules.contains_key(&digest) {
            return Err(KipError::constraint_violation(
                "a registered rule digest cannot be rebound; publish a new rule artifact",
            ));
        }
        rules.insert(digest.clone(), rule);
        Ok(digest)
    }

    pub fn evaluate(&self, input: &EvaluationInput) -> Result<Json, KipError> {
        let digest = crate::schema::contracts::digest(&input.rule)?;
        let rule = self.0.read().get(&digest).cloned().ok_or_else(|| {
            KipError::unsupported_capability(
                "no trusted host evaluator is registered for the pinned rule digest",
            )
        })?;
        let comparison = rule.evaluate(input)?;
        anda_kip::validate_json(&comparison)?;
        Ok(comparison)
    }
}

impl crate::nexus::CognitiveNexus {
    /// Bind trusted host code to exact rule bytes. Restore registrations on
    /// startup; persisted artifacts alone never execute code.
    pub fn register_evaluation_rule(
        &self,
        artifact: &Json,
        rule: Arc<dyn EvaluationRule>,
    ) -> Result<String, KipError> {
        self.store.evaluation_rules.register(artifact, rule)
    }
}
