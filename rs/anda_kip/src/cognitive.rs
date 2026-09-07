//! Host-side contracts shared by Nexus engines and Brain implementations.
use crate::{Json, KipError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactPin {
    pub artifact_ref: String,
    pub content_digest: String,
}

/// A protected observer binding. Distinct names do not establish independence:
/// the operator explicitly attests a control domain and permitted configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObserverControl {
    pub principal_id: String,
    pub configuration_digest: String,
    pub control_domain: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationPolicy {
    pub id: String,
    pub version: String,
    pub allowed_rules: Vec<String>,
    pub allowed_parameters: Vec<String>,
    pub observers: Vec<ObserverControl>,
    pub observer_control_digest: String,
    pub minimum_independent_attempts: u64,
    pub allow_same_principal_observer: bool,
    pub retain_adoption_on_insufficient: bool,
}

/// Input to the portable, deterministic reference evaluator. Each value is an
/// independent attempt aggregate. Missing, aborted and unknown count as zero.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EvaluationSamples {
    pub treatment: BTreeMap<String, Vec<f64>>,
    pub baseline: BTreeMap<String, Vec<f64>>,
}

/// Engine-verified, immutable material passed to a host's deterministic rule.
/// Registration is a trusted host operation, never execution of uploaded code.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvaluationInput {
    pub rule: Json,
    pub parameters: Json,
    pub trial: Json,
    pub attempts: Json,
    pub outcomes: Json,
    pub samples: EvaluationSamples,
    pub minimum_independent_attempts: u64,
}

pub trait EvaluationRule: Send + Sync {
    /// Must be deterministic, bounded, and free of side effects. Paired and
    /// off-policy rules must validate their own assignment/propensity contracts.
    fn evaluate(&self, input: &EvaluationInput) -> Result<Json, KipError>;
}

impl<F> EvaluationRule for F
where
    F: Fn(&EvaluationInput) -> Result<Json, KipError> + Send + Sync,
{
    fn evaluate(&self, input: &EvaluationInput) -> Result<Json, KipError> {
        self(input)
    }
}

/// Shared fenced-lease state machine. A fence is retained after completion,
/// so rescheduling can never reuse a previously issued worker token.
pub fn validate_lease_transition(
    before_status: &str,
    after_status: &str,
    before: Option<&Json>,
    after: Option<&Json>,
    actor: &str,
    now: &str,
) -> Result<(), KipError> {
    let fail = |message: &str| Err(KipError::constraint_violation(message));
    let instant = |value: &str| {
        chrono::DateTime::parse_from_rfc3339(value).map_err(|_| {
            KipError::constraint_violation("lease expiry must be an RFC 3339 timestamp")
        })
    };
    let now = instant(now)?;
    let Some(after) = after else {
        if before.is_some() || matches!(after_status, "running" | "completed" | "failed") {
            return fail("running tasks require a retained fenced lease");
        }
        return Ok(());
    };
    if after_status == "completed" && before_status != "running" {
        return fail("only a leased running task can complete");
    }
    let fence = after["fencing_token"].as_u64().unwrap_or(0);
    let attempts = after["attempt_count"].as_u64().unwrap_or(0);
    let expiry = instant(after["expires_at"].as_str().unwrap_or(""))?;
    if let Some(before) = before {
        let old_fence = before["fencing_token"].as_u64().unwrap_or(0);
        let old_attempts = before["attempt_count"].as_u64().unwrap_or(0);
        let old_expiry = instant(before["expires_at"].as_str().unwrap_or(""))?;
        let expired = old_expiry <= now;
        if after_status == "running" && (expired || before_status != "running") {
            if fence != old_fence + 1
                || attempts != old_attempts + 1
                || after["owner"] != actor
                || expiry <= now
            {
                return fail(
                    "takeover must advance fence and attempt count under the authenticated owner",
                );
            }
        } else if before_status == "running" {
            if expired
                || before["owner"] != actor
                || after["owner"] != actor
                || fence != old_fence
                || attempts != old_attempts
                || expiry < old_expiry
            {
                return fail("stale or expired lease cannot renew, complete or dispatch");
            }
        } else if before != after {
            return fail("inactive task lease changes require acquisition");
        }
    } else if after_status != "running"
        || !matches!(before_status, "pending" | "blocked" | "failed")
        || fence != 1
        || attempts != 1
        || after["owner"] != actor
        || expiry <= now
    {
        return fail("initial lease requires authenticated acquisition with fence 1");
    }
    Ok(())
}

/// The external identity remains stable across a lost response or restart.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DispatchRequest {
    pub attempt_ref: String,
    pub task_ref: String,
    pub fencing_token: u64,
    pub supports_idempotency: bool,
    pub supports_outcome_lookup: bool,
}

/// The rule is intentionally explicit and replaceable by a new digest/version.
/// Stratified means use predeclared weights, never observed sample frequencies.
/// Hoeffding's bound is conservative and deterministic across both engines.
pub fn evaluate_binary_rule(
    rule: &Json,
    parameters: &Json,
    comparability: &Json,
    samples: &EvaluationSamples,
) -> Result<Json, KipError> {
    if rule["engine"] != "kip:binary-stratified-v1" {
        return Err(KipError::unsupported_capability(
            "evaluation rule engine is unavailable",
        ));
    }
    if !matches!(
        comparability["method"].as_str(),
        Some("stratified" | "randomized")
    ) {
        return Err(KipError::unsupported_capability(
            "this rule supports stratified and randomized trials; use a rule with the required assignment model",
        ));
    }
    let alpha = parameters["alpha"]
        .as_f64()
        .filter(|a| *a > 0.0 && *a < 1.0)
        .ok_or_else(|| KipError::constraint_violation("rule alpha must be in (0,1)"))?;
    if comparability["missingness_policy"] != "count_as_failure"
        || comparability["uncertainty_rule"] != "hoeffding"
    {
        return Err(KipError::constraint_violation(
            "rule requires count_as_failure missingness and hoeffding uncertainty",
        ));
    }
    let weights = comparability["strata_weights"]
        .as_object()
        .ok_or_else(|| KipError::constraint_violation("predeclared strata weights required"))?;
    if weights.is_empty()
        || (weights.values().filter_map(Json::as_f64).sum::<f64>() - 1.0).abs() > 1e-12
    {
        return Err(KipError::constraint_violation(
            "strata weights must sum to one",
        ));
    }
    let mut effect = 0.0;
    let mut radius = 0.0;
    for (stratum, weight) in weights {
        let weight = weight
            .as_f64()
            .filter(|w| (0.0..=1.0).contains(w))
            .ok_or_else(|| KipError::constraint_violation("invalid stratum weight"))?;
        if weight == 0.0 {
            continue;
        }
        let treatment = samples
            .treatment
            .get(stratum)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let baseline = samples
            .baseline
            .get(stratum)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        if treatment.is_empty() || baseline.is_empty() {
            return Ok(
                serde_json::json!({"status":"insufficient","effect":null,"uncertainty":{"method":"hoeffding","alpha":alpha}}),
            );
        }
        if treatment
            .iter()
            .chain(baseline)
            .any(|x| !x.is_finite() || !(0.0..=1.0).contains(x))
        {
            return Err(KipError::constraint_violation("invalid attempt aggregate"));
        }
        effect += weight
            * (treatment.iter().sum::<f64>() / treatment.len() as f64
                - baseline.iter().sum::<f64>() / baseline.len() as f64);
        radius += weight * ((2.0 / alpha).ln() / (2.0 * treatment.len() as f64)).sqrt()
            + weight * ((2.0 / alpha).ln() / (2.0 * baseline.len() as f64)).sqrt();
    }
    // Round the public statistics to avoid libm's last-bit differences.
    let round = |n: f64| (n * 1e12).round() / 1e12;
    let lower = round(effect - radius);
    let margin = comparability["minimum_effect"].as_f64().unwrap_or(0.0);
    Ok(
        serde_json::json!({"status":if lower>=margin {"improved"} else {"not_improved"},"effect":round(effect),"uncertainty":{"method":"hoeffding","alpha":alpha,"lower_bound":lower,"radius":round(radius)}}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn positive_comparison_requires_uncertainty_margin_and_predeclared_weights() {
        let rule = json!({"engine":"kip:binary-stratified-v1"});
        let parameters = json!({"alpha":0.05});
        let comparison = json!({"method":"stratified","strata_weights":{"all":1},"minimum_effect":0.1,"missingness_policy":"count_as_failure","uncertainty_rule":"hoeffding"});
        let mut samples = EvaluationSamples {
            treatment: BTreeMap::from([("all".into(), vec![1.0; 100])]),
            baseline: BTreeMap::from([("all".into(), vec![0.0; 100])]),
        };
        let positive = evaluate_binary_rule(&rule, &parameters, &comparison, &samples).unwrap();
        assert_eq!(positive["status"], "improved");
        assert_eq!(positive["effect"], 1.0);
        samples.treatment.insert("all".into(), vec![1.0; 2]);
        samples.baseline.insert("all".into(), vec![0.0; 2]);
        assert_eq!(
            evaluate_binary_rule(&rule, &parameters, &comparison, &samples).unwrap()["status"],
            "not_improved"
        );
        let mut invalid = comparison;
        invalid["strata_weights"] = json!({"all":0.5});
        assert!(evaluate_binary_rule(&rule, &parameters, &invalid, &samples).is_err());
    }

    #[test]
    fn lease_expiry_and_takeover_reject_old_worker_completion() {
        let old = json!({"owner":"worker-a","fencing_token":1,"attempt_count":1,"expires_at":"2026-09-07T00:00:00Z"});
        let new = json!({"owner":"worker-b","fencing_token":2,"attempt_count":2,"expires_at":"2026-09-09T00:00:00Z"});
        let now = "2026-09-08T00:00:00Z";
        assert!(
            validate_lease_transition(
                "running",
                "completed",
                Some(&old),
                Some(&old),
                "worker-a",
                now
            )
            .is_err()
        );
        validate_lease_transition(
            "running",
            "running",
            Some(&old),
            Some(&new),
            "worker-b",
            now,
        )
        .unwrap();
        assert!(
            validate_lease_transition(
                "running",
                "completed",
                Some(&new),
                Some(&old),
                "worker-a",
                now
            )
            .is_err()
        );
        assert!(
            validate_lease_transition("pending", "completed", None, None, "worker-a", now).is_err()
        );
    }
}
