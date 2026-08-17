//! # Projection policy
//!
//! Belief is projected *under a policy*, and the policy identity travels with
//! the answer (Spec §54): "accepted" with no policy attached is not an
//! auditable statement, because the next reader cannot tell what "accepted"
//! required.
//!
//! ## What this policy deliberately does not do
//!
//! **It does not weight by mode.** A mode says how a claim was arrived at, and
//! it does not automatically grant trust (§26). Making `observed` outrank
//! `stated` here would be a trust model smuggled in as a constant — and one no
//! deployment agreed to. Modes gate *eligibility* instead: a hypothetical is
//! excluded from a factual projection because it was never offered as a fact.
//!
//! **It does not count.** Three Assertions by one actor are one voice, not
//! three (§93, §94). Aggregation runs over corroboration groups, and the policy
//! only decides where the boundaries between groups are.

use anda_kip::{AssertionMode, Json, KipError, Map};

/// The baseline policy's identity.
pub const BASELINE_ID: &str = "kip:policy:baseline";
/// The baseline policy's version. Any change to the constants below is a new
/// version, because it changes what a past "accepted" would have meant.
pub const BASELINE_VERSION: u64 = 1;

/// The knobs a projection runs under.
#[derive(Clone, Debug)]
pub struct Policy {
    /// The policy's name, reported with every answer.
    pub id: String,
    /// The policy's version.
    pub version: u64,
    /// The modes eligible for this projection.
    pub modes: Vec<AssertionMode>,
    /// The score at or above which one side is sufficient on its own.
    pub accept: f64,
    /// The score at or above which a side is *material* — enough to contest,
    /// not enough to decide.
    pub material: f64,
    /// The weight given to an Assertion whose actor stated no confidence.
    ///
    /// Not zero and not one: silence about confidence is neither a denial nor
    /// a certainty, and treating it as either would put a number in an actor's
    /// mouth (§5).
    pub unstated_confidence: f64,
    /// Whether a functional predicate's competing values oppose one another.
    pub expand_conflicts: bool,
}

impl Default for Policy {
    fn default() -> Self {
        Self::baseline()
    }
}

impl Policy {
    /// The policy a projection uses when the query names none.
    ///
    /// Ordinary factual recall: what somebody observed, said, inferred or
    /// imported. `hypothetical` is entertained without commitment and
    /// `predicted` is about the future, so neither is an answer to "what is
    /// the case" (§38, §39).
    pub fn baseline() -> Self {
        Self {
            id: BASELINE_ID.to_string(),
            version: BASELINE_VERSION,
            modes: vec![
                AssertionMode::Observed,
                AssertionMode::Stated,
                AssertionMode::Inferred,
                AssertionMode::Imported,
            ],
            accept: 0.7,
            material: 0.3,
            unstated_confidence: 0.5,
            expand_conflicts: true,
        }
    }

    /// The forecast policy: predictions instead of observations.
    ///
    /// A separate policy rather than a flag, because "what is the case" and
    /// "what is expected" are different questions and an answer must say which
    /// one it answered.
    pub fn forecast() -> Self {
        Self {
            id: "kip:policy:forecast".to_string(),
            modes: vec![AssertionMode::Predicted, AssertionMode::Inferred],
            ..Self::baseline()
        }
    }

    /// Reads a `WITH EPISTEMIC { ... }` block.
    ///
    /// Overriding a threshold produces a policy that is no longer the baseline,
    /// so the identity changes with it: an answer that reported
    /// `kip:policy:baseline` while running on different numbers would be a
    /// false audit trail.
    pub fn from_settings(settings: &Map<String, Json>) -> Result<Self, KipError> {
        let mut policy = match settings.get("policy") {
            None | Some(Json::Null) => Policy::baseline(),
            Some(Json::String(name)) => match name.as_str() {
                BASELINE_ID | "baseline" => Policy::baseline(),
                "forecast" | "kip:policy:forecast" => Policy::forecast(),
                other => {
                    return Err(KipError::new(
                        anda_kip::KipErrorCode::ProjectionPolicyUnavailable,
                        format!(
                            "this Nexus knows the epistemic policies \"baseline\" and \
                             \"forecast\"; it has no {other:?}"
                        ),
                    ));
                }
            },
            Some(other) => {
                return Err(KipError::type_mismatch(format!(
                    "`policy` names a policy, got {other}"
                )));
            }
        };

        let mut overridden = false;
        if let Some(value) = threshold(settings, "accept")? {
            policy.accept = value;
            overridden = true;
        }
        if let Some(value) = threshold(settings, "material")? {
            policy.material = value;
            overridden = true;
        }
        if let Some(modes) = settings.get("modes") {
            policy.modes = parse_modes(modes)?;
            overridden = true;
        }
        if policy.material > policy.accept {
            return Err(KipError::invalid_syntax(format!(
                "`material` ({}) must not exceed `accept` ({}); a side cannot be decisive at a \
                 lower score than it is merely material",
                policy.material, policy.accept
            )));
        }
        if overridden {
            policy.id = format!("{}+custom", policy.id);
        }
        Ok(policy)
    }

    /// Whether an Assertion's mode is eligible here.
    pub fn admits(&self, mode: Option<AssertionMode>) -> bool {
        match mode {
            // An Assertion with no recorded mode was never told how it was
            // arrived at. Admitting it would let unclassified claims decide
            // beliefs; the exclusion is reported rather than silent.
            None => false,
            Some(mode) => self.modes.contains(&mode),
        }
    }

    /// The exclusion reason a rejected mode carries (§137).
    pub fn mode_exclusion(&self, mode: Option<AssertionMode>) -> &'static str {
        match mode {
            Some(AssertionMode::Hypothetical) => "hypothetical_not_requested",
            Some(AssertionMode::Predicted) => "prediction_not_requested",
            None => "invalid_schema",
            _ => "policy_excluded",
        }
    }

    /// The policy identity reported alongside an answer.
    pub fn identity(&self) -> anda_kip::PolicyIdentity {
        anda_kip::PolicyIdentity {
            id: Some(self.id.clone()),
            version: Some(anda_kip::PolicyVersion::Integer(self.version)),
        }
    }
}

fn threshold(settings: &Map<String, Json>, key: &str) -> Result<Option<f64>, KipError> {
    match settings.get(key) {
        None | Some(Json::Null) => Ok(None),
        Some(Json::Number(n)) => {
            let value = n.as_f64().unwrap_or(f64::NAN);
            if !(0.0..=1.0).contains(&value) {
                return Err(KipError::invalid_syntax(format!(
                    "`{key}` is a score boundary in [0, 1], got {value}"
                )));
            }
            Ok(Some(value))
        }
        Some(other) => Err(KipError::type_mismatch(format!(
            "`{key}` must be a number in [0, 1], got {other}"
        ))),
    }
}

fn parse_modes(value: &Json) -> Result<Vec<AssertionMode>, KipError> {
    let Json::Array(items) = value else {
        return Err(KipError::type_mismatch("`modes` must be an array of modes"));
    };
    items
        .iter()
        .map(|item| {
            serde_json::from_value::<AssertionMode>(item.clone())
                .map_err(|_| KipError::type_mismatch(format!("{item} is not an Assertion mode")))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn settings(value: Json) -> Map<String, Json> {
        value.as_object().cloned().unwrap()
    }

    #[test]
    fn the_baseline_excludes_what_was_never_offered_as_fact() {
        // Spec §38, §39: a hypothetical is entertained without commitment and
        // a prediction is about the future. Neither answers "what is the case".
        let policy = Policy::baseline();
        assert!(policy.admits(Some(AssertionMode::Observed)));
        assert!(policy.admits(Some(AssertionMode::Stated)));
        assert!(policy.admits(Some(AssertionMode::Imported)));
        assert!(!policy.admits(Some(AssertionMode::Hypothetical)));
        assert!(!policy.admits(Some(AssertionMode::Predicted)));
        assert_eq!(
            policy.mode_exclusion(Some(AssertionMode::Hypothetical)),
            "hypothetical_not_requested"
        );

        // Forecast asks the other question, and says so by being a different
        // policy rather than a flag on this one.
        assert!(Policy::forecast().admits(Some(AssertionMode::Predicted)));
        assert!(!Policy::forecast().admits(Some(AssertionMode::Observed)));
    }

    #[test]
    fn an_unclassified_assertion_does_not_get_to_decide() {
        assert!(!Policy::baseline().admits(None));
    }

    #[test]
    fn overriding_a_threshold_changes_the_policy_identity() {
        // Reporting `kip:policy:baseline` while running on different numbers
        // would be a false audit trail.
        let custom = Policy::from_settings(&settings(json!({"accept": 0.9}))).unwrap();
        assert_eq!(custom.accept, 0.9);
        assert_ne!(custom.id, BASELINE_ID);
        assert!(custom.id.starts_with(BASELINE_ID));

        let plain = Policy::from_settings(&settings(json!({}))).unwrap();
        assert_eq!(plain.id, BASELINE_ID);
    }

    #[test]
    fn an_incoherent_boundary_is_refused() {
        let err =
            Policy::from_settings(&settings(json!({"accept": 0.2, "material": 0.8}))).unwrap_err();
        assert!(err.message.contains("must not exceed"));
    }

    #[test]
    fn an_unknown_policy_is_named_rather_than_silently_defaulted() {
        // Falling back to the baseline would answer a question nobody asked
        // and report a policy the caller did not choose.
        let err = Policy::from_settings(&settings(json!({"policy": "strict"}))).unwrap_err();
        assert_eq!(err.name(), "ProjectionPolicyUnavailable");
    }

    #[test]
    fn silence_about_confidence_is_neither_denial_nor_certainty() {
        let policy = Policy::baseline();
        assert!(policy.unstated_confidence > 0.0 && policy.unstated_confidence < 1.0);
    }
}
