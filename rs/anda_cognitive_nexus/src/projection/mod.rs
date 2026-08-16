//! # Epistemic Projection
//!
//! What is *currently believed*, computed from the Assertions on record under a
//! named policy. It is a view, never stored state (Spec §48, §240.40): storing
//! it would create a second answer that could disagree with the Assertions it
//! came from, and nothing would say which one was right.
//!
//! ## Three rules that shape the arithmetic
//!
//! **Absence of support is not rejection** (§24). A Proposition nobody has
//! asserted is `insufficient`, and `insufficient` is a real answer about the
//! state of the evidence — not a quiet "no".
//!
//! **Evidence weight is not frequency count** (§93, §94). Saying the same thing
//! three times is one voice repeated, not three independent voices. Assertions
//! are therefore grouped by corroboration before anything is aggregated, and a
//! group contributes once.
//!
//! **Two Assertions that cite the same Evidence are not independent** (§19,
//! §21). Manufactured corroboration is exactly what an attacker builds, so
//! shared Evidence merges two groups even when the actors differ.
//!
//! ## What this implementation does not do
//!
//! The full pipeline has fourteen stages (§55). This one implements
//! grounding, conflict-set expansion, lifecycle, temporal and mode
//! eligibility, corroboration grouping, aggregation, classification and the
//! explanation ledger. Governance visibility, trust evaluation and
//! evidence-quality evaluation are **not** implemented: there is no trust model
//! in this engine yet, so every eligible group counts equally, and the answer
//! says so in its warnings rather than implying a judgement it did not make.

pub mod policy;

use anda_kip::{AssertionMode, AssertionStatus, BeliefStatus, Json, KipError, Map};

use crate::id::ElementId;
use crate::kql::Context;
use crate::schema::{Intent, SymbolKind};
use crate::store::Element;
use crate::store::rows::AssertionRow;
pub use policy::Policy;

/// One Assertion, as the projection sees it.
struct Candidate {
    id: ElementId,
    actor: String,
    evidence: Vec<String>,
    stance: String,
    confidence: f64,
    /// Which side of the Proposition this lands on once conflict expansion is
    /// taken into account: supporting *this* tuple, or opposing it.
    opposes_target: bool,
}

/// An Assertion the projection left out, and why.
struct Excluded {
    id: ElementId,
    reason: &'static str,
}

/// The projected belief about one Proposition.
pub struct Belief {
    /// The Proposition projected.
    pub proposition: ElementId,
    /// The classification.
    pub status: BeliefStatus,
    /// Normalized support strength.
    pub support: f64,
    /// Normalized opposition strength.
    pub opposition: f64,
    /// The Assertions on each side, and the ones excluded.
    ledger: Ledger,
    /// The policy this ran under.
    policy: Policy,
    /// The world time it was projected at.
    valid_at: String,
}

#[derive(Default)]
struct Ledger {
    supporting: Vec<String>,
    opposing: Vec<String>,
    uncertain: Vec<String>,
    excluded: Vec<(String, &'static str)>,
    support_groups: usize,
    opposition_groups: usize,
    warnings: Vec<String>,
}

impl Belief {
    /// The projection output a query binds and projects (§75).
    pub fn to_json(&self) -> Json {
        serde_json::json!({
            "proposition_id": self.proposition.to_string(),
            "status": self.status,
            "support": {
                "score": self.support,
                // §76: an implementation MUST declare what its scores mean,
                // and MUST NOT present a normalized strength as a calibrated
                // probability. These combine self-reported commitments.
                "score_semantics": "normalized_support_not_probability",
                "assertion_ids": self.ledger.supporting,
                "independent_groups": self.ledger.support_groups,
            },
            "opposition": {
                "score": self.opposition,
                "score_semantics": "normalized_support_not_probability",
                "assertion_ids": self.ledger.opposing,
                "independent_groups": self.ledger.opposition_groups,
            },
            "uncertainty": {
                "level": self.uncertainty_level(),
                "reasons": self.uncertainty_reasons(),
            },
            "temporal": {"valid_at": self.valid_at},
            "policy": {"id": self.policy.id, "version": self.policy.version},
            "explanation": {
                "excluded": self
                    .ledger
                    .excluded
                    .iter()
                    .map(|(id, reason)| serde_json::json!({"assertion_id": id, "reason": reason}))
                    .collect::<Vec<_>>(),
                "uncertain_assertions": self.ledger.uncertain,
                "warnings": self.ledger.warnings,
            },
        })
    }

    fn uncertainty_level(&self) -> &'static str {
        match self.status {
            BeliefStatus::Insufficient => "total",
            BeliefStatus::Contested => "high",
            BeliefStatus::Uncertain => "high",
            _ if self.opposition >= self.policy.material => "medium",
            _ => "low",
        }
    }

    /// Why the answer is as uncertain as it is (§67).
    ///
    /// Uncertainty is not `1 - confidence`: it has causes, and naming them is
    /// what lets a caller decide whether to act or to go and look.
    fn uncertainty_reasons(&self) -> Vec<String> {
        let mut reasons = Vec::new();
        if self.ledger.support_groups == 0 && self.ledger.opposition_groups == 0 {
            reasons.push("no eligible Assertion bears on this Proposition".into());
        }
        if self.ledger.support_groups > 0 && self.ledger.opposition_groups > 0 {
            reasons.push(format!(
                "{} independent group(s) support and {} oppose",
                self.ledger.support_groups, self.ledger.opposition_groups
            ));
        }
        if self.ledger.support_groups == 1 && self.ledger.opposition_groups == 0 {
            reasons.push("a single source, with no independent corroboration".into());
        }
        if !self.ledger.uncertain.is_empty() {
            reasons.push(format!(
                "{} assertor(s) expressed uncertainty rather than a stance",
                self.ledger.uncertain.len()
            ));
        }
        if !self.ledger.excluded.is_empty() {
            reasons.push(format!(
                "{} Assertion(s) were excluded; see the explanation ledger",
                self.ledger.excluded.len()
            ));
        }
        reasons
    }
}

impl Context<'_> {
    /// Projects belief about one Proposition.
    pub async fn project_belief(
        &mut self,
        proposition: ElementId,
        policy: &Policy,
        at: &str,
    ) -> Result<Belief, KipError> {
        let mut ledger = Ledger {
            warnings: vec![
                // Not a caveat about this answer in particular: it is what the
                // engine structurally cannot do yet, and an answer that read
                // as trust-weighted when it is not would be worse than none.
                "this engine evaluates no source trust and no evidence quality; every eligible \
                 corroboration group counts equally"
                    .to_string(),
            ],
            ..Default::default()
        };

        let candidates = self
            .collect_candidates(proposition, policy, at, &mut ledger)
            .await?;

        let (support, support_groups) = aggregate(&candidates, false, policy);
        let (opposition, opposition_groups) = aggregate(&candidates, true, policy);
        ledger.support_groups = support_groups;
        ledger.opposition_groups = opposition_groups;

        let status = classify(support, opposition, &ledger, policy);
        Ok(Belief {
            proposition,
            status,
            support,
            opposition,
            ledger,
            policy: policy.clone(),
            valid_at: at.to_string(),
        })
    }

    /// The conflict set of one slot: every Proposition with this subject and
    /// predicate, each projected (§35).
    pub async fn project_slot(
        &mut self,
        subject_key: &str,
        predicate_ref: &str,
        policy: &Policy,
        at: &str,
    ) -> Result<Vec<Belief>, KipError> {
        let mut beliefs = Vec::new();
        for id in self.slot_propositions(subject_key, predicate_ref).await? {
            beliefs.push(self.project_belief(id, policy, at).await?);
        }
        Ok(beliefs)
    }

    /// Gathers eligible Assertions, from this Proposition and its rivals.
    async fn collect_candidates(
        &mut self,
        target: ElementId,
        policy: &Policy,
        at: &str,
        ledger: &mut Ledger,
    ) -> Result<Vec<Candidate>, KipError> {
        let mut candidates = Vec::new();

        for row in self.assertions_about(target).await? {
            match self.eligible(&row, policy, at) {
                Ok(candidate) => {
                    let id = candidate.id.to_string();
                    match candidate.stance.as_str() {
                        "support" => ledger.supporting.push(id),
                        "reject" => ledger.opposing.push(id),
                        // An `uncertain` stance is material — the actor engaged
                        // with the question — but it takes no side, so it can
                        // move the answer off `insufficient` without moving it
                        // toward either pole.
                        _ => ledger.uncertain.push(id),
                    }
                    candidates.push(candidate);
                }
                Err(excluded) => ledger
                    .excluded
                    .push((excluded.id.to_string(), excluded.reason)),
            }
        }

        // Stage 3 — conflict-set expansion (§58). Support for a rival value of
        // a functional predicate opposes this one, because the schema says only
        // one of them can apply.
        if policy.expand_conflicts {
            for rival in self.functional_rivals(target).await? {
                for row in self.assertions_about(rival).await? {
                    if let Ok(mut candidate) = self.eligible(&row, policy, at)
                        && candidate.stance == "support"
                    {
                        candidate.opposes_target = true;
                        ledger.opposing.push(candidate.id.to_string());
                        candidates.push(candidate);
                    }
                }
            }
        }
        Ok(candidates)
    }

    /// Stages 4–6: lifecycle, temporal and mode eligibility.
    fn eligible(
        &self,
        row: &AssertionRow,
        policy: &Policy,
        at: &str,
    ) -> Result<Candidate, Excluded> {
        let id = ElementId::new(anda_kip::ElementKind::Assertion, row._id);
        let reject = |reason| Err(Excluded { id, reason });

        // Stage 4 — lifecycle (§59). A retracted claim was withdrawn and a
        // superseded one was replaced; both stay on record for explanation.
        match row.status.as_str() {
            "active" => {}
            "retracted" => return reject("retracted"),
            "superseded" => return reject("superseded"),
            "expired" => return reject("expired"),
            _ => return reject("invalid_schema"),
        }
        if row.state != crate::store::rows::state::ACTIVE {
            return reject("not_visible");
        }

        // Stage 5 — temporal (§60). No window means "always", not "never".
        if !row.valid_from.is_empty() && row.valid_from.as_str() > at {
            return reject("outside_valid_time");
        }
        if !row.valid_until.is_empty() && row.valid_until.as_str() <= at {
            return reject("outside_valid_time");
        }

        // Stage 6 — mode (§61).
        let mode: Option<AssertionMode> =
            serde_json::from_value(Json::String(row.mode.clone())).ok();
        if !policy.admits(mode) {
            return reject(policy.mode_exclusion(mode));
        }

        Ok(Candidate {
            id,
            actor: if row.asserted_by_key.is_empty() {
                // An Assertion with no recorded actor cannot be grouped with
                // anything, so it is its own group rather than joining a
                // nameless one with every other unattributed claim.
                format!("anonymous:{id}")
            } else {
                row.asserted_by_key.clone()
            },
            evidence: row.evidence_ids.clone(),
            stance: row.stance.clone(),
            confidence: if row.confidence < 0.0 {
                policy.unstated_confidence
            } else {
                row.confidence
            },
            opposes_target: false,
        })
    }

    async fn assertions_about(
        &mut self,
        proposition: ElementId,
    ) -> Result<Vec<AssertionRow>, KipError> {
        let ids = self
            .store
            .assertions()
            .query_all_ids(crate::store::eq_field(
                "proposition_id",
                anda_db_schema::Fv::Text(proposition.to_string()),
            ))
            .await
            .map_err(crate::error::db_error)?;
        self.charge(ids.len())?;

        let mut rows = Vec::with_capacity(ids.len());
        for seq in ids {
            let id = ElementId::new(anda_kip::ElementKind::Assertion, seq);
            if let Some(Element::Assertion(row)) = self.load(id).await?
                && row.space == self.space
            {
                rows.push(*row);
            }
        }
        Ok(rows)
    }

    /// The Propositions that compete with this one for a functional slot.
    async fn functional_rivals(&mut self, target: ElementId) -> Result<Vec<ElementId>, KipError> {
        let Some(Element::Proposition(row)) = self.load(target).await? else {
            return Ok(vec![]);
        };
        let symbol = match row.predicate_ref.parse::<crate::schema::SymbolRef>() {
            Ok(symbol) => symbol,
            // A predicate this environment cannot resolve declares nothing, so
            // it declares no exclusivity either.
            Err(_) => return Ok(vec![]),
        };
        let functional = self
            .env
            .predicate_def(&symbol)
            .map(|def| def.functional)
            .unwrap_or(false);
        if !functional {
            return Ok(vec![]);
        }
        let _ =
            self.env
                .resolve_symbol(SymbolKind::PredicateType, &row.predicate_ref, Intent::Read);

        let mut rivals = self
            .slot_propositions(&row.subject_key, &row.predicate_ref)
            .await?;
        rivals.retain(|id| *id != target);
        Ok(rivals)
    }

    /// Every active Proposition in one `(subject, predicate)` slot.
    pub async fn slot_propositions(
        &mut self,
        subject_key: &str,
        predicate_ref: &str,
    ) -> Result<Vec<ElementId>, KipError> {
        let ids = self
            .store
            .propositions()
            .query_all_ids(anda_db::query::Filter::And(vec![
                Box::new(crate::store::eq_field(
                    "space",
                    anda_db_schema::Fv::Text(self.space.clone()),
                )),
                Box::new(crate::store::eq_field(
                    "state",
                    anda_db_schema::Fv::Text("active".to_string()),
                )),
                Box::new(crate::store::eq_field(
                    "subject_key",
                    anda_db_schema::Fv::Text(subject_key.to_string()),
                )),
                Box::new(crate::store::eq_field(
                    "predicate_ref",
                    anda_db_schema::Fv::Text(predicate_ref.to_string()),
                )),
            ]))
            .await
            .map_err(crate::error::db_error)?;
        self.charge(ids.len())?;
        Ok(ids
            .into_iter()
            .map(|seq| ElementId::new(anda_kip::ElementKind::Proposition, seq))
            .collect())
    }
}

/// Stages 8 and 11: group by corroboration, then aggregate the groups.
///
/// Two Assertions join one group when they share an actor *or* share Evidence.
/// The first is repetition (§94); the second is the same observation counted
/// twice, which is how manufactured corroboration is built (§19, §21).
///
/// A group contributes its strongest member, not the sum of its members —
/// saying something twice does not make it truer.
fn aggregate(candidates: &[Candidate], opposing: bool, policy: &Policy) -> (f64, usize) {
    let side: Vec<&Candidate> = candidates
        .iter()
        .filter(|candidate| {
            if opposing {
                candidate.opposes_target || candidate.stance == "reject"
            } else {
                !candidate.opposes_target && candidate.stance == "support"
            }
        })
        .collect();
    if side.is_empty() {
        return (0.0, 0);
    }

    // Union-find over actors and Evidence ids.
    let mut groups: Vec<(Vec<String>, f64)> = Vec::new();
    for candidate in side {
        let mut keys = vec![format!("actor:{}", candidate.actor)];
        keys.extend(candidate.evidence.iter().map(|id| format!("evidence:{id}")));

        let mut merged: Option<usize> = None;
        let mut index = 0;
        while index < groups.len() {
            if groups[index].0.iter().any(|key| keys.contains(key)) {
                match merged {
                    None => {
                        groups[index].0.extend(keys.clone());
                        groups[index].1 = groups[index].1.max(candidate.confidence);
                        merged = Some(index);
                        index += 1;
                    }
                    Some(target) => {
                        // This candidate bridges two groups that looked
                        // independent, so they were not.
                        let (keys, confidence) = groups.remove(index);
                        let target = if target > index { target - 1 } else { target };
                        groups[target].0.extend(keys);
                        groups[target].1 = groups[target].1.max(confidence);
                        merged = Some(target);
                    }
                }
                continue;
            }
            index += 1;
        }
        if merged.is_none() {
            groups.push((keys, candidate.confidence));
        }
    }

    // Independent groups accumulate, with diminishing returns: two moderate
    // independent sources say more than either alone, but nothing here is a
    // calibrated probability, so the score is declared as normalized strength.
    let score = 1.0
        - groups
            .iter()
            .fold(1.0, |acc, (_, c)| acc * (1.0 - c.clamp(0.0, 1.0)));
    let _ = policy;
    (score, groups.len())
}

/// Stage 13: belief-state classification (§68–§73).
fn classify(support: f64, opposition: f64, ledger: &Ledger, policy: &Policy) -> BeliefStatus {
    let has_material = support >= policy.material || opposition >= policy.material;
    let engaged =
        ledger.support_groups > 0 || ledger.opposition_groups > 0 || !ledger.uncertain.is_empty();

    if !engaged {
        // The open-world state. Nobody has spoken, which is not a denial.
        return BeliefStatus::Insufficient;
    }
    if support >= policy.accept && opposition < policy.material {
        return BeliefStatus::Accepted;
    }
    if opposition >= policy.accept && support < policy.material {
        // Rejection needs positive opposition, and is never inferred from an
        // absence of support (§21.5).
        return BeliefStatus::Rejected;
    }
    if support >= policy.material && opposition >= policy.material {
        return BeliefStatus::Contested;
    }
    if has_material {
        return BeliefStatus::Uncertain;
    }
    // Somebody engaged, but nothing reached materiality.
    BeliefStatus::Uncertain
}

/// Renders a slot projection: the conflict set, not a winner (§35).
///
/// The field names follow the agent-facing syntax card, which promises
/// `accepted_values` and `candidate_projections`. `accepted_values` is a list
/// rather than a value on purpose: a functional slot with two accepted
/// candidates is a contradiction the caller has to see, and collapsing it to
/// one would be the engine picking a winner nobody authorized.
pub fn slot_to_json(subject: &str, predicate: &str, beliefs: &[Belief]) -> Json {
    let accepted: Vec<String> = beliefs
        .iter()
        .filter(|belief| belief.status == BeliefStatus::Accepted)
        .map(|belief| belief.proposition.to_string())
        .collect();
    let leading = beliefs
        .iter()
        .filter(|belief| belief.status != BeliefStatus::Insufficient)
        .max_by(|a, b| {
            a.support
                .partial_cmp(&b.support)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    let contested = accepted.len() > 1
        || beliefs
            .iter()
            .any(|belief| belief.status == BeliefStatus::Contested);
    serde_json::json!({
        "subject": subject,
        "predicate_ref": predicate,
        "accepted_values": accepted,
        "candidate_projections": beliefs.iter().map(Belief::to_json).collect::<Vec<_>>(),
        // A leading side is not a settled answer, so it is named as leading.
        "leading": leading.map(|belief| belief.proposition.to_string()),
        "contested": contested,
    })
}

/// The settings block of `WITH EPISTEMIC { ... }`, evaluated.
pub fn settings_of(
    block: &anda_kip::BoundObject,
    resolve: impl Fn(&str) -> Result<Json, KipError>,
) -> Result<Map<String, Json>, KipError> {
    let mut settings = Map::new();
    for (key, value) in block {
        settings.insert(key.clone(), bound_to_json(value, &resolve)?);
    }
    Ok(settings)
}

fn bound_to_json(
    value: &anda_kip::BoundValue,
    resolve: &impl Fn(&str) -> Result<Json, KipError>,
) -> Result<Json, KipError> {
    Ok(match value {
        anda_kip::BoundValue::Value(literal) => Json::from(literal.clone()),
        anda_kip::BoundValue::Param(name) => resolve(name)?,
        anda_kip::BoundValue::Array(items) => Json::Array(
            items
                .iter()
                .map(|item| bound_to_json(item, resolve))
                .collect::<Result<_, _>>()?,
        ),
        anda_kip::BoundValue::Object(fields) => {
            let mut map = Map::new();
            for (key, item) in fields {
                map.insert(key.clone(), bound_to_json(item, resolve)?);
            }
            Json::Object(map)
        }
        other => {
            return Err(KipError::unsupported_capability(format!(
                "{other:?} is not a value an epistemic setting can take"
            )));
        }
    })
}

/// The lifecycle statuses that keep an Assertion out of a current projection.
pub fn is_historical(status: AssertionStatus) -> bool {
    !matches!(status, AssertionStatus::Active)
}

/// A grouped view for tests and callers that want the counts.
pub fn group_counts(belief: &Belief) -> (usize, usize) {
    (
        belief.ledger.support_groups,
        belief.ledger.opposition_groups,
    )
}

/// The exclusion reasons recorded for one projection.
pub fn exclusions(belief: &Belief) -> Vec<(String, &'static str)> {
    belief.ledger.excluded.clone()
}

impl Belief {
    /// The policy identity, for the result context.
    pub fn policy_identity(&self) -> anda_kip::PolicyIdentity {
        self.policy.identity()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(actor: &str, evidence: &[&str], stance: &str, confidence: f64) -> Candidate {
        Candidate {
            id: ElementId::new(anda_kip::ElementKind::Assertion, 1),
            actor: actor.to_string(),
            evidence: evidence.iter().map(|s| s.to_string()).collect(),
            stance: stance.to_string(),
            confidence,
            opposes_target: false,
        }
    }

    #[test]
    fn repetition_by_one_actor_is_one_voice() {
        // Spec §94: saying the same thing three times is one voice repeated.
        // Counting it three times is how a memory system talks itself into
        // certainty.
        let policy = Policy::baseline();
        let repeated = vec![
            candidate("actor:alice", &[], "support", 0.6),
            candidate("actor:alice", &[], "support", 0.6),
            candidate("actor:alice", &[], "support", 0.6),
        ];
        let (score, groups) = aggregate(&repeated, false, &policy);
        assert_eq!(groups, 1);
        assert!((score - 0.6).abs() < 1e-9, "got {score}");

        // Three genuinely independent actors say more.
        let independent = vec![
            candidate("actor:alice", &[], "support", 0.6),
            candidate("actor:bob", &[], "support", 0.6),
            candidate("actor:carol", &[], "support", 0.6),
        ];
        let (score, groups) = aggregate(&independent, false, &policy);
        assert_eq!(groups, 3);
        assert!(score > 0.9, "got {score}");
    }

    #[test]
    fn shared_evidence_merges_groups_that_look_independent() {
        // Spec §19, §21: two actors relaying one observation are not two
        // observations, and manufactured corroboration is built exactly there.
        let policy = Policy::baseline();
        let echo = vec![
            candidate("actor:alice", &["E-1"], "support", 0.6),
            candidate("actor:bob", &["E-1"], "support", 0.6),
        ];
        let (score, groups) = aggregate(&echo, false, &policy);
        assert_eq!(groups, 1, "one observation, relayed twice");
        assert!((score - 0.6).abs() < 1e-9);

        // A third actor with its own evidence is a second group.
        let mixed = vec![
            candidate("actor:alice", &["E-1"], "support", 0.6),
            candidate("actor:bob", &["E-1"], "support", 0.6),
            candidate("actor:carol", &["E-2"], "support", 0.6),
        ];
        assert_eq!(aggregate(&mixed, false, &policy).1, 2);
    }

    #[test]
    fn a_bridging_assertion_collapses_two_groups() {
        // Alice and Bob look independent until Carol turns out to have cited
        // both of their sources — at which point they never were.
        let policy = Policy::baseline();
        let bridged = vec![
            candidate("actor:alice", &["E-1"], "support", 0.5),
            candidate("actor:bob", &["E-2"], "support", 0.5),
            candidate("actor:carol", &["E-1", "E-2"], "support", 0.5),
        ];
        assert_eq!(aggregate(&bridged, false, &policy).1, 1);
    }

    #[test]
    fn silence_is_insufficient_and_never_rejection() {
        // Spec §21.5, §24: the open-world rule. Nothing on record is not "no".
        let policy = Policy::baseline();
        let ledger = Ledger::default();
        assert_eq!(
            classify(0.0, 0.0, &ledger, &policy),
            BeliefStatus::Insufficient
        );
        assert!(!BeliefStatus::Insufficient.is_decided());
    }

    #[test]
    fn rejection_needs_positive_opposition() {
        let policy = Policy::baseline();
        let opposed = Ledger {
            opposition_groups: 1,
            ..Default::default()
        };
        assert_eq!(
            classify(0.0, 0.9, &opposed, &policy),
            BeliefStatus::Rejected
        );
        // Weak opposition with no support is not a rejection either.
        assert_eq!(
            classify(0.0, 0.4, &opposed, &policy),
            BeliefStatus::Uncertain
        );
    }

    #[test]
    fn material_disagreement_is_contested_not_accepted() {
        // A leading side does not settle the question, and reporting the
        // leader as accepted would hide the disagreement entirely.
        let policy = Policy::baseline();
        let both = Ledger {
            support_groups: 2,
            opposition_groups: 1,
            ..Default::default()
        };
        assert_eq!(classify(0.85, 0.5, &both, &policy), BeliefStatus::Contested);
        // Accepted needs opposition below materiality, not merely below
        // support.
        assert_eq!(classify(0.85, 0.1, &both, &policy), BeliefStatus::Accepted);
    }

    #[test]
    fn an_uncertain_stance_is_engagement_without_a_side() {
        let policy = Policy::baseline();
        let hedged = Ledger {
            uncertain: vec!["A-1".into()],
            ..Default::default()
        };
        // Somebody engaged with the question, so this is not the open-world
        // unknown — but nobody took a side, so it is not decided either.
        assert_eq!(
            classify(0.0, 0.0, &hedged, &policy),
            BeliefStatus::Uncertain
        );
    }
}
