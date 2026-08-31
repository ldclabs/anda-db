//! # Epistemic Projection
//!
//! What is *currently believed*, computed from the Assertions on record under a
//! named policy. It is a view, never stored state (Spec §21.2): storing
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

use anda_kip::{
    AssertionMode, BeliefStatus, Json, KipError, Map, Projection, ProjectionSide,
    ProjectionTemporal, ProjectionUncertainty,
};

use crate::id::ElementId;
use crate::kql::Context;
use crate::store::Element;
use crate::store::rows::AssertionRow;
use crate::term::Endpoint;
pub use policy::{Explanation, Policy};

/// One Assertion, as the projection sees it.
struct Candidate {
    id: ElementId,
    /// The actor's equality key, for grouping.
    ///
    /// An internal key, never a wire value: it is the storage layer's answer to
    /// "are these the same actor", and it carries separators no reader should
    /// have to parse. [`Candidate::actor_ref`] is what a root group reports.
    actor: String,
    /// The actor as a reader can follow it.
    actor_ref: Json,
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
    /// The Proposition projected, when one durably exists.
    ///
    /// `None` is a real answer rather than a missing field: a fully grounded
    /// `BELIEF` over a tuple no Proposition has been created for answers
    /// `insufficient` with no id (§46.4), and a read must not create the
    /// Proposition just to have something to point at.
    pub proposition: Option<ElementId>,
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
    /// The cognitive coordinate it read, when the read was bound to one.
    as_of: Option<u64>,
}

#[derive(Default)]
struct Ledger {
    supporting: Vec<String>,
    opposing: Vec<String>,
    uncertain: Vec<String>,
    excluded: Vec<(String, &'static str)>,
    support_groups: Vec<Group>,
    opposition_groups: Vec<Group>,
    warnings: Vec<String>,
}

/// One corroboration group: the independent root §23.3 counts once.
///
/// Reported rather than merely counted, because §27.2 asks a side for its
/// `root_groups` and §27.4 lists corroboration groups among what an Epistemic
/// Ledger contains. A count says two sources agreed; the groups say *which*
/// two, which is what lets a reader check that they were really independent.
#[derive(Clone, Debug, Default)]
struct Group {
    /// The equality keys that merged into this root.
    ///
    /// Internal, and never on the wire: they carry the separators the storage
    /// layer compares on. [`Group::actors`] and [`Group::evidence`] are what a
    /// reader gets.
    keys: Vec<String>,
    /// The semantic actors behind this root, as references.
    actors: Vec<Json>,
    /// The Evidence records it rests on.
    evidence: Vec<String>,
    /// The Assertions it collapsed.
    assertion_ids: Vec<String>,
    /// What it contributed: its strongest member, never the sum.
    contribution: f64,
}

impl Group {
    fn to_json(&self) -> Json {
        serde_json::json!({
            "actors": self.actors,
            "evidence": self.evidence,
            "assertion_ids": self.assertion_ids,
            "contribution": self.contribution,
        })
    }
}

impl Belief {
    /// The projection output a query binds and projects (§27.2).
    ///
    /// Built through [`anda_kip::Projection`] rather than as hand-written
    /// JSON: the protocol crate owns this shape, so a member renamed there
    /// becomes a compile error here instead of a dot path that silently reads
    /// null. It is the same discipline `view.rs` applies to the Core types,
    /// and the reason `support.root_groups` was once spelled
    /// `independent_groups` on this side alone.
    pub fn to_json(&self) -> Json {
        let projection = Projection {
            proposition_id: self.proposition.map(|id| id.to_string()),
            status: self.status,
            support: Some(self.side(
                &self.ledger.supporting,
                self.support,
                &self.ledger.support_groups,
            )),
            opposition: Some(self.side(
                &self.ledger.opposing,
                self.opposition,
                &self.ledger.opposition_groups,
            )),
            uncertainty: Some(ProjectionUncertainty {
                level: Some(Json::from(self.uncertainty_level())),
                reasons: self.uncertainty_reasons(),
            }),
            temporal: Some(ProjectionTemporal {
                valid_at: Some(self.valid_at.clone()),
                as_of_seq: self.as_of,
            }),
            policy: Some(self.policy.identity()),
            explanation: self.explanation(),
        };
        serde_json::to_value(projection).unwrap_or(Json::Null)
    }

    /// The Epistemic Ledger, at the level the query asked for (§49.1, §49.2).
    ///
    /// `none` returns no ledger at all rather than an empty one: an empty
    /// object reads as "we looked and found nothing to explain", and what
    /// happened is that the caller declined to be told.
    fn explanation(&self) -> Option<Json> {
        match self.policy.explanation {
            Explanation::None => None,
            Explanation::Summary => Some(serde_json::json!({
                "excluded_count": self.ledger.excluded.len(),
                "uncertain_count": self.ledger.uncertain.len(),
                "warnings": self.ledger.warnings,
            })),
            Explanation::Ledger => Some(serde_json::json!({
                "excluded": self
                    .ledger
                    .excluded
                    .iter()
                    .map(|(id, reason)| serde_json::json!({"assertion_id": id, "reason": reason}))
                    .collect::<Vec<_>>(),
                "uncertain_assertions": self.ledger.uncertain,
                "warnings": self.ledger.warnings,
            })),
        }
    }

    /// One side of the projection, with the roots its score came from.
    ///
    /// The Assertion ids and the corroboration groups *are* the ledger: they
    /// name who said it and which observations stood behind them. A caller
    /// that asked for no explanation is not handed them under another key
    /// (§49.2).
    fn side(&self, assertion_ids: &[String], score: f64, groups: &[Group]) -> ProjectionSide {
        let disclosed = self.policy.explanation == Explanation::Ledger;
        ProjectionSide {
            score: Some(score),
            // §27.3: an implementation MUST declare what its scores mean, and
            // MUST NOT present a normalized strength as a calibrated
            // probability. These combine self-reported commitments.
            score_semantics: Some("normalized_support_not_probability".to_string()),
            assertion_ids: if disclosed {
                assertion_ids.to_vec()
            } else {
                Vec::new()
            },
            root_groups: if disclosed {
                groups.iter().map(Group::to_json).collect()
            } else {
                Vec::new()
            },
        }
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
        let support_groups = self.ledger.support_groups.len();
        let opposition_groups = self.ledger.opposition_groups.len();
        if support_groups == 0 && opposition_groups == 0 {
            reasons.push("no eligible Assertion bears on this Proposition".into());
        }
        if support_groups > 0 && opposition_groups > 0 {
            reasons.push(format!(
                "{support_groups} independent group(s) support and {opposition_groups} oppose"
            ));
        }
        if support_groups == 1 && opposition_groups == 0 {
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

        let (support, support_groups) = aggregate(&candidates, false);
        let (opposition, opposition_groups) = aggregate(&candidates, true);
        ledger.support_groups = support_groups;
        ledger.opposition_groups = opposition_groups;

        let status = classify(support, opposition, &ledger, policy);
        Ok(Belief {
            proposition: Some(proposition),
            status,
            support,
            opposition,
            ledger,
            policy: policy.clone(),
            valid_at: at.to_string(),
            as_of: self.as_of,
        })
    }

    /// The answer for a fully grounded `BELIEF` whose Proposition does not
    /// exist (§46.4).
    ///
    /// Nobody has asserted a tuple nobody has created, so the honest answer is
    /// `insufficient` with a null id — not an empty result set. Returning no
    /// row would make the Agent infer "unknown" from "the pattern did not
    /// match", which is the inference §24 exists to prevent, and it is
    /// indistinguishable from a query that was simply written wrong.
    ///
    /// A read must not create the Proposition to have something to point at.
    pub fn ungrounded_belief(&self, policy: &Policy, at: &str) -> Belief {
        Belief {
            proposition: None,
            status: BeliefStatus::Insufficient,
            support: 0.0,
            opposition: 0.0,
            ledger: Ledger {
                warnings: vec![
                    "no Proposition exists for this tuple in this Space, so nothing has been \
                     asserted about it; that is an open-world absence, not a denial"
                        .to_string(),
                ],
                ..Default::default()
            },
            policy: policy.clone(),
            valid_at: at.to_string(),
            as_of: self.as_of,
        }
    }

    /// The conflict set of one slot: every Proposition with this subject and
    /// predicate, each projected (§35).
    pub async fn project_slot(
        &mut self,
        subject_key: &str,
        predicate_ref: &str,
        policy: &Policy,
        at: &str,
    ) -> Result<Slot, KipError> {
        let mut candidates = Vec::new();
        for id in self.slot_propositions(subject_key, predicate_ref).await? {
            candidates.push(self.project_belief(id, policy, at).await?);
        }
        Ok(Slot {
            candidates,
            policy: policy.clone(),
            valid_at: at.to_string(),
            as_of: self.as_of,
            warnings: vec![
                "this engine evaluates no source trust and no evidence quality; every eligible \
                 corroboration group counts equally"
                    .to_string(),
            ],
        })
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
            // §14.3: expiry says the claim is no longer *current*, which is a
            // statement about time rather than about withdrawal. An Assertion
            // read at a moment its own validity window covered is still the
            // claim that applied then, so the temporal stage below decides it
            // — otherwise `FOR TIME` in the past would silently lose every
            // claim that has since lapsed, which is the one question that
            // asks about them.
            "expired" if !row.valid_from.is_empty() || !row.valid_until.is_empty() => {}
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
            actor_ref: if row.asserted_by.is_null() {
                Json::Null
            } else {
                row.asserted_by.clone()
            },
            evidence: row.evidence_ids.clone(),
            stance: row.stance.clone(),
            // Every negative reads as "the actor stated none", not only the
            // exact sentinel written today: a row stored before `[0, 1]` was
            // enforced on the way in may hold another, and taking one of those
            // as a real commitment would clamp it to zero and silently weigh
            // the claim as worthless.
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
        let target = proposition.to_string();
        let ids = self
            .candidates(
                anda_kip::ElementKind::Assertion,
                Some(crate::store::eq_field(
                    "proposition_id",
                    anda_db_schema::Fv::Text(target.clone()),
                )),
            )
            .await?;
        self.charge(ids.len())?;

        let mut rows = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(Element::Assertion(row)) = self.load(id).await?
                && row.space == self.space
                // At a coordinate the index could not narrow, so the claim is
                // matched to its Proposition here.
                && row.proposition_id == target
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
        let Ok(def) = self.env.predicate_def(&symbol) else {
            return Ok(vec![]);
        };
        let functional = def.functional;
        // §25.1 names two conflict shapes and §92 requires both. Functional is
        // the strong one: one subject, one true object, so every rival value
        // disagrees. Exclusive is the weaker one — only the values a schema
        // declared incompatible disagree, and everything else coexists. A
        // person may hold many tags without being both alive and dead.
        let group: Option<Vec<Json>> = def
            .exclusive_values
            .iter()
            .find(|group| group.iter().any(|value| same_object(value, &row.object)))
            .cloned();
        if !functional && group.is_none() {
            return Ok(vec![]);
        }
        let mut rivals = self
            .slot_propositions(&row.subject_key, &row.predicate_ref)
            .await?;
        rivals.retain(|id| *id != target);
        if functional {
            return Ok(rivals);
        }
        let group = group.unwrap_or_default();
        let mut exclusive = Vec::new();
        for id in rivals {
            if let Some(Element::Proposition(rival)) = self.load(id).await?
                && group.iter().any(|value| same_object(value, &rival.object))
            {
                exclusive.push(id);
            }
        }
        Ok(exclusive)
    }

    /// Every active Proposition in one `(subject, predicate)` slot.
    pub async fn slot_propositions(
        &mut self,
        subject_key: &str,
        predicate_ref: &str,
    ) -> Result<Vec<ElementId>, KipError> {
        let ids = self
            .candidates(
                anda_kip::ElementKind::Proposition,
                Some(anda_db::query::Filter::And(vec![
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
                ])),
            )
            .await?;
        self.charge(ids.len())?;

        if !self.is_historical() {
            return Ok(ids);
        }
        // At a coordinate the index could not narrow, so the slot is matched
        // against the historical rows.
        let mut slot = Vec::new();
        for id in ids {
            if let Some(Element::Proposition(row)) = self.load(id).await?
                && row.state == "active"
                && row.subject_key == subject_key
                && row.predicate_ref == predicate_ref
            {
                slot.push(id);
            }
        }
        Ok(slot)
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
fn aggregate(candidates: &[Candidate], opposing: bool) -> (f64, Vec<Group>) {
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
        return (0.0, Vec::new());
    }

    // Union-find over actors and Evidence ids.
    let mut groups: Vec<Group> = Vec::new();
    for candidate in side {
        let mut keys = vec![format!("actor:{}", candidate.actor)];
        keys.extend(candidate.evidence.iter().map(|id| format!("evidence:{id}")));
        let assertion = candidate.id.to_string();
        let arriving = Group {
            keys: keys.clone(),
            actors: vec![candidate.actor_ref.clone()],
            evidence: candidate.evidence.clone(),
            assertion_ids: vec![assertion],
            contribution: candidate.confidence,
        };

        let mut merged: Option<usize> = None;
        let mut index = 0;
        while index < groups.len() {
            if groups[index].keys.iter().any(|key| keys.contains(key)) {
                match merged {
                    None => {
                        absorb(&mut groups[index], arriving.clone());
                        merged = Some(index);
                        index += 1;
                    }
                    Some(target) => {
                        // This candidate bridges two groups that looked
                        // independent, so they were not.
                        let absorbed = groups.remove(index);
                        let target = if target > index { target - 1 } else { target };
                        absorb(&mut groups[target], absorbed);
                        merged = Some(target);
                    }
                }
                continue;
            }
            index += 1;
        }
        if merged.is_none() {
            groups.push(arriving);
        }
    }

    for group in &mut groups {
        group.keys.sort();
        group.keys.dedup();
        group.evidence.sort();
        group.evidence.dedup();
        group.assertion_ids.sort();
        group.assertion_ids.dedup();
        dedup_json(&mut group.actors);
    }

    // Independent groups accumulate, with diminishing returns: two moderate
    // independent sources say more than either alone, but nothing here is a
    // calibrated probability, so the score is declared as normalized strength.
    let score = 1.0
        - groups.iter().fold(1.0, |acc, group| {
            acc * (1.0 - group.contribution.clamp(0.0, 1.0))
        });
    (score, groups)
}

/// Folds one group into another, keeping the strongest contribution.
fn absorb(into: &mut Group, other: Group) {
    into.keys.extend(other.keys);
    into.actors.extend(other.actors);
    into.evidence.extend(other.evidence);
    into.assertion_ids.extend(other.assertion_ids);
    into.contribution = into.contribution.max(other.contribution);
}

/// Removes duplicate references, preserving order.
///
/// A reference has no total order to sort by, and the list is one group's
/// actors — small enough that the quadratic scan is cheaper than inventing a
/// canonical form to sort on.
fn dedup_json(values: &mut Vec<Json>) {
    let mut seen: Vec<Json> = Vec::new();
    values.retain(|value| {
        if seen.contains(value) {
            false
        } else {
            seen.push(value.clone());
            true
        }
    });
}

/// Whether a declared exclusive value names this Proposition object.
///
/// A schema writes the value the way a Proposition object is written — an
/// exact reference or a Literal — so a bare id string and `{"id": ...}` name
/// the same thing and have to compare equal.
fn same_object(declared: &Json, object: &Json) -> bool {
    fn id_of(value: &Json) -> Option<&str> {
        match value {
            Json::String(text) => Some(text.as_str()),
            Json::Object(map) => map.get("id").and_then(Json::as_str),
            _ => None,
        }
    }
    if declared == object {
        return true;
    }
    match (id_of(declared), id_of(object)) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}

/// Stage 13: belief-state classification (§68–§73).
fn classify(support: f64, opposition: f64, ledger: &Ledger, policy: &Policy) -> BeliefStatus {
    let engaged = !ledger.support_groups.is_empty()
        || !ledger.opposition_groups.is_empty()
        || !ledger.uncertain.is_empty();

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
    // Either something reached materiality without settling the question, or
    // somebody engaged and nothing did. Both are the same answer.
    BeliefStatus::Uncertain
}

/// Renders a slot projection: the conflict set, not a winner (§47.3).
///
/// `accepted_values` is a list rather than a value on purpose: a functional
/// slot with two accepted candidates is a contradiction the caller has to see,
/// and collapsing it to one would be the engine picking a winner nobody
/// authorized.
///
/// `status` leads, because §47.4 asks a grounded empty slot to answer
/// `insufficient` with an empty `accepted_values` rather than force the Agent
/// to infer unknown from zero raw rows — and an Agent that has to derive the
/// slot's state by scanning `candidate_projections` is doing exactly that.
/// `subject`, `predicate_ref`, `leading` and `contested` are additive: they
/// name what the slot was about and which side is ahead *without* claiming it
/// settled anything.
///
/// The slot reports its own `policy` and `temporal`, from the coordinates it
/// *ran* under rather than from whichever candidate happened to come first.
/// Reading them off a candidate leaves them null exactly when the slot is
/// empty — which is the case §47.4 is about, and the one where a caller most
/// needs to know the answer was computed rather than skipped.
pub fn slot_to_json(subject: &Endpoint, predicate: &str, slot: &Slot) -> Json {
    let beliefs = &slot.candidates;
    let accepted: Vec<String> = beliefs
        .iter()
        .filter(|belief| belief.status == BeliefStatus::Accepted)
        .filter_map(|belief| belief.proposition.map(|id| id.to_string()))
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

    // §47.3's four statuses, decided over the slot rather than over any one
    // candidate. Two accepted values in one slot is a contradiction, so it is
    // `contested` even though each candidate on its own was accepted.
    let status = if contested {
        BeliefStatus::Contested
    } else if !accepted.is_empty() {
        BeliefStatus::Accepted
    } else if beliefs
        .iter()
        .any(|belief| belief.status != BeliefStatus::Insufficient)
    {
        BeliefStatus::Uncertain
    } else {
        BeliefStatus::Insufficient
    };

    serde_json::json!({
        "status": status,
        // The reference shape §8 fixes, never the engine's internal endpoint
        // key: a caller cannot feed `id\u001fC-1` back into anything, and a
        // storage key on the wire is a detail that becomes a contract the
        // moment somebody parses it.
        "subject": subject.to_json(),
        "predicate_ref": predicate,
        "accepted_values": accepted,
        "candidate_projections": beliefs.iter().map(Belief::to_json).collect::<Vec<_>>(),
        // A leading side is not a settled answer, so it is named as leading.
        "leading": leading.and_then(|belief| belief.proposition.map(|id| id.to_string())),
        "contested": contested,
        "uncertainty": {
            "level": match status {
                BeliefStatus::Insufficient => "total",
                BeliefStatus::Contested | BeliefStatus::Uncertain => "high",
                _ => "low",
            },
            "reasons": leading
                .map(|belief| belief.uncertainty_reasons())
                .unwrap_or_default(),
        },
        "temporal": {"valid_at": slot.valid_at, "as_of_seq": slot.as_of},
        "policy": slot.policy.identity(),
        // §47.3 lists an explanation on the slot too. A slot's own explanation
        // is about the *set*: how many candidates competed for it, and what
        // this engine could not weigh between them.
        "explanation": {
            "candidate_count": beliefs.len(),
            "accepted_count": accepted.len(),
            "warnings": slot.warnings,
        },
    })
}

/// One subject-predicate slot, projected (§47.2).
///
/// Carries the coordinates the projection ran under, so the slot can report
/// them whether or not any candidate exists.
pub struct Slot {
    /// Every Proposition competing for the slot, each projected.
    pub candidates: Vec<Belief>,
    /// The policy it ran under.
    pub policy: Policy,
    /// The world time it evaluated for.
    pub valid_at: String,
    /// The cognitive coordinate it read, when bound to one.
    pub as_of: Option<u64>,
    /// What this engine could not do while deciding the slot.
    pub warnings: Vec<String>,
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
            actor_ref: Json::from(actor),
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
        let repeated = vec![
            candidate("actor:alice", &[], "support", 0.6),
            candidate("actor:alice", &[], "support", 0.6),
            candidate("actor:alice", &[], "support", 0.6),
        ];
        let (score, groups) = aggregate(&repeated, false);
        assert_eq!(groups.len(), 1);
        assert!((score - 0.6).abs() < 1e-9, "got {score}");

        // Three genuinely independent actors say more.
        let independent = vec![
            candidate("actor:alice", &[], "support", 0.6),
            candidate("actor:bob", &[], "support", 0.6),
            candidate("actor:carol", &[], "support", 0.6),
        ];
        let (score, groups) = aggregate(&independent, false);
        assert_eq!(groups.len(), 3);
        assert!(score > 0.9, "got {score}");
    }

    #[test]
    fn shared_evidence_merges_groups_that_look_independent() {
        // Spec §19, §21: two actors relaying one observation are not two
        // observations, and manufactured corroboration is built exactly there.
        let echo = vec![
            candidate("actor:alice", &["E-1"], "support", 0.6),
            candidate("actor:bob", &["E-1"], "support", 0.6),
        ];
        let (score, groups) = aggregate(&echo, false);
        assert_eq!(groups.len(), 1, "one observation, relayed twice");
        assert!((score - 0.6).abs() < 1e-9);

        // A third actor with its own evidence is a second group.
        let mixed = vec![
            candidate("actor:alice", &["E-1"], "support", 0.6),
            candidate("actor:bob", &["E-1"], "support", 0.6),
            candidate("actor:carol", &["E-2"], "support", 0.6),
        ];
        assert_eq!(aggregate(&mixed, false).1.len(), 2);
    }

    #[test]
    fn a_bridging_assertion_collapses_two_groups() {
        // Alice and Bob look independent until Carol turns out to have cited
        // both of their sources — at which point they never were.
        let bridged = vec![
            candidate("actor:alice", &["E-1"], "support", 0.5),
            candidate("actor:bob", &["E-2"], "support", 0.5),
            candidate("actor:carol", &["E-1", "E-2"], "support", 0.5),
        ];
        assert_eq!(aggregate(&bridged, false).1.len(), 1);
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
            opposition_groups: vec![Group::default()],
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
            support_groups: vec![Group::default(), Group::default()],
            opposition_groups: vec![Group::default()],
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
