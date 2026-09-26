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
//! ## Projection stages
//!
//! Grounding, conflict expansion, lifecycle/time/mode eligibility, Governance
//! visibility, protected actor weighting, corroboration and aggregation run at
//! one basis. Evidence quality is not automatically evaluated.

mod dependency;
pub mod policy;
pub(crate) mod strength;
pub(crate) mod world;

use anda_kip::{
    AssertionMode, BeliefStatus, Json, KipError, Map, Projection, ProjectionSide,
    ProjectionUncertainty,
};

use std::collections::BTreeMap;

use crate::id::ElementId;
use crate::kql::Context;
use crate::store::Element;
use crate::store::rows::AssertionRow;
use crate::term::Endpoint;
pub use policy::{Explanation, Policy};

/// One Assertion, as the projection sees it.
#[derive(Clone)]
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

/// The Propositions projected together, and how their values relate.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Frame {
    /// Every candidate value, the target included: the whole slot when the
    /// predicate constrains one (§20.15).
    members: Vec<ElementId>,
    /// Share target results only within an ordinary active slot and the same
    /// canonical subject / exact predicate definition.
    shareable: bool,
    sharing: BTreeMap<ElementId, (String, String)>,
    /// Each member's `functional_by` partition: its object's Concept Type
    /// lineage. Absent otherwise.
    partitions: BTreeMap<ElementId, String>,
    /// Functional or `functional_by` without `complete`: supported values
    /// form a conflict set (§21.11), never opposition.
    conflict: bool,
    /// Slot lines take part in temporal succession (§25.4).
    slot_lines: bool,
    /// For each member, the rivals whose support opposes it: exclusive
    /// values, `complete`, boolean negation (§25).
    opposing: BTreeMap<ElementId, Vec<ElementId>>,
    /// The slot subject's equality keys, for first-person testimony (§21.13).
    subject_keys: Vec<String>,
    /// Which constraint a standing conflict names.
    reason: &'static str,
}

impl Frame {
    fn partition(&self, member: ElementId) -> String {
        self.partitions.get(&member).cloned().unwrap_or_default()
    }
}

/// One eligible, inside support of a candidate, as §21.13's rules read it.
struct Support<'a> {
    contexts: &'a [String],
    /// Stated or observed by the slot's subject itself.
    first_person: bool,
    by_subject: bool,
    observed: bool,
    start_key: &'a str,
}

/// `kip:memory-default` precedence (§21.13): the first rule under which one
/// candidate prevails over every other candidate of the conflict set decides
/// it. `None` when no rule singles one out — the conflict stands.
fn precedence<'a>(
    set: &[usize],
    support_of: impl Fn(usize) -> Vec<Support<'a>>,
) -> Option<(&'static str, usize)> {
    let supports: BTreeMap<usize, Vec<Support<'a>>> =
        set.iter().map(|&i| (i, support_of(i))).collect();
    let strict_superset =
        |a: &[String], b: &[String]| a.len() > b.len() && b.iter().all(|value| a.contains(value));
    let newest = |i: usize| supports[&i].iter().map(|s| s.start_key).max();
    type Rule<'r, 'a> = (
        &'static str,
        &'r dyn Fn(&[Support<'a>], &[Support<'a>]) -> bool,
    );
    let specificity = |a: &[Support<'a>], b: &[Support<'a>]| {
        a.iter()
            .any(|x| b.iter().all(|y| strict_superset(x.contexts, y.contexts)))
    };
    let testimony = |a: &[Support<'a>], b: &[Support<'a>]| {
        a.iter().any(|x| x.first_person) && !b.iter().any(|y| y.by_subject || y.observed)
    };
    let rules: [Rule<'_, 'a>; 2] = [
        ("context_specificity", &specificity),
        ("first_person_testimony", &testimony),
    ];
    for (rule, prevails) in rules {
        let winners: Vec<usize> = set
            .iter()
            .copied()
            .filter(|&a| {
                set.iter()
                    .all(|&b| b == a || prevails(&supports[&a], &supports[&b]))
            })
            .collect();
        if let [winner] = winners[..] {
            return Some((rule, winner));
        }
    }
    // Recency compares start keys — when values were claimed to hold, never
    // when they were recorded (§13.2).
    let winners: Vec<usize> = set
        .iter()
        .copied()
        .filter(|&a| set.iter().all(|&b| b == a || newest(a) > newest(b)))
        .collect();
    match winners[..] {
        [winner] => Some(("recency", winner)),
        _ => None,
    }
}

/// A structural policy counts each eligible root group once (§21.10): no
/// confidence and no trust weight enter the arithmetic.
fn weighed(mut candidate: Candidate, policy: &Policy) -> Candidate {
    if policy.structural {
        candidate.confidence = 1.0;
    }
    candidate
}

/// An Assertion the projection left out, and why.
struct Excluded {
    id: ElementId,
    reason: &'static str,
}

/// The projected belief about one Proposition.
#[derive(Clone)]
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
    pub basis: anda_kip::ProjectionBasis,
    /// Normalized support strength.
    pub support: f64,
    /// Normalized opposition strength.
    pub opposition: f64,
    /// The Assertions on each side, and the ones excluded.
    ledger: Ledger,
    /// The precedence rule that decided this candidate, when one did
    /// (§21.13).
    precedence: Option<Json>,
    /// `leading` recomputed over a slot's final conflict set (§21.11).
    slot_leading: Option<&'static str>,
    /// The policy this ran under.
    policy: Policy,
}

#[derive(Clone, Default)]
struct Ledger {
    unverified_dependency: bool,
    candidate_status: BeliefStatus,
    conflict_refs: Vec<String>,
    conflict_reasons: Vec<String>,
    next_invalid_at: Option<String>,
    supporting: Vec<String>,
    opposing: Vec<String>,
    uncertain: Vec<String>,
    /// Eligible, but indeterminate at the instant (§25.5).
    indeterminate: Vec<String>,
    excluded: Vec<(String, &'static str)>,
    /// §27.2 uncertainty reasons: `temporal_indeterminate`, `outranked`.
    reasons: Vec<&'static str>,
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
        self.projection_json()
    }

    /// The side the policy would favor if forced to choose (§27.2).
    ///
    /// `support` under `accepted`, `opposition` under `rejected`, and under
    /// `contested` the side with more eligible independent roots — this
    /// engine's structural baseline (§21.10) counts corroboration groups and
    /// weighs nothing else — with an exact tie, `uncertain` and
    /// `insufficient` reporting `none`.
    pub fn leading(&self) -> &'static str {
        if let Some(leading) = self.slot_leading {
            return leading;
        }
        match self.status {
            BeliefStatus::Accepted => "support",
            BeliefStatus::Rejected => "opposition",
            BeliefStatus::Contested => {
                let support = self.ledger.support_groups.len();
                let opposition = self.ledger.opposition_groups.len();
                match support.cmp(&opposition) {
                    std::cmp::Ordering::Greater => "support",
                    std::cmp::Ordering::Less => "opposition",
                    std::cmp::Ordering::Equal => "none",
                }
            }
            BeliefStatus::Uncertain | BeliefStatus::Insufficient => "none",
        }
    }

    fn projection_json(&self) -> Json {
        let projection = Projection {
            basis: Some(self.basis.clone()),
            candidate_status: Some(self.ledger.candidate_status),
            slot_status: Some(self.status),
            conflict_refs: self.ledger.conflict_refs.clone(),
            conflict_reasons: self.ledger.conflict_reasons.clone(),
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
            // §27.2: the side the policy would favor if it were forced to
            // choose. Disclosure for a consumer that must act anyway; it
            // never changes `status` (§21.6).
            leading: Some(self.leading().to_string()),
            precedence: self.precedence.clone(),
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
                "indeterminate_assertions": self.ledger.indeterminate,
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
        // A structural policy weighs nothing and outputs no number (§21.10):
        // `score: null`, with no semantics to declare for it.
        let weighted = !self.policy.structural;
        ProjectionSide {
            score: weighted.then_some(score),
            // §27.3: an implementation MUST declare what its scores mean, and
            // MUST NOT present a normalized strength as a calibrated
            // probability. These combine self-reported commitments.
            score_semantics: weighted.then(|| "normalized_support_not_probability".to_string()),
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

    /// Why the answer is as uncertain as it is (§27.2): the machine codes a
    /// caller can act on — `temporal_indeterminate` (§25.5) and `outranked`
    /// (§21.13). The prose behind them is the Epistemic Ledger.
    fn uncertainty_reasons(&self) -> Vec<String> {
        self.ledger.reasons.iter().map(|r| r.to_string()).collect()
    }
}

impl Context<'_> {
    fn require_projection_history(&self, policy: &Policy) -> Result<(), KipError> {
        if policy.trust_version == "unavailable" {
            return Err(KipError::new(
                anda_kip::KipErrorCode::HistoricalSnapshotUnavailable,
                "historical projection control state unavailable",
            ));
        }
        Ok(())
    }

    /// Conservative version identities invalidate on every relevant Space or
    /// authorization change. There is no mutable trust model in this engine.
    pub fn projection_basis(
        &self,
        policy: &Policy,
        at: &str,
        next: Option<String>,
    ) -> anda_kip::ProjectionBasis {
        let digest =
            |v: &Json| crate::schema::contracts::digest(v).expect("validated projection basis");
        let mut authority = self.authority.clone();
        authority.space.seq = 0;
        authority.space.schema_environment_version = 0;
        if let Some(object) = authority.space.policies.as_object_mut() {
            object.remove("_kip_identity_changes");
        }
        anda_kip::ProjectionBasis {
            space_id: self.space.clone(),
            snapshot_seq: self.pinned_seq,
            schema_environment_version: self.env.version,
            identity_version: self.authority.space.policies["_kip_identity_changes"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Json::as_u64)
                .filter(|v| *v <= self.pinned_seq)
                .max()
                .unwrap_or(0),
            policy: anda_kip::ProjectionPolicyVersion {
                id: policy.id.clone(),
                version: digest(&serde_json::json!([
                    policy.version,
                    policy.accept,
                    policy.material,
                    policy.modes,
                    policy.expand_conflicts,
                    policy.unstated_confidence
                ])),
            },
            trust_version: policy.trust_version.clone(),
            authorization_view: digest(&Json::String(format!("{:?}:{:?}", authority, self.auth))),
            context_refs: policy.context_refs.clone(),
            purpose: if policy.purpose.is_empty() {
                if self.auth.purpose.is_empty() {
                    "unspecified".into()
                } else {
                    self.auth.purpose.clone()
                }
            } else {
                policy.purpose.clone()
            },
            risk: if policy.risk.is_empty() {
                if self.auth.risk.is_empty() {
                    "unspecified".into()
                } else {
                    self.auth.risk.clone()
                }
            } else {
                policy.risk.clone()
            },
            valid_at: at.into(),
            next_invalid_at: next,
        }
    }

    pub async fn resolve_projection_context(
        &mut self,
        policy: &mut Policy,
    ) -> Result<(), KipError> {
        let mut refs = Vec::new();
        for reference in &policy.context_refs {
            let id = reference.parse::<ElementId>()?;
            if id.kind != anda_kip::ElementKind::Concept || self.load(id).await?.is_none() {
                return Err(KipError::not_found_or_not_visible(
                    "projection context is unavailable",
                ));
            }
            refs.push(self.canonical_of(id).await?.to_string());
        }
        refs.sort();
        refs.dedup();
        policy.context_refs = refs;
        Ok(())
    }

    /// Projects belief about one Proposition.
    ///
    /// A Proposition in a constrained slot is projected together with its
    /// slot: world time (§25.4) and final belief (§21.11) are properties of
    /// the slot, so a single `BELIEF` and a `BELIEF SLOT` at one basis agree.
    pub async fn project_belief(
        &mut self,
        proposition: ElementId,
        policy: &Policy,
        at: &str,
    ) -> Result<Belief, KipError> {
        self.require_projection_history(policy)?;
        let basis = serde_json::to_string(&(policy, at, self.pinned_seq, self.env.version))
            .expect("policy serializes");
        if let Some(belief) = self.belief_cache.get(&(basis.clone(), proposition)) {
            return Ok(belief.clone());
        }
        let frame = self.frame(proposition).await?;
        let index = frame
            .members
            .iter()
            .position(|member| *member == proposition)
            .unwrap_or_default();
        // A target added explicitly to an active slot (for example an
        // archived Proposition) can change its frame. Share only identical
        // frames, including partitions, opposition and the exact member set.
        let shared: std::collections::BTreeSet<_> = if frame.shareable {
            frame
                .sharing
                .get(&proposition)
                .map(|anchor| {
                    frame
                        .sharing
                        .iter()
                        .filter_map(|(id, group)| (group == anchor).then_some(*id))
                        .collect()
                })
                .unwrap_or_default()
        } else {
            std::collections::BTreeSet::new()
        };
        let frame_key = (basis.clone(), frame);
        let beliefs = if let Some(cached) = self.frame_cache.get(&frame_key) {
            cached.clone()
        } else {
            let projected =
                std::sync::Arc::new(self.project_frame(&frame_key.1, policy, at).await?);
            self.frame_cache.insert(frame_key, projected.clone());
            projected
        };
        for belief in beliefs.iter() {
            if let Some(id) = belief.proposition
                && shared.contains(&id)
            {
                self.belief_cache
                    .insert((basis.clone(), id), belief.clone());
            }
        }
        let belief = beliefs[index].clone();
        self.belief_cache
            .insert((basis, proposition), belief.clone());
        Ok(belief)
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
    pub fn ungrounded_belief(&self, policy: &Policy, at: &str) -> Result<Belief, KipError> {
        self.require_projection_history(policy)?;
        Ok(Belief {
            proposition: None,
            basis: self.projection_basis(policy, at, None),
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
            precedence: None,
            slot_leading: None,
            policy: policy.clone(),
        })
    }

    /// The conflict set of one slot: every Proposition with this subject and
    /// predicate, each projected (§35).
    pub async fn project_slot(
        &mut self,
        subject: &Endpoint,
        predicate_ref: &str,
        policy: &Policy,
        at: &str,
    ) -> Result<Slot, KipError> {
        self.require_projection_history(policy)?;
        let members = self.slot_propositions(subject, predicate_ref).await?;
        let candidates = match members.first() {
            Some(first) => {
                let mut frame = self.frame(*first).await?;
                // An unconstrained slot projects every value on its own.
                if frame.members.len() < members.len() {
                    frame.members = members;
                }
                self.project_frame(&frame, policy, at).await?
            }
            None => Vec::new(),
        };
        let next = candidates
            .iter()
            .filter_map(|b| b.basis.next_invalid_at.clone())
            .min();
        Ok(Slot {
            basis: self.projection_basis(policy, at, next),
            candidates,
            warnings: vec![
                "protected actor trust weights are applied; evidence quality is not automatically graded"
                    .to_string(),
            ],
        })
    }

    /// The Propositions projected together with `target`, and how they
    /// relate: the whole slot when its predicate constrains one (§20.15).
    async fn frame(&mut self, target: ElementId) -> Result<Frame, KipError> {
        let mut frame = Frame {
            members: vec![target],
            ..Default::default()
        };
        let Some(Element::Proposition(row)) = self.load(target).await? else {
            return Ok(frame);
        };
        // A predicate this environment cannot resolve declares nothing.
        let Some(def) = row
            .predicate_ref
            .parse::<crate::schema::SymbolRef>()
            .ok()
            .and_then(|symbol| self.env.predicate_def(&symbol).ok())
            .cloned()
        else {
            return Ok(frame);
        };
        // §20.15, §25.2: values that never conflict on time form no slot.
        if def.temporal_conflict == "none" {
            return Ok(frame);
        }
        let by = def.functional_by.as_deref() == Some("object_type");
        let slot = def.functional || by;
        if !slot && def.exclusive_values.is_empty() && !def.boolean_completeness {
            return Ok(frame);
        }
        let subject = Endpoint::from_json(&row.subject)?;
        frame.members = self.slot_propositions(&subject, &row.predicate_ref).await?;
        frame.shareable = frame.members.contains(&target);
        if !frame.shareable {
            frame.members.push(target);
        }
        frame.subject_keys = self.endpoint_keys(&subject).await?;
        frame.slot_lines = slot;
        // `complete` makes a functional slot's values exclusive: accepting
        // one rejects the others. Without it, competing values are a conflict
        // set and never opposition (§21.11, §25).
        frame.conflict = slot && !def.complete;
        frame.reason = if slot {
            "functional_value"
        } else {
            "exclusive_value"
        };
        let mut objects = BTreeMap::new();
        for member in frame.members.clone() {
            let Some(Element::Proposition(value)) = self.load(member).await? else {
                continue;
            };
            if let Ok(subject) =
                Endpoint::from_json(&self.canonical_endpoint(&value.subject).await?)
            {
                frame
                    .sharing
                    .insert(member, (subject.key(), value.predicate_ref.clone()));
            }
            if by {
                // The partition is the object's Concept Type lineage.
                let partition = match Endpoint::from_json(&value.object) {
                    Ok(Endpoint::Local(id)) => match self.load(id).await? {
                        Some(Element::Concept(concept)) => self
                            .env
                            .lineage(crate::schema::SymbolKind::ConceptType, &concept.schema_ref),
                        _ => String::new(),
                    },
                    _ => String::new(),
                };
                frame.partitions.insert(member, partition);
            }
            objects.insert(member, value.object.clone());
        }
        for (member, object) in &objects {
            let group: Vec<Json> = def
                .exclusive_values
                .iter()
                .find(|group| group.iter().any(|value| same_object(value, object)))
                .cloned()
                .unwrap_or_default();
            // §12.7, §20.15: under `boolean_completeness`, object `false` is
            // the negation of object `true`, so each opposes the other.
            let negation = def
                .boolean_completeness
                .then(|| boolean_object(object).map(|flag| !flag))
                .flatten();
            let rivals: Vec<ElementId> = objects
                .iter()
                .filter(|(other, _)| *other != member)
                .filter(|(other, value)| {
                    (slot && def.complete && frame.partition(**other) == frame.partition(*member))
                        || group.iter().any(|g| same_object(g, value))
                        || negation.is_some_and(|flag| boolean_object(value) == Some(flag))
                })
                .map(|(other, _)| *other)
                .collect();
            if !rivals.is_empty() {
                frame.opposing.insert(*member, rivals);
            }
        }
        Ok(frame)
    }

    /// Projects every member of a frame at one basis.
    ///
    /// Eligibility (stages 4–6), then world time over the eligible set —
    /// succession narrows intervals, and each Assertion is inside, outside or
    /// indeterminate at `at` (§25.4, §25.5) — then candidate status from what
    /// is inside, then the slot stage: a conflict set stands `contested`
    /// unless the policy's precedence rules resolve it (§21.11, §21.13).
    async fn project_frame(
        &mut self,
        frame: &Frame,
        policy: &Policy,
        at: &str,
    ) -> Result<Vec<Belief>, KipError> {
        struct Row {
            timed: world::Timed,
            candidate: Candidate,
            source: AssertionRow,
            mode: String,
            contexts: Vec<String>,
            placement: world::Placement,
        }
        let mut rows: Vec<Row> = Vec::new();
        let mut excluded: BTreeMap<ElementId, Vec<(String, &'static str)>> = BTreeMap::new();
        for member in &frame.members {
            for row in self.assertions_about(*member).await? {
                match self.eligible(&row, policy).await? {
                    Ok(candidate) => {
                        // Lines and precedence compare the actor and the
                        // context set as they are now, merge-resolved: a
                        // context merged after the claim was written is still
                        // the same scope (§25.4).
                        let mut contexts = Vec::with_capacity(row.context_refs.len());
                        for reference in &row.context_refs {
                            let canonical = self.canonical_endpoint(reference).await?;
                            contexts.push(crate::kml::clauses::endpoint_key(&canonical));
                        }
                        contexts.sort();
                        contexts.dedup();
                        let mut timed = world::Timed::new(&row, *member, frame.partition(*member));
                        timed.context = contexts.join("\u{1f}");
                        if let Some(actor) = crate::term::element_reference(&row.asserted_by)
                            && actor.kind == anda_kip::ElementKind::Concept
                        {
                            let canonical = self.canonical_of(actor).await?;
                            timed.actor = crate::term::Endpoint::Local(canonical).key();
                        }
                        rows.push(Row {
                            timed,
                            candidate,
                            mode: row.mode.clone(),
                            contexts,
                            source: row,
                            placement: world::Placement::Outside,
                        })
                    }
                    Err(out) => excluded
                        .entry(*member)
                        .or_default()
                        .push((out.id.to_string(), out.reason)),
                }
            }
        }
        let mut timed: Vec<world::Timed> = rows.iter().map(|r| r.timed.clone()).collect();
        world::succeed(&mut timed, frame.slot_lines);
        let mut next_invalid_at: Option<String> = None;
        for (row, timed) in rows.iter_mut().zip(timed) {
            row.placement = timed.place(at);
            if let Some(next) = timed.boundaries_after(at).min()
                && next_invalid_at.as_deref().is_none_or(|old| next < old)
            {
                next_invalid_at = Some(next.to_string());
            }
            row.timed = timed;
        }

        let mut beliefs = Vec::with_capacity(frame.members.len());
        for member in &frame.members {
            let mut ledger = Ledger {
                warnings: vec![
                    // Not a caveat about this answer in particular: it is what
                    // the engine structurally cannot do yet, and an answer that
                    // read as trust-weighted when it is not would be worse
                    // than none.
                    "protected actor trust weights are applied; evidence quality is not automatically graded"
                        .to_string(),
                ],
                excluded: excluded.remove(member).unwrap_or_default(),
                next_invalid_at: next_invalid_at.clone(),
                ..Default::default()
            };
            let mut candidates = Vec::new();
            for row in rows.iter().filter(|r| r.timed.proposition == *member) {
                let id = row.candidate.id.to_string();
                match row.placement {
                    world::Placement::Outside => {
                        ledger.excluded.push((id, "outside_valid_time"));
                        continue;
                    }
                    world::Placement::Indeterminate => {
                        // Material, but it cannot decide a status (§25.5).
                        ledger.indeterminate.push(id);
                        continue;
                    }
                    world::Placement::Inside => {}
                }
                if row.source.mode == "inferred"
                    || self
                        .under_identity_review(crate::id::ElementId::new(
                            anda_kip::ElementKind::Assertion,
                            row.source._id,
                        ))
                        .await?
                {
                    let checked = self
                        .dependency_validity(
                            &Element::Assertion(Box::new(row.source.clone())),
                            policy,
                            at,
                        )
                        .await?;
                    if checked["action_eligible"] != true {
                        ledger.unverified_dependency = true;
                    }
                    if let Some(next) = checked["basis"]["next_invalid_at"].as_str()
                        && ledger
                            .next_invalid_at
                            .as_ref()
                            .is_none_or(|old| next < old.as_str())
                    {
                        ledger.next_invalid_at = Some(next.to_string());
                    }
                }
                match row.candidate.stance.as_str() {
                    "support" => ledger.supporting.push(id),
                    "reject" => ledger.opposing.push(id),
                    // An `uncertain` stance is material — the actor engaged
                    // with the question — but it takes no side, so it can move
                    // the answer off `insufficient` without moving it toward
                    // either pole.
                    _ => ledger.uncertain.push(id),
                }
                candidates.push(weighed(row.candidate.clone(), policy));
            }
            let (local_support, local_groups) = aggregate(&candidates, false);
            let (local_opposition, opposing_groups) = aggregate(&candidates, true);
            ledger.support_groups = local_groups;
            ledger.opposition_groups = opposing_groups;
            ledger.candidate_status = classify(local_support, local_opposition, &ledger, policy);
            // A candidate whose only material at the instant is indeterminate
            // is `uncertain`, and says why (§25.5). Indeterminate rows beside
            // material that is inside decide nothing and are only listed.
            if ledger.candidate_status == BeliefStatus::Insufficient
                && !ledger.indeterminate.is_empty()
            {
                ledger.candidate_status = BeliefStatus::Uncertain;
                ledger.reasons.push("temporal_indeterminate");
            }
            // Exclusive values (§25, `complete`, exclusive groups, boolean
            // negation): a rival's support opposes this value.
            if policy.expand_conflicts
                && let Some(rivals) = frame.opposing.get(member)
            {
                for rival in rivals {
                    let rival_candidates: Vec<Candidate> = rows
                        .iter()
                        .filter(|r| {
                            r.timed.proposition == *rival
                                && r.placement == world::Placement::Inside
                                && r.candidate.stance == "support"
                        })
                        .map(|r| weighed(r.candidate.clone(), policy))
                        .collect();
                    let (rival_support, _) = aggregate(&rival_candidates, false);
                    if local_support >= policy.material && rival_support >= policy.material {
                        ledger.conflict_refs.push(rival.to_string());
                    }
                    for mut candidate in rival_candidates {
                        candidate.opposes_target = true;
                        ledger.opposing.push(candidate.id.to_string());
                        candidates.push(candidate);
                    }
                }
            }
            let (support, support_groups) = aggregate(&candidates, false);
            let (opposition, opposition_groups) = aggregate(&candidates, true);
            ledger.support_groups = support_groups;
            ledger.opposition_groups = opposition_groups;
            let mut status = if !ledger.conflict_refs.is_empty() {
                ledger.conflict_reasons.push("exclusive_value".into());
                BeliefStatus::Contested
            } else if ledger.support_groups.is_empty()
                && ledger.opposition_groups.is_empty()
                && ledger.uncertain.is_empty()
            {
                ledger.candidate_status
            } else {
                classify(support, opposition, &ledger, policy)
            };
            if ledger.unverified_dependency && status == BeliefStatus::Accepted {
                status = BeliefStatus::Uncertain;
                ledger
                    .warnings
                    .push("inferred support has no verified recursive dependency basis".into());
            }
            beliefs.push(Belief {
                proposition: Some(*member),
                status,
                basis: self.projection_basis(policy, at, ledger.next_invalid_at.clone()),
                support,
                opposition,
                ledger,
                precedence: None,
                slot_leading: None,
                policy: policy.clone(),
            });
        }

        // The slot stage (§21.11): materially supported values of one
        // functional slot — per partition under `functional_by` — conflict.
        if frame.conflict {
            let mut partitions: Vec<String> = frame
                .members
                .iter()
                .map(|member| frame.partition(*member))
                .collect();
            partitions.sort();
            partitions.dedup();
            for partition in partitions {
                let supported: Vec<usize> = (0..beliefs.len())
                    // Materially supported by what is inside at the basis,
                    // and not held back by an unverified dependency.
                    .filter(|&i| {
                        frame.partition(frame.members[i]) == partition
                            && beliefs[i].support >= policy.material
                            && !beliefs[i].ledger.unverified_dependency
                    })
                    .collect();
                if supported.len() < 2 {
                    continue;
                }
                let support_rows = |i: usize| -> Vec<&Row> {
                    rows.iter()
                        .filter(|r| {
                            r.timed.proposition == frame.members[i]
                                && r.placement == world::Placement::Inside
                                && r.candidate.stance == "support"
                        })
                        .collect()
                };
                let winner = if policy.precedence {
                    precedence(&supported, |i| {
                        support_rows(i)
                            .into_iter()
                            .map(|r| Support {
                                contexts: &r.contexts,
                                first_person: frame.subject_keys.contains(&r.timed.actor)
                                    && matches!(r.mode.as_str(), "stated" | "observed"),
                                by_subject: frame.subject_keys.contains(&r.timed.actor),
                                observed: r.mode == "observed",
                                start_key: &r.timed.start_key,
                            })
                            .collect()
                    })
                } else {
                    None
                };
                let ids: Vec<String> = supported
                    .iter()
                    .map(|&i| frame.members[i].to_string())
                    .collect();
                // Leading compares eligible independent roots (§27.2), never
                // the numeric support a structural policy does not have.
                let roots: Vec<usize> = beliefs
                    .iter()
                    .map(|b| b.ledger.support_groups.len())
                    .collect();
                for &i in &supported {
                    let me = frame.members[i].to_string();
                    match winner {
                        Some((rule, won)) if won == i => {
                            beliefs[i].precedence = Some(serde_json::json!({
                                "rule": rule,
                                "prevailed_over": ids.iter().filter(|id| **id != me).collect::<Vec<_>>(),
                            }));
                        }
                        Some((rule, won)) => {
                            let belief = &mut beliefs[i];
                            belief.status = BeliefStatus::Uncertain;
                            belief.ledger.reasons.push("outranked");
                            belief.precedence = Some(serde_json::json!({
                                "rule": rule,
                                "outranked_by": frame.members[won].to_string(),
                            }));
                        }
                        None => {
                            // Leading over the final conflict set: a tie
                            // between the values is `none` (§21.11).
                            let best_rival = supported
                                .iter()
                                .filter(|&&j| j != i)
                                .map(|&j| roots[j])
                                .max()
                                .unwrap_or(0);
                            let belief = &mut beliefs[i];
                            belief.slot_leading = Some(match roots[i].cmp(&best_rival) {
                                std::cmp::Ordering::Greater => "support",
                                std::cmp::Ordering::Less => "opposition",
                                std::cmp::Ordering::Equal => "none",
                            });
                            belief.status = BeliefStatus::Contested;
                            belief.ledger.conflict_refs =
                                ids.iter().filter(|id| **id != me).cloned().collect();
                            belief.ledger.conflict_reasons = vec![frame.reason.to_string()];
                        }
                    }
                }
            }
        }
        Ok(beliefs)
    }

    /// Stages 4–6: lifecycle, visibility, context, Evidence and mode
    /// eligibility. World time is decided over the eligible set afterwards,
    /// because succession needs every eligible Assertion of the slot.
    async fn eligible(
        &mut self,
        row: &AssertionRow,
        policy: &Policy,
    ) -> Result<Result<Candidate, Excluded>, KipError> {
        let id = ElementId::new(anda_kip::ElementKind::Assertion, row._id);
        let reject = |reason| Ok(Err(Excluded { id, reason }));

        // Stage 4 — lifecycle (§59). A retracted claim was withdrawn and a
        // superseded one was replaced; both stay on record for explanation.
        // `expired` is computed from world time and never stored (§14.3); a
        // row written by an earlier draft that stored it is read as active.
        match row.status.as_str() {
            "active" | "expired" => {}
            "retracted" => return reject("retracted"),
            "superseded" => return reject("superseded"),
            _ => return reject("invalid_schema"),
        }
        if row.state != crate::store::rows::state::ACTIVE {
            return reject("not_visible");
        }
        // §57.8: an extraction a recording repair invalidated is not a claim
        // anyone made; it keeps its payload and history, and stays out.
        if crate::repair::repair_ref(&row.governance).is_some() {
            return reject("recording_invalidated");
        }

        for reference in &row.context_refs {
            let canonical = self.canonical_endpoint(reference).await?;
            let name = canonical
                .get("id")
                .and_then(Json::as_str)
                .or_else(|| canonical.as_str());
            if !name.is_some_and(|r| policy.context_refs.iter().any(|v| v == r)) {
                return reject("context_mismatch");
            }
        }
        for evidence in &row.evidence_ids {
            let Ok(eid) = evidence.parse::<ElementId>() else {
                return reject("evidence_unavailable");
            };
            match self.load(eid).await? {
                Some(Element::Evidence(root)) if root.status == "corrected" => {
                    return reject("corrected_evidence");
                }
                Some(Element::Evidence(_)) => {}
                _ => return reject("evidence_unavailable"),
            }
        }
        // Stage 6 — mode (§61).
        let mode: Option<AssertionMode> =
            serde_json::from_value(Json::String(row.mode.clone())).ok();
        if !policy.admits(mode) {
            return reject(policy.mode_exclusion(mode));
        }

        let actor_ref = row
            .asserted_by
            .as_str()
            .or_else(|| row.asserted_by["id"].as_str())
            .unwrap_or("");
        let global_trust = policy
            .trust_weights
            .get(actor_ref)
            .copied()
            .unwrap_or(policy.default_trust_weight);
        let trust = if policy.contextual_trust_rules.is_empty() {
            global_trust
        } else {
            let Some(Element::Proposition(proposition)) =
                self.load(row.proposition_id.parse()?).await?
            else {
                return reject("proposition_unavailable");
            };
            crate::trust::weight(
                &policy.contextual_trust_rules,
                global_trust,
                actor_ref,
                &proposition.predicate_ref,
                &policy.context_refs,
            )?
        };
        Ok(Ok(Candidate {
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
            confidence: (if row.confidence < 0.0 {
                policy.unstated_confidence
            } else {
                row.confidence
            }) * trust,
            opposes_target: false,
        }))
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

    /// Every active Proposition in one `(subject, predicate)` slot.
    ///
    /// The slot is canonical on both coordinates (§12.3, §20.14): the subject
    /// is its merge class, so a slot named by the surviving identity holds
    /// the tuples recorded on what was merged into it, and the predicate is
    /// its lineage, so a `BELIEF SLOT` over a later package version sees every
    /// Assertion the slot ever gathered.
    pub async fn slot_propositions(
        &mut self,
        subject: &Endpoint,
        predicate_ref: &str,
    ) -> Result<Vec<ElementId>, KipError> {
        let subject_keys = self.endpoint_keys(subject).await?;
        let predicate = self.symbol_filter(
            anda_kip::ElementKind::Proposition,
            "predicate_ref",
            &[predicate_ref.to_string()],
        )?;
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
                        anda_db_schema::Fv::Text(crate::store::rows::state::ACTIVE.into()),
                    )),
                    Box::new(crate::store::key_filter("subject_key", &subject_keys)),
                    Box::new(predicate),
                ])),
            )
            .await?;
        self.charge(ids.len())?;

        // The predicate index was ranged over a lineage, so the symbol is
        // settled here; at a past coordinate the index could not narrow at
        // all, so the whole slot is matched against the historical rows.
        let historical = self.is_historical();
        let mut slot = Vec::new();
        for id in ids {
            if let Some(Element::Proposition(row)) = self.load(id).await?
                && self.env.same_lineage(
                    crate::schema::SymbolKind::PredicateType,
                    &row.predicate_ref,
                    predicate_ref,
                )
                && (!historical
                    || (row.state == "active" && subject_keys.contains(&row.subject_key)))
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

/// The boolean a Proposition object carries, when it is a boolean Literal.
fn boolean_object(object: &Json) -> Option<bool> {
    match Endpoint::from_json(object) {
        Ok(Endpoint::Literal(literal)) => literal.value.as_bool(),
        _ => None,
    }
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
///
/// The slot's `basis` carries the policy, `valid_at` and snapshot it *ran*
/// under (§47.3), so an empty slot still says what it was computed against.
/// Which side leads is each candidate projection's own `leading`.
pub fn slot_to_json(subject: &Endpoint, predicate: &str, slot: &Slot) -> Json {
    let beliefs = &slot.candidates;
    let accepted: Vec<String> = beliefs
        .iter()
        .filter(|belief| belief.status == BeliefStatus::Accepted)
        .filter_map(|belief| belief.proposition.map(|id| id.to_string()))
        .collect();
    let contested = beliefs
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
    let mut reasons: Vec<String> = beliefs
        .iter()
        .flat_map(Belief::uncertainty_reasons)
        .collect();
    reasons.sort();
    reasons.dedup();

    serde_json::json!({
        "status": status,
        "basis": slot.basis,
        // The reference shape §8 fixes, never the engine's internal endpoint
        // key: a caller cannot feed `id\u001fC-1` back into anything, and a
        // storage key on the wire is a detail that becomes a contract the
        // moment somebody parses it.
        "subject": subject.to_json(),
        "predicate_ref": predicate,
        "accepted_values": accepted,
        "candidate_projections": beliefs.iter().map(Belief::to_json).collect::<Vec<_>>(),
        "uncertainty": {
            "level": match status {
                BeliefStatus::Insufficient => "total",
                BeliefStatus::Contested | BeliefStatus::Uncertain => "high",
                _ => "low",
            },
            "reasons": reasons,
        },
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
pub struct Slot {
    /// The coordinates it ran under, reported whether or not any candidate
    /// exists.
    pub basis: anda_kip::ProjectionBasis,
    /// Every Proposition competing for the slot, each projected.
    pub candidates: Vec<Belief>,
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
