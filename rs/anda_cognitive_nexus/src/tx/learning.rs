//! Standard CognitiveMemory lifecycle validation. No model output or mutable
//! tally can confer local standing; the final transaction is checked as a unit.
use super::cognitive::facet;
use super::*;
use anda_kip::cognitive::{EvaluationInput, EvaluationPolicy, EvaluationSamples};
use serde_json::json;

const PROFILE: &str = "kip://profiles/cognitive-memory@2.1.0/";
fn fail(message: &str) -> KipError {
    KipError::constraint_violation(message)
}
fn refs(value: &Json) -> Vec<String> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str().or_else(|| v["id"].as_str()).map(str::to_string))
        .collect()
}
fn same_json(a: &Json, b: &Json) -> bool {
    anda_kip::canonical_json(a) == anda_kip::canonical_json(b)
}
fn same_refs(a: &Json, b: &Json) -> bool {
    let mut a = refs(a);
    let mut b = refs(b);
    a.sort();
    b.sort();
    a == b
}
fn edge(element: &Element, name: &str) -> Vec<String> {
    element
        .structural()
        .iter()
        .find(|(key, _)| key.ends_with(&format!("/{name}")))
        .map(|(_, v)| refs(v))
        .unwrap_or_default()
}
fn is_type(element: &Element, name: &str) -> bool {
    matches!(element,Element::Concept(row) if row.schema_ref==format!("{PROFILE}{name}"))
}
fn record<'a>(element: &'a Element, name: &str) -> Result<&'a Json, KipError> {
    element
        .facets()
        .get(&format!("{PROFILE}{name}"))
        .ok_or_else(|| fail("required standard record is missing"))
}

impl Transaction {
    pub(super) fn require_changed_guards(
        &self,
        id: ElementId,
        staged: &Staged,
    ) -> Result<(), KipError> {
        let Some(before) = &staged.before else {
            return Ok(());
        };
        let guards = self.guarded.get(&id);
        if guards.is_some_and(|g| g.contains("version")) {
            return Ok(());
        }
        let diff = planes::diff(before, &staged.row);
        let mut required = vec![];
        if diff.attributes {
            required.push("attributes".to_string());
        }
        if diff.structural {
            required.push("structural".to_string());
        }
        if diff.retention {
            required.push("retention".to_string());
        }
        required.extend(diff.facets.iter().map(|name| format!("facets.{name}")));
        if required
            .iter()
            .any(|name| !guards.is_some_and(|g| g.contains(name)))
        {
            return Err(KipError::version_conflict(
                "every changed lifecycle/cache plane requires an explicit version guard",
            ));
        }
        Ok(())
    }

    async fn referenced_record(
        &self,
        reference: &str,
        name: &str,
    ) -> Result<(Element, Json), KipError> {
        let row = self.final_element(reference.parse()?).await?;
        if row.state() != state::ACTIVE && !self.staged.get(&row.id()).is_some_and(|s| s.is_new) {
            return Err(fail("contract reference is not active"));
        }
        let value = record(&row, name)?.clone();
        Ok((row, value))
    }

    async fn require_revision(&self, reference: &str) -> Result<Element, KipError> {
        let row = self.final_element(reference.parse()?).await?;
        if !is_type(&row, "SkillRevision") {
            return Err(fail("applied behavior must name an exact SkillRevision"));
        }
        Ok(row)
    }

    fn origin_principal(&self, row: &Element) -> String {
        if row.envelope().origin.get("import").is_some()
            || self.staged.get(&row.id()).is_some_and(|s| s.is_new)
                && self.cx.origin.get("import").is_some()
        {
            String::new()
        } else if self.staged.get(&row.id()).is_some_and(|s| s.is_new) {
            self.auth.principal_id.clone()
        } else {
            row.envelope().origin["principal_id"]
                .as_str()
                .unwrap_or("")
                .to_string()
        }
    }

    async fn learning_universe(&self, kind: ElementKind) -> Result<Vec<Element>, KipError> {
        let ids = self
            .store
            .elements(kind)
            .query_all_ids(crate::store::eq_field(
                "space",
                anda_db_schema::Fv::Text(self.cx.space.clone()),
            ))
            .await
            .map_err(db_error)?;
        let mut rows = BTreeMap::new();
        for id in ids {
            let id = ElementId::new(kind, id);
            rows.insert(id, self.store.get_element(id).await?);
        }
        for (id, staged) in &self.staged {
            if id.kind == kind {
                rows.insert(*id, staged.row.clone());
            }
        }
        Ok(rows.into_values().collect())
    }

    pub(crate) async fn validate_learning(&self) -> Result<(), KipError> {
        if self.cx.origin.get("import").is_some() {
            return Ok(());
        }
        let pending: Vec<_> = self
            .staged
            .iter()
            .filter(|(_, s)| s.changed && s.op != ChangeOp::Purge)
            .collect();
        if !pending.iter().any(|(_, s)| {
            is_type(&s.row, "Skill")
                || is_type(&s.row, "SkillRevision")
                || s.row.facets().keys().any(|k| {
                    k.starts_with(PROFILE)
                        && [
                            "DecisionRecord",
                            "AttemptRecord",
                            "OutcomeRecord",
                            "TrialRecord",
                            "EvaluationRecord",
                            "TrialState",
                            "GradingState",
                        ]
                        .iter()
                        .any(|n| k.ends_with(&format!("/{n}")))
                })
        }) {
            return Ok(());
        }
        let activities = self.learning_universe(ElementKind::Activity).await?;
        let evidence = self.learning_universe(ElementKind::Evidence).await?;
        let mut attempts = BTreeSet::new();
        let mut observations = BTreeSet::new();
        for row in &activities {
            if let Ok(value) = record(row, "AttemptRecord")
                && !attempts.insert(value["attempt_id"].as_str().unwrap_or(""))
            {
                return Err(fail("attempt_id must be Space-unique"));
            }
        }
        for row in &evidence {
            if let Ok(value) = record(row, "OutcomeRecord")
                && !observations.insert(value["observation_key"].as_str().unwrap_or(""))
            {
                return Err(fail("observation_key must deduplicate source events"));
            }
        }
        for (id, staged) in pending {
            let row = &staged.row;
            if is_type(row, "SkillRevision") {
                let families = edge(row, "revision_of");
                if families.len() != 1
                    || !is_type(&self.final_element(families[0].parse()?).await?, "Skill")
                {
                    return Err(fail("SkillRevision requires one revision_of Skill"));
                }
                if staged
                    .before
                    .as_ref()
                    .is_some_and(|old| edge(old, "revision_of") != families)
                {
                    return Err(KipError::new(
                        KipErrorCode::ImmutableField,
                        "a revision cannot change its family",
                    ));
                }
                // Revisions are immutable behavior. Their family may point at a newer revision.
            }
            if is_type(row, "Skill") {
                if activities.iter().any(|a|self.staged.get(&a.id()).is_some_and(|s|s.is_new) && record(a,"EvaluationRecord").is_ok() && matches!(a,Element::Activity(a) if a.outputs.iter().any(|r|r.as_str().or_else(||r["id"].as_str())==Some(id.to_string().as_str())))) {self.require_changed_guards(*id,staged)?;}

                let revisions = edge(row, "current_revision");
                if revisions.len() != 1 {
                    return Err(fail("Skill requires exactly one current_revision"));
                }
                let revision = self.require_revision(&revisions[0]).await?;
                if edge(&revision, "revision_of") != vec![id.to_string()] {
                    return Err(fail(
                        "current_revision and revision_of must be bidirectional",
                    ));
                }
                let view = crate::view::render(row);
                let status = view["attributes"]["status"].as_str().unwrap_or("");
                let changed_revision = staged
                    .before
                    .as_ref()
                    .is_none_or(|before| edge(before, "current_revision") != revisions);
                if changed_revision {
                    self.require_changed_guards(*id, staged)?;
                    if status != "proposed"
                        || facet(row, "TrialState").is_some()
                        || facet(row, "GradingState").is_some()
                    {
                        return Err(fail(
                            "selecting new behavior resets standing to proposed and clears current trial and grade",
                        ));
                    }
                } else {
                    let before = staged.before.as_ref().unwrap();
                    let old = crate::view::render(before);
                    let lifecycle = old["attributes"]["status"] != view["attributes"]["status"]
                        || facet(before, "TrialState") != facet(row, "TrialState")
                        || facet(before, "GradingState") != facet(row, "GradingState");
                    if lifecycle {
                        self.require_changed_guards(*id, staged)?;
                        let eval=activities.iter().find(|a| self.staged.get(&a.id()).is_some_and(|s|s.changed && s.is_new) && record(a,"EvaluationRecord").is_ok_and(|e| e["from_status"]==old["attributes"]["status"] && e["to_status"]==view["attributes"]["status"] && refs(&e["revision_refs"]).contains(&revisions[0])) && matches!(a,Element::Activity(a) if a.output_keys.iter().any(|k|k.contains(&id.to_string())) || a.outputs.iter().any(|r|r["id"]==id.to_string() || r.as_str()==Some(id.to_string().as_str())))).ok_or_else(||fail("lifecycle and caches require a new validated EvaluationRecord in the same transaction"))?;
                        let evaluation = record(eval, "EvaluationRecord")?;
                        if let Some(grade) = facet(row, "GradingState") {
                            if grade["revision_ref"] != revisions[0]
                                || grade["evaluation_ref"] != eval.id().to_string()
                            {
                                return Err(fail(
                                    "grade cache must bind the exact revision and evaluation",
                                ));
                            }
                            let mut success = 0u64;
                            let mut partial = 0u64;
                            for outcome_ref in refs(&evaluation["outcome_refs"]) {
                                let (_, outcome) = self
                                    .referenced_record(&outcome_ref, "OutcomeRecord")
                                    .await?;
                                if outcome["outcome_status"] == "success" {
                                    success += 1;
                                } else if outcome["outcome_status"] == "partial" {
                                    partial += 1;
                                }
                            }
                            let graded = refs(&evaluation["attempt_refs"]).len() as u64;
                            for (key, expected) in [
                                ("success_count", success),
                                ("failure_count", graded.saturating_sub(success + partial)),
                                ("graded_count", graded),
                            ] {
                                if let Some(actual) = grade.get(key)
                                    && actual.as_u64() != Some(expected)
                                {
                                    return Err(fail(
                                        "grade cache counts must match independent attempts in the evaluation",
                                    ));
                                }
                            }
                        }
                        if let Some(trial) = facet(row, "TrialState")
                            && (trial["revision_ref"] != revisions[0]
                                || trial["trial_ref"] != evaluation["trial_ref"])
                        {
                            return Err(fail(
                                "trial cache must bind the evaluation trial and revision",
                            ));
                        }
                    }
                }
            }
            if let Ok(decision) = record(row, "DecisionRecord") {
                if let Some(contract) = facet(row, "DependencyBasis")
                    && (!same_json(&contract["policy_basis"], &decision["basis"])
                        || contract["basis_seq"] != decision["basis"]["snapshot_seq"])
                {
                    return Err(fail(
                        "DecisionRecord and DependencyBasis must describe the same read basis",
                    ));
                }
                for name in ["retrieved_refs", "used_refs"] {
                    for reference in refs(&decision[name]) {
                        self.final_element(reference.parse()?).await?;
                    }
                }
                let Element::Activity(activity) = row else {
                    return Err(fail("DecisionRecord needs Activity"));
                };
                for reference in refs(&decision["applied_revisions"]) {
                    self.require_revision(&reference).await?;
                    if !activity.inputs.iter().any(|r| {
                        r.as_str().or_else(|| r["id"].as_str()) == Some(reference.as_str())
                    }) {
                        return Err(fail("applied revisions must occur in decision inputs"));
                    }
                }
            }
            if let Ok(attempt) = record(row, "AttemptRecord") {
                let (decision_row, decision) = self
                    .referenced_record(
                        attempt["decision_ref"].as_str().unwrap_or(""),
                        "DecisionRecord",
                    )
                    .await?;
                if !same_refs(
                    &attempt["applied_revisions"],
                    &decision["applied_revisions"],
                ) || decision["decision"] != "act"
                {
                    return Err(fail(
                        "attempt must bind the acting decision's exact revision bundle",
                    ));
                }
                let started = crate::time::normalize(
                    attempt["started_at"].as_str().unwrap_or(""),
                    "attempt started_at",
                )?;
                if started > self.cx.at
                    || (!self
                        .staged
                        .get(&decision_row.id())
                        .is_some_and(|s| s.is_new)
                        && started.as_str() < decision_row.envelope().created_at.as_str())
                {
                    return Err(fail(
                        "attempt must follow its decision and cannot be future-dated",
                    ));
                }
                self.store
                    .artifact_value(&self.cx.space, &attempt["selection_policy"])
                    .await?;
                if let Some(trial_ref) = attempt["trial_ref"].as_str() {
                    let (trial_row, trial) =
                        self.referenced_record(trial_ref, "TrialRecord").await?;
                    if !same_refs(&attempt["applied_revisions"], &trial["revision_refs"])
                        || *trial_row.envelope().seq >= *decision_row.envelope().seq
                            && !self
                                .staged
                                .get(&decision_row.id())
                                .is_some_and(|s| s.is_new)
                        || self.staged.get(&trial_row.id()).is_some_and(|s| s.is_new)
                    {
                        return Err(fail(
                            "trial must precede the decision; attempts cannot be enrolled retrospectively",
                        ));
                    }
                    if attempt["environment_digest"] != trial["comparability"]["environment_digest"]
                    {
                        return Err(fail("attempt environment is outside its trial"));
                    }
                }
            }
            if let Ok(outcome) = record(row, "OutcomeRecord")
                && let Some(attempt_ref) = outcome["attempt_ref"].as_str()
            {
                let (attempt_row, attempt) =
                    self.referenced_record(attempt_ref, "AttemptRecord").await?;
                if self.staged.get(&attempt_row.id()).is_some_and(|s| s.is_new) {
                    return Err(fail(
                        "attempt must be committed before observing its outcome",
                    ));
                }
                let Element::Evidence(evidence) = row else {
                    return Err(fail("OutcomeRecord needs Evidence"));
                };
                if evidence.observed_at.as_str() < attempt["started_at"].as_str().unwrap_or("") {
                    return Err(fail("observation cannot predate its attempt"));
                }
                let observation = if evidence.generated_by.is_empty() {
                    activities.iter().find(|candidate|matches!(candidate,Element::Activity(a) if a.activity_class=="outcome_observation" && a.status=="completed" && a.outputs.iter().any(|r|r.as_str().or_else(||r["id"].as_str())==Some(id.to_string().as_str())))).cloned().ok_or_else(||fail("instrument outcome needs outcome_observation provenance"))?
                } else {
                    self.final_element(evidence.generated_by.parse()?).await?
                };
                let Element::Activity(activity) = &observation else {
                    return Err(fail(
                        "instrument outcome needs outcome_observation provenance",
                    ));
                };
                if activity.activity_class != "outcome_observation"
                    || activity.status != "completed"
                    || !activity
                        .inputs
                        .iter()
                        .any(|r| r.as_str().or_else(|| r["id"].as_str()) == Some(attempt_ref))
                    || !activity.inputs.iter().any(|r| {
                        r.as_str().or_else(|| r["id"].as_str()) == attempt["decision_ref"].as_str()
                    })
                {
                    return Err(fail(
                        "outcome observation must link its attempt and decision",
                    ));
                }
                if !(staged.is_new && self.staged.get(&observation.id()).is_some_and(|s| s.is_new))
                    && activity.created_tx != evidence.created_tx
                {
                    return Err(fail(
                        "an observation cannot be retrospectively attached to an outcome",
                    ));
                }
            }
            if let Ok(trial) = record(row, "TrialRecord")
                && staged
                    .before
                    .as_ref()
                    .is_none_or(|old| facet(old, "TrialRecord").is_none())
            {
                for reference in refs(&trial["revision_refs"]) {
                    self.require_revision(&reference).await?;
                }
                self.validate_trial(trial, row).await?;
            }
            if let Ok(evaluation) = record(row, "EvaluationRecord")
                && staged
                    .before
                    .as_ref()
                    .is_none_or(|old| facet(old, "EvaluationRecord").is_none())
            {
                self.validate_evaluation(evaluation, row, &activities, &evidence)
                    .await?;
            }
        }
        Ok(())
    }

    async fn evaluation_policy(&self, pin: &Json, seq: u64) -> Result<EvaluationPolicy, KipError> {
        let key = format!("evaluation_policy/{}", pin["id"].as_str().unwrap_or(""));
        let row = self
            .store
            .control_at(&self.cx.space, &key, seq)
            .await?
            .ok_or_else(|| fail("protected evaluation policy unavailable"))?;
        if row.value["version"] != pin["version"]
            || crate::schema::contracts::digest(&row.value)?.as_str()
                != pin["content_digest"].as_str().unwrap_or("")
        {
            return Err(fail("evaluation policy pin does not match protected bytes"));
        }
        serde_json::from_value(row.value).map_err(|e| fail(&e.to_string()))
    }

    async fn validate_trial(&self, trial: &Json, row: &Element) -> Result<(), KipError> {
        let policy = self
            .evaluation_policy(&trial["evaluation_policy"], u64::MAX)
            .await?;
        if !policy.allowed_rules.contains(
            &trial["rule"]["content_digest"]
                .as_str()
                .unwrap_or("")
                .into(),
        ) || !policy.allowed_parameters.contains(
            &trial["parameters"]["content_digest"]
                .as_str()
                .unwrap_or("")
                .into(),
        ) || trial["comparability"]["observer_control_digest"] != policy.observer_control_digest
        {
            return Err(fail(
                "trial rule, parameters or observer control are not allowed by policy",
            ));
        }
        if trial["quota"].as_u64().unwrap_or(0) < policy.minimum_independent_attempts {
            return Err(fail("trial quota is below policy minimum"));
        }
        if !self
            .store
            .evaluation_rules
            .supports(trial["rule"]["content_digest"].as_str().unwrap_or(""))
        {
            return Err(KipError::unsupported_capability(
                "the pinned trial rule has no trusted host evaluator",
            ));
        }
        let rule = self
            .store
            .artifact_value(&self.cx.space, &trial["rule"])
            .await?;
        let parameters = self
            .store
            .artifact_value(&self.cx.space, &trial["parameters"])
            .await?;
        let replay = self
            .store
            .artifact_value(&self.cx.space, &trial["replay_artifact"])
            .await?;
        if !same_json(&replay["rule"], &rule)
            || !same_json(&replay["parameters"], &parameters)
            || !same_json(&replay["basis"], &trial["basis"])
        {
            return Err(fail(
                "trial replay must retain exact rule, parameters and basis",
            ));
        }
        for reference in refs(&trial["baseline_attempt_refs"]) {
            let (attempt, _) = self.referenced_record(&reference, "AttemptRecord").await?;
            if self.staged.get(&attempt.id()).is_some_and(|s| s.is_new)
                || *attempt.envelope().seq >= *row.envelope().seq
                    && !self.staged.get(&row.id()).is_some_and(|s| s.is_new)
            {
                return Err(fail("baseline attempts must precede trial assignment"));
            }
        }
        for reference in refs(&trial["baseline_outcome_refs"]) {
            self.referenced_record(&reference, "OutcomeRecord").await?;
        }
        let mut material = refs(&trial["revision_refs"]);
        material.extend(refs(&trial["baseline_attempt_refs"]));
        material.extend(refs(&trial["baseline_outcome_refs"]));
        self.store
            .require_artifact_material(&self.cx.space, &trial["replay_artifact"], &material)
            .await?;
        let mut baseline_attempts = Map::new();
        let mut baseline_outcomes = Map::new();
        for reference in refs(&trial["baseline_attempt_refs"]) {
            let (row, record) = self.referenced_record(&reference, "AttemptRecord").await?;
            baseline_attempts.insert(
                reference,
                json!({"record":record,"principal_id":self.origin_principal(&row)}),
            );
        }
        for reference in refs(&trial["baseline_outcome_refs"]) {
            let (row, record) = self.referenced_record(&reference, "OutcomeRecord").await?;
            if let Element::Evidence(e) = &row {
                baseline_outcomes.insert(reference,json!({"record":record,"status":e.status,"corrected_by":e.corrected_by,"principal_id":self.origin_principal(&row),"observed_at":e.observed_at}));
            }
        }
        if !same_json(
            &replay["baseline_attempts"],
            &Json::Object(baseline_attempts),
        ) || !same_json(
            &replay["baseline_outcomes"],
            &Json::Object(baseline_outcomes),
        ) {
            return Err(fail(
                "trial replay must retain exact predeclared baseline inputs",
            ));
        }
        Ok(())
    }

    async fn validate_evaluation(
        &self,
        evaluation: &Json,
        row: &Element,
        activities: &[Element],
        evidence: &[Element],
    ) -> Result<(), KipError> {
        if crate::time::normalize(
            evaluation["cutoff"].as_str().unwrap_or(""),
            "evaluation cutoff",
        )? > self.cx.at
        {
            return Err(fail("evaluation cutoff cannot be in the future"));
        }
        let from = evaluation["from_status"].as_str().unwrap_or("");
        let to = evaluation["to_status"].as_str().unwrap_or("");
        let promotion = from == "trialed" && to == "adopted";
        for reference in refs(&evaluation["revision_refs"]) {
            let revision = self.require_revision(&reference).await?;
            for family_ref in edge(&revision, "revision_of") {
                let id = family_ref.parse()?;
                let final_family = self.final_element(id).await?;
                let before = self
                    .staged
                    .get(&id)
                    .and_then(|s| s.before.clone())
                    .unwrap_or_else(|| final_family.clone());
                if crate::view::render(&before)["attributes"]["status"] != from
                    || crate::view::render(&final_family)["attributes"]["status"] != to
                    || edge(&final_family, "current_revision") != vec![reference.clone()]
                {
                    return Err(fail(
                        "verdict must describe the actual selected revision and lifecycle transition",
                    ));
                }
                if !matches!(row,Element::Activity(a) if a.outputs.iter().any(|r|r.as_str().or_else(||r["id"].as_str())==Some(family_ref.as_str())))
                {
                    return Err(fail("verdict must name its affected Skill in outputs"));
                }
            }
        }
        self.store
            .require_artifact_material(
                &self.cx.space,
                &evaluation["replay_artifact"],
                &refs(&evaluation["revision_refs"]),
            )
            .await?;

        let replay = self
            .store
            .artifact_value(&self.cx.space, &evaluation["replay_artifact"])
            .await?;
        let Some(trial_ref) = evaluation["trial_ref"].as_str() else {
            if to != "revoked"
                || !refs(&evaluation["attempt_refs"]).is_empty()
                || !refs(&evaluation["outcome_refs"]).is_empty()
                || !matches!(
                    evaluation["comparison"]["status"].as_str(),
                    Some("safety_failure" | "withdrawal")
                )
            {
                return Err(fail(
                    "a verdict without a trial can only withdraw or urgently demote",
                ));
            }
            if replay["comparison"] != evaluation["comparison"] {
                return Err(fail("withdrawal replay must retain its reason"));
            }
            return Ok(());
        };
        let (trial_row, trial) = self.referenced_record(trial_ref, "TrialRecord").await?;
        if !same_refs(&evaluation["revision_refs"], &trial["revision_refs"])
            || evaluation["rule_digest"] != trial["rule"]["content_digest"]
            || evaluation["parameters_digest"] != trial["parameters"]["content_digest"]
        {
            return Err(fail(
                "evaluation must bind its trial revision bundle and artifacts",
            ));
        }
        if to == "revoked"
            && matches!(
                evaluation["comparison"]["status"].as_str(),
                Some("safety_failure" | "withdrawal")
            )
            && refs(&evaluation["attempt_refs"]).is_empty()
            && refs(&evaluation["outcome_refs"]).is_empty()
        {
            if replay["comparison"] != evaluation["comparison"] {
                return Err(fail("withdrawal replay must retain its reason"));
            }
            return Ok(());
        }
        let policy = self
            .evaluation_policy(
                &trial["evaluation_policy"],
                if self.staged.get(&trial_row.id()).is_some_and(|s| s.is_new) {
                    self.cx.seq - 1
                } else {
                    *trial_row.envelope().seq
                },
            )
            .await?;
        // Retained policy supports replay; current policy must still allow use.
        let current = self
            .store
            .control_at(
                &self.cx.space,
                &format!("evaluation_policy/{}", policy.id),
                u64::MAX,
            )
            .await?
            .ok_or_else(|| fail("current evaluation policy unavailable"))?;
        let current: EvaluationPolicy =
            serde_json::from_value(current.value).map_err(|e| fail(&e.to_string()))?;
        if !current
            .allowed_rules
            .contains(&evaluation["rule_digest"].as_str().unwrap_or("").into())
            || !current.allowed_parameters.contains(
                &evaluation["parameters_digest"]
                    .as_str()
                    .unwrap_or("")
                    .into(),
            )
            || current.observer_control_digest != policy.observer_control_digest
        {
            return Err(fail("current policy no longer allows this evaluation"));
        }
        if trial["quota"].as_u64().unwrap_or(0) < current.minimum_independent_attempts {
            return Err(fail("trial is below the current policy minimum"));
        }
        let selected = refs(&evaluation["attempt_refs"]);
        let outcomes = refs(&evaluation["outcome_refs"]);
        if selected.iter().collect::<BTreeSet<_>>().len() != selected.len() {
            return Err(fail("attempts cannot be counted twice"));
        }
        let mut all_attempts = selected.clone();
        all_attempts.extend(refs(&trial["baseline_attempt_refs"]));
        let mut all_outcomes = outcomes.clone();
        all_outcomes.extend(refs(&trial["baseline_outcome_refs"]));
        let mut material = all_attempts.clone();
        material.extend(all_outcomes.clone());
        material.extend(refs(&trial["revision_refs"]));
        material.push(trial_ref.into());
        self.store
            .require_artifact_material(&self.cx.space, &evaluation["replay_artifact"], &material)
            .await?;
        if all_attempts.iter().collect::<BTreeSet<_>>().len() != all_attempts.len() {
            return Err(fail("treatment and baseline attempts must be distinct"));
        }
        let mut samples = EvaluationSamples::default();
        let mut units = BTreeSet::new();
        let mut replay_attempts = Map::new();
        let mut replay_outcomes = Map::new();
        let missing = refs(&evaluation["missing_attempt_refs"]);
        for reference in &all_attempts {
            let (attempt_row, attempt) = self.referenced_record(reference, "AttemptRecord").await?;
            let treatment = selected.contains(reference);
            if treatment
                && (attempt["trial_ref"] != trial_ref
                    || !same_refs(&attempt["applied_revisions"], &trial["revision_refs"]))
            {
                return Err(fail(
                    "treatment belongs to another trial or revision bundle",
                ));
            }
            if attempt["environment_digest"] != trial["comparability"]["environment_digest"]
                || attempt["preconditions_satisfied"] != "yes"
                || attempt["started_at"].as_str().unwrap_or("")
                    > evaluation["cutoff"].as_str().unwrap_or("")
            {
                return Err(fail(
                    "attempt is outside evaluation comparability or cutoff",
                ));
            }
            let unit_name = trial["comparability"]["sampling_unit"]
                .as_str()
                .unwrap_or("");
            let unit = if unit_name == "attempt" {
                attempt["attempt_id"].as_str().unwrap_or("")
            } else {
                attempt["context"][unit_name].as_str().unwrap_or("")
            };
            if unit.is_empty() || !units.insert((treatment, unit.to_string())) {
                return Err(fail(
                    "evaluation needs independent predeclared sampling units",
                ));
            }
            let mut measured = None;
            for outcome_ref in &all_outcomes {
                let (outcome_row, outcome) =
                    self.referenced_record(outcome_ref, "OutcomeRecord").await?;
                if outcome["attempt_ref"] != *reference {
                    continue;
                }
                let Element::Evidence(e) = &outcome_row else {
                    return Err(fail("invalid outcome"));
                };
                if e.status == "corrected"
                    || !e.corrected_by.is_empty()
                    || e.observed_at.as_str() > evaluation["cutoff"].as_str().unwrap_or("")
                    || outcome["terminal"] != true
                    || outcome["metric"] != trial["comparability"]["metric"]
                    || outcome["window"] != trial["observation_window"]
                {
                    return Err(fail(
                        "selected outcome is corrected, intermediate, outside cutoff, metric or window",
                    ));
                }
                let principal = self.origin_principal(&outcome_row);
                if principal.is_empty()
                    || ((!policy.allow_same_principal_observer
                        || !current.allow_same_principal_observer)
                        && principal == self.origin_principal(&attempt_row))
                    || !policy.observers.iter().any(|o| {
                        o.principal_id == principal
                            && Some(o.configuration_digest.as_str())
                                == outcome["observer_config_digest"].as_str()
                    })
                {
                    return Err(fail(
                        "outcome instrument lacks protected observer-control eligibility",
                    ));
                }
                if measured.is_some() {
                    return Err(fail(
                        "multiple terminal aggregates require an adjudication rule; they are not independent successes",
                    ));
                }
                measured = Some(match outcome["outcome_status"].as_str() {
                    Some("success") => 1.0,
                    Some("partial") => outcome["magnitude"].as_f64().unwrap_or(0.0),
                    _ => 0.0,
                });
                replay_outcomes.insert(outcome_ref.clone(),json!({"record":outcome,"status":e.status,"corrected_by":e.corrected_by,"principal_id":principal,"observed_at":e.observed_at}));
            }
            if treatment && measured.is_none() && !missing.contains(reference) {
                return Err(fail("missing attempts must be accounted for explicitly"));
            }
            if measured.is_some() && missing.contains(reference) {
                return Err(fail("observed attempt cannot also be marked missing"));
            }
            let stratum = attempt["context"]["stratum"]
                .as_str()
                .unwrap_or("all")
                .to_string();
            if trial["comparability"]["strata_weights"]
                .get(&stratum)
                .is_none()
            {
                return Err(fail("attempt stratum was not predeclared"));
            }
            let into = if treatment {
                &mut samples.treatment
            } else {
                &mut samples.baseline
            };
            into.entry(stratum)
                .or_default()
                .push(measured.unwrap_or(0.0));
            replay_attempts.insert(
                reference.clone(),
                json!({"record":attempt,"principal_id":self.origin_principal(&attempt_row)}),
            );
        }
        if replay_outcomes.len() != all_outcomes.len() {
            return Err(fail("evaluation includes unlinked outcomes"));
        }
        // Every eligible enrolled attempt at cutoff appears in the selected or
        // explicitly excluded set. A successful subset cannot be cherry-picked.
        for attempt in activities {
            if let Ok(a) = record(attempt, "AttemptRecord") {
                if a["trial_ref"] == trial_ref
                    && a["started_at"].as_str().unwrap_or("")
                        <= evaluation["cutoff"].as_str().unwrap_or("")
                    && !selected.contains(&attempt.id().to_string())
                    && a["preconditions_satisfied"] == "yes"
                {
                    return Err(fail(
                        "eligible assigned attempts cannot be excluded by an author-written reason",
                    ));
                }
                if a["trial_ref"] == trial_ref
                    && a["started_at"].as_str().unwrap_or("")
                        <= evaluation["cutoff"].as_str().unwrap_or("")
                    && !selected.contains(&attempt.id().to_string())
                    && !evaluation["excluded_samples"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .any(|e| {
                            e["ref"] == attempt.id().to_string()
                                && e["reason"].as_str().is_some_and(|s| !s.is_empty())
                        })
                {
                    return Err(fail("trial attempt omitted without exclusion accounting"));
                }
            }
        }
        for e in evidence {
            if let Ok(outcome) = record(e, "OutcomeRecord") {
                if outcome["terminal"] == true
                    && selected.iter().any(|r| outcome["attempt_ref"] == *r)
                    && outcome["metric"] == trial["comparability"]["metric"]
                    && outcome["window"] == trial["observation_window"]
                    && !outcomes.contains(&e.id().to_string())
                    && matches!(e,Element::Evidence(row) if row.status!="corrected" && row.corrected_by.is_empty() && row.observed_at.as_str()<=evaluation["cutoff"].as_str().unwrap_or(""))
                {
                    return Err(fail(
                        "conflicting terminal observations need a supported adjudication rule",
                    ));
                }

                if outcome["terminal"] == true
                    && selected.iter().any(|r| outcome["attempt_ref"] == *r)
                    && outcome["metric"] == trial["comparability"]["metric"]
                    && outcome["window"] == trial["observation_window"]
                    && !outcomes.contains(&e.id().to_string())
                    && !evaluation["excluded_samples"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .any(|x| x["ref"] == e.id().to_string())
                {
                    return Err(fail(
                        "terminal observation omitted without adjudication accounting",
                    ));
                }
            }
        }
        let rule = self
            .store
            .artifact_value(&self.cx.space, &trial["rule"])
            .await?;
        let parameters = self
            .store
            .artifact_value(&self.cx.space, &trial["parameters"])
            .await?;
        if !same_json(&replay["rule"], &rule)
            || !same_json(&replay["parameters"], &parameters)
            || !same_json(&replay["trial_record"], &trial)
            || !same_json(&replay["attempts"], &Json::Object(replay_attempts.clone()))
            || !same_json(&replay["outcomes"], &Json::Object(replay_outcomes.clone()))
        {
            return Err(fail(
                "evaluation replay must retain exact rule, parameters, trial and material inputs",
            ));
        }
        let quota = trial["quota"]
            .as_u64()
            .unwrap_or(2)
            .max(policy.minimum_independent_attempts)
            .max(current.minimum_independent_attempts);
        let comparison = self.store.evaluation_rules.evaluate(&EvaluationInput {
            rule,
            parameters,
            trial: trial.clone(),
            attempts: Json::Object(replay_attempts),
            outcomes: Json::Object(replay_outcomes),
            samples,
            minimum_independent_attempts: quota,
        })?;
        if selected.len() < (quota as usize) && comparison["status"] != "insufficient" {
            return Err(fail(
                "evaluation rule must report insufficient below the independent-attempt quota",
            ));
        }
        if !same_json(&comparison, &evaluation["comparison"]) {
            return Err(fail(
                "evaluation comparison disagrees with deterministic replay",
            ));
        }
        if promotion
            && (comparison["status"] != "improved"
                || !comparison["effect"].as_f64().is_some_and(|effect| {
                    effect
                        >= trial["comparability"]["minimum_effect"]
                            .as_f64()
                            .unwrap_or(0.0)
                }))
        {
            return Err(fail("adoption requires comparable independent improvement"));
        }
        if from == "adopted"
            && to == "adopted"
            && comparison["status"] == "insufficient"
            && (!policy.retain_adoption_on_insufficient || !current.retain_adoption_on_insufficient)
        {
            return Err(fail(
                "policy does not retain adoption on insufficient monitoring",
            ));
        }
        if from == "revoked"
            && to == "trialed"
            && !self.staged.get(&trial_row.id()).is_some_and(|s| s.is_new)
        {
            let last_revocation = activities
                .iter()
                .filter(|a| a.envelope().origin.get("import").is_none())
                .filter(|a| {
                    record(a, "EvaluationRecord").is_ok_and(|r| {
                        r["to_status"] == "revoked"
                            && same_refs(&r["revision_refs"], &evaluation["revision_refs"])
                    })
                })
                .map(|a| *a.envelope().seq)
                .max()
                .unwrap_or(0);
            if *trial_row.envelope().seq <= last_revocation {
                return Err(fail("re-entry requires a trial opened after revocation"));
            }
        }
        let _ = row;
        Ok(())
    }
}
