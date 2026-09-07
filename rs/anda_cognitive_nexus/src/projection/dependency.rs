//! Read-time dependency validity. Stored cognition is never rewritten by this check.
use super::Policy;
use crate::{
    id::ElementId,
    kql::Context,
    store::{Element, rows::state},
};
use anda_kip::{ElementKind, Json, KipError};

#[derive(Default)]
struct Validity {
    // 0=current, 1=needs_review, 2=unverifiable. Reasons reveal no hidden refs/counts.
    state: u8,
    reasons: Vec<String>,
    next: Option<String>,
}
impl Validity {
    fn issue(state: u8, reason: &str) -> Self {
        Self {
            state,
            reasons: vec![reason.into()],
            next: None,
        }
    }
    fn absorb(&mut self, other: Self) {
        self.state = self.state.max(other.state);
        self.reasons.extend(other.reasons);
        self.next = self.next.iter().chain(other.next.iter()).min().cloned();
    }
}

impl Context<'_> {
    pub(crate) async fn dependency_validity(
        &mut self,
        element: &Element,
        policy: &Policy,
        at: &str,
    ) -> Result<Json, KipError> {
        if self.as_of.is_some() && !policy.explicit_selection {
            let mut basis = self.projection_basis(policy, at, None);
            basis.policy.version = "unavailable".into();
            basis.trust_version = "unavailable".into();
            return Ok(
                serde_json::json!({"status":"unverifiable", "action_eligible":false,
                "reasons":["historical projection control state unavailable"], "basis":basis}),
            );
        }
        let mut result = self
            .check_dependencies(element, policy, at, &mut Vec::new())
            .await?;
        result.reasons.sort();
        result.reasons.dedup();
        Ok(serde_json::json!({
            "status": (["current", "needs_review", "unverifiable"][result.state as usize]),
            "action_eligible": result.state == 0,
            "reasons": result.reasons,
            "basis": self.projection_basis(policy, at, result.next),
        }))
    }

    async fn check_dependencies(
        &mut self,
        element: &Element,
        policy: &Policy,
        at: &str,
        path: &mut Vec<ElementId>,
    ) -> Result<Validity, KipError> {
        let id = element.id();
        if path.contains(&id) || path.len() >= 64 {
            return Ok(Validity::issue(2, "dependency cycle or traversal limit"));
        }
        let Some(visibility) = self
            .authority
            .may_read(element, self.auth)
            .filter(|v| v.content)
        else {
            return Ok(Validity::issue(2, "dependency source unavailable"));
        };
        // A masked source cannot establish that the actor may inspect all of a
        // pin's prerequisites. Do not turn masked fields into a positive check.
        if !visibility.constraints.fields.is_empty() {
            return Ok(Validity::issue(2, "dependency source is field-restricted"));
        }
        if element.state() != state::ACTIVE && element.state() != state::MERGED {
            return Ok(Validity::issue(1, "dependency lifecycle changed"));
        }
        let mut result = Validity::default();
        match element {
            Element::Evidence(row) if row.status == "corrected" => {
                return Ok(Validity::issue(1, "dependency evidence corrected"));
            }
            Element::Assertion(row) => {
                if row.status != "active"
                    || (!row.valid_from.is_empty() && row.valid_from.as_str() > at)
                    || (!row.valid_until.is_empty() && row.valid_until.as_str() <= at)
                {
                    return Ok(Validity::issue(1, "dependency no longer eligible"));
                }
                result.next = [&row.valid_from, &row.valid_until]
                    .into_iter()
                    .filter(|t| t.as_str() > at)
                    .min()
                    .cloned();
            }
            _ => {}
        }
        if !crate::schema::contracts::is_derived(element) {
            return Ok(result);
        }
        let mut producer = None;
        let ids = self.candidates(ElementKind::Activity, None).await?;
        self.charge(ids.len())?;
        for aid in ids {
            let Some(Element::Activity(row)) = self.load_unattached(aid).await? else {
                continue;
            };
            if !self
                .authority
                .may_read(&Element::Activity(row.clone()), self.auth)
                .is_some_and(|v| v.content)
            {
                continue;
            }
            if row.created_tx != *element.envelope().created_tx
                && row.updated_tx != *element.envelope().updated_tx
            {
                continue; // A retrospective audit is not the producing computation.
            }
            if !matches!(row.status.as_str(), "completed" | "failed" | "cancelled")
                || row.state != state::ACTIVE
            {
                continue;
            }
            if row.origin["_kip_runtime"]["output_versions"][id.to_string()].as_u64()
                != Some(element.version())
            {
                continue;
            }
            if !row
                .outputs
                .iter()
                .any(|r| r.as_str().or_else(|| r["id"].as_str()) == Some(id.to_string().as_str()))
            {
                continue;
            }
            if let Some((_, basis)) = row
                .facets
                .iter()
                .find(|(name, _)| name.ends_with("/DependencyBasis"))
                && producer.as_ref().is_none_or(|(seq, _)| row.seq > *seq)
            {
                producer = Some((row.seq, basis.clone()));
            }
        }
        let Some((_, contract)) = producer else {
            return Ok(Validity::issue(
                2,
                "exact producing DependencyBasis unavailable",
            ));
        };
        if crate::schema::contracts::validate_value(&serde_json::json!({"$ref":"urn:kip:2.0:schema:cognitive-records#/$defs/DependencyBasis"}), &contract).is_err() {
            return Ok(Validity::issue(2, "dependency contract is invalid"));
        }
        let basis = serde_json::to_value(self.projection_basis(policy, at, None)).unwrap();
        for coordinate in [
            "schema_environment_version",
            "identity_version",
            "policy",
            "trust_version",
            "purpose",
            "risk",
        ] {
            if contract["policy_basis"][coordinate] != basis[coordinate] {
                result.absorb(Validity::issue(1, "dependency computation policy changed"));
            }
        }
        if let Some(contexts) = contract["policy_basis"]["context_refs"].as_array()
            && !contexts.iter().all(|c| {
                basis["context_refs"]
                    .as_array()
                    .is_some_and(|refs| refs.contains(c))
            })
        {
            result.absorb(Validity::issue(1, "dependency context mismatch"));
        }
        path.push(id);
        for group in contract["groups"].as_array().into_iter().flatten() {
            let mut members = Vec::new();
            for pin in group["pins"].as_array().into_iter().flatten() {
                let source = pin["id"]
                    .as_str()
                    .and_then(|id| id.parse::<ElementId>().ok());
                let Some(source) = source else {
                    members.push(Validity::issue(2, "dependency source unavailable"));
                    continue;
                };
                let Some(row) = self.load_unattached(source).await? else {
                    members.push(Validity::issue(2, "dependency source unavailable"));
                    continue;
                };
                if !self
                    .authority
                    .may_read(&row, self.auth)
                    .is_some_and(|v| v.content)
                {
                    members.push(Validity::issue(2, "dependency source unavailable"));
                    continue;
                }
                let pinned_planes = pin["planes"].as_object().filter(|v| !v.is_empty());
                let changed = if let Some(planes) = pinned_planes {
                    let current = serde_json::to_value(row.plane_versions()).unwrap();
                    planes.iter().any(|(name, value)| {
                        crate::schema::contracts::pinned_plane(&current, name) != value.as_u64()
                    })
                } else {
                    pin["version"].as_u64() != Some(row.version())
                };
                if changed {
                    members.push(Validity::issue(1, "dependency source version changed"));
                } else {
                    members.push(Box::pin(self.check_dependencies(&row, policy, at, path)).await?);
                }
            }
            match group["role"].as_str() {
                Some("context") => {
                    if members.iter().any(|m| m.state != 0) {
                        result.reasons.push("context dependency changed".into());
                    }
                }
                Some("any_of") if members.iter().any(|m| m.state == 0) => {
                    for member in members.into_iter().filter(|m| m.state == 0) {
                        result.absorb(member);
                    }
                }
                _ => {
                    for member in members {
                        result.absorb(member);
                    }
                }
            }
        }
        path.pop();
        Ok(result)
    }
}
