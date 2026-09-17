use super::cognitive::facet;
use super::*;

impl Transaction {
    async fn erasure_edges(&self) -> Result<BTreeSet<(String, String)>, KipError> {
        let mut rows = vec![];
        for kind in ElementKind::ALL {
            let ids = self
                .store
                .elements(*kind)
                .query_all_ids(crate::store::eq_field(
                    "space",
                    anda_db_schema::Fv::Text(self.cx.space.clone()),
                ))
                .await
                .map_err(db_error)?;
            if rows.len() + ids.len() > crate::kql::MAX_CANDIDATES {
                return Err(KipError::constraint_violation(
                    "erasure closure exceeds scan budget; completion cannot be claimed",
                ));
            }
            for id in ids {
                rows.push(self.store.get_element(ElementId::new(*kind, id)).await?);
            }
        }
        rows.extend(self.staged.values().map(|s| s.row.clone()));
        let mut edges = BTreeSet::new();
        for row in rows {
            if facet(&row, "ErasurePlan").is_some() {
                continue;
            }
            let id = row.id().to_string();
            if let Element::Activity(activity) = &row {
                for input in activity.inputs.iter().filter_map(reference_id) {
                    edges.insert((input.to_string(), id.clone()));
                }
                for output in activity.outputs.iter().filter_map(reference_id) {
                    edges.insert((id.clone(), output.to_string()));
                }
                if let Some(basis) = facet(&row, "DependencyBasis") {
                    for pin in basis["groups"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .flat_map(|g| g["pins"].as_array().into_iter().flatten())
                    {
                        if let Some(source) = pin["id"].as_str() {
                            edges.insert((source.into(), id.clone()));
                        }
                    }
                }
            } else {
                for source in row.references() {
                    edges.insert((source.to_string(), id.clone()));
                }
            }
        }
        let table = self.store.control_records();
        for id in table
            .query_all_ids(crate::store::eq_field(
                "space",
                anda_db_schema::Fv::Text(self.cx.space.clone()),
            ))
            .await
            .map_err(db_error)?
        {
            let row: ControlRecordRow = table.get_as(id).await.map_err(db_error)?;
            if row.key.starts_with("erasure_edges/") {
                for edge in row.value["edges"].as_array().into_iter().flatten() {
                    if let (Some(a), Some(b)) = (edge[0].as_str(), edge[1].as_str()) {
                        edges.insert((a.into(), b.into()));
                    }
                }
            }
        }
        Ok(edges)
    }

    /// Keep only non-content edges crossing a purged node. Otherwise deleting a
    /// producing Activity could hide a still-retained summary from later erasure.
    pub(crate) async fn capture_erasure_edges(&mut self) -> Result<(), KipError> {
        let roots: BTreeSet<String> = self
            .purges
            .keys()
            .chain(self.payload_purges.keys())
            .map(ToString::to_string)
            .collect();
        if roots.is_empty() {
            return Ok(());
        }
        let edges: Vec<_> = self
            .erasure_edges()
            .await?
            .into_iter()
            .filter(|(a, b)| roots.contains(a) || roots.contains(b))
            .collect();
        let key = format!("erasure_edges/{}", self.cx.tx_id);
        self.control_effects.push(ControlRecordRow {
            _id: 0,
            record_id: key.clone(),
            space: self.cx.space.clone(),
            key,
            seq: self.cx.seq,
            version: 1,
            kind: "erasure".into(),
            value: serde_json::json!({"edges":edges}),
            origin: self.cx.origin.clone(),
        });
        Ok(())
    }

    pub(crate) async fn artifact_erasure_replacements(
        &self,
    ) -> Result<Vec<ControlRecordRow>, KipError> {
        let erased: BTreeSet<String> = self
            .purges
            .keys()
            .chain(self.payload_purges.keys())
            .map(ToString::to_string)
            .collect();
        if erased.is_empty() {
            return Ok(vec![]);
        }
        let table = self.store.control_records();
        let mut replacements = vec![];
        for id in table
            .query_all_ids(crate::store::eq_field(
                "space",
                anda_db_schema::Fv::Text(self.cx.space.clone()),
            ))
            .await
            .map_err(db_error)?
        {
            let mut row: ControlRecordRow = table.get_as(id).await.map_err(db_error)?;
            if row.key.starts_with("artifact/")
                && row.value["state"] == "available"
                && row.value["source_refs"].as_array().is_some_and(|refs| {
                    refs.iter()
                        .any(|r| r.as_str().is_some_and(|r| erased.contains(r)))
                })
            {
                row.value.as_object_mut().unwrap().remove("content");
                row.value["state"] = serde_json::json!("erased");
                replacements.push(row);
            }
        }
        Ok(replacements)
    }

    pub(crate) async fn validate_erasure(&self) -> Result<(), KipError> {
        for staged in self.staged.values().filter(|s| s.changed) {
            if let Some(plan) = facet(&staged.row, "ErasurePlan") {
                self.validate_erasure_plan(plan).await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn validate_erasure_plan(&self, plan: &Json) -> Result<(), KipError> {
        crate::schema::contracts::validate_value(
            &serde_json::json!({"$ref":"urn:kip:2.0:schema:cognitive-records#/$defs/ErasurePlan"}),
            plan,
        )?;
        if plan["status"] == "completed" && plan["scope"] == "semantic_forgetting" {
            let mut pending: Vec<String> = plan["source_event_refs"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Json::as_str)
                .map(str::to_string)
                .collect();
            if pending.is_empty() {
                return Err(KipError::constraint_violation(
                    "semantic erasure completion requires explicit source roots",
                ));
            }
            let edges = self.erasure_edges().await?;
            let mut seen = BTreeSet::new();
            while let Some(id) = pending.pop() {
                if !seen.insert(id.clone()) {
                    continue;
                }
                if self.final_element(id.parse()?).await?.state() != state::PURGED {
                    return Err(KipError::constraint_violation(
                        "semantic erasure cannot complete while an owned dependent remains",
                    ));
                }
                pending.extend(
                    edges
                        .iter()
                        .filter(|(source, _)| source == &id)
                        .map(|(_, target)| target.clone()),
                );
            }
        }
        for target in plan["targets"].as_array().into_iter().flatten() {
            if target["state"] != "erased" {
                continue;
            }
            let reference = target["ref"].as_str().unwrap_or("");
            let erased = match target["surface"].as_str() {
                Some("element" | "summary" | "index" | "cache") => {
                    self.final_element(reference.parse()?).await?.state() == state::PURGED
                }
                Some("payload") => {
                    matches!(self.final_element(reference.parse()?).await?,Element::Evidence(row) if row.payload_mode==PAYLOAD_PURGED || row.state==state::PURGED)
                }
                Some("replay" | "blob") => {
                    self.store
                        .control_at(&self.cx.space, &format!("artifact/{reference}"), u64::MAX)
                        .await?
                        .is_some_and(|row| row.value["state"] == "erased")
                        || self
                            .artifact_erasure_replacements()
                            .await?
                            .iter()
                            .any(|r| r.key == format!("artifact/{reference}"))
                }
                // Backend backups need their own verified deletion receipt;
                // a model-authored plan is not such a receipt.
                _ => false,
            };
            if !erased {
                return Err(KipError::constraint_violation(
                    "ErasurePlan cannot complete while a target is retained, held or lacks verified erasure coverage",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn validate_durable(&mut self) -> Result<(), KipError> {
        for (id, staged) in &self.staged {
            if staged.is_new
                && let Element::Activity(row) = &staged.row
                && row.client_key.starts_with("watch_fire:")
                && !self.authorized_watch_fires.contains(id)
            {
                return Err(KipError::not_authorized(
                    "watch_fire client keys are reserved for protected Watch advancement",
                ));
            }
        }
        for staged in self.staged.values_mut() {
            let Element::Concept(row) = &mut staged.row else {
                continue;
            };
            if row.schema_ref == "kip://profiles/cognitive-memory@2.1.0/SleepTask"
                && let Some(lease) = row
                    .facets
                    .get_mut("kip://profiles/cognitive-memory@2.1.0/LeaseState")
            {
                lease["expires_at"] = Json::String(crate::time::normalize(
                    lease["expires_at"].as_str().unwrap_or(""),
                    "lease expires_at",
                )?);
            }
        }
        for (id, s) in &self.staged {
            if !s.changed || s.op == ChangeOp::Purge {
                continue;
            }
            let Element::Concept(row) = &s.row else {
                continue;
            };
            if row.schema_ref == "kip://profiles/cognitive-memory@2.1.0/SleepTask" {
                self.require_changed_guards(*id, s)?;
                let before = s
                    .before
                    .as_ref()
                    .map(crate::view::render)
                    .unwrap_or_default();
                let before_lease = s.before.as_ref().and_then(|r| facet(r, "LeaseState"));
                let after_lease = facet(&s.row, "LeaseState");
                anda_kip::cognitive::validate_lease_transition(
                    before["attributes"]["status"].as_str().unwrap_or("pending"),
                    row.attributes
                        .get("status")
                        .and_then(Json::as_str)
                        .unwrap_or(""),
                    before_lease,
                    after_lease,
                    &self.auth.principal_id,
                    &self.cx.at,
                )?;
            }
            if row.schema_ref == "kip://profiles/cognitive-memory@2.1.0/Watch" {
                self.require_changed_guards(*id, s)?;
                let old = s.before.as_ref().and_then(|r| facet(r, "WatchState"));
                let new = facet(&s.row, "WatchState");
                let before_status = s
                    .before
                    .as_ref()
                    .map(crate::view::render)
                    .and_then(|view| view["attributes"]["status"].as_str().map(str::to_string));
                let after_status = row
                    .attributes
                    .get("status")
                    .and_then(Json::as_str)
                    .unwrap_or("");
                if s.before.is_none() && (after_status != "disarmed" || new.is_some()) {
                    return Err(KipError::constraint_violation(
                        "a new Watch must be disarmed without WatchState",
                    ));
                }
                if before_status
                    .as_deref()
                    .is_some_and(|old| old != after_status)
                    && !self.authorized_watch_updates.contains(id)
                {
                    return Err(KipError::not_authorized(
                        "Watch status is updated by the protected arm/advance binding",
                    ));
                }
                if new.is_none()
                    && (old.is_some()
                        || row.attributes.get("status") == Some(&Json::String("armed".into())))
                {
                    return Err(KipError::constraint_violation(
                        "armed Watch requires a persisted WatchState",
                    ));
                }
                if let Some(new) = new {
                    if !self.authorized_watch_updates.contains(id)
                        && let Some(before) = &s.before
                        && ["watch_class", "due_at"].iter().any(|field| {
                            crate::view::render(before)["attributes"][*field]
                                != row.attributes.get(*field).cloned().unwrap_or(Json::Null)
                        })
                    {
                        return Err(KipError::not_authorized(
                            "changing an armed Watch deadline/class requires a protected new generation",
                        ));
                    }
                    if new["condition_digest"].as_str()
                        != Some(
                            crate::schema::contracts::digest(
                                row.attributes.get("condition").unwrap_or(&Json::Null),
                            )?
                            .as_str(),
                        )
                    {
                        return Err(KipError::constraint_violation(
                            "Watch condition digest mismatch",
                        ));
                    }
                    if old != Some(new) && !self.authorized_watch_updates.contains(id) {
                        return Err(KipError::not_authorized(
                            "WatchState is updated by the protected arm/advance binding",
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}
