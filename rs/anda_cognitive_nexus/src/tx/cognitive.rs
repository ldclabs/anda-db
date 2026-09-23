//! Invariants that span more than one cognitive row.
use super::*;
use anda_db_schema::Fv;

pub(crate) fn facet<'a>(element: &'a Element, local: &str) -> Option<&'a Json> {
    element
        .facets()
        .iter()
        .find(|(name, _)| name.ends_with(&format!("/{local}")))
        .map(|(_, value)| value)
}

fn premises(contract: &Json) -> Json {
    let mut groups: Vec<Json> = contract["groups"].as_array().into_iter().flatten().map(|group| {
        let mut pins: Vec<Json> = group["pins"].as_array().into_iter().flatten().map(|pin| {
            serde_json::json!({"id":pin["id"], "planes":pin["planes"].as_object().map(|p| p.keys().collect::<Vec<_>>())})
        }).collect();
        pins.sort_by_key(anda_kip::canonical_json);
        serde_json::json!({"role":group["role"], "pins":pins})
    }).collect();
    groups.sort_by_key(anda_kip::canonical_json);
    Json::Array(groups)
}

impl Transaction {
    /// KML and Capsule rows share the same final Core schema checks. This is
    /// independent of the host-only learning standing granted to native records.
    pub(crate) async fn validate_core_schema(&mut self) -> Result<(), KipError> {
        use crate::schema::{EndpointFacts, Intent, PackageState, SymbolRef};
        let importing = self.cx.origin.get("import").is_some();
        let intent = if importing {
            Intent::Read
        } else {
            Intent::Write
        };
        // Existing KML edits validate the fields they change in their clause.
        // Do not rebind untouched historical types to the current Schema Lock.
        let rows: Vec<_> = self
            .staged
            .values()
            .filter(|s| s.is_new && s.changed && s.op != ChangeOp::Purge)
            .map(|s| s.row.clone())
            .collect();
        for element in rows {
            if importing {
                let symbols = std::iter::once(element.schema_ref())
                    .chain(element.facets().keys().map(String::as_str))
                    .chain(element.structural().keys().map(String::as_str));
                for name in symbols.filter(|name| name.starts_with("kip://")) {
                    let symbol: SymbolRef = name.parse()?;
                    let state = self.env.state(&symbol.package.package_id);
                    if !state.allows_write() && state != PackageState::ValidationOnly {
                        return Err(KipError::new(
                            KipErrorCode::ProtectedSchemaState,
                            "package does not admit imported records",
                        ));
                    }
                }
            }
            let carrier = EndpointFacts::Element {
                kind: element.kind(),
                schema_ref: match &element {
                    Element::Concept(row) => Some(row.schema_ref.clone()).filter(|s| !s.is_empty()),
                    _ => None,
                },
            };
            match &element {
                Element::Concept(row) => {
                    self.env
                        .prepare_concept(&row.schema_ref, &row.attributes, &row.facets, intent)?
                        .1
                        .into_result()?;
                }
                Element::Proposition(row) => {
                    let subject = self.schema_endpoint(&row.subject).await?;
                    let object = self.schema_endpoint(&row.object).await?;
                    self.env
                        .prepare_proposition(&row.predicate_ref, &subject, &object, intent)?
                        .1
                        .into_result()?;
                    self.env
                        .validate_facets(element.facets(), &carrier, intent)?
                        .into_result()?;
                }
                _ => {
                    self.env
                        .validate_facets(element.facets(), &carrier, intent)?
                        .into_result()?;
                }
            }
            for (field, values) in element.structural() {
                let values = values.as_array().ok_or_else(|| {
                    KipError::type_mismatch("structural references must be an array")
                })?;
                let mut targets = Vec::with_capacity(values.len());
                for value in values {
                    let endpoint = crate::term::Endpoint::from_json(value)?;
                    targets.push((endpoint.key(), self.schema_endpoint(value).await?));
                }
                self.env
                    .prepare_structural(field, &carrier, &targets, intent)?
                    .1
                    .into_result()?;
            }
        }
        Ok(())
    }

    async fn schema_endpoint(
        &self,
        value: &Json,
    ) -> Result<crate::schema::EndpointFacts, KipError> {
        use crate::{schema::EndpointFacts, term::Endpoint};
        Ok(match Endpoint::from_json(value)? {
            Endpoint::Literal(literal) => EndpointFacts::Literal {
                datatype: literal.datatype,
                value: literal.value,
            },
            Endpoint::Local(id) => {
                let row = match self.staged.get(&id) {
                    Some(staged) => staged.row.clone(),
                    None => self.store.get_element(id).await?,
                };
                EndpointFacts::Element {
                    kind: id.kind,
                    schema_ref: match row {
                        Element::Concept(row) => {
                            Some(row.schema_ref.clone()).filter(|s| !s.is_empty())
                        }
                        _ => None,
                    },
                }
            }
            _ => EndpointFacts::Unresolved,
        })
    }

    pub(crate) async fn final_element(&self, id: ElementId) -> Result<Element, KipError> {
        let row = if let Some(s) = self.staged.get(&id) {
            if s.is_new {
                return Ok(s.row.clone());
            }
            s.row.clone()
        } else {
            self.store.get_element(id).await?
        };
        if row.space() != self.cx.space {
            return Err(KipError::not_found_or_not_visible(
                "cross-Space contract reference",
            ));
        }
        if !self
            .authority
            .may_read(&row, &self.auth)
            .is_some_and(|v| v.content && v.constraints.fields.is_empty())
        {
            return Err(KipError::not_found_or_not_visible(
                "complete contract input unavailable",
            ));
        }
        Ok(row)
    }

    pub(crate) async fn validate_revalidation(&self, element: &Element) -> Result<(), KipError> {
        if crate::schema::contracts::is_derived(element)
            || facet(element, "DependencyBasis").is_some()
        {
            self.authority
                .authorize(
                    Permission::Derive,
                    &ResourceContext::of_element(element),
                    &self.auth,
                )
                .into_result()?;
        }
        let Element::Activity(activity) = element else {
            return Ok(());
        };
        let Some(contract) = facet(element, "DependencyBasis") else {
            if activity.activity_class == "dependency_validation" {
                return Err(KipError::constraint_violation(
                    "revalidation requires DependencyBasis",
                ));
            }
            return Ok(());
        };
        // Pins and topology describe the same read set, in both directions.
        for pin in contract["groups"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|g| g["pins"].as_array().into_iter().flatten())
        {
            let id = pin["id"].as_str().unwrap_or("");
            if !activity
                .inputs
                .iter()
                .any(|r| r.as_str().or_else(|| r["id"].as_str()) == Some(id))
            {
                return Err(KipError::constraint_violation(
                    "dependency pin is absent from Activity inputs",
                ));
            }
        }
        if activity.activity_class != "dependency_validation" {
            return Ok(());
        }
        if activity.status != "completed" || activity.outputs.is_empty() {
            return Err(KipError::constraint_violation(
                "revalidation must complete and name its exact outputs",
            ));
        }
        for output in &activity.outputs {
            let id = reference_id(output).ok_or_else(|| {
                KipError::constraint_violation("revalidation needs local output references")
            })?;
            let target = self.final_element(id).await?;
            if self.staged.get(&id).is_some_and(|s| s.changed) {
                return Err(KipError::constraint_violation(
                    "refreshing an output requires a producing Activity, not revalidation",
                ));
            }
            self.authority
                .authorize(
                    Permission::Derive,
                    &ResourceContext::of_element(&target),
                    &self.auth,
                )
                .into_result()?;
            if !matches!(target, Element::Assertion(_)) {
                continue;
            }
            let candidates = self
                .store
                .activities()
                .query_all_ids(crate::store::eq_fields(&[
                    ("space", Fv::Text(self.cx.space.clone())),
                    (
                        "output_keys",
                        Fv::Text(crate::term::Endpoint::Local(id).key()),
                    ),
                ]))
                .await
                .map_err(db_error)?;
            let mut original = None;
            for aid in &candidates {
                let row: ActivityRow = self
                    .store
                    .activities()
                    .get_as(*aid)
                    .await
                    .map_err(db_error)?;
                if row.activity_class == "dependency_validation"
                    || row.status != "completed"
                    || row.state != state::ACTIVE
                    || (row.created_tx != *target.envelope().created_tx
                        && row.updated_tx != *target.envelope().updated_tx)
                    || row.origin["_kip_runtime"]["output_versions"][id.to_string()].as_u64()
                        != Some(target.version())
                {
                    continue;
                }
                let producer = Element::Activity(Box::new(row));
                self.authority
                    .authorize(
                        Permission::Read,
                        &ResourceContext::of_element(&producer),
                        &self.auth,
                    )
                    .into_result()?;
                if let Some(basis) = facet(&producer, "DependencyBasis") {
                    original = Some(premises(basis));
                }
            }
            if original != Some(premises(contract)) {
                return Err(KipError::constraint_violation(
                    "revalidation cannot replace an Assertion's original premises; create a new Assertion",
                ));
            }
        }
        Ok(())
    }
}
