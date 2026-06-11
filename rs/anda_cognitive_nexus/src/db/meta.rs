//! META execution: `DESCRIBE` introspection, BM25-backed `SEARCH`
//! with transient `_score`, and `EXPORT` knowledge-capsule generation.

use super::*;

impl CognitiveNexus {
    pub(super) async fn execute_describe_primer(&self) -> Result<Json, KipError> {
        let cache = QueryCache::default();
        let matcher = ConceptMatcher::Object {
            r#type: PERSON_TYPE.to_string(),
            name: META_SELF_NAME.to_string(),
        };

        // Query identity and domains in parallel
        let domain_matcher = ConceptMatcher::Type(DOMAIN_TYPE.to_string());
        let (me_ids, domain_ids) = try_join!(
            self.query_concept_ids(&matcher),
            self.query_concept_ids(&domain_matcher)
        )?;

        let me_id = me_ids
            .first()
            .ok_or_else(|| KipError::not_found(format!("Concept {matcher} not found")))?;
        let me = self
            .try_get_concept_with(&cache, *me_id, |concept| Ok(ConceptInfo::from(concept)))
            .await?;

        let mut domain_map: Vec<DomainInfo> = Vec::with_capacity(domain_ids.len().min(256));
        let total_domains = domain_ids.len();
        for id in domain_ids.into_iter().take(256) {
            let mut info = self
                .try_get_concept_with(&cache, id, |concept| Ok(DomainInfo::from(concept)))
                .await?;
            let subjects = self
                .find_propositions(&cache, &EntityID::Concept(id), BELONGS_TO_DOMAIN_TYPE, true)
                .await?;
            let subjects = subjects.into_iter().map(|(_, id)| id).collect::<Vec<_>>();
            for sub in subjects {
                if let EntityID::Concept(id) = sub {
                    let _ = self
                        .try_get_concept_with(&cache, id, |concept| {
                            if concept.r#type == META_CONCEPT_TYPE {
                                info.key_concept_types.push(concept.name.clone());
                            } else if concept.r#type == META_PROPOSITION_TYPE {
                                info.key_proposition_types.push(concept.name.clone());
                            }
                            Ok(())
                        })
                        .await;
                }
            }

            domain_map.push(info);
        }

        Ok(json!({
            "identity": me,
            "domain_map": domain_map,
            "total_domains": total_domains,
        }))
    }

    pub(super) async fn execute_describe_domains(&self) -> Result<Json, KipError> {
        let ids = self
            .query_concept_ids(&ConceptMatcher::Type(DOMAIN_TYPE.to_string()))
            .await?;
        let cache = QueryCache::default();
        let mut result: Vec<ConceptInfo> = Vec::with_capacity(ids.len());
        for id in ids {
            let concept = self
                .try_get_concept_with(&cache, id, |concept| Ok(ConceptInfo::from(concept)))
                .await?;
            result.push(concept);
        }
        Ok(json!(result))
    }

    pub(super) async fn execute_describe_concept_types(
        &self,
        limit: Option<usize>,
        cursor: Option<String>,
    ) -> Result<(Json, Option<String>), KipError> {
        self.execute_describe_type_names(META_CONCEPT_TYPE, limit, cursor)
            .await
    }

    pub(super) async fn execute_describe_concept_type(
        &self,
        name: String,
    ) -> Result<Json, KipError> {
        let id = self
            .query_concept_ids(&ConceptMatcher::Object {
                r#type: META_CONCEPT_TYPE.to_string(),
                name: name.clone(),
            })
            .await?;

        let id = id
            .first()
            .ok_or_else(|| KipError::not_found(format!("Concept type {name:?} not found")))?;
        let result = self
            .try_get_concept_with(&QueryCache::default(), *id, |concept| {
                Ok(ConceptInfo::from(concept))
            })
            .await?;
        Ok(json!(result))
    }

    pub(super) async fn execute_describe_proposition_types(
        &self,
        limit: Option<usize>,
        cursor: Option<String>,
    ) -> Result<(Json, Option<String>), KipError> {
        self.execute_describe_type_names(META_PROPOSITION_TYPE, limit, cursor)
            .await
    }

    pub(super) async fn execute_describe_type_names(
        &self,
        meta_type: &str,
        limit: Option<usize>,
        cursor: Option<String>,
    ) -> Result<(Json, Option<String>), KipError> {
        let ids = self
            .query_concept_ids(&ConceptMatcher::Type(meta_type.to_string()))
            .await?;
        let cache = QueryCache::default();
        let mut names = Vec::with_capacity(ids.len());

        for id in ids {
            let name = self
                .try_get_concept_with(&cache, id, |concept| Ok(concept.name.clone()))
                .await?;
            names.push(name);
        }

        names.sort();
        names.dedup();

        let start = cursor
            .as_deref()
            .map(|cursor| names.partition_point(|name| name.as_str() <= cursor))
            .unwrap_or(0);
        let mut page = if start < names.len() {
            names[start..].to_vec()
        } else {
            Vec::new()
        };

        let mut next_cursor = None;
        if let Some(limit) = limit
            && limit > 0
            && page.len() > limit
        {
            page.truncate(limit);
            next_cursor = page.last().cloned();
        }

        Ok((json!(page), next_cursor))
    }

    pub(super) async fn execute_describe_proposition_type(
        &self,
        name: String,
    ) -> Result<Json, KipError> {
        let id = self
            .query_concept_ids(&ConceptMatcher::Object {
                r#type: META_PROPOSITION_TYPE.to_string(),
                name: name.clone(),
            })
            .await?;

        let id = id
            .first()
            .ok_or_else(|| KipError::not_found(format!("Proposition type {name:?} not found")))?;
        let result = self
            .try_get_concept_with(&QueryCache::default(), *id, |concept| {
                Ok(ConceptInfo::from(concept))
            })
            .await?;
        Ok(json!(result))
    }

    /// Executes a `SEARCH` statement (KIP §5.2) — index-driven grounding and
    /// associative retrieval.
    ///
    /// This engine has no embedding store, so the `semantic` / `hybrid`
    /// retrieval modes degrade to `keyword` as the spec mandates (degraded
    /// recall beats no recall). Every hit carries the transient normalized
    /// relevance score `metadata._score` in `[0, 1]`; `THRESHOLD` drops hits
    /// scoring below it and results are ordered by descending `_score`.
    pub(super) async fn execute_search(&self, command: SearchCommand) -> Result<Json, KipError> {
        let SearchCommand {
            target,
            term,
            in_type,
            mode: _mode, // no semantic capability: every mode is lexical
            threshold,
            limit,
        } = command;
        let limit = limit.unwrap_or(100).min(100);
        let top_k = limit.saturating_mul(10);
        let threshold = threshold.and_then(|v| v.as_f64()).unwrap_or(0.0);

        match target {
            SearchTarget::Concept => {
                let index = self
                    .concepts
                    .get_bm25_index(&["name", "attributes", "metadata"])
                    .map_err(db_to_kip_error)?;
                let scored = index.search_advanced(&term, top_k, None);
                let max_score = scored
                    .first()
                    .map(|(_, score)| *score)
                    .filter(|score| *score > 0.0)
                    .unwrap_or(1.0);

                let cache = QueryCache::default();
                let mut result: Vec<Json> = Vec::new();
                for (id, score) in scored {
                    let score = normalize_search_score(score, max_score);
                    if score < threshold {
                        continue;
                    }
                    let node = self
                        .try_get_concept_with(&cache, id, |concept| {
                            if let Some(ty) = &in_type
                                && concept.r#type != *ty
                            {
                                return Ok(None);
                            }
                            Ok(Some(concept.to_concept_node()))
                        })
                        .await;
                    // A stale index hit (row removed meanwhile) is skipped
                    // rather than failing the whole search.
                    let Ok(Some(mut node)) = node else {
                        continue;
                    };
                    attach_search_score(&mut node, score);
                    result.push(node);
                    if result.len() >= limit {
                        break;
                    }
                }
                Ok(json!(result))
            }
            SearchTarget::Proposition => {
                let index = self
                    .propositions
                    .get_bm25_index(&["predicates", "properties"])
                    .map_err(db_to_kip_error)?;
                let scored = index.search_advanced(&term, top_k, None);
                let max_score = scored
                    .first()
                    .map(|(_, score)| *score)
                    .filter(|score| *score > 0.0)
                    .unwrap_or(1.0);

                let tokens = self.propositions.tokenize(&term);
                let cache = QueryCache::default();
                let mut result: Vec<Json> = Vec::new();
                'scored: for (id, score) in scored {
                    let score = normalize_search_score(score, max_score);
                    if score < threshold {
                        continue;
                    }
                    let links = self
                        .try_get_proposition_with(&cache, id, |proposition| {
                            let mut rt: Vec<Json> = Vec::new();
                            for (predicate, prop) in &proposition.properties {
                                if let Some(ty) = &in_type
                                    && predicate != ty
                                {
                                    continue;
                                }
                                // collect searchable texts
                                let mut texts: Vec<&str> = vec![predicate];
                                for (_, val) in &prop.attributes {
                                    extract_json_text(&mut texts, val);
                                }
                                for (_, val) in &prop.metadata {
                                    extract_json_text(&mut texts, val);
                                }
                                let texts = texts.join("\n");
                                if tokens.iter().any(|t| texts.contains(t.as_str()))
                                    && let Some(val) = proposition.to_proposition_link(predicate)
                                {
                                    rt.push(val);
                                }
                            }

                            Ok(rt)
                        })
                        .await;
                    let Ok(links) = links else {
                        continue; // stale index hit
                    };
                    for mut link in links {
                        attach_search_score(&mut link, score);
                        result.push(link);
                        if result.len() >= limit {
                            break 'scored;
                        }
                    }
                }
                Ok(json!(result))
            }
        }
    }

    /// Executes an `EXPORT` statement (KIP §5.3): serializes the matched
    /// concept nodes and proposition links into an idempotent `UPSERT`
    /// capsule. Read-only. Endpoints inside the export set are referenced by
    /// local handles; endpoints outside it are referenced structurally
    /// (`{type, name}` for concepts, nested `(s, "p", o)` clauses for links),
    /// so importing requires those targets to exist (`KIP_3002`). Reserved
    /// `_` metadata is never exported.
    pub(super) async fn execute_export(&self, command: ExportCommand) -> Result<Json, KipError> {
        let ExportCommand {
            target,
            where_clauses,
            limit,
        } = command;

        let mut ctx = QueryContext::default();
        for clause in where_clauses {
            self.execute_where_clause(&mut ctx, clause).await?;
        }
        let target_entities = ctx.entities.get(&target).cloned().ok_or_else(|| {
            KipError::reference_error(format!("Target term '{target}' not found in context"))
        })?;
        let mut targets: Vec<EntityID> = target_entities.into();
        if let Some(limit) = limit
            && targets.len() > limit
        {
            targets.truncate(limit);
        }

        let mut concept_eids: Vec<EntityID> = Vec::new();
        let mut link_eids: Vec<EntityID> = Vec::new();
        for eid in targets {
            match eid {
                EntityID::Concept(_) => concept_eids.push(eid),
                EntityID::Proposition(_, _) => link_eids.push(eid),
            }
        }

        if concept_eids.is_empty() && link_eids.is_empty() {
            return Ok(json!({ "capsule": "", "concepts": 0, "propositions": 0 }));
        }

        // Local handles: `?c<n>` for concepts, `?p<n>` for links.
        let mut handles: FxHashMap<EntityID, String> = FxHashMap::default();
        for (i, eid) in concept_eids.iter().enumerate() {
            handles.insert(eid.clone(), format!("c{}", i + 1));
        }
        for (i, eid) in link_eids.iter().enumerate() {
            handles.insert(eid.clone(), format!("p{}", i + 1));
        }

        // Preload link endpoints, then order links so that in-set endpoints
        // are emitted before the higher-order links referencing them.
        let link_set: FxHashSet<EntityID> = link_eids.iter().cloned().collect();
        let mut endpoints: FxHashMap<EntityID, (EntityID, EntityID)> = FxHashMap::default();
        for eid in &link_eids {
            if let EntityID::Proposition(id, _) = eid {
                let endpoint = self
                    .try_get_proposition_with(&ctx.cache, *id, |p| {
                        Ok((p.subject.clone(), p.object.clone()))
                    })
                    .await?;
                endpoints.insert(eid.clone(), endpoint);
            }
        }
        let mut ordered_links: Vec<EntityID> = Vec::with_capacity(link_eids.len());
        let mut satisfied: FxHashSet<EntityID> = concept_eids.iter().cloned().collect();
        let mut remaining = link_eids;
        while !remaining.is_empty() {
            let before = ordered_links.len();
            remaining.retain(|eid| {
                let (subject, object) = &endpoints[eid];
                let ready = [subject, object]
                    .into_iter()
                    .all(|endpoint| !link_set.contains(endpoint) || satisfied.contains(endpoint));
                if ready {
                    satisfied.insert(eid.clone());
                    ordered_links.push(eid.clone());
                }
                !ready
            });
            if ordered_links.len() == before {
                // Unreachable for well-formed graphs (higher-order references
                // cannot be cyclic); emit the rest with structural endpoints.
                ordered_links.append(&mut remaining);
            }
        }

        let mut capsule = String::from("UPSERT {\n");
        let mut rendered: FxHashSet<EntityID> = FxHashSet::default();
        let mut concepts_count: u64 = 0;
        for eid in &concept_eids {
            let EntityID::Concept(id) = eid else {
                continue;
            };
            let concept = self
                .try_get_concept_with(&ctx.cache, *id, |c| Ok(c.clone()))
                .await?;
            capsule.push_str(&format!(
                "  CONCEPT ?{} {{\n    {{type: {}, name: {}}}\n",
                handles[eid],
                to_kip_json(&concept.r#type),
                to_kip_json(&concept.name),
            ));
            if !concept.attributes.is_empty() {
                capsule.push_str(&format!(
                    "    SET ATTRIBUTES {}\n",
                    to_kip_json(&concept.attributes)
                ));
            }
            capsule.push_str("  }");
            let metadata = strip_reserved_metadata(&concept.metadata);
            if !metadata.is_empty() {
                capsule.push_str(&format!(" WITH METADATA {}", to_kip_json(&metadata)));
            }
            capsule.push_str("\n\n");
            rendered.insert(eid.clone());
            concepts_count += 1;
        }

        let mut links_count: u64 = 0;
        for eid in &ordered_links {
            let EntityID::Proposition(id, predicate) = eid else {
                continue;
            };
            let (subject, object) = endpoints[eid].clone();
            let properties = self
                .try_get_proposition_with(&ctx.cache, *id, |p| {
                    Ok(p.properties.get(predicate).cloned().unwrap_or_default())
                })
                .await?;
            let subject_ref = self
                .render_export_target(&ctx.cache, &handles, &rendered, &subject)
                .await?;
            let object_ref = self
                .render_export_target(&ctx.cache, &handles, &rendered, &object)
                .await?;
            capsule.push_str(&format!(
                "  PROPOSITION ?{} {{\n    ({subject_ref}, {}, {object_ref})\n",
                handles[eid],
                to_kip_json(predicate),
            ));
            if !properties.attributes.is_empty() {
                capsule.push_str(&format!(
                    "    SET ATTRIBUTES {}\n",
                    to_kip_json(&properties.attributes)
                ));
            }
            capsule.push_str("  }");
            let metadata = strip_reserved_metadata(&properties.metadata);
            if !metadata.is_empty() {
                capsule.push_str(&format!(" WITH METADATA {}", to_kip_json(&metadata)));
            }
            capsule.push_str("\n\n");
            rendered.insert(eid.clone());
            links_count += 1;
        }
        capsule.push_str("}\n");

        Ok(json!({
            "capsule": capsule,
            "concepts": concepts_count,
            "propositions": links_count,
        }))
    }

    /// Renders a proposition endpoint for `EXPORT`: a local handle when the
    /// endpoint belongs to the export set (and is already emitted), otherwise
    /// a structural reference — `{type, name}` for concepts and a nested
    /// `(subject, "predicate", object)` clause for proposition links.
    pub(super) async fn render_export_target(
        &self,
        cache: &QueryCache,
        handles: &FxHashMap<EntityID, String>,
        rendered: &FxHashSet<EntityID>,
        eid: &EntityID,
    ) -> Result<String, KipError> {
        if rendered.contains(eid)
            && let Some(handle) = handles.get(eid)
        {
            return Ok(format!("?{handle}"));
        }
        match eid {
            EntityID::Concept(id) => {
                let (ty, name) = self
                    .try_get_concept_with(cache, *id, |c| Ok((c.r#type.clone(), c.name.clone())))
                    .await?;
                Ok(format!(
                    "{{type: {}, name: {}}}",
                    to_kip_json(&ty),
                    to_kip_json(&name)
                ))
            }
            EntityID::Proposition(id, predicate) => {
                let (subject, object) = self
                    .try_get_proposition_with(cache, *id, |p| {
                        Ok((p.subject.clone(), p.object.clone()))
                    })
                    .await?;
                let subject_ref =
                    Box::pin(self.render_export_target(cache, handles, rendered, &subject)).await?;
                let object_ref =
                    Box::pin(self.render_export_target(cache, handles, rendered, &object)).await?;
                Ok(format!(
                    "({subject_ref}, {}, {object_ref})",
                    to_kip_json(predicate)
                ))
            }
        }
    }
}
