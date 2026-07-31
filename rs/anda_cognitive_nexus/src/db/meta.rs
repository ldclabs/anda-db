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

        // `$self` is applied by the application, not the bundled capsules; a
        // nexus without it still has a useful domain map, so degrade the
        // identity layer to `null` instead of failing the whole PRIMER.
        let me_ids = match self.query_concept_ids(&matcher).await {
            Ok(ids) => ids,
            Err(err) if err.code == KipErrorCode::NotFound => Vec::new(),
            Err(err) => return Err(err),
        };
        let domain_matcher = ConceptMatcher::Type(DOMAIN_TYPE.to_string());
        let domain_ids = self.query_concept_ids(&domain_matcher).await?;

        let me = match me_ids.first() {
            Some(me_id) => {
                self.try_get_concept_with(&cache, *me_id, |concept| {
                    Ok(json!(ConceptInfo::from(concept)))
                })
                .await?
            }
            None => Json::Null,
        };

        // Populate each domain's key schema types by walking the (bounded)
        // set of `$ConceptType` / `$PropositionType` definition nodes and
        // their `belongs_to_domain` links — O(#type definitions) queries,
        // independent of how many ordinary members each domain has.
        let mut domain_types: FxHashMap<EntityID, (Vec<String>, Vec<String>)> =
            FxHashMap::default();
        let concept_type_matcher = ConceptMatcher::Type(META_CONCEPT_TYPE.to_string());
        let proposition_type_matcher = ConceptMatcher::Type(META_PROPOSITION_TYPE.to_string());
        let (concept_type_ids, proposition_type_ids) = try_join!(
            self.query_concept_ids(&concept_type_matcher),
            self.query_concept_ids(&proposition_type_matcher)
        )?;
        for (type_ids, is_concept_type) in [(concept_type_ids, true), (proposition_type_ids, false)]
        {
            for id in type_ids {
                let name = self
                    .try_get_concept_with(&cache, id, |concept| Ok(concept.name.clone()))
                    .await?;
                let links = self
                    .find_propositions(
                        &cache,
                        &EntityID::Concept(id),
                        BELONGS_TO_DOMAIN_TYPE,
                        false,
                    )
                    .await?;
                for (_, domain_id) in links {
                    let entry = domain_types.entry(domain_id).or_default();
                    if is_concept_type {
                        entry.0.push(name.clone());
                    } else {
                        entry.1.push(name.clone());
                    }
                }
            }
        }

        let mut domain_map: Vec<DomainInfo> = Vec::with_capacity(domain_ids.len().min(256));
        let total_domains = domain_ids.len();
        for id in domain_ids.into_iter().take(256) {
            let mut info = self
                .try_get_concept_with(&cache, id, |concept| Ok(DomainInfo::from(concept)))
                .await?;
            if let Some((concept_types, proposition_types)) =
                domain_types.get(&EntityID::Concept(id))
            {
                info.key_concept_types.extend(concept_types.iter().cloned());
                info.key_proposition_types
                    .extend(proposition_types.iter().cloned());
            }

            domain_map.push(info);
        }

        Ok(json!({
            "identity": me,
            "domain_map": domain_map,
            "total_domains": total_domains,
            // Out-of-band capability advertisement (KIP §5.2.1): this engine
            // has no embedding store, so SEARCH degrades semantic/hybrid to
            // keyword retrieval.
            "search_modes": ["keyword"],
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
    /// recall beats no recall); the degradation is advertised out of band
    /// via `DESCRIBE PRIMER` (`search_modes`). Every hit carries the
    /// transient normalized relevance score `metadata._score` in `[0, 1]`;
    /// `THRESHOLD` drops hits scoring below it and results are ordered by
    /// descending `_score`. `LIMIT` is capped at 100 hits per call (engine
    /// resource guard).
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
        // `IN <type>` filtering happens after BM25 scoring, so widen the
        // candidate pool when a type filter may drop most of the top hits —
        // otherwise a rare type can starve even though matches exist.
        let top_k = limit.saturating_mul(if in_type.is_some() { 100 } else { 10 });
        let threshold = threshold.and_then(|v| v.as_f64()).unwrap_or(0.0);

        match target {
            SearchTarget::Concept => {
                let concepts = self.concepts();
                let index = concepts
                    .get_bm25_index(&["name", "attributes", "metadata"])
                    .map_err(db_to_kip_error)?;
                let scored = index.search_advanced(&term, top_k, None);

                let cache = QueryCache::default();
                let mut result: Vec<Json> = Vec::new();
                for (id, score) in scored {
                    let score = normalize_search_score(score);
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
                let propositions = self.propositions();
                let index = propositions
                    .get_bm25_index(&["predicates", "properties"])
                    .map_err(db_to_kip_error)?;
                let scored = index.search_advanced(&term, top_k, None);

                // Distinct query tokens, produced by the same tokenizer chain
                // (and hence the same lowercasing / segmentation) that fed the
                // index.
                let query_tokens: FxHashSet<String> =
                    self.propositions().tokenize(&term).into_iter().collect();
                let cache = QueryCache::default();
                let mut hits: Vec<(f64, Json)> = Vec::new();
                for (id, score) in scored {
                    // One row holds every predicate connecting the same
                    // (subject, object) pair, so this score is row-level: it
                    // is the *upper bound* of the per-link scores derived from
                    // it below, which is what makes it a sound early filter.
                    let row_score = normalize_search_score(score);
                    if row_score < threshold {
                        continue;
                    }
                    let links = self
                        .try_get_proposition_with(&cache, id, |proposition| {
                            let mut rt: Vec<(f64, Json)> = Vec::new();
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
                                // Token-aware re-check: run this link's own
                                // text through the real tokenizer instead of
                                // testing raw substrings, or a link joins the
                                // result because "cat" happens to sit inside
                                // a *sibling* predicate's "concatenated dosing
                                // notes".
                                let link_tokens: FxHashSet<String> = self
                                    .propositions()
                                    .tokenize(&texts.join("\n"))
                                    .into_iter()
                                    .collect();
                                let matched = query_tokens
                                    .iter()
                                    .filter(|token| link_tokens.contains(*token))
                                    .count();
                                if matched == 0 {
                                    continue;
                                }
                                // Per-link score: the row score scaled by the
                                // share of query tokens *this* link carries. A
                                // link covering the whole term keeps the row
                                // score; a partial match is graded down, so
                                // THRESHOLD can separate a genuine hit from an
                                // incidental one (KIP §5.2.2 honest-miss gate)
                                // instead of seeing one flat row-level score.
                                let coverage = matched as f64 / query_tokens.len() as f64;
                                let link_score = (row_score * coverage * 1e6).round() / 1e6;
                                if link_score < threshold {
                                    continue;
                                }
                                if let Some(val) = proposition.to_proposition_link(predicate) {
                                    rt.push((link_score, val));
                                }
                            }

                            Ok(rt)
                        })
                        .await;
                    let Ok(links) = links else {
                        continue; // stale index hit
                    };
                    hits.extend(links);
                }

                // Rank by the per-link score across the whole candidate pool
                // before truncating: `limit` counts links, so stopping at the
                // first row that fills the page would let one many-predicate
                // row starve better-scoring links behind it. The sort is
                // stable, so links of equal score keep BM25 row order.
                hits.sort_by(|(left, _), (right, _)| right.total_cmp(left));
                hits.truncate(limit);
                let result: Vec<Json> = hits
                    .into_iter()
                    .map(|(score, mut link)| {
                        attach_search_score(&mut link, score);
                        link
                    })
                    .collect();
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
    ///
    /// Pagination: matched elements are ordered deterministically (concepts
    /// before links, by entity id). When `LIMIT` truncates the page, the
    /// returned cursor resumes after the last exported element; each page is
    /// an independently valid, idempotent capsule.
    pub(super) async fn execute_export(
        &self,
        command: ExportCommand,
    ) -> Result<(Json, Option<String>), KipError> {
        let ExportCommand {
            target,
            where_clauses,
            limit,
            cursor,
        } = command;

        let mut ctx = QueryContext::default();
        for clause in where_clauses {
            self.execute_where_clause(&mut ctx, clause).await?;
        }
        let target_entities = ctx.entity_values(&target).ok_or_else(|| {
            KipError::reference_error(format!("Target term '{target}' not found in context"))
        })?;
        let mut targets: Vec<EntityID> = target_entities.into();
        // Deterministic page order is what makes the cursor resumable:
        // EntityID orders concepts (`Concept(id)`) before propositions.
        targets.sort();
        targets.dedup();

        let cursor: Option<EntityID> = BTree::from_cursor(&cursor)
            .map_err(|err| KipError::invalid_syntax(format!("Invalid CURSOR token: {err}")))?;
        if let Some(cursor) = &cursor {
            targets.retain(|eid| eid > cursor);
        }

        let mut next_cursor: Option<String> = None;
        if let Some(limit) = limit
            && limit > 0
            && targets.len() > limit
        {
            targets.truncate(limit);
            next_cursor = targets.last().and_then(BTree::to_cursor);
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
            return Ok((
                json!({ "capsule": "", "concepts": 0, "propositions": 0 }),
                None,
            ));
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

        Ok((
            json!({
                "capsule": capsule,
                "concepts": concepts_count,
                "propositions": links_count,
            }),
            next_cursor,
        ))
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
