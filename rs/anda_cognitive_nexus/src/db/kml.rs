//! KML execution: `UPSERT` (with `EXPECT VERSION` guards), `UPDATE`,
//! `MERGE`, and `DELETE`, plus the protected-scope checks and
//! engine-maintained `_version` / `_updated_at` bookkeeping.

use super::*;

impl CognitiveNexus {
    pub(super) async fn execute_upsert(
        &self,
        upsert_blocks: Vec<UpsertBlock>,
        dry_run: bool,
    ) -> Result<Json, KipError> {
        let blocks = upsert_blocks.len();
        self.preflight_upsert(&upsert_blocks).await?;

        if dry_run {
            return Ok(json!(UpsertResult {
                blocks,
                upsert_concept_nodes: Vec::<String>::new(),
                upsert_proposition_links: Vec::<String>::new(),
            }));
        }

        let mut concept_nodes: Vec<EntityID> = Vec::new();
        let mut proposition_links: Vec<EntityID> = Vec::new();
        let mut cached_pks: FxHashMap<EntityPK, EntityID> = FxHashMap::default();
        for block in upsert_blocks {
            let mut handle_map: FxHashMap<String, EntityID> = FxHashMap::default();
            let default_metadata: Map<String, Json> = block.metadata.unwrap_or_default();

            for item in block.items {
                match item {
                    UpsertItem::Concept(concept_block) => {
                        if let Some(entity_id) = self
                            .execute_concept_block(
                                concept_block,
                                &default_metadata,
                                &mut handle_map,
                                &mut cached_pks,
                                dry_run,
                            )
                            .await?
                        {
                            concept_nodes.push(entity_id);
                        }
                    }
                    UpsertItem::Proposition(proposition_block) => {
                        if let Some(entity_id) = self
                            .execute_proposition_block(
                                proposition_block,
                                &default_metadata,
                                &mut handle_map,
                                &mut cached_pks,
                                dry_run,
                            )
                            .await?
                        {
                            proposition_links.push(entity_id);
                        }
                    }
                }
            }
        }

        let now_ms = unix_ms();
        try_join!(self.concepts.flush(now_ms), self.propositions.flush(now_ms))
            .map_err(db_to_kip_error)?;

        Ok(json!(UpsertResult {
            blocks,
            upsert_concept_nodes: concept_nodes.into_iter().map(|id| id.to_string()).collect(),
            upsert_proposition_links: proposition_links
                .into_iter()
                .map(|id| id.to_string())
                .collect(),
        }))
    }

    pub(super) async fn preflight_upsert(
        &self,
        upsert_blocks: &[UpsertBlock],
    ) -> Result<(), KipError> {
        let mut cached_pks: FxHashMap<EntityPK, EntityID> = FxHashMap::default();

        for block in upsert_blocks.iter().cloned() {
            let mut handle_map: FxHashMap<String, EntityID> = FxHashMap::default();
            let default_metadata: Map<String, Json> = block.metadata.unwrap_or_default();
            reject_reserved_metadata_keys(default_metadata.keys())?;

            for item in block.items {
                match item {
                    UpsertItem::Concept(concept_block) => {
                        self.execute_concept_block(
                            concept_block,
                            &default_metadata,
                            &mut handle_map,
                            &mut cached_pks,
                            true,
                        )
                        .await?;
                    }
                    UpsertItem::Proposition(proposition_block) => {
                        self.execute_proposition_block(
                            proposition_block,
                            &default_metadata,
                            &mut handle_map,
                            &mut cached_pks,
                            true,
                        )
                        .await?;
                    }
                }
            }
        }

        Ok(())
    }

    pub(super) async fn execute_concept_block(
        &self,
        concept_block: ConceptBlock,
        default_metadata: &Map<String, Json>,
        handle_map: &mut FxHashMap<String, EntityID>,
        cached_pks: &mut FxHashMap<EntityPK, EntityID>,
        dry_run: bool,
    ) -> Result<Option<EntityID>, KipError> {
        let concept_pk = ConceptPK::try_from(concept_block.concept)?;
        match &concept_pk {
            ConceptPK::ID(id) => {
                if !self.concepts.contains(*id) {
                    return Err(KipError::not_found(format!(
                        "Concept {} not found",
                        ConceptPK::ID(*id)
                    )));
                }
            }
            ConceptPK::Object { r#type, .. } => {
                self.ensure_concept_type_for_kml(r#type, cached_pks).await?;
            }
        }

        if let Some(local) = &concept_block.metadata {
            reject_reserved_metadata_keys(local.keys())?;
        }

        if let Some(attributes) = &concept_block.set_attributes {
            self.ensure_concept_attributes_mutable_for_kml(&concept_pk, attributes, cached_pks)
                .await?;
        }

        if let Some(propositions) = &concept_block.set_propositions {
            for set_prop in propositions {
                self.validate_set_proposition_for_kml(set_prop, handle_map, cached_pks)
                    .await?;
            }
        }

        // `EXPECT VERSION` guards are evaluated against statement-entry state
        // during the preflight pass (`dry_run == true`); a mismatch aborts the
        // whole UPSERT before any write occurs (KIP §2.11.2). The execution
        // pass runs under the same exclusive KML lock, so the guarded state
        // cannot change in between.
        if dry_run && let Some(expected) = concept_block.expect_version {
            let current = self
                .concept_version_for_guard(&concept_pk, cached_pks)
                .await?;
            if current != expected {
                return Err(KipError::version_conflict(format!(
                    "Concept {concept_pk} EXPECT VERSION {expected} does not match current _version {current}; the UPSERT was aborted"
                )));
            }
        }

        if dry_run {
            let entity_pk = EntityPK::Concept(concept_pk);
            let entity_id = cached_pks
                .get(&entity_pk)
                .cloned()
                .unwrap_or_else(|| self.next_dry_run_entity_id(cached_pks, None));
            cached_pks.insert(entity_pk, entity_id.clone());
            if let Some(handle) = concept_block.handle {
                handle_map.insert(handle, entity_id);
            }
            return Ok(None);
        }

        let attributes = concept_block
            .set_attributes
            .map(|val| val.into_iter().collect())
            .unwrap_or_default();
        let mut metadata = default_metadata.clone();
        if let Some(local) = concept_block.metadata {
            metadata.extend(local);
        }

        let entity_pk = EntityPK::Concept(concept_pk.clone());
        let entity_id = self
            .upsert_concept(concept_pk, attributes, metadata.clone())
            .await?;
        cached_pks.insert(entity_pk, entity_id.clone());

        if let Some(handle) = concept_block.handle {
            handle_map.insert(handle, entity_id.clone());
        }

        if let Some(propositions) = concept_block.set_propositions {
            for set_prop in propositions {
                self.execute_set_proposition(
                    &entity_id, set_prop, &metadata, handle_map, cached_pks,
                )
                .await?;
            }
        }

        Ok(Some(entity_id))
    }

    pub(super) async fn execute_proposition_block(
        &self,
        proposition_block: PropositionBlock,
        default_metadata: &Map<String, Json>,
        handle_map: &mut FxHashMap<String, EntityID>,
        cached_pks: &mut FxHashMap<EntityPK, EntityID>,
        dry_run: bool,
    ) -> Result<Option<EntityID>, KipError> {
        let proposition_pk = self
            .resolve_kml_proposition_pk(proposition_block.proposition, handle_map, cached_pks)
            .await?;

        if let Some(local) = &proposition_block.metadata {
            reject_reserved_metadata_keys(local.keys())?;
        }

        // See `execute_concept_block` for the guard-evaluation contract.
        if dry_run && let Some(expected) = proposition_block.expect_version {
            let current = self
                .proposition_version_for_guard(&proposition_pk, cached_pks)
                .await?;
            if current != expected {
                return Err(KipError::version_conflict(format!(
                    "Proposition {proposition_pk} EXPECT VERSION {expected} does not match current _version {current}; the UPSERT was aborted"
                )));
            }
        }

        if dry_run {
            let predicate = match &proposition_pk {
                PropositionPK::ID(_, predicate) | PropositionPK::Object { predicate, .. } => {
                    predicate.clone()
                }
            };
            let entity_pk = EntityPK::Proposition(proposition_pk);
            let entity_id = cached_pks
                .get(&entity_pk)
                .cloned()
                .unwrap_or_else(|| self.next_dry_run_entity_id(cached_pks, Some(predicate)));
            cached_pks.insert(entity_pk, entity_id.clone());
            if let Some(handle) = proposition_block.handle {
                handle_map.insert(handle, entity_id);
            }
            return Ok(None);
        }

        let attributes = proposition_block
            .set_attributes
            .map(|val| val.into_iter().collect())
            .unwrap_or_default();

        let mut metadata = default_metadata.clone();
        if let Some(local) = proposition_block.metadata {
            metadata.extend(local);
        }

        let entity_pk = EntityPK::Proposition(proposition_pk.clone());
        let entity_id = self
            .upsert_proposition(proposition_pk, attributes, metadata, cached_pks)
            .await?;
        cached_pks.insert(entity_pk, entity_id.clone());

        if let Some(handle) = proposition_block.handle {
            handle_map.insert(handle, entity_id.clone());
        }

        Ok(Some(entity_id))
    }

    pub(super) async fn execute_set_proposition(
        &self,
        subject: &EntityID,
        set_prop: SetProposition,
        default_metadata: &Map<String, Json>,
        handle_map: &FxHashMap<String, EntityID>,
        cached_pks: &mut FxHashMap<EntityPK, EntityID>,
    ) -> Result<EntityID, KipError> {
        self.ensure_proposition_type_for_kml(&set_prop.predicate, cached_pks)
            .await?;

        let object_id = self
            .resolve_target_term(set_prop.object, handle_map, cached_pks)
            .await?;

        let proposition_pk = PropositionPK::Object {
            subject: Box::new(subject.clone().into()),
            predicate: set_prop.predicate,
            object: Box::new(object_id.clone().into()),
        };

        let mut metadata = default_metadata.clone();
        if let Some(local) = set_prop.metadata {
            metadata.extend(local);
        }

        let entity_id = self
            .upsert_proposition(proposition_pk, Map::new(), metadata, cached_pks)
            .await?;

        Ok(entity_id)
    }

    /// Returns true if the concept identified by `(type, name)` belongs to the
    /// protected schema infrastructure per `KIP_3004` — meta-type definition
    /// nodes (`$ConceptType`, `$PropositionType`), the foundational `Domain`
    /// type and `belongs_to_domain` predicate definitions, and core domains
    /// (e.g. `CoreSchema`). System actors are covered separately by
    /// [`is_system_actor`](Self::is_system_actor) because their ordinary
    /// attributes are designed to evolve.
    pub(super) fn is_protected_schema_concept(r#type: &str, name: &str) -> bool {
        // Meta-type definition nodes and the foundational `Domain` type.
        if r#type == META_CONCEPT_TYPE
            && (name == META_CONCEPT_TYPE || name == META_PROPOSITION_TYPE || name == DOMAIN_TYPE)
        {
            return true;
        }
        // The foundational `belongs_to_domain` predicate definition.
        if r#type == META_PROPOSITION_TYPE && name == BELONGS_TO_DOMAIN_TYPE {
            return true;
        }
        // Core domains. The spec lists `CoreSchema` as a representative example.
        if r#type == DOMAIN_TYPE && name == "CoreSchema" {
            return true;
        }
        false
    }

    /// Returns true if the concept identified by `(type, name)` is fully
    /// system-protected per `KIP_3004` (DELETE CONCEPT / MERGE scope):
    /// protected schema infrastructure plus the system actor identity tuples
    /// (`$self`, `$system`).
    pub(super) fn is_protected_concept(r#type: &str, name: &str) -> bool {
        Self::is_protected_schema_concept(r#type, name) || Self::is_system_actor(r#type, name)
    }

    pub(super) fn is_system_actor(r#type: &str, name: &str) -> bool {
        r#type == PERSON_TYPE && (name == META_SELF_NAME || name == META_SYSTEM_NAME)
    }

    pub(super) fn immutable_core_directives_error(r#type: &str, name: &str) -> KipError {
        KipError::immutable_target(format!(
            "Concept {{type: \"{ty}\", name: \"{name}\"}} core_directives are system-protected and cannot be modified",
            ty = r#type
        ))
    }

    pub(super) async fn execute_delete(
        &self,
        delete_statement: DeleteStatement,
        dry_run: bool,
    ) -> Result<Json, KipError> {
        let result = match delete_statement {
            DeleteStatement::DeleteAttributes {
                attributes,
                target,
                where_clauses,
            } => {
                self.execute_delete_attributes(attributes, target, where_clauses, dry_run)
                    .await
            }
            DeleteStatement::DeleteMetadata {
                keys,
                target,
                where_clauses,
            } => {
                self.execute_delete_metadata(keys, target, where_clauses, dry_run)
                    .await
            }
            DeleteStatement::DeletePropositions {
                target,
                where_clauses,
            } => {
                self.execute_delete_propositions(target, where_clauses, dry_run)
                    .await
            }
            DeleteStatement::DeleteConcept {
                target,
                where_clauses,
            } => {
                self.execute_delete_concepts(target, where_clauses, dry_run)
                    .await
            }
        }?;

        if !dry_run {
            let now_ms = unix_ms();
            try_join!(self.concepts.flush(now_ms), self.propositions.flush(now_ms))
                .map_err(db_to_kip_error)?;
        }

        Ok(result)
    }

    pub(super) async fn execute_delete_attributes(
        &self,
        attributes: Vec<String>,
        target: String,
        where_clauses: Vec<WhereClause>,
        dry_run: bool,
    ) -> Result<Json, KipError> {
        let mut ctx = QueryContext::default();
        for clause in where_clauses {
            self.execute_where_clause(&mut ctx, clause).await?;
        }

        let target_entities = ctx.entities.get(&target).cloned().ok_or_else(|| {
            KipError::reference_error(format!("Target term '{}' not found in context", target))
        })?;

        if attributes.iter().any(|name| name == "core_directives") {
            for entity_id in target_entities.as_ref() {
                if let EntityID::Concept(id) = entity_id {
                    let (ty, name) = self
                        .try_get_concept_with(&ctx.cache, *id, |concept| {
                            Ok((concept.r#type.clone(), concept.name.clone()))
                        })
                        .await?;
                    if Self::is_system_actor(&ty, &name) {
                        return Err(Self::immutable_core_directives_error(&ty, &name));
                    }
                }
            }
        }

        if dry_run {
            return Ok(json!({
                "updated_concepts": 0,
                "updated_propositions": 0,
            }));
        }

        let mut updated_concepts: u64 = 0;
        let mut updated_propositions: u64 = 0;
        for entity_id in target_entities.as_ref() {
            match entity_id {
                EntityID::Concept(id) => {
                    if let Ok(mut concept) = self
                        .try_get_concept_with(&ctx.cache, *id, |concept| Ok(concept.clone()))
                        .await
                    {
                        let length = concept.attributes.len();
                        for attr in &attributes {
                            concept.attributes.remove(attr);
                        }
                        if concept.attributes.len() < length {
                            bump_system_metadata(&mut concept.metadata, unix_ms());
                            if self
                                .concepts
                                .update(
                                    *id,
                                    BTreeMap::from([
                                        ("attributes".to_string(), concept.attributes.into()),
                                        ("metadata".to_string(), concept.metadata.into()),
                                    ]),
                                )
                                .await
                                .is_ok()
                            {
                                // Invalidate stale cache entry so subsequent
                                // iterations on the same id (rare for concepts,
                                // but defensive) re-read the freshest version.
                                ctx.cache.concepts.write().remove(id);
                                updated_concepts += 1;
                            }
                        }
                    }
                }
                EntityID::Proposition(id, predicate) => {
                    if let Ok(mut proposition) = self
                        .try_get_proposition_with(&ctx.cache, *id, |prop| Ok(prop.clone()))
                        .await
                        && let Some(prop) = proposition.properties.get_mut(predicate)
                    {
                        let length = prop.attributes.len();
                        for attr in &attributes {
                            prop.attributes.remove(attr);
                        }

                        if prop.attributes.len() < length {
                            bump_system_metadata(&mut prop.metadata, unix_ms());
                            if self
                                .propositions
                                .update(
                                    *id,
                                    BTreeMap::from([(
                                        "properties".to_string(),
                                        proposition.properties.into(),
                                    )]),
                                )
                                .await
                                .is_ok()
                            {
                                // A single proposition may appear multiple times
                                // in target_entities (one (id, predicate) per
                                // predicate). Invalidate the cache so the next
                                // iteration sees the post-update state and does
                                // not resurrect already-removed attributes.
                                ctx.cache.propositions.write().remove(id);
                                updated_propositions += 1;
                            }
                        }
                    }
                }
            }
        }

        Ok(json!({
            "updated_concepts": updated_concepts,
            "updated_propositions": updated_propositions,
        }))
    }

    pub(super) async fn execute_delete_metadata(
        &self,
        keys: Vec<String>,
        target: String,
        where_clauses: Vec<WhereClause>,
        dry_run: bool,
    ) -> Result<Json, KipError> {
        // Reserved `_` metadata is engine-maintained and cannot be deleted by
        // KML (KIP §2.11.1). Checked before the dry-run short-circuit so
        // agents can probe for safety.
        reject_reserved_metadata_keys(keys.iter())?;

        if dry_run {
            return Ok(json!({
                "updated_concepts": 0,
                "updated_propositions": 0,
            }));
        }

        let mut ctx = QueryContext::default();
        for clause in where_clauses {
            self.execute_where_clause(&mut ctx, clause).await?;
        }

        let target_entities = ctx.entities.get(&target).cloned().ok_or_else(|| {
            KipError::reference_error(format!("Target term '{}' not found in context", target))
        })?;
        let mut updated_concepts: u64 = 0;
        let mut updated_propositions: u64 = 0;
        for entity_id in target_entities.as_ref() {
            match entity_id {
                EntityID::Concept(id) => {
                    if let Ok(mut concept) = self
                        .try_get_concept_with(&ctx.cache, *id, |concept| Ok(concept.clone()))
                        .await
                    {
                        let length = concept.metadata.len();
                        for name in &keys {
                            concept.metadata.remove(name);
                        }
                        if concept.metadata.len() < length {
                            bump_system_metadata(&mut concept.metadata, unix_ms());
                            if self
                                .concepts
                                .update(
                                    *id,
                                    BTreeMap::from([(
                                        "metadata".to_string(),
                                        concept.metadata.into(),
                                    )]),
                                )
                                .await
                                .is_ok()
                            {
                                ctx.cache.concepts.write().remove(id);
                                updated_concepts += 1;
                            }
                        }
                    }
                }
                EntityID::Proposition(id, predicate) => {
                    if let Ok(mut proposition) = self
                        .try_get_proposition_with(&ctx.cache, *id, |prop| Ok(prop.clone()))
                        .await
                        && let Some(prop) = proposition.properties.get_mut(predicate)
                    {
                        let length = prop.metadata.len();
                        for name in &keys {
                            prop.metadata.remove(name);
                        }

                        if prop.metadata.len() < length {
                            bump_system_metadata(&mut prop.metadata, unix_ms());
                            if self
                                .propositions
                                .update(
                                    *id,
                                    BTreeMap::from([(
                                        "properties".to_string(),
                                        proposition.properties.into(),
                                    )]),
                                )
                                .await
                                .is_ok()
                            {
                                // See execute_delete_attributes for rationale:
                                // the same proposition id may appear under
                                // multiple predicates in target_entities.
                                ctx.cache.propositions.write().remove(id);
                                updated_propositions += 1;
                            }
                        }
                    }
                }
            }
        }

        Ok(json!({
            "updated_concepts": updated_concepts,
            "updated_propositions": updated_propositions,
        }))
    }

    pub(super) async fn execute_delete_propositions(
        &self,
        target: String,
        where_clauses: Vec<WhereClause>,
        dry_run: bool,
    ) -> Result<Json, KipError> {
        if dry_run {
            return Ok(json!({
                "deleted_propositions": 0
            }));
        }

        let mut ctx = QueryContext::default();
        for clause in where_clauses {
            self.execute_where_clause(&mut ctx, clause).await?;
        }

        let target_entities = ctx.entities.get(&target).cloned().ok_or_else(|| {
            KipError::reference_error(format!("Target term '{}' not found in context", target))
        })?;

        let mut deleted_propositions: u64 = 0;
        for entity_id in target_entities.as_ref() {
            match entity_id {
                EntityID::Concept(_) => {
                    // ignore
                }
                EntityID::Proposition(id, predicate) => {
                    if let Ok(mut proposition) = self
                        .try_get_proposition_with(&ctx.cache, *id, |prop| Ok(prop.clone()))
                        .await
                    {
                        // Remove specified predicates
                        proposition.predicates.remove(predicate);
                        proposition.properties.remove(predicate);

                        // If no predicates left, delete the proposition
                        if proposition.predicates.is_empty() {
                            if self.propositions.remove(*id).await.is_ok() {
                                ctx.cache.propositions.write().remove(id);
                                deleted_propositions += 1;
                            }
                        } else {
                            // Otherwise, update the proposition with remaining predicates
                            if self
                                .propositions
                                .update(
                                    *id,
                                    BTreeMap::from([
                                        ("predicates".to_string(), proposition.predicates.into()),
                                        ("properties".to_string(), proposition.properties.into()),
                                    ]),
                                )
                                .await
                                .is_ok()
                            {
                                // CRITICAL: a single proposition row may be
                                // listed under multiple predicates in the
                                // target set. Without invalidating the cache,
                                // the next iteration would read the pre-update
                                // state and write back removed predicates,
                                // resurrecting them.
                                ctx.cache.propositions.write().remove(id);
                                deleted_propositions += 1;
                            }
                        }
                    }
                }
            }
        }

        Ok(json!({
            "deleted_propositions": deleted_propositions
        }))
    }

    pub(super) async fn execute_delete_concepts(
        &self,
        target: String,
        where_clauses: Vec<WhereClause>,
        dry_run: bool,
    ) -> Result<Json, KipError> {
        let mut ctx = QueryContext::default();
        for clause in where_clauses {
            self.execute_where_clause(&mut ctx, clause).await?;
        }

        let target_entities = ctx.entities.get(&target).cloned().ok_or_else(|| {
            KipError::reference_error(format!("Target term '{}' not found in context", target))
        })?;

        // Collect target concept ids and pre-flight protected-scope check (KIP_3004).
        // We must reject *before* performing any destructive work so the operation is
        // atomic w.r.t. protected nodes — and the same check applies to dry runs so
        // agents can probe for safety without side effects.
        let mut concept_ids: Vec<u64> = Vec::new();
        for entity_id in target_entities.as_ref() {
            if let EntityID::Concept(id) = entity_id {
                if let Ok((ty, name)) = self
                    .try_get_concept_with(&ctx.cache, *id, |c| {
                        Ok((c.r#type.clone(), c.name.clone()))
                    })
                    .await
                    && Self::is_protected_concept(&ty, &name)
                {
                    return Err(KipError::immutable_target(format!(
                        "Concept {{type: \"{ty}\", name: \"{name}\"}} is system-protected and cannot be deleted",
                    )));
                }
                concept_ids.push(*id);
            }
            // EntityID::Proposition is silently ignored (DELETE CONCEPT only deletes
            // concepts; proposition targets must use DELETE PROPOSITIONS).
        }

        if dry_run {
            return Ok(json!({
                "deleted_propositions": 0,
                "deleted_concepts": 0
            }));
        }

        // Compute the transitive cascade closure: every proposition whose subject
        // or object refers (directly or via higher-order chains) to one of the
        // concepts being deleted must also be removed so no dangling references
        // remain after a DETACH (KIP v1.0-RC7 §4.2.4).
        let mut to_delete_proposition_ids: BTreeSet<u64> = BTreeSet::new();
        let mut frontier: Vec<EntityID> = concept_ids
            .iter()
            .map(|id| EntityID::Concept(*id))
            .collect();

        while !frontier.is_empty() {
            let mut filters: Vec<Box<Filter>> = Vec::with_capacity(frontier.len() * 2);
            for eid in &frontier {
                let v: Fv = eid.to_string().into();
                filters.push(Box::new(Filter::Field((
                    "subject".to_string(),
                    RangeQuery::Eq(v.clone()),
                ))));
                filters.push(Box::new(Filter::Field((
                    "object".to_string(),
                    RangeQuery::Eq(v),
                ))));
            }
            let filter = if filters.len() == 1 {
                *filters.into_iter().next().unwrap()
            } else {
                Filter::Or(filters)
            };

            let ids = self
                .propositions
                .query_ids(filter, None)
                .await
                .unwrap_or_default();

            let mut next: Vec<EntityID> = Vec::new();
            for id in ids {
                if to_delete_proposition_ids.insert(id)
                    && let Ok(predicates) = self
                        .try_get_proposition_with(&ctx.cache, id, |p| {
                            Ok(p.predicates.iter().cloned().collect::<Vec<_>>())
                        })
                        .await
                {
                    // Newly discovered proposition — enqueue all of its EntityID
                    // forms (one per predicate) so higher-order propositions that
                    // reference it can be picked up on the next iteration.
                    for pred in predicates {
                        next.push(EntityID::Proposition(id, pred));
                    }
                }
            }
            frontier = next;
        }

        let mut deleted_propositions: u64 = 0;
        for id in to_delete_proposition_ids {
            if self.propositions.remove(id).await.is_ok() {
                deleted_propositions += 1;
            }
        }

        let mut deleted_concepts: u64 = 0;
        for id in concept_ids {
            if self.concepts.remove(id).await.is_ok() {
                deleted_concepts += 1;
            }
        }

        Ok(json!({
            "deleted_propositions": deleted_propositions,
            "deleted_concepts": deleted_concepts
        }))
    }

    /// Executes an `UPDATE` statement — pattern-matched bulk mutation of
    /// existing concept nodes or proposition links (KIP §4.3). Unlike
    /// `UPSERT` it never creates elements; `SET ATTRIBUTES` / `SET METADATA`
    /// follow shallow-merge semantics and value positions may be numeric
    /// update expressions computed per element from its own current state.
    pub(super) async fn execute_update(
        &self,
        statement: UpdateStatement,
        dry_run: bool,
    ) -> Result<Json, KipError> {
        let UpdateStatement {
            target,
            set_attributes,
            set_metadata,
            where_clauses,
            limit,
        } = statement;

        let set_attributes = set_attributes.unwrap_or_default();
        let set_metadata = set_metadata.unwrap_or_default();
        if set_attributes.is_empty() && set_metadata.is_empty() {
            return Err(KipError::invalid_syntax(
                "UPDATE requires at least one SET ATTRIBUTES or SET METADATA block",
            ));
        }
        // `SET METADATA` writes author-asserted metadata only; the reserved
        // `_` namespace is engine-maintained (KIP §2.11.1).
        reject_reserved_metadata_keys(set_metadata.iter().map(|(k, _)| k))?;
        // Update expressions may only address the UPDATE target itself, on
        // `attributes.*` / `metadata.*` paths, so every element's new value
        // is computable from its own state (KIP §4.3).
        for (_, value) in set_attributes.iter().chain(set_metadata.iter()) {
            if let UpdateValue::Expr(expr) = value {
                for path in expr.referenced_paths() {
                    if path.var != target {
                        return Err(KipError::invalid_syntax(format!(
                            "Update expression path {path} must be on the UPDATE target ?{target}"
                        )));
                    }
                    if !matches!(
                        path.path.first().map(|s| s.as_str()),
                        Some("attributes") | Some("metadata")
                    ) {
                        return Err(KipError::invalid_syntax(format!(
                            "Update expression path {path} must address `attributes.*` or `metadata.*`"
                        )));
                    }
                }
            }
        }

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
            // LIMIT is a blast-radius guard; without ORDER BY semantics the
            // selection of capped elements is implementation-defined.
            targets.truncate(limit);
        }

        // Protected-scope preflight (KIP_3004): the whole statement fails
        // before any write when a protected structure is matched. Ordinary
        // attributes of `$self` / `$system` may evolve, but their
        // `core_directives` stay immutable.
        let set_core_directives = set_attributes.iter().any(|(k, _)| k == "core_directives");
        for entity_id in &targets {
            if let EntityID::Concept(id) = entity_id {
                let (ty, name) = self
                    .try_get_concept_with(&ctx.cache, *id, |c| {
                        Ok((c.r#type.clone(), c.name.clone()))
                    })
                    .await?;
                if Self::is_protected_schema_concept(&ty, &name) {
                    return Err(KipError::immutable_target(format!(
                        "Concept {{type: \"{ty}\", name: \"{name}\"}} is system-protected; narrow the WHERE pattern"
                    )));
                }
                if set_core_directives && Self::is_system_actor(&ty, &name) {
                    return Err(Self::immutable_core_directives_error(&ty, &name));
                }
            }
        }

        let matched = targets.len() as u64;
        if dry_run {
            return Ok(json!({ "updated": 0, "matched": matched }));
        }

        let now_ms = unix_ms();
        let mut updated: u64 = 0;
        for entity_id in &targets {
            match entity_id {
                EntityID::Concept(id) => {
                    let mut concept = self
                        .try_get_concept_with(&ctx.cache, *id, |c| Ok(c.clone()))
                        .await?;
                    let attributes = Self::evaluate_update_values(&set_attributes, |path| {
                        extract_concept_field_value(&concept, &path.path).unwrap_or(Json::Null)
                    });
                    let metadata = Self::evaluate_update_values(&set_metadata, |path| {
                        extract_concept_field_value(&concept, &path.path).unwrap_or(Json::Null)
                    });
                    if attributes.is_empty() && metadata.is_empty() {
                        continue; // every key skipped for this element
                    }
                    concept.attributes.extend(attributes);
                    concept.metadata.extend(metadata);
                    bump_system_metadata(&mut concept.metadata, now_ms);
                    if self
                        .concepts
                        .update(
                            *id,
                            BTreeMap::from([
                                ("attributes".to_string(), concept.attributes.into()),
                                ("metadata".to_string(), concept.metadata.into()),
                            ]),
                        )
                        .await
                        .is_ok()
                    {
                        ctx.cache.concepts.write().remove(id);
                        updated += 1;
                    }
                }
                EntityID::Proposition(id, predicate) => {
                    let mut proposition = self
                        .try_get_proposition_with(&ctx.cache, *id, |p| Ok(p.clone()))
                        .await?;
                    if !proposition.predicates.contains(predicate) {
                        continue;
                    }
                    let attributes = Self::evaluate_update_values(&set_attributes, |path| {
                        extract_proposition_field_value(&proposition, predicate, &path.path)
                            .unwrap_or(Json::Null)
                    });
                    let metadata = Self::evaluate_update_values(&set_metadata, |path| {
                        extract_proposition_field_value(&proposition, predicate, &path.path)
                            .unwrap_or(Json::Null)
                    });
                    if attributes.is_empty() && metadata.is_empty() {
                        continue;
                    }
                    let prop = proposition.properties.entry(predicate.clone()).or_default();
                    prop.attributes.extend(attributes);
                    prop.metadata.extend(metadata);
                    bump_system_metadata(&mut prop.metadata, now_ms);
                    if self
                        .propositions
                        .update(
                            *id,
                            BTreeMap::from([(
                                "properties".to_string(),
                                proposition.properties.into(),
                            )]),
                        )
                        .await
                        .is_ok()
                    {
                        // The same proposition row may appear under multiple
                        // predicates in the target set; invalidate so the next
                        // iteration reads the post-update state.
                        ctx.cache.propositions.write().remove(id);
                        updated += 1;
                    }
                }
            }
        }

        let now_ms = unix_ms();
        try_join!(self.concepts.flush(now_ms), self.propositions.flush(now_ms))
            .map_err(db_to_kip_error)?;

        Ok(json!({ "updated": updated, "matched": matched }))
    }

    /// Evaluates the value positions of an UPDATE `SET` block for one
    /// element. Plain JSON values pass through; expression results of `null`
    /// skip that key for the element (KIP §4.3).
    pub(super) fn evaluate_update_values<F>(
        values: &[(String, UpdateValue)],
        resolve: F,
    ) -> Map<String, Json>
    where
        F: Fn(&DotPathVar) -> Json,
    {
        let mut rt = Map::new();
        for (key, value) in values {
            match value {
                UpdateValue::Json(v) => {
                    rt.insert(key.clone(), v.clone());
                }
                UpdateValue::Expr(expr) => {
                    let v = expr.evaluate(&resolve);
                    if !v.is_null() {
                        rt.insert(key.clone(), v);
                    }
                }
            }
        }
        rt
    }

    /// Executes a `MERGE CONCEPT ?source INTO ?target` statement — atomic
    /// entity consolidation (KIP §4.4): repoints all links, fills missing
    /// attributes (target wins; `aliases` unioned), deletes the source, and
    /// records `_merged_from` provenance.
    pub(super) async fn execute_merge(
        &self,
        statement: MergeStatement,
        dry_run: bool,
    ) -> Result<Json, KipError> {
        let MergeStatement {
            source,
            target,
            where_clauses,
        } = statement;

        let mut ctx = QueryContext::default();
        for clause in where_clauses {
            self.execute_where_clause(&mut ctx, clause).await?;
        }

        let source_id = Self::single_merge_concept(&ctx, &source)?;
        let target_id = Self::single_merge_concept(&ctx, &target)?;

        if source_id == target_id {
            // Source and target bind the same node: no-op success (KIP §4.4).
            return Ok(json!({
                "merged": true,
                "links_repointed": 0,
                "links_deduplicated": 0,
                "attributes_filled": 0,
            }));
        }

        let source_concept: Concept = self
            .concepts
            .get_as(source_id)
            .await
            .map_err(db_to_kip_error)?;
        let target_concept: Concept = self
            .concepts
            .get_as(target_id)
            .await
            .map_err(db_to_kip_error)?;
        if source_concept.r#type != target_concept.r#type {
            return Err(KipError::constraint_violation(format!(
                "MERGE across differing concept types: ?{source} is {:?}, ?{target} is {:?}",
                source_concept.r#type, target_concept.r#type
            )));
        }
        for concept in [&source_concept, &target_concept] {
            if Self::is_protected_concept(&concept.r#type, &concept.name) {
                return Err(KipError::immutable_target(format!(
                    "Concept {{type: \"{}\", name: \"{}\"}} is system-protected and cannot be merged",
                    concept.r#type, concept.name
                )));
            }
        }

        if dry_run {
            return Ok(json!({
                "merged": false,
                "links_repointed": 0,
                "links_deduplicated": 0,
                "attributes_filled": 0,
            }));
        }

        // 1. Repoint every link in which the source appears as an endpoint.
        let (links_repointed, links_deduplicated) = self
            .repoint_entity_links(EntityID::Concept(source_id), EntityID::Concept(target_id))
            .await?;

        // 2. Fill attributes: keys present on the source but absent on the
        //    target are copied; on conflict the target wins. `aliases` arrays
        //    are unioned and the source's name is appended so old grounding
        //    paths survive the merge.
        let mut target_concept = target_concept;
        let mut attributes_filled: u64 = 0;
        for (key, value) in &source_concept.attributes {
            if key == "aliases" {
                continue;
            }
            if !target_concept.attributes.contains_key(key) {
                target_concept.attributes.insert(key.clone(), value.clone());
                attributes_filled += 1;
            }
        }
        let mut aliases: Vec<Json> = match target_concept.attributes.get("aliases") {
            Some(Json::Array(values)) => values.clone(),
            _ => Vec::new(),
        };
        let mut aliases_changed = false;
        if let Some(Json::Array(source_aliases)) = source_concept.attributes.get("aliases") {
            for alias in source_aliases {
                if !aliases.contains(alias) {
                    aliases.push(alias.clone());
                    aliases_changed = true;
                }
            }
        }
        let source_name = Json::String(source_concept.name.clone());
        if !aliases.contains(&source_name) {
            aliases.push(source_name);
            aliases_changed = true;
        }
        if aliases_changed {
            target_concept
                .attributes
                .insert("aliases".to_string(), Json::Array(aliases));
            attributes_filled += 1;
        }

        // 3. Record `_merged_from` provenance, then delete the source.
        let mut merged_from = match target_concept.metadata.get(METADATA_MERGED_FROM) {
            Some(Json::Array(values)) => values.clone(),
            _ => Vec::new(),
        };
        merged_from.push(Json::String(format!(
            "{}:{}",
            source_concept.r#type, source_concept.name
        )));
        target_concept
            .metadata
            .insert(METADATA_MERGED_FROM.to_string(), Json::Array(merged_from));
        bump_system_metadata(&mut target_concept.metadata, unix_ms());

        self.concepts
            .update(
                target_id,
                BTreeMap::from([
                    ("attributes".to_string(), target_concept.attributes.into()),
                    ("metadata".to_string(), target_concept.metadata.into()),
                ]),
            )
            .await
            .map_err(db_to_kip_error)?;

        self.concepts
            .remove(source_id)
            .await
            .map_err(db_to_kip_error)?;

        let now_ms = unix_ms();
        try_join!(self.concepts.flush(now_ms), self.propositions.flush(now_ms))
            .map_err(db_to_kip_error)?;

        Ok(json!({
            "merged": true,
            "links_repointed": links_repointed,
            "links_deduplicated": links_deduplicated,
            "attributes_filled": attributes_filled,
        }))
    }

    /// Resolves a MERGE variable to exactly one concept node id:
    /// zero matches → `KIP_3002`, more than one → `KIP_3003`, a proposition
    /// binding → `KIP_2002`.
    pub(super) fn single_merge_concept(ctx: &QueryContext, var: &str) -> Result<u64, KipError> {
        let ids = ctx.entities.get(var).ok_or_else(|| {
            KipError::reference_error(format!("Variable ?{var} not bound in WHERE clause"))
        })?;
        if ids.is_empty() {
            return Err(KipError::not_found(format!(
                "MERGE variable ?{var} matched no concept node"
            )));
        }
        if ids.len() > 1 {
            return Err(KipError::duplicate_exists(format!(
                "MERGE variable ?{var} matched {} nodes; narrow the WHERE pattern until it matches exactly one",
                ids.len()
            )));
        }
        match &ids.as_ref()[0] {
            EntityID::Concept(id) => Ok(*id),
            other => Err(KipError::constraint_violation(format!(
                "MERGE CONCEPT requires concept nodes, but ?{var} bound {other}"
            ))),
        }
    }

    /// Repoints every proposition link in which `old` appears as subject or
    /// object to `new`. Plain repointing preserves the link's row (and
    /// therefore its id); a row whose new `(subject, object)` pair collides
    /// with an existing row is merged into it under the (S, P, O) uniqueness
    /// constraint — the surviving link's keys win, and higher-order
    /// references to the dropped link ids are repointed in turn (worklist).
    /// Links that would become self-referential are removed together with
    /// the higher-order propositions that referenced them.
    ///
    /// Returns `(links_repointed, links_deduplicated)`.
    pub(super) async fn repoint_entity_links(
        &self,
        old: EntityID,
        new: EntityID,
    ) -> Result<(u64, u64), KipError> {
        let mut links_repointed: u64 = 0;
        let mut links_deduplicated: u64 = 0;
        let mut worklist: Vec<(EntityID, EntityID)> = vec![(old, new)];
        let now_ms = unix_ms();

        while let Some((old_eid, new_eid)) = worklist.pop() {
            let old_fv: Fv = old_eid.to_string().into();
            let row_ids = self
                .propositions
                .query_ids(
                    Filter::Or(vec![
                        Box::new(Filter::Field((
                            "subject".to_string(),
                            RangeQuery::Eq(old_fv.clone()),
                        ))),
                        Box::new(Filter::Field((
                            "object".to_string(),
                            RangeQuery::Eq(old_fv),
                        ))),
                    ]),
                    None,
                )
                .await
                .map_err(db_to_kip_error)?;

            for row_id in row_ids {
                let mut row: Proposition = match self.propositions.get_as(row_id).await {
                    Ok(row) => row,
                    Err(_) => continue, // already merged away by an earlier step
                };
                let mut subject = row.subject.clone();
                let mut object = row.object.clone();
                if subject == old_eid {
                    subject = new_eid.clone();
                }
                if object == old_eid {
                    object = new_eid.clone();
                }
                if subject == row.subject && object == row.object {
                    continue; // stale index hit
                }

                if subject == object {
                    // The link tied `old` directly to `new`; after the merge
                    // it would be self-referential. Drop it, cascading to the
                    // higher-order propositions that referenced it.
                    links_deduplicated += row.predicates.len() as u64;
                    let seeds: Vec<EntityID> = row
                        .predicates
                        .iter()
                        .map(|pred| EntityID::Proposition(row_id, pred.clone()))
                        .collect();
                    self.propositions
                        .remove(row_id)
                        .await
                        .map_err(db_to_kip_error)?;
                    links_deduplicated += self.cascade_remove_propositions(seeds).await?;
                    continue;
                }

                // (S, P, O) uniqueness: a row with the new pair may already exist.
                let virtual_name = virtual_field_name(&["subject", "object"]);
                let virtual_val = virtual_field_value(&[
                    Some(&Fv::Text(subject.to_string())),
                    Some(&Fv::Text(object.to_string())),
                ])
                .unwrap();
                let existing = self
                    .propositions
                    .query_ids(
                        Filter::Field((virtual_name, RangeQuery::Eq(virtual_val))),
                        None,
                    )
                    .await
                    .map_err(db_to_kip_error)?
                    .into_iter()
                    .find(|id| *id != row_id);

                match existing {
                    None => {
                        // Simple repoint: the row and all its link ids survive.
                        // Endpoint repointing is a mutation of each link
                        // element (KIP §2.11.1), so versions advance.
                        for pred in row.predicates.clone() {
                            let prop = row.properties.entry(pred).or_default();
                            bump_system_metadata(&mut prop.metadata, now_ms);
                        }
                        links_repointed += row.predicates.len() as u64;
                        self.propositions
                            .update(
                                row_id,
                                BTreeMap::from([
                                    ("subject".to_string(), Fv::Text(subject.to_string())),
                                    ("object".to_string(), Fv::Text(object.to_string())),
                                    ("properties".to_string(), row.properties.into()),
                                ]),
                            )
                            .await
                            .map_err(db_to_kip_error)?;
                    }
                    Some(dst_row_id) => {
                        let mut dst_row: Proposition = self
                            .propositions
                            .get_as(dst_row_id)
                            .await
                            .map_err(db_to_kip_error)?;
                        for pred in row.predicates.iter() {
                            let src_props = row.properties.get(pred).cloned().unwrap_or_default();
                            if dst_row.predicates.contains(pred) {
                                // Duplicate under (S, P, O): the target's link
                                // survives; fill its missing keys from the
                                // source's link (reserved `_` bookkeeping is
                                // not copied).
                                let dst_props = dst_row.properties.entry(pred.clone()).or_default();
                                let mut changed = false;
                                for (k, v) in src_props.attributes {
                                    if !dst_props.attributes.contains_key(&k) {
                                        dst_props.attributes.insert(k, v);
                                        changed = true;
                                    }
                                }
                                for (k, v) in src_props.metadata {
                                    if is_reserved_metadata_key(&k) {
                                        continue;
                                    }
                                    if !dst_props.metadata.contains_key(&k) {
                                        dst_props.metadata.insert(k, v);
                                        changed = true;
                                    }
                                }
                                if changed {
                                    bump_system_metadata(&mut dst_props.metadata, now_ms);
                                }
                                links_deduplicated += 1;
                            } else {
                                // Move the link onto the surviving row. Its id
                                // changes, so higher-order references are
                                // repointed via the worklist below.
                                let mut props = src_props;
                                bump_system_metadata(&mut props.metadata, now_ms);
                                dst_row.predicates.insert(pred.clone());
                                dst_row.properties.insert(pred.clone(), props);
                                links_repointed += 1;
                            }
                            worklist.push((
                                EntityID::Proposition(row_id, pred.clone()),
                                EntityID::Proposition(dst_row_id, pred.clone()),
                            ));
                        }
                        self.propositions
                            .update(
                                dst_row_id,
                                BTreeMap::from([
                                    ("predicates".to_string(), dst_row.predicates.into()),
                                    ("properties".to_string(), dst_row.properties.into()),
                                ]),
                            )
                            .await
                            .map_err(db_to_kip_error)?;
                        self.propositions
                            .remove(row_id)
                            .await
                            .map_err(db_to_kip_error)?;
                    }
                }
            }
        }

        Ok((links_repointed, links_deduplicated))
    }

    /// Removes every proposition row that (transitively) references one of
    /// the seed link ids as subject or object, returning the number of
    /// removed links.
    pub(super) async fn cascade_remove_propositions(
        &self,
        seeds: Vec<EntityID>,
    ) -> Result<u64, KipError> {
        let mut removed_links: u64 = 0;
        let mut frontier = seeds;
        let mut visited_rows: FxHashSet<u64> = FxHashSet::default();

        while !frontier.is_empty() {
            let mut filters: Vec<Box<Filter>> = Vec::with_capacity(frontier.len() * 2);
            for eid in &frontier {
                let v: Fv = eid.to_string().into();
                filters.push(Box::new(Filter::Field((
                    "subject".to_string(),
                    RangeQuery::Eq(v.clone()),
                ))));
                filters.push(Box::new(Filter::Field((
                    "object".to_string(),
                    RangeQuery::Eq(v),
                ))));
            }
            let filter = if filters.len() == 1 {
                *filters.into_iter().next().unwrap()
            } else {
                Filter::Or(filters)
            };
            let ids = self
                .propositions
                .query_ids(filter, None)
                .await
                .unwrap_or_default();

            let mut next: Vec<EntityID> = Vec::new();
            for id in ids {
                if !visited_rows.insert(id) {
                    continue;
                }
                if let Ok(row) = self.propositions.get_as::<Proposition>(id).await {
                    removed_links += row.predicates.len() as u64;
                    for pred in row.predicates.iter() {
                        next.push(EntityID::Proposition(id, pred.clone()));
                    }
                    let _ = self.propositions.remove(id).await;
                }
            }
            frontier = next;
        }

        Ok(removed_links)
    }

    pub(super) async fn upsert_concept(
        &self,
        pk: ConceptPK,
        attributes: Map<String, Json>,
        metadata: Map<String, Json>,
    ) -> Result<EntityID, KipError> {
        match pk {
            ConceptPK::ID(id) => {
                self.update_concept(id, attributes, metadata).await?;
                Ok(EntityID::Concept(id))
            }
            ConceptPK::Object { r#type, name } => {
                if let Ok(id) = self.query_concept_id(&r#type, &name).await {
                    self.update_concept(id, attributes, metadata).await?;
                    return Ok(EntityID::Concept(id));
                }

                let mut metadata = metadata;
                init_system_metadata(&mut metadata, unix_ms());
                let concept = Concept {
                    _id: 0, // Will be set by the database
                    r#type,
                    name,
                    attributes,
                    metadata,
                };
                let id = self
                    .concepts
                    .add_from(&concept)
                    .await
                    .map_err(db_to_kip_error)?;
                Ok(EntityID::Concept(id))
            }
        }
    }

    pub(super) async fn upsert_proposition(
        &self,
        pk: PropositionPK,
        attributes: Map<String, Json>,
        metadata: Map<String, Json>,
        cached_pks: &mut FxHashMap<EntityPK, EntityID>,
    ) -> Result<EntityID, KipError> {
        let predicate_name = match &pk {
            PropositionPK::ID(_, predicate) | PropositionPK::Object { predicate, .. } => {
                predicate.as_str()
            }
        };
        self.ensure_proposition_type_for_kml(predicate_name, cached_pks)
            .await?;

        match pk {
            PropositionPK::ID(id, predicate) => {
                self.update_proposition(id, predicate.clone(), attributes, metadata)
                    .await?;
                Ok(EntityID::Proposition(id, predicate))
            }
            PropositionPK::Object {
                subject,
                predicate,
                object,
            } => {
                // Convert EntityPK to EntityID for searching
                let subject = self.resolve_entity_id(subject.as_ref(), cached_pks).await?;
                let object = self.resolve_entity_id(object.as_ref(), cached_pks).await?;
                if subject == object {
                    return Err(KipError::invalid_syntax(format!(
                        "Subject and object cannot be the same: {}",
                        subject
                    )));
                }

                let virtual_name = virtual_field_name(&["subject", "object"]);
                let virtual_val = virtual_field_value(&[
                    Some(&Fv::Text(subject.to_string())),
                    Some(&Fv::Text(object.to_string())),
                ])
                .unwrap();

                let ids = self
                    .propositions
                    .query_ids(
                        Filter::Field((virtual_name, RangeQuery::Eq(virtual_val))),
                        None,
                    )
                    .await
                    .map_err(db_to_kip_error)?;

                if let Some(id) = ids.first() {
                    // Proposition exists, update it
                    self.update_proposition(*id, predicate.clone(), attributes, metadata)
                        .await?;
                    return Ok(EntityID::Proposition(*id, predicate));
                }

                // Create new proposition
                let mut metadata = metadata;
                init_system_metadata(&mut metadata, unix_ms());
                let predicates = BTreeSet::from([predicate.clone()]);
                let properties = BTreeMap::from([(
                    predicate.clone(),
                    Properties {
                        attributes,
                        metadata,
                    },
                )]);

                let proposition = Proposition {
                    _id: 0, // Will be set by the database
                    subject,
                    object,
                    predicates,
                    properties,
                };

                let id = self
                    .propositions
                    .add_from(&proposition)
                    .await
                    .map_err(db_to_kip_error)?;
                Ok(EntityID::Proposition(id, predicate))
            }
        }
    }

    pub(super) async fn update_concept(
        &self,
        id: u64,
        attributes: Map<String, Json>,
        metadata: Map<String, Json>,
    ) -> Result<(), KipError> {
        if !self.concepts.contains(id) {
            return Err(KipError::not_found(format!(
                "Concept {} not found",
                ConceptPK::ID(id)
            )));
        }

        // nothing to update
        if attributes.is_empty() && metadata.is_empty() {
            return Ok(());
        }

        let concept: Concept = self.concepts.get_as(id).await.map_err(db_to_kip_error)?;
        if attributes.contains_key("core_directives")
            && Self::is_system_actor(&concept.r#type, &concept.name)
        {
            return Err(Self::immutable_core_directives_error(
                &concept.r#type,
                &concept.name,
            ));
        }

        let mut update_fields: BTreeMap<String, Fv> = BTreeMap::new();
        if !attributes.is_empty() {
            let mut fv = concept.attributes;
            fv.extend(attributes);
            update_fields.insert("attributes".to_string(), fv.into());
        }
        // The element is mutated, so the engine-maintained `_version` /
        // `_updated_at` advance even when only attributes changed.
        let mut fv = concept.metadata;
        fv.extend(metadata);
        bump_system_metadata(&mut fv, unix_ms());
        update_fields.insert("metadata".to_string(), fv.into());
        self.concepts
            .update(id, update_fields)
            .await
            .map_err(db_to_kip_error)?;

        Ok(())
    }

    pub(super) async fn update_proposition(
        &self,
        id: u64,
        predicate: String,
        attributes: Map<String, Json>,
        metadata: Map<String, Json>,
    ) -> Result<(), KipError> {
        if !self.propositions.contains(id) {
            return Err(KipError::not_found(format!(
                "Proposition {} not found",
                PropositionPK::ID(id, predicate)
            )));
        }

        let proposition: Proposition = self
            .propositions
            .get_as(id)
            .await
            .map_err(db_to_kip_error)?;
        if proposition.predicates.contains(&predicate)
            && attributes.is_empty()
            && metadata.is_empty()
        {
            return Ok(());
        }

        let mut update_fields: BTreeMap<String, Fv> = BTreeMap::new();
        let mut predicates = proposition.predicates;
        let created = predicates.insert(predicate.clone());
        if created {
            update_fields.insert("predicates".to_string(), predicates.into());
        }

        let mut properties = proposition.properties;
        let prop = properties.entry(predicate).or_default();
        prop.attributes.extend(attributes);
        prop.metadata.extend(metadata);
        if created {
            // A new (subject, predicate, object) link element is born.
            init_system_metadata(&mut prop.metadata, unix_ms());
        } else {
            bump_system_metadata(&mut prop.metadata, unix_ms());
        }
        update_fields.insert("properties".to_string(), properties.into());

        self.propositions
            .update(id, update_fields)
            .await
            .map_err(db_to_kip_error)?;

        Ok(())
    }

    pub(super) async fn ensure_concept_type_for_kml(
        &self,
        type_name: &str,
        cached_pks: &FxHashMap<EntityPK, EntityID>,
    ) -> Result<(), KipError> {
        if type_name == META_CONCEPT_TYPE
            || self
                .has_concept(&ConceptPK::Object {
                    r#type: META_CONCEPT_TYPE.to_string(),
                    name: type_name.to_string(),
                })
                .await
            || cached_pks.contains_key(&EntityPK::Concept(ConceptPK::Object {
                r#type: META_CONCEPT_TYPE.to_string(),
                name: type_name.to_string(),
            }))
        {
            Ok(())
        } else {
            Err(KipError::type_mismatch(format!(
                "Concept type {type_name} is not defined"
            )))
        }
    }

    pub(super) async fn ensure_proposition_type_for_kml(
        &self,
        predicate: &str,
        cached_pks: &FxHashMap<EntityPK, EntityID>,
    ) -> Result<(), KipError> {
        if self
            .has_concept(&ConceptPK::Object {
                r#type: META_PROPOSITION_TYPE.to_string(),
                name: predicate.to_string(),
            })
            .await
            || cached_pks.contains_key(&EntityPK::Concept(ConceptPK::Object {
                r#type: META_PROPOSITION_TYPE.to_string(),
                name: predicate.to_string(),
            }))
        {
            Ok(())
        } else {
            Err(KipError::type_mismatch(format!(
                "Proposition type {predicate} is not defined"
            )))
        }
    }

    pub(super) async fn ensure_concept_attributes_mutable_for_kml(
        &self,
        pk: &ConceptPK,
        attributes: &Map<String, Json>,
        cached_pks: &FxHashMap<EntityPK, EntityID>,
    ) -> Result<(), KipError> {
        if !attributes.contains_key("core_directives") {
            return Ok(());
        }

        match pk {
            ConceptPK::ID(id) => {
                let concept: Concept = self.concepts.get_as(*id).await.map_err(db_to_kip_error)?;
                if Self::is_system_actor(&concept.r#type, &concept.name) {
                    return Err(Self::immutable_core_directives_error(
                        &concept.r#type,
                        &concept.name,
                    ));
                }
            }
            ConceptPK::Object { r#type, name } => {
                if Self::is_system_actor(r#type, name)
                    && (self.has_concept(pk).await
                        || cached_pks.contains_key(&EntityPK::Concept(pk.clone())))
                {
                    return Err(Self::immutable_core_directives_error(r#type, name));
                }
            }
        }

        Ok(())
    }

    pub(super) fn next_dry_run_entity_id(
        &self,
        cached_pks: &FxHashMap<EntityPK, EntityID>,
        predicate: Option<String>,
    ) -> EntityID {
        let id = u64::MAX.saturating_sub(cached_pks.len() as u64);
        match predicate {
            Some(predicate) => EntityID::Proposition(id, predicate),
            None => EntityID::Concept(id),
        }
    }

    pub(super) async fn validate_set_proposition_for_kml(
        &self,
        set_prop: &SetProposition,
        handle_map: &FxHashMap<String, EntityID>,
        cached_pks: &mut FxHashMap<EntityPK, EntityID>,
    ) -> Result<(), KipError> {
        self.ensure_proposition_type_for_kml(&set_prop.predicate, cached_pks)
            .await?;
        if let Some(metadata) = &set_prop.metadata {
            reject_reserved_metadata_keys(metadata.keys())?;
        }
        self.validate_target_term_for_kml(&set_prop.object, handle_map, cached_pks)
            .await
    }

    pub(super) async fn validate_target_term_for_kml(
        &self,
        target: &TargetTerm,
        handle_map: &FxHashMap<String, EntityID>,
        cached_pks: &mut FxHashMap<EntityPK, EntityID>,
    ) -> Result<(), KipError> {
        match target {
            TargetTerm::Variable(handle) => {
                if !handle_map.contains_key(handle) {
                    return Err(KipError::reference_error(format!(
                        "Undefined handle: {handle}"
                    )));
                }
            }
            TargetTerm::Concept(_) | TargetTerm::Proposition(_) => {
                self.resolve_target_term(target.clone(), handle_map, cached_pks)
                    .await?;
            }
        }

        Ok(())
    }

    pub(super) async fn resolve_kml_proposition_pk(
        &self,
        matcher: PropositionMatcher,
        handle_map: &FxHashMap<String, EntityID>,
        cached_pks: &mut FxHashMap<EntityPK, EntityID>,
    ) -> Result<PropositionPK, KipError> {
        match matcher {
            PropositionMatcher::ID(id) => {
                let id = EntityID::from_str(&id).map_err(KipError::invalid_syntax)?;
                match id {
                    EntityID::Proposition(id, predicate) => {
                        self.ensure_proposition_type_for_kml(&predicate, cached_pks)
                            .await?;
                        if !self.propositions.contains(id) {
                            return Err(KipError::not_found(format!(
                                "Proposition {} not found",
                                PropositionPK::ID(id, predicate)
                            )));
                        }
                        Ok(PropositionPK::ID(id, predicate))
                    }
                    _ => Err(KipError::invalid_syntax(format!(
                        "PropositionMatcher::ID must be a Proposition ID, got: {id:?}"
                    ))),
                }
            }
            PropositionMatcher::Object {
                subject,
                predicate,
                object,
            } => {
                let predicate = match predicate {
                    PredTerm::Literal(value) => value,
                    val => {
                        return Err(KipError::invalid_syntax(format!(
                            "PropositionMatcher::Object's predicate must be a literal string, got: {val:?}"
                        )));
                    }
                };
                self.ensure_proposition_type_for_kml(&predicate, cached_pks)
                    .await?;

                let subject_id =
                    Box::pin(self.resolve_target_term(subject, handle_map, cached_pks)).await?;
                let object_id =
                    Box::pin(self.resolve_target_term(object, handle_map, cached_pks)).await?;
                if subject_id == object_id {
                    return Err(KipError::invalid_syntax(format!(
                        "Subject and object cannot be the same: {}",
                        subject_id
                    )));
                }

                Ok(PropositionPK::Object {
                    subject: Box::new(subject_id.into()),
                    predicate,
                    object: Box::new(object_id.into()),
                })
            }
        }
    }

    pub(super) async fn resolve_target_term(
        &self,
        target: TargetTerm,
        handle_map: &FxHashMap<String, EntityID>,
        cached_pks: &mut FxHashMap<EntityPK, EntityID>,
    ) -> Result<EntityID, KipError> {
        match target {
            TargetTerm::Variable(handle) => handle_map
                .get(&handle)
                .cloned()
                .ok_or_else(|| KipError::reference_error(format!("Undefined handle: {handle}"))),
            TargetTerm::Concept(concept_matcher) => {
                let concept_pk = ConceptPK::try_from(concept_matcher)?;
                self.resolve_entity_id(&EntityPK::Concept(concept_pk), cached_pks)
                    .await
            }
            TargetTerm::Proposition(proposition_matcher) => {
                let proposition_pk = Box::pin(self.resolve_kml_proposition_pk(
                    *proposition_matcher,
                    handle_map,
                    cached_pks,
                ))
                .await?;
                self.resolve_entity_id(&EntityPK::Proposition(proposition_pk), cached_pks)
                    .await
            }
        }
    }

    // Helper method to resolve EntityPK to EntityID
    pub(super) async fn resolve_entity_id(
        &self,
        entity_pk: &EntityPK,
        cached_pks: &mut FxHashMap<EntityPK, EntityID>,
    ) -> Result<EntityID, KipError> {
        {
            if let Some(id) = cached_pks.get(entity_pk) {
                return Ok(id.clone());
            }
        }

        let id = match entity_pk {
            EntityPK::Concept(concept_pk) => match concept_pk {
                ConceptPK::ID(id) => {
                    if self.concepts.contains(*id) {
                        Ok(EntityID::Concept(*id))
                    } else {
                        Err(KipError::not_found(format!(
                            "Concept {} not found",
                            ConceptPK::ID(*id)
                        )))
                    }
                }
                ConceptPK::Object { r#type, name } => {
                    let id = self.query_concept_id(r#type, name).await?;
                    Ok(EntityID::Concept(id))
                }
            },
            EntityPK::Proposition(proposition_pk) => match proposition_pk {
                PropositionPK::ID(id, predicate) => {
                    if !self.propositions.contains(*id) {
                        return Err(KipError::not_found(format!(
                            "Proposition {} not found",
                            PropositionPK::ID(*id, predicate.clone())
                        )));
                    }
                    self.try_get_proposition_with(&QueryCache::default(), *id, |proposition| {
                        if proposition.predicates.contains(predicate) {
                            Ok(EntityID::Proposition(*id, predicate.clone()))
                        } else {
                            Err(KipError::not_found(format!(
                                "proposition link not found: {}",
                                PropositionPK::ID(*id, predicate.clone())
                            )))
                        }
                    })
                    .await
                }
                PropositionPK::Object {
                    subject,
                    predicate,
                    object,
                } => {
                    // 使用 Box::pin 来处理递归调用
                    let subject_id =
                        Box::pin(self.resolve_entity_id(subject.as_ref(), cached_pks)).await?;

                    let object_id =
                        Box::pin(self.resolve_entity_id(object.as_ref(), cached_pks)).await?;

                    let virtual_name = virtual_field_name(&["subject", "object"]);
                    let virtual_val = virtual_field_value(&[
                        Some(&Fv::Text(subject_id.to_string())),
                        Some(&Fv::Text(object_id.to_string())),
                    ])
                    .unwrap();

                    let ids = self
                        .propositions
                        .query_ids(
                            Filter::Field((virtual_name, RangeQuery::Eq(virtual_val))),
                            None,
                        )
                        .await
                        .map_err(db_to_kip_error)?;

                    if let Some(id) = ids.first() {
                        self.try_get_proposition_with(&QueryCache::default(), *id, |proposition| {
                            if proposition.predicates.contains(predicate) {
                                Ok(EntityID::Proposition(*id, predicate.clone()))
                            } else {
                                Err(KipError::not_found(format!(
                                    "proposition link not found: {}",
                                    proposition_pk
                                )))
                            }
                        })
                        .await
                    } else {
                        Err(KipError::not_found(format!(
                            "proposition link not found: {}",
                            proposition_pk
                        )))
                    }
                }
            },
        }?;

        cached_pks.insert(entity_pk.clone(), id.clone());
        Ok(id)
    }

    /// Returns the current `_version` of the concept identified by `pk` for
    /// `EXPECT VERSION` evaluation: `0` when the element does not exist yet
    /// (the value `EXPECT VERSION 0` asserts). A concept created earlier in
    /// the same UPSERT statement (present in `cached_pks` but not persisted)
    /// counts as freshly created (`1`).
    pub(super) async fn concept_version_for_guard(
        &self,
        pk: &ConceptPK,
        cached_pks: &FxHashMap<EntityPK, EntityID>,
    ) -> Result<u64, KipError> {
        let id = match pk {
            ConceptPK::ID(id) => Some(*id),
            ConceptPK::Object { r#type, name } => self.query_concept_id(r#type, name).await.ok(),
        };
        if let Some(id) = id
            && self.concepts.contains(id)
        {
            let concept: Concept = self.concepts.get_as(id).await.map_err(db_to_kip_error)?;
            return Ok(system_metadata_version(&concept.metadata));
        }
        if cached_pks.contains_key(&EntityPK::Concept(pk.clone())) {
            return Ok(1);
        }
        Ok(0)
    }

    /// Proposition-link counterpart of
    /// [`concept_version_for_guard`](Self::concept_version_for_guard). The
    /// element is the `(subject, predicate, object)` link; a link missing the
    /// predicate (or whose row does not exist) has version `0`.
    pub(super) async fn proposition_version_for_guard(
        &self,
        pk: &PropositionPK,
        cached_pks: &FxHashMap<EntityPK, EntityID>,
    ) -> Result<u64, KipError> {
        let row_and_predicate: Option<(u64, &String)> = match pk {
            PropositionPK::ID(id, predicate) => Some((*id, predicate)),
            PropositionPK::Object {
                subject,
                predicate,
                object,
            } => {
                // Resolve both endpoints to concrete EntityIDs. An unresolvable
                // endpoint means the link cannot exist yet.
                let mut scratch = cached_pks.clone();
                let subject_id =
                    match Box::pin(self.resolve_entity_id(subject.as_ref(), &mut scratch)).await {
                        Ok(id) => id,
                        Err(_) => return Ok(0),
                    };
                let object_id =
                    match Box::pin(self.resolve_entity_id(object.as_ref(), &mut scratch)).await {
                        Ok(id) => id,
                        Err(_) => return Ok(0),
                    };
                let virtual_name = virtual_field_name(&["subject", "object"]);
                let virtual_val = virtual_field_value(&[
                    Some(&Fv::Text(subject_id.to_string())),
                    Some(&Fv::Text(object_id.to_string())),
                ])
                .unwrap();
                let ids = self
                    .propositions
                    .query_ids(
                        Filter::Field((virtual_name, RangeQuery::Eq(virtual_val))),
                        None,
                    )
                    .await
                    .map_err(db_to_kip_error)?;
                ids.first().map(|id| (*id, predicate))
            }
        };

        if let Some((id, predicate)) = row_and_predicate
            && self.propositions.contains(id)
        {
            let proposition: Proposition = self
                .propositions
                .get_as(id)
                .await
                .map_err(db_to_kip_error)?;
            if proposition.predicates.contains(predicate) {
                return Ok(proposition
                    .properties
                    .get(predicate)
                    .map(|p| system_metadata_version(&p.metadata))
                    .unwrap_or(1));
            }
        }
        if cached_pks.contains_key(&EntityPK::Proposition(pk.clone())) {
            return Ok(1);
        }
        Ok(0)
    }
}
