//! Proposition pattern matching: subject/predicate/object resolution
//! against the `propositions` collection, including multi-hop BFS
//! traversal (`"pred"{m,n}`) and target-term resolution.

use super::*;

impl CognitiveNexus {
    // 处理多跳匹配
    pub(super) async fn handle_multi_hop_matching(
        &self,
        ctx: &QueryContext,
        subjects: TargetEntities,
        predicate: String,
        min: u16,
        max: Option<u16>,
        objects: TargetEntities,
    ) -> Result<PropositionsMatchResult, KipError> {
        let mut result = PropositionsMatchResult::default();

        if matches!(&subjects, TargetEntities::IDs(_)) {
            let start_nodes = match subjects {
                TargetEntities::IDs(ids) => ids,
                _ => unreachable!(),
            };

            let max_hops = max.unwrap_or(10).min(10);

            for start_node in start_nodes {
                let paths = self
                    .bfs_multi_hop(
                        &ctx.cache,
                        start_node.clone(),
                        &predicate,
                        min,
                        max_hops,
                        &objects,
                        false,
                    )
                    .await?;

                for path in paths {
                    result.matched_subjects.push(path.start);
                    result.matched_objects.push(path.end);
                    result.matched_predicates.push(predicate.clone());
                    result
                        .matched_propositions
                        .extend(path.propositions.into_vec());
                }
            }
        } else {
            let start_nodes = match objects {
                TargetEntities::IDs(ids) => ids,
                _ => {
                    return Err(KipError::invalid_syntax(
                        "The subject or object cannot both be variables in multi-hop matching"
                            .to_string(),
                    ));
                }
            };

            let max_hops = max.unwrap_or(10).min(10);
            for start_node in start_nodes {
                let paths = self
                    .bfs_multi_hop(
                        &ctx.cache,
                        start_node.clone(),
                        &predicate,
                        min,
                        max_hops,
                        &subjects,
                        true,
                    )
                    .await?;

                for path in paths {
                    result.matched_subjects.push(path.end);
                    result.matched_objects.push(path.start);
                    result.matched_predicates.push(predicate.clone());
                    result
                        .matched_propositions
                        .extend(path.propositions.into_vec());
                }
            }
        }

        Ok(result)
    }

    // 处理主体和客体都是具体ID的匹配
    //
    // 优化：将 N×M 个 (subject, object) 串行查询合并为单个
    // `(subject,object)` 虚拟字段 OR 查询，避免多次索引查找。
    pub(super) async fn handle_subject_object_ids_matching(
        &self,
        ctx: &QueryContext,
        subject_ids: Vec<EntityID>,
        object_ids: Vec<EntityID>,
        predicate: PredTerm,
    ) -> Result<PropositionsMatchResult, KipError> {
        let mut result = PropositionsMatchResult::default();
        if subject_ids.is_empty() || object_ids.is_empty() {
            return Ok(result);
        }

        let virtual_name = virtual_field_name(&["subject", "object"]);
        let mut variants: Vec<Box<RangeQuery<Fv>>> =
            Vec::with_capacity(subject_ids.len() * object_ids.len());
        for subject_id in &subject_ids {
            for object_id in &object_ids {
                let virtual_val = virtual_field_value(&[
                    Some(&Fv::Text(subject_id.to_string())),
                    Some(&Fv::Text(object_id.to_string())),
                ])
                .unwrap();
                variants.push(Box::new(RangeQuery::Eq(virtual_val)));
            }
        }

        let range = if variants.len() == 1 {
            *variants.into_iter().next().unwrap()
        } else {
            RangeQuery::Or(variants)
        };

        let ids = self
            .propositions
            .query_ids(Filter::Field((virtual_name, range)), None)
            .await
            .map_err(db_to_kip_error)?;

        for id in ids {
            if let Some((subj, preds, obj)) = self
                .try_get_proposition_with(&ctx.cache, id, |proposition| {
                    match_predicate_against_proposition(proposition, &predicate)
                })
                .await?
            {
                result.add_match(subj, obj, preds, id);
            }
        }

        Ok(result)
    }

    // 处理主体ID和任意对象的匹配
    //
    // 优化：将多个 subject 查询合并为单个 `subject IN [...]` OR 查询。
    pub(super) async fn handle_subject_ids_any_matching(
        &self,
        ctx: &QueryContext,
        subject_ids: Vec<EntityID>,
        predicate: PredTerm,
        any_propositions: bool,
    ) -> Result<PropositionsMatchResult, KipError> {
        let mut result = PropositionsMatchResult::default();
        if subject_ids.is_empty() {
            return Ok(result);
        }

        let range = if subject_ids.len() == 1 {
            RangeQuery::Eq(Fv::Text(subject_ids[0].to_string()))
        } else {
            RangeQuery::Or(
                subject_ids
                    .iter()
                    .map(|id| Box::new(RangeQuery::Eq(Fv::Text(id.to_string()))))
                    .collect(),
            )
        };

        let ids = self
            .propositions
            .query_ids(Filter::Field(("subject".to_string(), range)), None)
            .await
            .map_err(db_to_kip_error)?;

        for id in ids {
            if let Some((subj, preds, obj)) = self
                .try_get_proposition_with(&ctx.cache, id, |proposition| {
                    if any_propositions && matches!(proposition.object, EntityID::Concept(_)) {
                        return Ok(None);
                    }
                    match_predicate_against_proposition(proposition, &predicate)
                })
                .await?
            {
                result.add_match(subj, obj, preds, id);
            }
        }

        Ok(result)
    }

    // 处理任意主体和对象ID的匹配
    //
    // 优化：将多个 object 查询合并为单个 `object IN [...]` OR 查询。
    pub(super) async fn handle_any_to_object_ids_matching(
        &self,
        ctx: &QueryContext,
        object_ids: Vec<EntityID>,
        predicate: PredTerm,
        any_propositions: bool,
    ) -> Result<PropositionsMatchResult, KipError> {
        let mut result = PropositionsMatchResult::default();
        if object_ids.is_empty() {
            return Ok(result);
        }

        let range = if object_ids.len() == 1 {
            RangeQuery::Eq(Fv::Text(object_ids[0].to_string()))
        } else {
            RangeQuery::Or(
                object_ids
                    .iter()
                    .map(|id| Box::new(RangeQuery::Eq(Fv::Text(id.to_string()))))
                    .collect(),
            )
        };

        let ids = self
            .propositions
            .query_ids(Filter::Field(("object".to_string(), range)), None)
            .await
            .map_err(db_to_kip_error)?;

        for id in ids {
            if let Some((subj, preds, obj)) = self
                .try_get_proposition_with(&ctx.cache, id, |proposition| {
                    if any_propositions && matches!(proposition.subject, EntityID::Concept(_)) {
                        return Ok(None);
                    }
                    match_predicate_against_proposition(proposition, &predicate)
                })
                .await?
            {
                result.add_match(subj, obj, preds, id);
            }
        }

        Ok(result)
    }

    // 处理谓词匹配
    pub(super) async fn handle_predicate_matching(
        &self,
        ctx: &QueryContext,
        predicate: PredTerm,
    ) -> Result<PropositionsMatchResult, KipError> {
        let mut result = PropositionsMatchResult::default();
        let predicates = match &predicate {
            PredTerm::Literal(pred) => vec![pred.clone()],
            PredTerm::Alternative(preds) => preds.clone(),
            _ => {
                return Err(KipError::invalid_syntax(format!(
                    "Predicate must be either Literal or Alternative, got: {predicate:?}"
                )));
            }
        };

        let ids = self
            .propositions
            .query_ids(
                Filter::Field((
                    "predicates".to_string(),
                    RangeQuery::Or(
                        predicates
                            .into_iter()
                            .map(|v| Box::new(RangeQuery::Eq(v.into())))
                            .collect(),
                    ),
                )),
                None,
            )
            .await
            .map_err(db_to_kip_error)?;

        for id in ids {
            if let Some((subj, preds, obj)) = self
                .try_get_proposition_with(&ctx.cache, id, |proposition| {
                    match_predicate_against_proposition(proposition, &predicate)
                })
                .await?
            {
                result.add_match(subj, obj, preds, id);
            }
        }

        Ok(result)
    }

    // BFS 路径查找实现
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn bfs_multi_hop(
        &self,
        cache: &QueryCache,
        start: EntityID,
        predicate: &str,
        min_hops: u16,
        max_hops: u16,
        targets: &TargetEntities,
        reverse: bool,
    ) -> Result<Vec<GraphPath>, KipError> {
        use std::collections::VecDeque;

        let mut queue: VecDeque<GraphPath> = VecDeque::new();
        let mut results: Vec<GraphPath> = Vec::new();
        let mut visited: FxHashSet<(EntityID, u16)> = FxHashSet::default(); // (node, depth) 防止循环

        // 初始化队列
        queue.push_back(GraphPath {
            start: start.clone(),
            end: start.clone(),
            propositions: UniqueVec::new(),
            hops: 0,
        });

        while let Some(current_path) = queue.pop_front() {
            // 检查是否已访问过此节点在此深度
            let state = (current_path.end.clone(), current_path.hops);
            if visited.contains(&state) {
                continue;
            }
            visited.insert(state);

            // 如果达到最大跳数，停止扩展此路径
            if current_path.hops >= max_hops {
                if current_path.hops >= min_hops {
                    match targets {
                        TargetEntities::IDs(ids) => {
                            if ids.contains(&current_path.end) {
                                results.push(current_path);
                            }
                        }
                        TargetEntities::AnyPropositions => {
                            if matches!(current_path.end, EntityID::Proposition(_, _)) {
                                results.push(current_path);
                            }
                        }
                        TargetEntities::Any => {
                            results.push(current_path);
                        }
                    }
                }
                continue;
            }

            // 查找从当前节点出发的所有指定谓词的边
            let props = self
                .find_propositions(cache, &current_path.end, predicate, reverse)
                .await?;

            for (prop_id, target_node) in props {
                let mut new_path = current_path.clone();
                new_path.end = target_node;
                new_path.propositions.push(prop_id);
                new_path.hops += 1;

                // 如果满足最小跳数要求，检查是否为有效结果
                if new_path.hops >= min_hops {
                    match targets {
                        TargetEntities::IDs(ids) => {
                            if ids.contains(&new_path.end) {
                                results.push(new_path.clone());
                            }
                        }
                        TargetEntities::AnyPropositions => {
                            if matches!(new_path.end, EntityID::Proposition(_, _)) {
                                results.push(new_path.clone());
                            }
                        }
                        TargetEntities::Any => {
                            results.push(new_path.clone());
                        }
                    }
                }

                // 如果未达到最大跳数，继续扩展
                if new_path.hops < max_hops {
                    queue.push_back(new_path);
                }
            }
        }

        Ok(results)
    }

    pub(super) async fn find_propositions(
        &self,
        cache: &QueryCache,
        node: &EntityID,
        predicate: &str,
        reverse: bool,
    ) -> Result<Vec<(EntityID, EntityID)>, KipError> {
        let ids = self
            .propositions
            .query_ids(
                Filter::Field((
                    if reverse {
                        "object".to_string()
                    } else {
                        "subject".to_string()
                    },
                    RangeQuery::Eq(Fv::Text(node.to_string())),
                )),
                None,
            )
            .await
            .map_err(db_to_kip_error)?;

        let mut results = Vec::with_capacity(ids.len());
        for id in ids {
            let rt = self
                .try_get_proposition_with(cache, id, |proposition| {
                    if proposition.predicates.contains(predicate) {
                        Ok(Some((
                            EntityID::Proposition(id, predicate.to_string()),
                            if reverse {
                                proposition.subject.clone()
                            } else {
                                proposition.object.clone()
                            },
                        )))
                    } else {
                        Ok(None)
                    }
                })
                .await?;

            if let Some(rt) = rt {
                results.push(rt)
            }
        }

        Ok(results)
    }

    pub(super) async fn match_propositions(
        &self,
        ctx: &mut QueryContext,
        subject: TargetTerm,
        predicate: PredTerm,
        object: TargetTerm,
        proposition_var: Option<String>,
    ) -> Result<TargetEntities, KipError> {
        let subject_var = match &subject {
            TargetTerm::Variable(var) => Some(var.clone()),
            _ => None,
        };
        let predicate_var = match &predicate {
            PredTerm::Variable(var) => Some(var.clone()),
            _ => None,
        };
        let object_var = match &object {
            TargetTerm::Variable(var) => Some(var.clone()),
            _ => None,
        };
        let subject_var_clone = subject_var.clone();

        let subjects = self.resolve_target_term_ids(ctx, subject).await?;
        let objects = self.resolve_target_term_ids(ctx, object).await?;

        let result = match (subjects, predicate, objects) {
            (
                subjects,
                PredTerm::MultiHop {
                    predicate,
                    min,
                    max,
                },
                objects,
            ) => {
                self.handle_multi_hop_matching(ctx, subjects, predicate, min, max, objects)
                    .await?
            }
            (TargetEntities::IDs(subject_ids), predicate, TargetEntities::IDs(object_ids)) => {
                self.handle_subject_object_ids_matching(ctx, subject_ids, object_ids, predicate)
                    .await?
            }
            (TargetEntities::IDs(subject_ids), predicate, TargetEntities::AnyPropositions) => {
                self.handle_subject_ids_any_matching(ctx, subject_ids, predicate, true)
                    .await?
            }
            (TargetEntities::IDs(subject_ids), predicate, TargetEntities::Any) => {
                self.handle_subject_ids_any_matching(ctx, subject_ids, predicate, false)
                    .await?
            }
            (TargetEntities::AnyPropositions, predicate, TargetEntities::IDs(object_ids)) => {
                self.handle_any_to_object_ids_matching(ctx, object_ids, predicate, true)
                    .await?
            }
            (TargetEntities::Any, predicate, TargetEntities::IDs(object_ids)) => {
                self.handle_any_to_object_ids_matching(ctx, object_ids, predicate, false)
                    .await?
            }
            (_, predicate, _) => {
                if matches!(&predicate, PredTerm::Variable(_)) {
                    return Ok(TargetEntities::AnyPropositions);
                }

                self.handle_predicate_matching(ctx, predicate).await?
            }
        };

        if proposition_var.is_some()
            || subject_var.is_some()
            || predicate_var.is_some()
            || object_var.is_some()
        {
            ctx.relations.push(QueryRelationBinding {
                proposition_var,
                subject_var: subject_var.clone(),
                predicate_var: predicate_var.clone(),
                object_var: object_var.clone(),
                rows: result.rows.clone(),
            });
        }

        if let Some(var) = subject_var {
            ctx.entities.insert(var.clone(), result.matched_subjects);

            // Store group relationships: subject_var → object_var
            if let Some(obj_var) = &object_var
                && !result.subject_to_objects.is_empty()
            {
                let group_map = ctx.groups.entry((var, obj_var.clone())).or_default();
                for (subj, objs) in result.subject_to_objects {
                    group_map.entry(subj).or_default().extend(objs.into_vec());
                }
            }
        }
        if let Some(var) = predicate_var {
            ctx.predicates.insert(var, result.matched_predicates);
        }
        if let Some(var) = object_var {
            ctx.entities.insert(var.clone(), result.matched_objects);

            // Store group relationships: object_var → subject_var
            if let Some(subj_var) = &subject_var_clone
                && !result.object_to_subjects.is_empty()
            {
                let group_map = ctx.groups.entry((var, subj_var.clone())).or_default();
                for (obj, subjs) in result.object_to_subjects {
                    group_map.entry(obj).or_default().extend(subjs.into_vec());
                }
            }
        }

        Ok(TargetEntities::IDs(result.matched_propositions.into()))
    }

    // 解析目标项为实体ID列表
    pub(super) async fn resolve_target_term_ids(
        &self,
        ctx: &mut QueryContext,
        target: TargetTerm,
    ) -> Result<TargetEntities, KipError> {
        match target {
            TargetTerm::Variable(var) => {
                if let Some(ids) = ctx.entities.get(&var) {
                    Ok(TargetEntities::IDs(ids.clone().into()))
                } else {
                    Ok(TargetEntities::Any)
                }
            }
            TargetTerm::Concept(concept_matcher) => {
                let ids: Vec<EntityID> = self
                    .query_concept_ids(&concept_matcher)
                    .await?
                    .into_iter()
                    .map(EntityID::Concept)
                    .collect();
                Ok(TargetEntities::IDs(ids))
            }
            TargetTerm::Proposition(proposition_matcher) => {
                let result = match *proposition_matcher {
                    PropositionMatcher::ID(id) => {
                        let entity_id =
                            EntityID::from_str(&id).map_err(KipError::invalid_syntax)?;
                        if !matches!(entity_id, EntityID::Proposition(_, _)) {
                            return Err(KipError::invalid_syntax(format!(
                                "Invalid proposition link ID: {id:?}"
                            )));
                        }
                        TargetEntities::IDs(vec![entity_id])
                    }
                    PropositionMatcher::Object {
                        subject: TargetTerm::Variable(_),
                        predicate: PredTerm::Variable(_),
                        object: TargetTerm::Variable(_),
                    } => TargetEntities::AnyPropositions,
                    PropositionMatcher::Object {
                        subject,
                        predicate,
                        object,
                    } => {
                        // 递归查询命题
                        Box::pin(self.match_propositions(ctx, subject, predicate, object, None))
                            .await?
                    }
                };

                Ok(result)
            }
        }
    }
}
