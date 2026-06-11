//! KQL execution: `WHERE` clause evaluation (concept / proposition /
//! `FILTER` / `NOT` / `OPTIONAL` / `UNION`), `FIND` projection with
//! grouping, aggregation, ordering and cursor pagination.

use super::*;

impl CognitiveNexus {
    pub(super) async fn execute_where_clause(
        &self,
        ctx: &mut QueryContext,
        clause: WhereClause,
    ) -> Result<(), KipError> {
        match clause {
            WhereClause::Concept(clause) => self.execute_concept_clause(ctx, clause).await,
            WhereClause::Proposition(clause) => self.execute_proposition_clause(ctx, clause).await,
            WhereClause::Filter(clause) => self.execute_filter_clause(ctx, clause).await,
            WhereClause::Not(clauses) => self.execute_not_clause(ctx, clauses).await,
            WhereClause::Optional(clauses) => self.execute_optional_clause(ctx, clauses).await,
            WhereClause::Union(clauses) => self.execute_union_clause(ctx, clauses).await,
        }?;

        Ok(())
    }

    pub(super) async fn execute_concept_clause(
        &self,
        ctx: &mut QueryContext,
        clause: ConceptClause,
    ) -> Result<(), KipError> {
        let concept_ids: Vec<EntityID> = self
            .query_concept_ids(&clause.matcher)
            .await?
            .into_iter()
            .map(EntityID::Concept)
            .collect();

        if let Some(existing) = ctx.entities.get_mut(&clause.variable) {
            // Variable already bound: filter (intersect) existing bindings
            existing.retain(|id| concept_ids.contains(id));
        } else {
            ctx.entities.insert(clause.variable, concept_ids.into());
        }

        Ok(())
    }

    pub(super) async fn execute_proposition_clause(
        &self,
        ctx: &mut QueryContext,
        clause: PropositionClause,
    ) -> Result<(), KipError> {
        let result = match clause.matcher {
            PropositionMatcher::ID(id) => {
                let entity_id = EntityID::from_str(&id).map_err(KipError::invalid_syntax)?;
                if !matches!(entity_id, EntityID::Proposition(_, _)) {
                    return Err(KipError::invalid_syntax(format!(
                        "Invalid proposition link ID: {id:?}"
                    )));
                }
                TargetEntities::IDs(vec![entity_id])
            }
            PropositionMatcher::Object {
                subject,
                predicate,
                object,
            } => {
                self.match_propositions(ctx, subject, predicate, object, clause.variable.clone())
                    .await?
            }
        };

        if let TargetEntities::IDs(ids) = result
            && let Some(var) = clause.variable
        {
            if let Some(existing) = ctx.entities.get_mut(&var) {
                // Variable already bound: filter (intersect) existing bindings
                let new_ids: FxHashSet<EntityID> = ids.into_iter().collect();
                existing.retain(|id| new_ids.contains(id));
            } else {
                ctx.entities.insert(var, ids.into());
            }
        }

        Ok(())
    }

    pub(super) async fn execute_filter_clause(
        &self,
        ctx: &mut QueryContext,
        clause: FilterClause,
    ) -> Result<(), KipError> {
        Self::collect_filter_row_sensitive_vars(&clause.expression, &mut ctx.row_sensitive_vars);

        let mut entities: FxHashMap<String, Vec<EntityID>> = ctx
            .entities
            .iter()
            .map(|(var, ids)| (var.clone(), ids.to_vec()))
            .collect();

        loop {
            let mut bindings_snapshot = entities.clone();
            let mut bindings_cursor = FxHashMap::default();
            match self
                .evaluate_filter_expression(
                    ctx,
                    clause.expression.clone(),
                    &mut bindings_snapshot,
                    &mut bindings_cursor,
                )
                .await?
            {
                Some(true) => {
                    // 继续处理剩余绑定
                    entities = bindings_snapshot;
                }
                Some(false) => {
                    // 过滤不通过，移除相关值
                    for (var, id) in bindings_cursor {
                        if let Some(existing) = ctx.entities.get_mut(&var)
                            && let Some(idx) = existing.iter().position(|x| x == &id)
                        {
                            existing.remove(idx);
                        }
                    }
                    // 继续处理剩余绑定
                    entities = bindings_snapshot;
                }
                None => {
                    // 没有更多符合条件的绑定可供处理，退出循环
                    return Ok(());
                }
            }
        }
    }

    pub(super) async fn execute_not_clause(
        &self,
        ctx: &mut QueryContext,
        clauses: Vec<WhereClause>,
    ) -> Result<(), KipError> {
        // 优化：检测是否可以使用快速路径
        // 快速路径适用于: NOT { (?bound_var, "predicate", ?unbound_var) }
        // 这种模式可以通过单次批量查询完成，而不需要对每个 entity 单独查询
        if clauses.len() == 1
            && let WhereClause::Proposition(prop_clause) = &clauses[0]
            && let PropositionMatcher::Object {
                subject: TargetTerm::Variable(subj_var),
                predicate: PredTerm::Literal(pred),
                object: TargetTerm::Variable(obj_var),
            } = &prop_clause.matcher
        {
            // 检查 subject 变量是否已绑定，object 变量是否未绑定
            let subj_bound = ctx.entities.contains_key(subj_var);
            let obj_bound = ctx.entities.contains_key(obj_var);

            if subj_bound && !obj_bound {
                // 快速路径：批量查询所有有此谓词关系的 subjects
                return self
                    .execute_not_proposition_fast_path(ctx, subj_var, pred)
                    .await;
            }
        }

        // 标准路径
        let mut not_context = ctx.clone();
        for clause in clauses {
            Box::pin(self.execute_where_clause(&mut not_context, clause)).await?;
        }

        for (var, ids) in &not_context.entities {
            if ids.is_empty() {
                continue;
            }
            // 如果 NOT 子句中有变量绑定，则从当前上下文中移除这些绑定
            if let Some(existing) = ctx.entities.get_mut(var) {
                existing.retain(|id| !ids.contains(id));
            }
        }

        for (pred, ids) in not_context.predicates {
            if ids.is_empty() {
                continue;
            }
            // 如果 NOT 子句中有谓词绑定，则从当前上下文中移除这些绑定
            if let Some(existing) = ctx.predicates.get_mut(&pred) {
                existing.retain(|id| !ids.contains(id));
            }
        }

        // 清理 groups 中被排除的实体
        for ((gvar, _), group_map) in ctx.groups.iter_mut() {
            if let Some(excluded_ids) = not_context.entities.get(gvar)
                && !excluded_ids.is_empty()
            {
                group_map.retain(|gid, _| !excluded_ids.contains(gid));
            }
        }

        Ok(())
    }

    /// 快速路径处理 NOT { (?bound_var, "predicate", ?unbound_var) } 模式
    ///
    /// 优化策略：
    /// 1. 一次性查询所有具有指定谓词的命题
    /// 2. 收集所有这些命题的 subject
    /// 3. 从原始绑定中排除这些 subjects
    ///
    /// 复杂度：O(1) 数据库查询 + O(M) 内存操作
    pub(super) async fn execute_not_proposition_fast_path(
        &self,
        ctx: &mut QueryContext,
        subject_var: &str,
        predicate: &str,
    ) -> Result<(), KipError> {
        // 一次性查询所有具有此谓词的命题
        let proposition_ids = self
            .propositions
            .query_ids(
                Filter::Field((
                    "predicates".to_string(),
                    RangeQuery::Eq(Fv::Text(predicate.to_string())),
                )),
                None,
            )
            .await
            .map_err(db_to_kip_error)?;

        // 收集所有有此关系的 subjects
        let mut subjects_with_relation: FxHashSet<EntityID> =
            FxHashSet::with_capacity_and_hasher(proposition_ids.len(), Default::default());

        for id in proposition_ids {
            let subject = self
                .try_get_proposition_with(&ctx.cache, id, |prop| Ok(prop.subject.clone()))
                .await?;

            subjects_with_relation.insert(subject);
        }

        // 从原始绑定中排除有此关系的 subjects
        if let Some(existing) = ctx.entities.get_mut(subject_var) {
            existing.retain(|id| !subjects_with_relation.contains(id));
        }

        Ok(())
    }

    pub(super) async fn execute_optional_clause(
        &self,
        ctx: &mut QueryContext,
        clauses: Vec<WhereClause>,
    ) -> Result<(), KipError> {
        let mut optional_context = ctx.clone();
        let base_relation_len = optional_context.relations.len();
        for clause in clauses {
            Box::pin(self.execute_where_clause(&mut optional_context, clause)).await?;
        }

        // 合并 OPTIONAL 子句
        for (var, ids) in optional_context.entities {
            ctx.entities.entry(var).or_default().extend(ids.into_vec());
        }

        for (pred, ids) in optional_context.predicates {
            ctx.predicates
                .entry(pred)
                .or_default()
                .extend(ids.into_vec());
        }

        // 合并 OPTIONAL 子句的 groups
        for (key, group_map) in optional_context.groups {
            let entry = ctx.groups.entry(key).or_default();
            for (gid, mids) in group_map {
                entry.entry(gid).or_default().extend(mids.into_vec());
            }
        }

        ctx.relations.extend(
            optional_context
                .relations
                .into_iter()
                .skip(base_relation_len),
        );
        ctx.row_sensitive_vars
            .extend(optional_context.row_sensitive_vars);

        Ok(())
    }

    pub(super) async fn execute_union_clause(
        &self,
        ctx: &mut QueryContext,
        clauses: Vec<WhereClause>,
    ) -> Result<(), KipError> {
        let mut union_context = QueryContext {
            cache: ctx.cache.clone(),
            ..Default::default()
        };

        for clause in clauses {
            Box::pin(self.execute_where_clause(&mut union_context, clause)).await?;
        }

        // 合并 UNION 子句
        for (var, ids) in union_context.entities {
            ctx.entities.entry(var).or_default().extend(ids.into_vec());
        }
        for (pred, ids) in union_context.predicates {
            ctx.predicates
                .entry(pred)
                .or_default()
                .extend(ids.into_vec());
        }
        // 合并 UNION 子句的 groups
        for (key, group_map) in union_context.groups {
            let entry = ctx.groups.entry(key).or_default();
            for (gid, mids) in group_map {
                entry.entry(gid).or_default().extend(mids.into_vec());
            }
        }
        ctx.relations.extend(union_context.relations);
        ctx.row_sensitive_vars
            .extend(union_context.row_sensitive_vars);

        Ok(())
    }

    /// Resolves a FIND variable, checking entity bindings first, then predicate bindings.
    ///
    /// Predicate variables (bound via triple patterns like `(?s, ?p, ?o)`) are stored
    /// separately from entity variables. This method handles both cases.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn resolve_find_var(
        &self,
        ctx: &QueryContext,
        bindings: &FxHashMap<String, Vec<EntityID>>,
        var: &str,
        fields: &[String],
        order_by: &[OrderByCondition],
        cursor: Option<&EntityID>,
        raw_cursor: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<Json>, Option<String>), KipError> {
        if bindings.contains_key(var) {
            return self
                .resolve_result(&ctx.cache, bindings, var, fields, order_by, cursor, limit)
                .await;
        }

        // Check if it's a predicate variable
        if let Some(predicates) = ctx.predicates.get(var) {
            let values: Vec<Json> = predicates.iter().map(|p| Json::String(p.clone())).collect();
            let start = raw_cursor
                .and_then(|cursor| cursor.parse::<usize>().ok())
                .unwrap_or(0)
                .min(values.len());
            let remaining = &values[start..];
            let next_cursor = if limit > 0 && limit < remaining.len() {
                Some((start + limit).to_string())
            } else {
                None
            };
            let limited = if limit > 0 && limit < remaining.len() {
                remaining[..limit].to_vec()
            } else {
                remaining.to_vec()
            };
            return Ok((limited, next_cursor));
        }

        Err(KipError::reference_error(format!(
            "Unbound variable: {var:?}"
        )))
    }

    pub(super) fn collect_find_variable_groups(
        clause: &FindClause,
    ) -> Option<Vec<(String, Vec<DotPathVar>)>> {
        let mut groups: Vec<(String, Vec<DotPathVar>)> = Vec::new();

        for expr in &clause.expressions {
            let FindExpression::Variable(dot_path) = expr else {
                return None;
            };

            if let Some((var, fields)) = groups.last_mut()
                && var == &dot_path.var
            {
                fields.push(dot_path.clone());
                continue;
            }

            groups.push((dot_path.var.clone(), vec![dot_path.clone()]));
        }

        Some(groups)
    }

    pub(super) fn collect_filter_row_sensitive_vars(
        expr: &FilterExpression,
        vars: &mut FxHashSet<String>,
    ) {
        match expr {
            FilterExpression::Comparison { left, right, .. } => {
                Self::collect_filter_operand_row_sensitive_vars(left, vars);
                Self::collect_filter_operand_row_sensitive_vars(right, vars);
            }
            FilterExpression::Logical { left, right, .. } => {
                Self::collect_filter_row_sensitive_vars(left, vars);
                Self::collect_filter_row_sensitive_vars(right, vars);
            }
            FilterExpression::Not(inner) => Self::collect_filter_row_sensitive_vars(inner, vars),
            FilterExpression::Function { args, .. } => {
                for arg in args {
                    Self::collect_filter_operand_row_sensitive_vars(arg, vars);
                }
            }
        }
    }

    pub(super) fn collect_filter_operand_row_sensitive_vars(
        operand: &FilterOperand,
        vars: &mut FxHashSet<String>,
    ) {
        if let FilterOperand::Variable(dot_path) = operand
            && !dot_path.path.is_empty()
        {
            vars.insert(dot_path.var.clone());
        }
    }

    pub(super) fn relation_covers_var(relation: &QueryRelationBinding, var: &str) -> bool {
        relation.proposition_var.as_deref() == Some(var)
            || relation.subject_var.as_deref() == Some(var)
            || relation.predicate_var.as_deref() == Some(var)
            || relation.object_var.as_deref() == Some(var)
    }

    pub(super) fn relation_row_entity<'a>(
        relation: &'a QueryRelationBinding,
        row: &'a QueryRelationRow,
        var: &str,
    ) -> Option<&'a EntityID> {
        if relation.proposition_var.as_deref() == Some(var) {
            Some(&row.proposition)
        } else if relation.subject_var.as_deref() == Some(var) {
            Some(&row.subject)
        } else if relation.object_var.as_deref() == Some(var) {
            Some(&row.object)
        } else {
            None
        }
    }

    pub(super) fn relation_row_predicate<'a>(
        relation: &'a QueryRelationBinding,
        row: &'a QueryRelationRow,
        var: &str,
    ) -> Option<&'a str> {
        if relation.predicate_var.as_deref() == Some(var) {
            Some(&row.predicate)
        } else {
            None
        }
    }

    pub(super) fn relation_row_matches_context(
        ctx: &QueryContext,
        relation: &QueryRelationBinding,
        row: &QueryRelationRow,
    ) -> bool {
        for var in [
            relation.proposition_var.as_deref(),
            relation.subject_var.as_deref(),
            relation.object_var.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if let Some(allowed) = ctx.entities.get(var)
                && let Some(entity_id) = Self::relation_row_entity(relation, row, var)
                && !allowed.iter().any(|id| id == entity_id)
            {
                return false;
            }
        }

        if let Some(var) = relation.predicate_var.as_deref()
            && let Some(allowed) = ctx.predicates.get(var)
            && !allowed.iter().any(|predicate| predicate == &row.predicate)
        {
            return false;
        }

        true
    }

    pub(super) async fn load_relation_row_value(
        &self,
        cache: &QueryCache,
        relation: &QueryRelationBinding,
        row: &QueryRelationRow,
        dot_path: &DotPathVar,
    ) -> Result<Json, KipError> {
        if let Some(entity_id) = Self::relation_row_entity(relation, row, &dot_path.var) {
            return self
                .load_entity_field(cache, entity_id, &dot_path.to_pointer())
                .await;
        }

        if let Some(predicate) = Self::relation_row_predicate(relation, row, &dot_path.var) {
            return if dot_path.path.is_empty() {
                Ok(Json::String(predicate.to_string()))
            } else {
                Ok(Json::Null)
            };
        }

        Err(KipError::reference_error(format!(
            "Unbound variable: {:?}",
            dot_path.var
        )))
    }

    pub(super) async fn try_execute_relation_row_find(
        &self,
        ctx: &QueryContext,
        clause: &FindClause,
        order_by: &[OrderByCondition],
        cursor: Option<&EntityID>,
        limit: usize,
    ) -> Result<Option<(Vec<Json>, Option<String>)>, KipError> {
        let Some(groups) = Self::collect_find_variable_groups(clause) else {
            return Ok(None);
        };

        let mut referenced: FxHashSet<String> = FxHashSet::default();
        for (_, dot_paths) in &groups {
            for dot_path in dot_paths {
                referenced.insert(dot_path.var.clone());
            }
        }
        for cond in order_by {
            if !cond.is_aggregation() {
                referenced.insert(cond.variable.var.clone());
            }
        }

        let relation = ctx.relations.iter().rev().find(|relation| {
            let proposition_var = relation.proposition_var.as_deref();
            let orders_by_proposition_field = proposition_var
                .map(|var| {
                    order_by.iter().any(|cond| {
                        !cond.is_aggregation()
                            && cond.variable.var == var
                            && !cond.variable.path.is_empty()
                    })
                })
                .unwrap_or(false);
            let filters_by_proposition_field = proposition_var
                .map(|var| ctx.row_sensitive_vars.contains(var))
                .unwrap_or(false);

            (orders_by_proposition_field || filters_by_proposition_field)
                && referenced
                    .iter()
                    .all(|var| Self::relation_covers_var(relation, var))
        });

        let Some(relation) = relation.cloned() else {
            return Ok(None);
        };

        let mut rows: Vec<QueryRelationRow> = relation
            .rows
            .iter()
            .filter(|row| Self::relation_row_matches_context(ctx, &relation, row))
            .cloned()
            .collect();

        let order_conditions: Vec<&OrderByCondition> = order_by
            .iter()
            .filter(|cond| !cond.is_aggregation())
            .collect();
        if !order_conditions.is_empty() {
            let mut keyed_rows: Vec<(QueryRelationRow, Vec<Json>)> = Vec::with_capacity(rows.len());
            for row in rows {
                let mut sort_values = Vec::with_capacity(order_conditions.len());
                for cond in &order_conditions {
                    sort_values.push(
                        self.load_relation_row_value(&ctx.cache, &relation, &row, &cond.variable)
                            .await?,
                    );
                }
                keyed_rows.push((row, sort_values));
            }

            keyed_rows.sort_by(|(_, left_values), (_, right_values)| {
                for (idx, cond) in order_conditions.iter().enumerate() {
                    if let Some(ordering) = compare_json(&left_values[idx], &right_values[idx]) {
                        let ordering = match cond.direction {
                            OrderDirection::Asc => ordering,
                            OrderDirection::Desc => ordering.reverse(),
                        };

                        if ordering != std::cmp::Ordering::Equal {
                            return ordering;
                        }
                    }
                }

                std::cmp::Ordering::Equal
            });

            rows = keyed_rows.into_iter().map(|(row, _)| row).collect();
        }

        if let Some(cursor) = cursor
            && let Some(idx) = rows.iter().position(|row| &row.proposition == cursor)
            && idx < rows.len()
        {
            rows = rows.split_off(idx + 1);
        }

        let mut next_cursor: Option<String> = None;
        if limit > 0 && limit <= rows.len() {
            rows.truncate(limit);
            next_cursor = rows
                .last()
                .and_then(|row| BTree::to_cursor(&row.proposition));
        }

        let mut result: Vec<Json> = Vec::with_capacity(groups.len());
        for (_, dot_paths) in groups {
            let mut column = Vec::with_capacity(rows.len());
            for row in &rows {
                if dot_paths.len() == 1 {
                    column.push(
                        self.load_relation_row_value(&ctx.cache, &relation, row, &dot_paths[0])
                            .await?,
                    );
                } else {
                    let mut values = Vec::with_capacity(dot_paths.len());
                    for dot_path in &dot_paths {
                        values.push(
                            self.load_relation_row_value(&ctx.cache, &relation, row, dot_path)
                                .await?,
                        );
                    }
                    column.push(Json::Array(values));
                }
            }
            result.push(Json::Array(column));
        }

        Ok(Some((result, next_cursor)))
    }

    pub(super) async fn execute_find_clause(
        &self,
        ctx: &mut QueryContext,
        clause: FindClause,
        order_by: Option<Vec<OrderByCondition>>,
        cursor: Option<String>,
        limit: Option<usize>,
    ) -> Result<(Vec<Json>, Option<String>), KipError> {
        let bindings: FxHashMap<String, Vec<EntityID>> = ctx
            .entities
            .iter()
            .map(|(var, ids)| (var.clone(), ids.to_vec()))
            .collect();

        let order_by = order_by.unwrap_or_default();
        let limit = limit.unwrap_or(0);
        let raw_cursor = cursor.as_deref();

        // GROUP BY 检测：扫描 FIND 表达式，识别 Variable(X) + Aggregation(Y) 模式
        // 其中 X ≠ Y 且 ctx.groups 存在 (X, Y) 映射
        if let Some(grouped) = self
            .detect_and_execute_grouped_find(ctx, &clause, &bindings, &order_by, &cursor, limit)
            .await?
        {
            return Ok(grouped);
        }

        // 非分组模式
        let cursor: Option<EntityID> = BTree::from_cursor(&cursor).ok().flatten();
        if let Some(row_result) = self
            .try_execute_relation_row_find(ctx, &clause, &order_by, cursor.as_ref(), limit)
            .await?
        {
            return Ok(row_result);
        }

        let mut result: Vec<Json> = Vec::with_capacity(clause.expressions.len());
        let mut next_cursor: Option<String> = None;
        let mut group_var: Option<(String, Vec<String>)> = None;

        for expr in clause.expressions {
            match expr {
                FindExpression::Variable(dot_path) => {
                    // 如果当前 group_var 存在且变量不同，处理之前的 group_var
                    match &group_var {
                        Some((var, fields)) if var != &dot_path.var => {
                            let (col, cur) = self
                                .resolve_find_var(
                                    ctx,
                                    &bindings,
                                    var,
                                    fields,
                                    &order_by,
                                    cursor.as_ref(),
                                    raw_cursor,
                                    limit,
                                )
                                .await?;

                            if cur.is_some() && next_cursor.is_none() {
                                next_cursor = cur;
                            }

                            result.push(Json::Array(col));
                            group_var = None;
                        }
                        _ => {}
                    }

                    match &mut group_var {
                        None => {
                            group_var = Some((dot_path.var.clone(), vec![dot_path.to_pointer()]));
                        }
                        Some((_, fields)) => {
                            fields.push(dot_path.to_pointer());
                        }
                    }
                }
                FindExpression::Aggregation {
                    func,
                    var,
                    distinct,
                } => {
                    // 处理之前的 group_var
                    if let Some((var, fields)) = &group_var {
                        let (col, cur) = self
                            .resolve_find_var(
                                ctx,
                                &bindings,
                                var,
                                fields,
                                &order_by,
                                cursor.as_ref(),
                                raw_cursor,
                                limit,
                            )
                            .await?;

                        if cur.is_some() && next_cursor.is_none() {
                            next_cursor = cur;
                        }

                        result.push(Json::Array(col));
                        group_var = None;
                    }

                    // COUNT 优化：直接从绑定 ID 计数，跳过完整实体 IO
                    if matches!(func, AggregationFunction::Count) {
                        let count = if let Some(ids) = bindings.get(&var.var) {
                            // entity bindings: UniqueVec 已去重，distinct 无影响
                            ids.len()
                        } else if let Some(preds) = ctx.predicates.get(&var.var) {
                            if distinct {
                                preds.iter().collect::<FxHashSet<_>>().len()
                            } else {
                                preds.len()
                            }
                        } else {
                            0
                        };
                        result.push(Json::from(count));
                    } else {
                        let (col, _) = self
                            .resolve_find_var(
                                ctx,
                                &bindings,
                                &var.var,
                                &[var.to_pointer_or("id")],
                                &[],
                                None,
                                None,
                                0,
                            )
                            .await?;

                        result.push(func.calculate(&col, distinct));
                    }
                }
            }
        }

        // 处理最后的 group_var
        if let Some((var, fields)) = &group_var {
            let (col, cur) = self
                .resolve_find_var(
                    ctx,
                    &bindings,
                    var,
                    fields,
                    &order_by,
                    cursor.as_ref(),
                    raw_cursor,
                    limit,
                )
                .await?;

            if cur.is_some() && next_cursor.is_none() {
                next_cursor = cur;
            }

            result.push(Json::Array(col));
        }

        Ok((result, next_cursor))
    }

    /// GROUP BY 检测与执行：当 FIND 混合 Variable(X) + Aggregation(Y) 且存在分组关系时，
    /// 按 X 分组计算每组的聚合值，返回索引对齐的列数组。
    ///
    /// 例如 `FIND(?d.name, COUNT(?n))` 其中 ctx.groups 有 ("d", "n") 映射，
    /// 则对每个 ?d 实体查找其对应的 ?n 成员集合，计算 COUNT。
    /// 返回 `[["Domain1", "Domain2", ...], [15, 3, ...]]`
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn detect_and_execute_grouped_find(
        &self,
        ctx: &mut QueryContext,
        clause: &FindClause,
        bindings: &FxHashMap<String, Vec<EntityID>>,
        order_by: &[OrderByCondition],
        cursor: &Option<String>,
        limit: usize,
    ) -> Result<Option<(Vec<Json>, Option<String>)>, KipError> {
        // 收集所有 Variable 的基变量名和所有 Aggregation 的基变量名
        let mut var_names: Vec<&str> = Vec::new();
        let mut agg_vars: Vec<&str> = Vec::new();
        let mut has_agg = false;

        for expr in &clause.expressions {
            match expr {
                FindExpression::Variable(dot_path) => {
                    if !var_names.contains(&&*dot_path.var) {
                        var_names.push(&dot_path.var);
                    }
                }
                FindExpression::Aggregation { var, .. } => {
                    has_agg = true;
                    if !agg_vars.contains(&&*var.var) {
                        agg_vars.push(&var.var);
                    }
                }
            }
        }

        // 需要同时存在 Variable 和 Aggregation，且它们引用不同变量
        if !has_agg || var_names.is_empty() {
            return Ok(None);
        }

        // 查找分组关系：Variable(X) → Aggregation(Y) 的 (X, Y) 映射
        let mut group_key: Option<(&str, &str)> = None;
        for &gvar in &var_names {
            for &mvar in &agg_vars {
                if gvar != mvar
                    && ctx
                        .groups
                        .contains_key(&(gvar.to_string(), mvar.to_string()))
                {
                    group_key = Some((gvar, mvar));
                    break;
                }
            }
            if group_key.is_some() {
                break;
            }
        }

        let (gvar, mvar) = match group_key {
            Some(k) => k,
            None => return Ok(None),
        };

        // 获取 group variable 的实体 ID 列表
        let group_ids = match bindings.get(gvar) {
            Some(ids) => ids.clone(),
            None => return Ok(None),
        };

        let groups_map = ctx
            .groups
            .get(&(gvar.to_string(), mvar.to_string()))
            .cloned()
            .unwrap_or_default();

        // 构造每行数据：(group_entity_id, member_count, member_ids)
        struct GroupRow {
            gid: EntityID,
            member_ids: Vec<EntityID>,
        }
        let mut rows: Vec<GroupRow> = Vec::with_capacity(group_ids.len());
        for gid in &group_ids {
            let member_ids = groups_map.get(gid).map(|v| v.to_vec()).unwrap_or_default();
            rows.push(GroupRow {
                gid: gid.clone(),
                member_ids,
            });
        }

        // 检查是否有聚合排序（ORDER BY 中引用了聚合变量的路径）
        // 对于 ORDER BY COUNT(?n) ASC，解析器会生成对聚合结果的排序
        let has_agg_order = order_by.iter().any(|o| o.is_aggregation());
        let has_var_order = order_by
            .iter()
            .any(|o| !o.is_aggregation() && o.variable.var == gvar);

        if has_agg_order {
            // 按聚合值排序
            let agg_direction = order_by
                .iter()
                .find(|o| o.is_aggregation())
                .map(|o| &o.direction)
                .unwrap_or(&OrderDirection::Asc);

            rows.sort_by(|a, b| {
                let ord = a.member_ids.len().cmp(&b.member_ids.len());
                match agg_direction {
                    OrderDirection::Asc => ord,
                    OrderDirection::Desc => ord.reverse(),
                }
            });
        } else if has_var_order {
            // 按 group variable 字段排序 — 需要加载实体数据才能排序
            // 这里延迟到 resolve 阶段处理
        }

        // 应用 cursor (基于 group entity ID)
        let cursor_id: Option<EntityID> = BTree::from_cursor(cursor).ok().flatten();
        if let Some(ref cid) = cursor_id
            && let Some(pos) = rows.iter().position(|r| &r.gid == cid)
        {
            rows = rows.split_off(pos + 1);
        }

        // 应用 limit
        let mut next_cursor: Option<String> = None;
        if limit > 0 && rows.len() > limit {
            rows.truncate(limit);
            next_cursor = rows.last().and_then(|r| BTree::to_cursor(&r.gid));
        }

        // 生成结果列
        let mut result: Vec<Json> = Vec::with_capacity(clause.expressions.len());

        for expr in &clause.expressions {
            match expr {
                FindExpression::Variable(dot_path) => {
                    if dot_path.var == gvar {
                        // 按行顺序加载 group variable 的字段
                        let field = dot_path.to_pointer();
                        let mut col: Vec<Json> = Vec::with_capacity(rows.len());
                        for row in &rows {
                            let val = self.load_entity_field(&ctx.cache, &row.gid, &field).await?;
                            col.push(val);
                        }
                        result.push(Json::Array(col));
                    } else {
                        // 非 group variable — 按全局绑定解析
                        let eid_cursor: Option<EntityID> =
                            BTree::from_cursor(cursor).ok().flatten();
                        let (col, _) = self
                            .resolve_find_var(
                                ctx,
                                bindings,
                                &dot_path.var,
                                &[dot_path.to_pointer()],
                                order_by,
                                eid_cursor.as_ref(),
                                cursor.as_deref(),
                                limit,
                            )
                            .await?;
                        result.push(Json::Array(col));
                    }
                }
                FindExpression::Aggregation {
                    func,
                    var: agg_dot_path,
                    distinct,
                } => {
                    if agg_dot_path.var == mvar {
                        // 分组聚合：对每个 group 的 member 集合计算聚合
                        let mut col: Vec<Json> = Vec::with_capacity(rows.len());
                        for row in &rows {
                            let agg_val = self
                                .compute_group_aggregation(
                                    ctx,
                                    func,
                                    agg_dot_path,
                                    &row.member_ids,
                                    *distinct,
                                )
                                .await?;
                            col.push(agg_val);
                        }
                        result.push(Json::Array(col));
                    } else {
                        // 非分组聚合变量 — 全局聚合
                        if matches!(func, AggregationFunction::Count) {
                            let count = bindings
                                .get(&agg_dot_path.var)
                                .map(|ids| ids.len())
                                .unwrap_or(0);
                            result.push(Json::from(count));
                        } else {
                            let (vals, _) = self
                                .resolve_find_var(
                                    ctx,
                                    bindings,
                                    &agg_dot_path.var,
                                    &[agg_dot_path.to_pointer_or("id")],
                                    &[],
                                    None,
                                    None,
                                    0,
                                )
                                .await?;
                            result.push(func.calculate(&vals, *distinct));
                        }
                    }
                }
            }
        }

        Ok(Some((result, next_cursor)))
    }

    /// 为分组模式加载单个实体的指定字段值
    pub(super) async fn load_entity_field(
        &self,
        cache: &QueryCache,
        eid: &EntityID,
        field: &str,
    ) -> Result<Json, KipError> {
        match eid {
            EntityID::Concept(id) => {
                self.try_get_concept_with(cache, *id, |concept| {
                    let val = extract_concept_field_value(concept, &[])?;
                    if field.is_empty() {
                        Ok(val)
                    } else {
                        Ok(val.pointer(field).cloned().unwrap_or(Json::Null))
                    }
                })
                .await
            }
            EntityID::Proposition(id, predicate) => {
                self.try_get_proposition_with(cache, *id, |prop| {
                    let val = extract_proposition_field_value(prop, predicate, &[])?;
                    if field.is_empty() {
                        Ok(val)
                    } else {
                        Ok(val.pointer(field).cloned().unwrap_or(Json::Null))
                    }
                })
                .await
            }
        }
    }

    /// 计算分组聚合值
    pub(super) async fn compute_group_aggregation(
        &self,
        ctx: &QueryContext,
        func: &AggregationFunction,
        agg_dot_path: &DotPathVar,
        member_ids: &[EntityID],
        distinct: bool,
    ) -> Result<Json, KipError> {
        // COUNT 优化：直接计数，无需加载实体数据
        if matches!(func, AggregationFunction::Count) {
            return Ok(Json::from(member_ids.len()));
        }

        // 其他聚合函数需要加载实体字段值
        let field = agg_dot_path.to_pointer_or("id");
        let mut values: Vec<Json> = Vec::with_capacity(member_ids.len());
        for eid in member_ids {
            let val = self.load_entity_field(&ctx.cache, eid, &field).await?;
            values.push(val);
        }
        Ok(func.calculate(&values, distinct))
    }

    pub(super) async fn evaluate_filter_operand(
        &self,
        ctx: &mut QueryContext,
        operand: FilterOperand,
        bindings_snapshot: &mut FxHashMap<String, Vec<EntityID>>,
        bindings_cursor: &mut FxHashMap<String, EntityID>,
    ) -> Result<Option<Json>, KipError> {
        match operand {
            FilterOperand::Variable(dot_path) => {
                self.consume_bindings(&ctx.cache, dot_path, bindings_snapshot, bindings_cursor)
                    .await
            }
            FilterOperand::Literal(value) => Ok(Some(value.into())),
            FilterOperand::List(values) => Ok(Some(Json::Array(
                values.into_iter().map(Json::from).collect(),
            ))),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn resolve_result(
        &self,
        cache: &QueryCache,
        bindings: &FxHashMap<String, Vec<EntityID>>,
        var: &str,
        fields: &[String],
        order_by: &[OrderByCondition],
        cursor: Option<&EntityID>,
        limit: usize,
    ) -> Result<(Vec<Json>, Option<String>), KipError> {
        let ids = bindings
            .get(var)
            .ok_or_else(|| KipError::reference_error(format!("Unbound variable: {var:?}")))?;

        let mut result = Vec::with_capacity(ids.len());
        let has_order_by = order_by
            .iter()
            .any(|v| !v.is_aggregation() && v.variable.var == var);
        for eid in ids {
            if !has_order_by && cursor.map(|v| eid <= v).unwrap_or(false) {
                continue;
            }

            match eid {
                EntityID::Concept(id) => {
                    let rt = self
                        .try_get_concept_with(cache, *id, |concept| {
                            extract_concept_field_value(concept, &[])
                        })
                        .await?;
                    result.push((eid, rt));
                }
                EntityID::Proposition(id, predicate) => {
                    let rt = self
                        .try_get_proposition_with(cache, *id, |prop| {
                            extract_proposition_field_value(prop, predicate, &[])
                        })
                        .await?;
                    result.push((eid, rt));
                }
            };

            if !has_order_by && limit > 0 && result.len() >= limit {
                break;
            }
        }

        if has_order_by {
            result = apply_order_by(result, var, order_by);
            if let Some(cursor) = cursor
                && let Some(idx) = result.iter().position(|(eid, _)| eid == &cursor)
                && idx < result.len()
            {
                result = result.split_off(idx + 1);
            }
        }

        let mut next_cursor: Option<String> = None;
        if limit > 0 && limit <= result.len() {
            result.truncate(limit);
            next_cursor = result.last().and_then(|(eid, _)| BTree::to_cursor(eid));
        }

        match fields.len() {
            0 => Ok((result.into_iter().map(|(_, v)| v).collect(), next_cursor)),
            1 if fields[0].is_empty() => {
                Ok((result.into_iter().map(|(_, v)| v).collect(), next_cursor))
            }
            1 => Ok((
                result
                    .into_iter()
                    .map(|(_, v)| v.pointer(&fields[0]).cloned().unwrap_or(Json::Null))
                    .collect(),
                next_cursor,
            )),
            _ => Ok((
                result
                    .into_iter()
                    .map(|(_, v)| {
                        let v: Vec<Json> = fields
                            .iter()
                            .map(|p| v.pointer(p).cloned().unwrap_or(Json::Null))
                            .collect();
                        Json::Array(v)
                    })
                    .collect(),
                next_cursor,
            )),
        }
    }

    pub(super) async fn consume_bindings(
        &self,
        cache: &QueryCache,
        dot_path: DotPathVar,
        bindings_snapshot: &mut FxHashMap<String, Vec<EntityID>>,
        bindings_cursor: &mut FxHashMap<String, EntityID>,
    ) -> Result<Option<Json>, KipError> {
        let entity_id = match bindings_cursor.get(&dot_path.var) {
            Some(id) => id.clone(),
            None => {
                // 如果当前游标没有绑定，尝试从快照中获取
                let ids = bindings_snapshot.get_mut(&dot_path.var).ok_or_else(|| {
                    KipError::reference_error(format!("Unbound variable: {:?}", dot_path.var))
                })?;

                let id = match ids.pop() {
                    Some(id) => id,
                    None => return Ok(None), // 如果没有更多ID，返回None
                };

                bindings_cursor.insert(dot_path.var.clone(), id.clone());
                id
            }
        };

        match entity_id {
            EntityID::Concept(id) => {
                let rt = self
                    .try_get_concept_with(cache, id, |concept| {
                        extract_concept_field_value(concept, &dot_path.path)
                    })
                    .await?;

                Ok(Some(rt))
            }
            EntityID::Proposition(id, predicate) => {
                let rt = self
                    .try_get_proposition_with(cache, id, |proposition| {
                        extract_proposition_field_value(proposition, &predicate, &dot_path.path)
                    })
                    .await?;

                Ok(Some(rt))
            }
        }
    }

    pub(super) async fn evaluate_filter_expression(
        &self,
        ctx: &mut QueryContext,
        expr: FilterExpression,
        bindings_snapshot: &mut FxHashMap<String, Vec<EntityID>>,
        bindings_cursor: &mut FxHashMap<String, EntityID>,
    ) -> Result<Option<bool>, KipError> {
        match expr {
            FilterExpression::Comparison {
                left,
                operator,
                right,
            } => {
                let left_val = match self
                    .evaluate_filter_operand(ctx, left, bindings_snapshot, bindings_cursor)
                    .await?
                {
                    Some(val) => val,
                    None => return Ok(None),
                };
                let right_val = match self
                    .evaluate_filter_operand(ctx, right, bindings_snapshot, bindings_cursor)
                    .await?
                {
                    Some(val) => val,
                    None => return Ok(None),
                };

                Ok(Some(operator.compare(&left_val, &right_val)))
            }
            FilterExpression::Logical {
                left,
                operator,
                right,
            } => {
                let left_result = match Box::pin(self.evaluate_filter_expression(
                    ctx,
                    *left,
                    bindings_snapshot,
                    bindings_cursor,
                ))
                .await?
                {
                    Some(result) => result,
                    None => return Ok(None),
                };

                // Short-circuit: skip right evaluation when result is already determined
                // and right side won't consume new bindings (all its variables are
                // already bound in bindings_cursor from left side evaluation).
                let can_short_circuit = match &operator {
                    LogicalOperator::And if !left_result => true,
                    LogicalOperator::Or if left_result => true,
                    _ => false,
                };
                if can_short_circuit && !right.has_unbound_variables(bindings_cursor) {
                    return Ok(Some(left_result));
                }

                let right_result = match Box::pin(self.evaluate_filter_expression(
                    ctx,
                    *right,
                    bindings_snapshot,
                    bindings_cursor,
                ))
                .await?
                {
                    Some(result) => result,
                    None => return Ok(None),
                };

                Ok(match operator {
                    LogicalOperator::And => Some(left_result && right_result),
                    LogicalOperator::Or => Some(left_result || right_result),
                })
            }
            FilterExpression::Not(expr) => {
                let result = Box::pin(self.evaluate_filter_expression(
                    ctx,
                    *expr,
                    bindings_snapshot,
                    bindings_cursor,
                ))
                .await?;
                Ok(result.map(|r| !r))
            }
            FilterExpression::Function { func, args } => {
                self.evaluate_filter_function(ctx, func, args, bindings_snapshot, bindings_cursor)
                    .await
            }
        }
    }

    pub(super) async fn evaluate_filter_function(
        &self,
        ctx: &mut QueryContext,
        func: FilterFunction,
        mut args: Vec<FilterOperand>,
        bindings_snapshot: &mut FxHashMap<String, Vec<EntityID>>,
        bindings_cursor: &mut FxHashMap<String, EntityID>,
    ) -> Result<Option<bool>, KipError> {
        match func {
            FilterFunction::IsNull | FilterFunction::IsNotNull => {
                if args.len() != 1 {
                    return Err(KipError::invalid_syntax(format!(
                        "{func:?} requires exactly 1 argument"
                    )));
                }
                let arg = args.pop().unwrap();
                let val = self
                    .evaluate_filter_operand(ctx, arg, bindings_snapshot, bindings_cursor)
                    .await?;
                match func {
                    FilterFunction::IsNull => Ok(val.map(|v| v.is_null())),
                    FilterFunction::IsNotNull => Ok(val.map(|v| !v.is_null())),
                    _ => unreachable!(),
                }
            }
            FilterFunction::In => {
                if args.len() != 2 {
                    return Err(KipError::invalid_syntax(
                        "IN requires exactly 2 arguments".to_string(),
                    ));
                }
                let list_arg = args.pop().unwrap();
                let expr_arg = args.pop().unwrap();
                let expr_val = match self
                    .evaluate_filter_operand(ctx, expr_arg, bindings_snapshot, bindings_cursor)
                    .await?
                {
                    Some(val) => val,
                    None => return Ok(None),
                };
                let list_val = match self
                    .evaluate_filter_operand(ctx, list_arg, bindings_snapshot, bindings_cursor)
                    .await?
                {
                    Some(val) => val,
                    None => return Ok(None),
                };
                match list_val {
                    Json::Array(arr) => Ok(Some(arr.contains(&expr_val))),
                    _ => Err(KipError::invalid_syntax(
                        "IN second argument must be a list".to_string(),
                    )),
                }
            }
            _ => {
                if args.len() != 2 {
                    return Err(KipError::invalid_syntax(
                        "Filter functions require exactly 2 arguments".to_string(),
                    ));
                }
                let pattern_arg = args.pop().unwrap();
                let str_arg = args.pop().unwrap();
                let str_val = match self
                    .evaluate_filter_operand(ctx, str_arg, bindings_snapshot, bindings_cursor)
                    .await?
                {
                    Some(val) => val,
                    None => return Ok(None),
                };
                let pattern_val = match self
                    .evaluate_filter_operand(ctx, pattern_arg, bindings_snapshot, bindings_cursor)
                    .await?
                {
                    Some(val) => val,
                    None => return Ok(None),
                };

                let string = str_val.as_str().unwrap_or("");
                let pattern = pattern_val.as_str().unwrap_or("");

                match func {
                    FilterFunction::Contains => Ok(Some(string.contains(pattern))),
                    FilterFunction::StartsWith => Ok(Some(string.starts_with(pattern))),
                    FilterFunction::EndsWith => Ok(Some(string.ends_with(pattern))),
                    FilterFunction::Regex => {
                        let rt = if let Some(compiled) = ctx.regex_cache.get(pattern) {
                            compiled.is_match(string)
                        } else {
                            let compiled = regex::Regex::new(pattern).map_err(|e| {
                                KipError::invalid_syntax(format!("Invalid regex: {e:?}"))
                            })?;
                            let rt = compiled.is_match(string);
                            ctx.regex_cache.insert(pattern.to_string(), compiled);
                            rt
                        };
                        Ok(Some(rt))
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
}
