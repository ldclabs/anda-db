//! KQL execution: `WHERE` clause evaluation (concept / proposition /
//! `FILTER` / `NOT` / `OPTIONAL` / `UNION`), `FIND` projection with
//! grouping, aggregation, ordering and cursor pagination.

use super::*;

/// One variable's binding inside a single candidate solution.
///
/// `FILTER` evaluation and cartesian `FIND` materialization both test one
/// concrete assignment (variable → binding) at a time; evaluation is pure,
/// with no binding consumption or iteration state.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) enum FilterBindingValue {
    /// The variable binds a concept node or proposition link.
    Entity(EntityID),
    /// The variable binds a predicate name (KIP §3.4.2 — a string).
    Predicate(String),
    /// OPTIONAL-padded position: every dot projection yields `null`.
    Null,
}

/// A concrete assignment of filter variables to bindings for one evaluation.
pub(super) type FilterAssignment = FxHashMap<String, FilterBindingValue>;

/// Engine cap on the number of solution rows a single clause materializes:
/// disconnected cross-variable `FILTER` / cartesian `FIND` combinations and
/// the unconstrained `(?s, ?p, ?o)` full scan. Beyond it the command fails
/// with `KIP_4002` — connect the variables through graph patterns or narrow
/// them first.
pub(super) const MAX_SOLUTION_COMBINATIONS: usize = 65_536;

/// Parses a numeric offset cursor (issued by the relation-row / cartesian
/// FIND paths and predicate-variable pagination). A token that is not a
/// plain decimal offset is rejected instead of silently treated as `0`,
/// which would hand the client duplicate pages.
pub(super) fn parse_offset_cursor(raw_cursor: Option<&str>) -> Result<usize, KipError> {
    match raw_cursor {
        None => Ok(0),
        Some(cursor) => cursor
            .parse::<usize>()
            .map_err(|_| KipError::invalid_syntax(format!("Invalid CURSOR token: {cursor:?}"))),
    }
}

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
        let concept_ids: Vec<EntityID> = match self.query_concept_ids(&clause.matcher).await {
            Ok(ids) => ids.into_iter().map(EntityID::Concept).collect(),
            // Dangling `{id:}` / `{type, name}` grounding inside a NOT /
            // OPTIONAL / UNION sub-block degrades to an empty match (KIP
            // §3.4.7); only the precise KIP_3002 grounding error is
            // degraded — storage failures still propagate.
            Err(err) if ctx.lenient_grounding && err.code == KipErrorCode::NotFound => Vec::new(),
            Err(err) => return Err(err),
        };

        if let Some(existing) = ctx.entities.get_mut(&clause.variable) {
            // Variable already bound: filter (intersect) existing bindings
            let allowed: FxHashSet<&EntityID> = concept_ids.iter().collect();
            existing.retain(|id| allowed.contains(id));
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
                // Match-only `(id:)` target: KIP_3002 when dangling (spec
                // RC8). Inside a NOT / OPTIONAL / UNION sub-block the
                // dangling link degrades to an empty match instead (KIP
                // §3.4.7); storage failures still propagate.
                match self
                    .ensure_proposition_link_exists(&ctx.cache, &entity_id)
                    .await
                {
                    Ok(()) => TargetEntities::IDs(vec![entity_id]),
                    Err(err) if ctx.lenient_grounding && err.code == KipErrorCode::NotFound => {
                        TargetEntities::IDs(Vec::new())
                    }
                    Err(err) => return Err(err),
                }
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

    /// Executes a `FILTER` clause with per-solution semantics.
    ///
    /// Dispatch:
    /// 1. **No variables** — constant expression: `false` clears every
    ///    binding (no solution survives).
    /// 2. **Row path** — the referenced variables are all covered by one
    ///    relation (and the filter is cross-variable or involves a predicate
    ///    variable): each relation row is tested as one solution; the
    ///    relation's rows and *all* of its variables are narrowed to the
    ///    survivors. This is what makes
    ///    `?link (?s, ?p, ?o) FILTER(?p != "belongs_to_domain")` narrow
    ///    `?link` itself (the memory-metabolism idiom).
    /// 3. **Cross-product path** — cross-variable filter with no covering
    ///    relation: solutions are the cartesian product of the variables'
    ///    bindings (capped at [`MAX_SOLUTION_COMBINATIONS`]); a binding
    ///    survives when it participates in ≥1 satisfying combination, and
    ///    the satisfying pairs are recorded as a synthetic relation so FIND
    ///    can keep the columns solution-aligned.
    /// 4. **Single-variable path** — each binding is tested independently.
    pub(super) async fn execute_filter_clause(
        &self,
        ctx: &mut QueryContext,
        clause: FilterClause,
    ) -> Result<(), KipError> {
        Self::collect_filter_row_sensitive_vars(&clause.expression, &mut ctx.row_sensitive_vars);

        let mut filter_vars_set: FxHashSet<String> = FxHashSet::default();
        Self::collect_filter_vars(&clause.expression, &mut filter_vars_set);
        let mut filter_vars: Vec<String> = filter_vars_set.into_iter().collect();
        filter_vars.sort();

        for var in &filter_vars {
            if !ctx.entities.contains_key(var) && !ctx.predicates.contains_key(var) {
                return Err(KipError::reference_error(format!(
                    "Unbound variable: {var:?}"
                )));
            }
        }

        // The evaluator needs the shared entity cache immutably and the regex
        // cache mutably; take the regex cache out for the duration.
        let cache = ctx.cache.clone();
        let mut regex_cache = std::mem::take(&mut ctx.regex_cache);
        let result = self
            .execute_filter_clause_inner(
                ctx,
                &clause.expression,
                &filter_vars,
                &cache,
                &mut regex_cache,
            )
            .await;
        ctx.regex_cache = regex_cache;
        result
    }

    async fn execute_filter_clause_inner(
        &self,
        ctx: &mut QueryContext,
        expr: &FilterExpression,
        filter_vars: &[String],
        cache: &QueryCache,
        regex_cache: &mut FxHashMap<String, regex::Regex>,
    ) -> Result<(), KipError> {
        if filter_vars.is_empty() {
            // Constant expression: `FILTER(false)` discards every solution.
            let keep = self
                .eval_filter_assigned(cache, regex_cache, expr, &FilterAssignment::default())
                .await?;
            if !keep {
                for ids in ctx.entities.values_mut() {
                    ids.retain(|_| false);
                }
                for preds in ctx.predicates.values_mut() {
                    preds.retain(|_| false);
                }
                for relation in ctx.relations.iter_mut() {
                    relation.rows.clear();
                }
            }
            return Ok(());
        }

        let has_pred_var = filter_vars
            .iter()
            .any(|var| !ctx.entities.contains_key(var) && ctx.predicates.contains_key(var));
        let multi = filter_vars.len() >= 2;

        if multi || has_pred_var {
            // Row path: the latest relation covering every filter variable.
            if let Some(idx) = ctx.relations.iter().rposition(|relation| {
                filter_vars
                    .iter()
                    .all(|var| Self::relation_covers_var(relation, var))
            }) {
                return self
                    .filter_relation_rows(ctx, idx, expr, cache, regex_cache)
                    .await;
            }
            if multi {
                return self
                    .filter_cross_product(ctx, expr, filter_vars, cache, regex_cache)
                    .await;
            }
        }

        self.filter_single_var(ctx, expr, &filter_vars[0], cache, regex_cache)
            .await
    }

    /// Collects every variable referenced by a FILTER expression (bare or
    /// dot-notation).
    pub(super) fn collect_filter_vars(expr: &FilterExpression, vars: &mut FxHashSet<String>) {
        fn collect_operand(operand: &FilterOperand, vars: &mut FxHashSet<String>) {
            if let FilterOperand::Variable(dot_path) = operand {
                vars.insert(dot_path.var.clone());
            }
        }

        match expr {
            FilterExpression::Comparison { left, right, .. } => {
                collect_operand(left, vars);
                collect_operand(right, vars);
            }
            FilterExpression::Logical { left, right, .. } => {
                Self::collect_filter_vars(left, vars);
                Self::collect_filter_vars(right, vars);
            }
            FilterExpression::Not(inner) => Self::collect_filter_vars(inner, vars),
            FilterExpression::Function { args, .. } => {
                for arg in args {
                    collect_operand(arg, vars);
                }
            }
        }
    }

    /// Single-variable FILTER: tests each binding independently and removes
    /// the ones that fail. Relation rows referencing removed bindings are
    /// filtered later by [`Self::relation_row_matches_context`].
    async fn filter_single_var(
        &self,
        ctx: &mut QueryContext,
        expr: &FilterExpression,
        var: &str,
        cache: &QueryCache,
        regex_cache: &mut FxHashMap<String, regex::Regex>,
    ) -> Result<(), KipError> {
        let mut assign = FilterAssignment::default();

        if let Some(ids) = ctx.entities.get(var) {
            let snapshot = ids.to_vec();
            let mut removed: FxHashSet<EntityID> = FxHashSet::default();
            for id in snapshot {
                assign.insert(var.to_string(), FilterBindingValue::Entity(id.clone()));
                if !self
                    .eval_filter_assigned(cache, regex_cache, expr, &assign)
                    .await?
                {
                    removed.insert(id);
                }
            }
            if !removed.is_empty()
                && let Some(existing) = ctx.entities.get_mut(var)
            {
                existing.retain(|id| !removed.contains(id));
            }
            return Ok(());
        }

        if let Some(preds) = ctx.predicates.get(var) {
            let snapshot = preds.to_vec();
            let mut removed: FxHashSet<String> = FxHashSet::default();
            for pred in snapshot {
                assign.insert(var.to_string(), FilterBindingValue::Predicate(pred.clone()));
                if !self
                    .eval_filter_assigned(cache, regex_cache, expr, &assign)
                    .await?
                {
                    removed.insert(pred);
                }
            }
            if !removed.is_empty()
                && let Some(existing) = ctx.predicates.get_mut(var)
            {
                existing.retain(|pred| !removed.contains(pred));
            }
            return Ok(());
        }

        Err(KipError::reference_error(format!(
            "Unbound variable: {var:?}"
        )))
    }

    /// Row-path FILTER: each row of the covering relation is one candidate
    /// solution. Surviving rows replace the relation's rows, and every
    /// variable the relation binds is narrowed by removing the bindings
    /// that only appeared in discarded rows (bindings contributed by other
    /// clauses — e.g. another `UNION` branch — are preserved).
    async fn filter_relation_rows(
        &self,
        ctx: &mut QueryContext,
        idx: usize,
        expr: &FilterExpression,
        cache: &QueryCache,
        regex_cache: &mut FxHashMap<String, regex::Regex>,
    ) -> Result<(), KipError> {
        let rows = std::mem::take(&mut ctx.relations[idx].rows);
        let relation_meta = ctx.relations[idx].clone(); // rows already taken: cheap

        let slot_vars: Vec<&str> = [
            relation_meta.proposition_var.as_deref(),
            relation_meta.subject_var.as_deref(),
            relation_meta.predicate_var.as_deref(),
            relation_meta.object_var.as_deref(),
        ]
        .into_iter()
        .flatten()
        .collect();

        // Rows are evaluated in place and tagged with a keep flag — no
        // duplicate copies of the row set are materialized.
        let mut evaluated: Vec<(QueryRelationRow, bool)> = Vec::with_capacity(rows.len());
        for row in rows {
            if !Self::relation_row_matches_context(ctx, &relation_meta, &row) {
                continue;
            }
            let mut assign = FilterAssignment::default();
            for var in &slot_vars {
                let binding =
                    if let Some(entity) = Self::relation_row_entity(&relation_meta, &row, var) {
                        match entity {
                            Some(entity) => FilterBindingValue::Entity(entity.clone()),
                            None => FilterBindingValue::Null,
                        }
                    } else if let Some(predicate) =
                        Self::relation_row_predicate(&relation_meta, &row, var)
                    {
                        match predicate {
                            Some(predicate) => FilterBindingValue::Predicate(predicate.to_string()),
                            None => FilterBindingValue::Null,
                        }
                    } else {
                        FilterBindingValue::Null
                    };
                assign.insert((*var).to_string(), binding);
            }

            let keep = self
                .eval_filter_assigned(cache, regex_cache, expr, &assign)
                .await?;
            evaluated.push((row, keep));
        }

        // Narrow each relation variable: remove bindings that appeared in the
        // relation's rows but survive in none of them.
        fn narrow_entities(
            ctx: &mut QueryContext,
            var: Option<&str>,
            evaluated: &[(QueryRelationRow, bool)],
            slot: impl Fn(&QueryRelationRow) -> Option<&EntityID>,
        ) {
            let Some(var) = var else { return };
            let kept: FxHashSet<&EntityID> = evaluated
                .iter()
                .filter(|(_, keep)| *keep)
                .filter_map(|(row, _)| slot(row))
                .collect();
            let removed: FxHashSet<&EntityID> = evaluated
                .iter()
                .filter(|(_, keep)| !*keep)
                .filter_map(|(row, _)| slot(row))
                .filter(|id| !kept.contains(*id))
                .collect();
            if removed.is_empty() {
                return;
            }
            if let Some(existing) = ctx.entities.get_mut(var) {
                existing.retain(|id| !removed.contains(id));
            }
        }

        narrow_entities(
            ctx,
            relation_meta.proposition_var.as_deref(),
            &evaluated,
            |row| row.proposition.as_ref(),
        );
        narrow_entities(
            ctx,
            relation_meta.subject_var.as_deref(),
            &evaluated,
            |row| row.subject.as_ref(),
        );
        narrow_entities(
            ctx,
            relation_meta.object_var.as_deref(),
            &evaluated,
            |row| row.object.as_ref(),
        );

        if let Some(var) = relation_meta.predicate_var.as_deref() {
            let kept: FxHashSet<&str> = evaluated
                .iter()
                .filter(|(_, keep)| *keep)
                .filter_map(|(row, _)| row.predicate.as_deref())
                .collect();
            let removed: FxHashSet<&str> = evaluated
                .iter()
                .filter(|(_, keep)| !*keep)
                .filter_map(|(row, _)| row.predicate.as_deref())
                .filter(|pred| !kept.contains(*pred))
                .collect();
            if !removed.is_empty()
                && let Some(existing) = ctx.predicates.get_mut(var)
            {
                existing.retain(|pred| !removed.contains(pred.as_str()));
            }
        }

        ctx.relations[idx].rows = evaluated
            .into_iter()
            .filter(|(_, keep)| *keep)
            .map(|(row, _)| row)
            .collect();
        Ok(())
    }

    /// Cross-product FILTER for variables not covered by any relation:
    /// existential narrowing (a binding survives when some combination
    /// satisfies the filter), with the satisfying combinations recorded as a
    /// synthetic relation (up to two entity variables and one predicate
    /// variable) so FIND can project the exact pairs.
    async fn filter_cross_product(
        &self,
        ctx: &mut QueryContext,
        expr: &FilterExpression,
        filter_vars: &[String],
        cache: &QueryCache,
        regex_cache: &mut FxHashMap<String, regex::Regex>,
    ) -> Result<(), KipError> {
        struct Dim {
            var: String,
            is_pred: bool,
            values: Vec<FilterBindingValue>,
        }

        let mut dims: Vec<Dim> = Vec::with_capacity(filter_vars.len());
        for var in filter_vars {
            if let Some(ids) = ctx.entities.get(var) {
                dims.push(Dim {
                    var: var.clone(),
                    is_pred: false,
                    values: ids
                        .iter()
                        .map(|id| FilterBindingValue::Entity(id.clone()))
                        .collect(),
                });
            } else if let Some(preds) = ctx.predicates.get(var) {
                dims.push(Dim {
                    var: var.clone(),
                    is_pred: true,
                    values: preds
                        .iter()
                        .map(|pred| FilterBindingValue::Predicate(pred.clone()))
                        .collect(),
                });
            } else {
                return Err(KipError::reference_error(format!(
                    "Unbound variable: {var:?}"
                )));
            }
        }

        // An empty dimension means the solution set is empty: no combination
        // can satisfy the filter, so every referenced variable empties.
        if dims.iter().any(|dim| dim.values.is_empty()) {
            for dim in &dims {
                if dim.is_pred {
                    if let Some(existing) = ctx.predicates.get_mut(&dim.var) {
                        existing.retain(|_| false);
                    }
                } else if let Some(existing) = ctx.entities.get_mut(&dim.var) {
                    existing.retain(|_| false);
                }
            }
            return Ok(());
        }

        let mut total: usize = 1;
        for dim in &dims {
            total = total.saturating_mul(dim.values.len());
        }
        if total > MAX_SOLUTION_COMBINATIONS {
            return Err(KipError::resource_exhausted(format!(
                "FILTER over {total} disconnected variable combinations exceeds the engine cap \
                 of {MAX_SOLUTION_COMBINATIONS}; connect the variables through graph patterns \
                 or narrow them before filtering"
            )));
        }

        // The satisfying combinations are recordable as a synthetic relation
        // when they fit its slots: ≤2 entity variables + ≤1 predicate variable.
        let entity_dims: Vec<usize> = (0..dims.len()).filter(|i| !dims[*i].is_pred).collect();
        let pred_dims: Vec<usize> = (0..dims.len()).filter(|i| dims[*i].is_pred).collect();
        let can_record = entity_dims.len() <= 2 && pred_dims.len() <= 1;

        let mut sat: Vec<FxHashSet<usize>> = vec![FxHashSet::default(); dims.len()];
        let mut rows: Vec<QueryRelationRow> = Vec::new();
        let mut odometer = vec![0usize; dims.len()];
        let mut assign = FilterAssignment::default();

        'combos: loop {
            for (i, dim) in dims.iter().enumerate() {
                assign.insert(dim.var.clone(), dim.values[odometer[i]].clone());
            }
            if self
                .eval_filter_assigned(cache, regex_cache, expr, &assign)
                .await?
            {
                for (i, pos) in odometer.iter().enumerate() {
                    sat[i].insert(*pos);
                }
                if can_record {
                    let entity_at = |slot: usize| -> Option<EntityID> {
                        entity_dims
                            .get(slot)
                            .and_then(|&i| match &dims[i].values[odometer[i]] {
                                FilterBindingValue::Entity(id) => Some(id.clone()),
                                _ => None,
                            })
                    };
                    let predicate =
                        pred_dims
                            .first()
                            .and_then(|&i| match &dims[i].values[odometer[i]] {
                                FilterBindingValue::Predicate(pred) => Some(pred.clone()),
                                _ => None,
                            });
                    rows.push(QueryRelationRow {
                        proposition: None,
                        subject: entity_at(0),
                        predicate,
                        object: entity_at(1),
                    });
                }
            }

            // Advance the odometer.
            let mut i = dims.len();
            loop {
                if i == 0 {
                    break 'combos;
                }
                i -= 1;
                odometer[i] += 1;
                if odometer[i] < dims[i].values.len() {
                    break;
                }
                odometer[i] = 0;
            }
        }

        // Existential narrowing per variable.
        for (i, dim) in dims.iter().enumerate() {
            let keep: FxHashSet<&FilterBindingValue> =
                sat[i].iter().map(|&pos| &dim.values[pos]).collect();
            if dim.is_pred {
                if let Some(existing) = ctx.predicates.get_mut(&dim.var) {
                    existing
                        .retain(|pred| keep.contains(&FilterBindingValue::Predicate(pred.clone())));
                }
            } else if let Some(existing) = ctx.entities.get_mut(&dim.var) {
                existing.retain(|id| keep.contains(&FilterBindingValue::Entity(id.clone())));
            }
        }

        // Record the exact satisfying combinations so multi-variable FIND can
        // keep the columns solution-aligned (KIP §6.2.2).
        if can_record {
            ctx.relations.push(QueryRelationBinding {
                proposition_var: None,
                subject_var: entity_dims.first().map(|&i| dims[i].var.clone()),
                predicate_var: pred_dims.first().map(|&i| dims[i].var.clone()),
                object_var: entity_dims.get(1).map(|&i| dims[i].var.clone()),
                rows,
                origin: RelationOrigin::Pattern,
            });
        } else {
            // The exact combinations are lost: a later multi-column FIND
            // over these variables would re-materialize a full cross
            // product (misaligned columns). Mark them so FIND can reject
            // that projection with KIP_4002 instead.
            for dim in &dims {
                ctx.unaligned_filter_vars.insert(dim.var.clone());
            }
        }

        Ok(())
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

        // 标准路径。NOT 是纯过滤器（KIP §3.4.7.1）：只允许它收窄自己
        // **引用过**的外部变量；未被 NOT 模式提及的外部绑定必须原样保留
        // （否则克隆进 not_context 的无关变量会被整列减掉）。
        let mut not_vars: FxHashSet<String> = FxHashSet::default();
        Self::collect_clause_vars(&clauses, &mut not_vars);

        // Lightweight child: NOT only reads/narrows bindings, so relations,
        // groups and the regex cache need not be cloned in. Grounding is
        // lenient inside the block (KIP §3.4.7.1): a dangling id makes the
        // NOT pattern unmatchable, i.e. the clause succeeds and excludes
        // nothing — it must not abort the query.
        let mut not_context = ctx.scoped_child();
        not_context.lenient_grounding = true;
        for clause in clauses {
            Box::pin(self.execute_where_clause(&mut not_context, clause)).await?;
        }

        // Row-level narrowing inside the NOT block (KIP §3.4.7.1): the
        // block's clauses are conjunctive, so a binding only participates in
        // the NOT pattern when it still appears in a row of every covering
        // pattern relation that is consistent with the block's *final*
        // bindings. A later clause (`?b {type: "Bot"}` after
        // `(?a, "blocked", ?b)`, a FILTER, or a dangling-id matcher that
        // emptied a variable) must shrink the exclusion sets of the earlier
        // pattern's other variables too — otherwise the NOT over-excludes
        // solutions its full pattern never matched.
        Self::narrow_bindings_to_valid_relation_rows(&mut not_context);

        // Per-solution anti-join (KIP §3.4.7.1: "exclude solutions that make
        // the internal pattern valid"). When the NOT pattern references two
        // or more outer-bound entity variables and both sides expose a
        // relation covering them, excluded *tuples* are removed from the
        // outer relation rows instead of subtracting whole binding columns
        // — column subtraction would also kill unrelated solutions sharing
        // one endpoint. Variables consumed here are exempted from the
        // column-level subtraction below; bindings are narrowed to the
        // values that still appear in some surviving row.
        let mut anti_joined_vars: FxHashSet<String> = FxHashSet::default();
        let shared_vars: Vec<String> = not_vars
            .iter()
            .filter(|var| ctx.entities.contains_key(*var))
            .cloned()
            .collect();
        if shared_vars.len() >= 2
            && let Some(not_relation) = not_context.relations.iter().rev().find(|relation| {
                shared_vars
                    .iter()
                    .all(|var| Self::relation_covers_var(relation, var))
            })
            && ctx.relations.iter().any(|relation| {
                relation.origin != RelationOrigin::Optional
                    && shared_vars
                        .iter()
                        .all(|var| Self::relation_covers_var(relation, var))
            })
        {
            let project = |relation: &QueryRelationBinding,
                           row: &QueryRelationRow|
             -> Option<Vec<EntityID>> {
                let mut tuple = Vec::with_capacity(shared_vars.len());
                for var in &shared_vars {
                    match Self::relation_row_entity(relation, row, var) {
                        Some(Some(entity)) => tuple.push(entity.clone()),
                        // Padded/unbound positions never contribute an
                        // excluded tuple and are never excluded themselves.
                        _ => return None,
                    }
                }
                Some(tuple)
            };
            // Only rows still consistent with the NOT block's final
            // bindings contribute excluded tuples (KIP §3.4.7.1): later
            // clauses in the block narrow the pattern's rows, not just
            // their own column.
            let excluded_tuples: FxHashSet<Vec<EntityID>> = not_relation
                .rows
                .iter()
                .filter(|row| Self::relation_row_matches_context(&not_context, not_relation, row))
                .filter_map(|row| project(not_relation, row))
                .collect();

            let mut removed_candidates: FxHashMap<String, FxHashSet<EntityID>> =
                FxHashMap::default();
            for idx in 0..ctx.relations.len() {
                if ctx.relations[idx].origin == RelationOrigin::Optional
                    || !shared_vars
                        .iter()
                        .all(|var| Self::relation_covers_var(&ctx.relations[idx], var))
                {
                    continue;
                }
                let relation_meta = &ctx.relations[idx];
                let kept: Vec<QueryRelationRow> = relation_meta
                    .rows
                    .iter()
                    .filter(|row| {
                        let keep = match project(relation_meta, row) {
                            Some(tuple) => !excluded_tuples.contains(&tuple),
                            None => true,
                        };
                        if !keep {
                            for var in &shared_vars {
                                if let Some(Some(entity)) =
                                    Self::relation_row_entity(relation_meta, row, var)
                                {
                                    removed_candidates
                                        .entry(var.clone())
                                        .or_default()
                                        .insert(entity.clone());
                                }
                            }
                        }
                        keep
                    })
                    .cloned()
                    .collect();
                ctx.relations[idx].rows = kept;
            }

            // A binding removed from one branch remains globally live when
            // any other surviving Pattern/UNION/OPTIONAL row still uses it.
            // Narrowing against only the anti-joined relation erases valid
            // solutions from sibling UNION branches.
            let mut live_bindings: FxHashMap<String, FxHashSet<EntityID>> = FxHashMap::default();
            for relation in &ctx.relations {
                for row in &relation.rows {
                    if !Self::relation_row_matches_context(ctx, relation, row) {
                        continue;
                    }
                    for var in &shared_vars {
                        if let Some(Some(entity)) = Self::relation_row_entity(relation, row, var) {
                            live_bindings
                                .entry(var.clone())
                                .or_default()
                                .insert(entity.clone());
                        }
                    }
                }
            }
            for (var, removed) in removed_candidates {
                let live = live_bindings.get(&var);
                if let Some(existing) = ctx.entities.get_mut(&var) {
                    existing.retain(|id| {
                        !removed.contains(id) || live.is_some_and(|values| values.contains(id))
                    });
                }
            }

            // Grouped aggregates follow the per-solution semantics too:
            // rebuild each affected (group, member) map from the final
            // branch-aware relation pairs. Mandatory Pattern relations are
            // intersected (AND); independent UNION relations are appended
            // (OR). Rebuilding from only the relation touched by NOT would
            // clear group pairs still supplied by a sibling branch.
            let group_keys: Vec<(String, String)> = ctx
                .groups
                .keys()
                .filter(|(gvar, mvar)| {
                    shared_vars.iter().any(|var| var == gvar)
                        && shared_vars.iter().any(|var| var == mvar)
                })
                .cloned()
                .collect();
            for key in group_keys {
                let pairs_for = |relation: &QueryRelationBinding| {
                    let mut pairs: FxHashSet<(EntityID, EntityID)> = FxHashSet::default();
                    for row in &relation.rows {
                        if !Self::relation_row_matches_context(ctx, relation, row) {
                            continue;
                        }
                        if let (Some(Some(gid)), Some(Some(mid))) = (
                            Self::relation_row_entity(relation, row, &key.0),
                            Self::relation_row_entity(relation, row, &key.1),
                        ) {
                            pairs.insert((gid.clone(), mid.clone()));
                        }
                    }
                    pairs
                };
                let pattern_sets: Vec<FxHashSet<(EntityID, EntityID)>> = ctx
                    .relations
                    .iter()
                    .filter(|relation| {
                        relation.origin == RelationOrigin::Pattern
                            && Self::relation_covers_var(relation, &key.0)
                            && Self::relation_covers_var(relation, &key.1)
                    })
                    .map(pairs_for)
                    .collect();
                let mut surviving_pairs = if let Some((first, rest)) = pattern_sets.split_first() {
                    let mut intersection = first.clone();
                    for pairs in rest {
                        intersection.retain(|pair| pairs.contains(pair));
                    }
                    intersection
                } else {
                    FxHashSet::default()
                };
                for relation in ctx.relations.iter().filter(|relation| {
                    relation.origin == RelationOrigin::Union
                        && Self::relation_covers_var(relation, &key.0)
                        && Self::relation_covers_var(relation, &key.1)
                }) {
                    surviving_pairs.extend(pairs_for(relation));
                }
                if pattern_sets.is_empty() && surviving_pairs.is_empty() {
                    for relation in ctx.relations.iter().filter(|relation| {
                        relation.origin == RelationOrigin::Optional
                            && Self::relation_covers_var(relation, &key.0)
                            && Self::relation_covers_var(relation, &key.1)
                    }) {
                        surviving_pairs.extend(pairs_for(relation));
                    }
                }
                let mut narrowed: FxHashMap<EntityID, FxHashSet<EntityID>> = FxHashMap::default();
                for (gid, mid) in surviving_pairs {
                    narrowed.entry(gid).or_default().insert(mid);
                }
                if let Some(group_map) = ctx.groups.get_mut(&key) {
                    group_map.retain(|gid, members| match narrowed.get(gid) {
                        Some(mids) => {
                            members.retain(|mid| mids.contains(mid));
                            !members.is_empty()
                        }
                        None => false,
                    });
                }
            }

            anti_joined_vars.extend(shared_vars);
        }

        for (var, ids) in &not_context.entities {
            if ids.is_empty() || !not_vars.contains(var) || anti_joined_vars.contains(var) {
                continue;
            }
            // 如果 NOT 子句中有变量绑定，则从当前上下文中移除这些绑定
            if let Some(existing) = ctx.entities.get_mut(var) {
                let excluded: FxHashSet<&EntityID> = ids.iter().collect();
                existing.retain(|id| !excluded.contains(id));
            }
        }

        for (var, preds) in not_context.predicates {
            if preds.is_empty() || !not_vars.contains(&var) {
                continue;
            }
            // 如果 NOT 子句中有谓词绑定，则从当前上下文中移除这些绑定
            if let Some(existing) = ctx.predicates.get_mut(&var) {
                let excluded: FxHashSet<&String> = preds.iter().collect();
                existing.retain(|pred| !excluded.contains(pred));
            }
        }

        // 清理 groups 中被排除的实体。经过 per-solution 反连接的分组变量
        // 必须豁免整列删除（上面已按存活行收窄 group 成员）：整列删除会把
        // 仍有存活成员的 group 整个抹掉，分组 COUNT 错误归零。
        for ((gvar, _), group_map) in ctx.groups.iter_mut() {
            if not_vars.contains(gvar)
                && !anti_joined_vars.contains(gvar)
                && let Some(excluded_ids) = not_context.entities.get(gvar)
                && !excluded_ids.is_empty()
            {
                let excluded: FxHashSet<&EntityID> = excluded_ids.iter().collect();
                group_map.retain(|gid, _| !excluded.contains(gid));
            }
        }

        Ok(())
    }

    /// Narrows a sub-block context's entity bindings to the values that
    /// still appear in at least one **valid** row of every covering
    /// `Pattern` relation, where a row is valid when it is consistent with
    /// the block's final bindings ([`Self::relation_row_matches_context`]).
    ///
    /// Sequential clauses inside a block are conjunctive: a later clause
    /// that narrows one variable of an earlier proposition pattern also
    /// invalidates that pattern's rows, and the rows' *other* variables
    /// must shrink with them. Runs to a fixpoint (each pass only removes
    /// bindings, so it terminates); `Optional` / `Union` relations never
    /// restrict and are skipped.
    fn narrow_bindings_to_valid_relation_rows(ctx: &mut QueryContext) {
        if ctx.relations.is_empty() {
            return;
        }
        let mut vars: Vec<String> = ctx.entities.keys().cloned().collect();
        vars.sort();
        loop {
            let mut changed = false;
            for var in &vars {
                let mut allowed: Option<FxHashSet<EntityID>> = None;
                for relation in &ctx.relations {
                    if relation.origin != RelationOrigin::Pattern
                        || !Self::relation_covers_var(relation, var)
                    {
                        continue;
                    }
                    let mut values: FxHashSet<EntityID> = FxHashSet::default();
                    for row in &relation.rows {
                        if Self::relation_row_matches_context(ctx, relation, row)
                            && let Some(Some(entity)) =
                                Self::relation_row_entity(relation, row, var)
                        {
                            values.insert(entity.clone());
                        }
                    }
                    allowed = Some(match allowed {
                        None => values,
                        Some(prev) => prev.intersection(&values).cloned().collect(),
                    });
                }
                if let Some(allowed) = allowed
                    && let Some(existing) = ctx.entities.get_mut(var)
                {
                    let before = existing.len();
                    existing.retain(|id| allowed.contains(id));
                    if existing.len() != before {
                        changed = true;
                    }
                }
            }
            if !changed {
                return;
            }
        }
    }

    /// Collects every variable name referenced by a clause tree: concept /
    /// proposition clause bindings (including subject / predicate / object
    /// variables and nested proposition matchers) and FILTER operands.
    pub(super) fn collect_clause_vars(clauses: &[WhereClause], vars: &mut FxHashSet<String>) {
        fn collect_target_term(term: &TargetTerm, vars: &mut FxHashSet<String>) {
            match term {
                TargetTerm::Variable(var) => {
                    vars.insert(var.clone());
                }
                TargetTerm::Concept(_) => {}
                TargetTerm::Proposition(matcher) => collect_prop_matcher(matcher, vars),
            }
        }

        fn collect_prop_matcher(matcher: &PropositionMatcher, vars: &mut FxHashSet<String>) {
            if let PropositionMatcher::Object {
                subject,
                predicate,
                object,
            } = matcher
            {
                collect_target_term(subject, vars);
                if let PredTerm::Variable(var) = predicate {
                    vars.insert(var.clone());
                }
                collect_target_term(object, vars);
            }
        }

        fn collect_filter_expr(expr: &FilterExpression, vars: &mut FxHashSet<String>) {
            match expr {
                FilterExpression::Comparison { left, right, .. } => {
                    collect_filter_operand(left, vars);
                    collect_filter_operand(right, vars);
                }
                FilterExpression::Logical { left, right, .. } => {
                    collect_filter_expr(left, vars);
                    collect_filter_expr(right, vars);
                }
                FilterExpression::Not(inner) => collect_filter_expr(inner, vars),
                FilterExpression::Function { args, .. } => {
                    for arg in args {
                        collect_filter_operand(arg, vars);
                    }
                }
            }
        }

        fn collect_filter_operand(operand: &FilterOperand, vars: &mut FxHashSet<String>) {
            if let FilterOperand::Variable(dot_path) = operand {
                vars.insert(dot_path.var.clone());
            }
        }

        for clause in clauses {
            match clause {
                WhereClause::Concept(c) => {
                    vars.insert(c.variable.clone());
                }
                WhereClause::Proposition(p) => {
                    if let Some(var) = &p.variable {
                        vars.insert(var.clone());
                    }
                    collect_prop_matcher(&p.matcher, vars);
                }
                WhereClause::Filter(f) => collect_filter_expr(&f.expression, vars),
                WhereClause::Not(inner)
                | WhereClause::Optional(inner)
                | WhereClause::Union(inner) => Self::collect_clause_vars(inner, vars),
            }
        }
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
        // Lightweight child (shares the cache, copies only bindings): every
        // relation the child produces is a relation of the OPTIONAL block.
        // Grounding is lenient inside the block (KIP §3.4.7.2): a dangling
        // id makes the optional pattern unmatchable — the outer solutions
        // are kept and the block's variables project `null`; it must not
        // abort the query.
        let mut optional_context = ctx.scoped_child();
        optional_context.lenient_grounding = true;
        for clause in clauses {
            Box::pin(self.execute_where_clause(&mut optional_context, clause)).await?;
        }

        // Left-join padding (KIP §3.4.7.2): for every relation produced
        // inside the OPTIONAL block whose subject or object is an
        // outer-bound variable, outer entities without a match get a padded
        // row whose other positions are `None` — they project `null`. A row
        // only counts as a match while it is still consistent with the
        // block's final bindings: a later clause (a dangling-id matcher, a
        // FILTER, …) may have narrowed a variable after the relation was
        // produced, in which case the anchor entity falls back to a padded
        // (null-projecting) row instead of silently losing its solution.
        let mut padded_rows: Vec<(usize, Vec<QueryRelationRow>)> = Vec::new();
        for (idx, relation) in optional_context.relations.iter().enumerate() {
            let subject_outer = relation
                .subject_var
                .as_deref()
                .filter(|var| ctx.entities.contains_key(*var));
            let object_outer = relation
                .object_var
                .as_deref()
                .filter(|var| ctx.entities.contains_key(*var));
            let (anchor_var, anchor_is_subject) = match (subject_outer, object_outer) {
                (Some(var), None) => (var, true),
                (None, Some(var)) => (var, false),
                // Both or neither endpoint is outer-bound: no unambiguous
                // left side to pad.
                _ => continue,
            };

            let matched: FxHashSet<&EntityID> = relation
                .rows
                .iter()
                .filter(|row| Self::relation_row_matches_context(&optional_context, relation, row))
                .filter_map(|row| {
                    if anchor_is_subject {
                        row.subject.as_ref()
                    } else {
                        row.object.as_ref()
                    }
                })
                .collect();
            let padded: Vec<QueryRelationRow> = ctx.entities[anchor_var]
                .iter()
                .filter(|entity| !matched.contains(entity))
                .map(|entity| QueryRelationRow {
                    proposition: None,
                    subject: anchor_is_subject.then(|| entity.clone()),
                    predicate: None,
                    object: (!anchor_is_subject).then(|| entity.clone()),
                })
                .collect();
            if !padded.is_empty() {
                padded_rows.push((idx, padded));
            }
        }
        for (idx, padded) in padded_rows {
            optional_context.relations[idx].rows.extend(padded);
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

        // Relations produced inside OPTIONAL must never restrict the outer
        // solution set (KIP §3.4.7.2) — tag them so FIND excludes them from
        // conjunctive equi-joins.
        for mut relation in optional_context.relations {
            relation.origin = RelationOrigin::Optional;
            ctx.relations.push(relation);
        }
        ctx.row_sensitive_vars
            .extend(optional_context.row_sensitive_vars);
        ctx.unaligned_filter_vars
            .extend(optional_context.unaligned_filter_vars);

        Ok(())
    }

    fn relation_row_assignment(
        relation: &QueryRelationBinding,
        row: &QueryRelationRow,
    ) -> FilterAssignment {
        let mut assignment = FilterAssignment::default();
        for var in [
            relation.proposition_var.as_deref(),
            relation.subject_var.as_deref(),
            relation.object_var.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            let value = Self::relation_row_entity(relation, row, var)
                .flatten()
                .cloned()
                .map(FilterBindingValue::Entity)
                .unwrap_or(FilterBindingValue::Null);
            assignment.insert(var.to_string(), value);
        }
        if let Some(var) = relation.predicate_var.as_deref() {
            let value = Self::relation_row_predicate(relation, row, var)
                .flatten()
                .map(|predicate| FilterBindingValue::Predicate(predicate.to_string()))
                .unwrap_or(FilterBindingValue::Null);
            assignment.insert(var.to_string(), value);
        }
        assignment
    }

    fn merge_solution_assignments(
        left: &FilterAssignment,
        right: &FilterAssignment,
    ) -> Option<FilterAssignment> {
        let mut merged = left.clone();
        for (var, value) in right {
            if let Some(existing) = merged.get(var)
                && existing != value
            {
                return None;
            }
            merged.insert(var.clone(), value.clone());
        }
        Some(merged)
    }

    /// Materializes one UNION block as one logical solution relation.
    ///
    /// Clauses inside the block are conjunctive, so their pattern relations
    /// are natural-joined before the block is tagged as a UNION alternative.
    /// Concept-only variables are expanded as loose dimensions. Keeping this
    /// branch relation independent is essential: appending its rows to one
    /// outer pattern loses provenance and lets a different mandatory pattern
    /// incorrectly intersect the UNION rows away.
    fn materialize_union_relation(
        union_context: &QueryContext,
    ) -> Result<Option<QueryRelationBinding>, KipError> {
        let mut entity_vars: Vec<String> = union_context.entities.keys().cloned().collect();
        let mut predicate_vars: Vec<String> = union_context.predicates.keys().cloned().collect();
        entity_vars.sort();
        predicate_vars.sort();

        if entity_vars.is_empty() && predicate_vars.is_empty() {
            return Ok(None);
        }
        // QueryRelationBinding has three entity slots and one predicate slot.
        // Reject an unrepresentable branch instead of silently flattening its
        // solution rows back into independent binding columns.
        if entity_vars.len() > 3 || predicate_vars.len() > 1 {
            return Err(KipError::resource_exhausted(format!(
                "UNION branch binds {} entity variables and {} predicate variables, exceeding \
                 the row representation capacity (3 entity + 1 predicate); split the branch",
                entity_vars.len(),
                predicate_vars.len()
            )));
        }

        let relation_assignments = |relation: &QueryRelationBinding| {
            relation
                .rows
                .iter()
                .filter(|row| Self::relation_row_matches_context(union_context, relation, row))
                .map(|row| Self::relation_row_assignment(relation, row))
                .collect::<Vec<_>>()
        };

        let patterns: Vec<&QueryRelationBinding> = union_context
            .relations
            .iter()
            .filter(|relation| relation.origin == RelationOrigin::Pattern)
            .collect();
        let nested_unions: Vec<&QueryRelationBinding> = union_context
            .relations
            .iter()
            .filter(|relation| relation.origin == RelationOrigin::Union)
            .collect();
        let optionals: Vec<&QueryRelationBinding> = union_context
            .relations
            .iter()
            .filter(|relation| relation.origin == RelationOrigin::Optional)
            .collect();
        let relation_bound_entity_vars: FxHashSet<&str> = union_context
            .relations
            .iter()
            .flat_map(|relation| {
                [
                    relation.proposition_var.as_deref(),
                    relation.subject_var.as_deref(),
                    relation.object_var.as_deref(),
                ]
                .into_iter()
                .flatten()
            })
            .collect();
        let relation_bound_predicate_vars: FxHashSet<&str> = union_context
            .relations
            .iter()
            .filter_map(|relation| relation.predicate_var.as_deref())
            .collect();

        let mut solutions: Vec<FilterAssignment> =
            if let Some((first, rest)) = patterns.split_first() {
                let mut joined = relation_assignments(first);
                for relation in rest {
                    let right = relation_assignments(relation);
                    let projected = joined.len().saturating_mul(right.len());
                    if projected > MAX_SOLUTION_COMBINATIONS {
                        return Err(KipError::resource_exhausted(format!(
                            "UNION branch natural join materializes more than \
                         {MAX_SOLUTION_COMBINATIONS} candidate solutions"
                        )));
                    }
                    joined = joined
                        .iter()
                        .flat_map(|left| {
                            right
                                .iter()
                                .filter_map(|right| Self::merge_solution_assignments(left, right))
                        })
                        .collect();
                    if joined.is_empty() {
                        break;
                    }
                }
                joined
            } else {
                Vec::new()
            };

        // A nested UNION is disjunctive with the branch's mandatory-pattern
        // solution set. Its own executor has already materialized each nested
        // block as a single relation.
        for relation in &nested_unions {
            solutions.extend(relation_assignments(relation));
            if solutions.len() > MAX_SOLUTION_COMBINATIONS {
                return Err(KipError::resource_exhausted(format!(
                    "nested UNION branches exceed {MAX_SOLUTION_COMBINATIONS} solutions"
                )));
            }
        }
        if patterns.is_empty() && nested_unions.is_empty() {
            // Concept-only branch: one empty seed, expanded below by every
            // loose binding dimension.
            solutions.push(FilterAssignment::default());
        }

        // OPTIONAL relations left-join onto each current branch solution.
        for relation in optionals {
            let right = relation_assignments(relation);
            let mut joined = Vec::new();
            for left in solutions {
                let mut matched = false;
                for candidate in &right {
                    if let Some(merged) = Self::merge_solution_assignments(&left, candidate) {
                        joined.push(merged);
                        matched = true;
                    }
                }
                if !matched {
                    joined.push(left);
                }
            }
            if joined.len() > MAX_SOLUTION_COMBINATIONS {
                return Err(KipError::resource_exhausted(format!(
                    "UNION branch OPTIONAL join exceeds {MAX_SOLUTION_COMBINATIONS} solutions"
                )));
            }
            solutions = joined;
        }

        // Enforce the branch's final concept/predicate constraints. This is
        // required when a later concept clause narrows a variable bound by an
        // earlier relation: the outer context will contain bindings from all
        // branches and cannot recover that branch-local constraint later.
        solutions.retain(|assignment| {
            assignment.iter().all(|(var, value)| match value {
                FilterBindingValue::Entity(id) => union_context
                    .entities
                    .get(var)
                    .is_none_or(|ids| ids.contains(id)),
                FilterBindingValue::Predicate(predicate) => union_context
                    .predicates
                    .get(var)
                    .is_none_or(|predicates| predicates.contains(predicate)),
                FilterBindingValue::Null => true,
            })
        });

        // Cross product any concept-only variables not covered by a relation.
        for var in &entity_vars {
            let mut expanded = Vec::new();
            for solution in solutions {
                if solution.contains_key(var) {
                    expanded.push(solution);
                    continue;
                }
                if relation_bound_entity_vars.contains(var.as_str()) {
                    let mut padded = solution;
                    padded.insert(var.clone(), FilterBindingValue::Null);
                    expanded.push(padded);
                    continue;
                }
                let Some(ids) = union_context.entities.get(var) else {
                    continue;
                };
                if expanded.len().saturating_add(ids.len()) > MAX_SOLUTION_COMBINATIONS {
                    return Err(KipError::resource_exhausted(format!(
                        "UNION branch materializes more than {MAX_SOLUTION_COMBINATIONS} solutions"
                    )));
                }
                for id in ids.iter() {
                    let mut next = solution.clone();
                    next.insert(var.clone(), FilterBindingValue::Entity(id.clone()));
                    expanded.push(next);
                }
            }
            solutions = expanded;
        }
        for var in &predicate_vars {
            let mut expanded = Vec::new();
            for solution in solutions {
                if solution.contains_key(var) {
                    expanded.push(solution);
                    continue;
                }
                if relation_bound_predicate_vars.contains(var.as_str()) {
                    let mut padded = solution;
                    padded.insert(var.clone(), FilterBindingValue::Null);
                    expanded.push(padded);
                    continue;
                }
                let Some(predicates) = union_context.predicates.get(var) else {
                    continue;
                };
                if expanded.len().saturating_add(predicates.len()) > MAX_SOLUTION_COMBINATIONS {
                    return Err(KipError::resource_exhausted(format!(
                        "UNION branch materializes more than {MAX_SOLUTION_COMBINATIONS} solutions"
                    )));
                }
                for predicate in predicates.iter() {
                    let mut next = solution.clone();
                    next.insert(
                        var.clone(),
                        FilterBindingValue::Predicate(predicate.clone()),
                    );
                    expanded.push(next);
                }
            }
            solutions = expanded;
        }

        // Deduplicate using a structured, stable key before converting the
        // assignments to the fixed relation slots.
        let mut seen: FxHashSet<Vec<FilterBindingValue>> = FxHashSet::default();
        solutions.retain(|assignment| {
            let key = entity_vars
                .iter()
                .chain(predicate_vars.iter())
                .map(|var| {
                    assignment
                        .get(var)
                        .cloned()
                        .unwrap_or(FilterBindingValue::Null)
                })
                .collect();
            seen.insert(key)
        });

        let subject_var = entity_vars.first().cloned();
        let object_var = entity_vars.get(1).cloned();
        let proposition_var = entity_vars.get(2).cloned();
        let predicate_var = predicate_vars.first().cloned();
        let rows = solutions
            .into_iter()
            .map(|assignment| {
                let entity = |var: Option<&String>| match var.and_then(|var| assignment.get(var)) {
                    Some(FilterBindingValue::Entity(id)) => Some(id.clone()),
                    _ => None,
                };
                let predicate = match predicate_var.as_ref().and_then(|var| assignment.get(var)) {
                    Some(FilterBindingValue::Predicate(predicate)) => Some(predicate.clone()),
                    _ => None,
                };
                QueryRelationRow {
                    proposition: entity(proposition_var.as_ref()),
                    subject: entity(subject_var.as_ref()),
                    predicate,
                    object: entity(object_var.as_ref()),
                }
            })
            .collect();

        Ok(Some(QueryRelationBinding {
            proposition_var,
            subject_var,
            predicate_var,
            object_var,
            rows,
            origin: RelationOrigin::Union,
        }))
    }

    pub(super) async fn execute_union_clause(
        &self,
        ctx: &mut QueryContext,
        clauses: Vec<WhereClause>,
    ) -> Result<(), KipError> {
        let mut union_context = QueryContext {
            cache: ctx.cache.clone(),
            // A dangling id inside the branch degrades to an empty match:
            // the branch contributes nothing instead of failing the whole
            // query (KIP §3.4.7.3 — the branch is an independent scope).
            lenient_grounding: true,
            ..Default::default()
        };

        for clause in clauses {
            Box::pin(self.execute_where_clause(&mut union_context, clause)).await?;
        }

        Self::narrow_bindings_to_valid_relation_rows(&mut union_context);
        let union_relation = Self::materialize_union_relation(&union_context)?;

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
        for (key, mut group_map) in union_context.groups {
            if let Some(relation) = union_relation.as_ref()
                && Self::relation_covers_var(relation, &key.0)
                && Self::relation_covers_var(relation, &key.1)
            {
                let mut allowed: FxHashMap<EntityID, FxHashSet<EntityID>> = FxHashMap::default();
                for row in &relation.rows {
                    if let (Some(Some(gid)), Some(Some(mid))) = (
                        Self::relation_row_entity(relation, row, &key.0),
                        Self::relation_row_entity(relation, row, &key.1),
                    ) {
                        allowed.entry(gid.clone()).or_default().insert(mid.clone());
                    }
                }
                group_map.retain(|gid, members| match allowed.get(gid) {
                    Some(allowed_members) => {
                        members.retain(|member| allowed_members.contains(member));
                        !members.is_empty()
                    }
                    None => false,
                });
            }
            let entry = ctx.groups.entry(key).or_default();
            for (gid, mids) in group_map {
                entry.entry(gid).or_default().extend(mids.into_vec());
            }
        }

        // Preserve the branch as one independent logical relation. FIND can
        // now evaluate mandatory Pattern relations with AND semantics, then
        // append each UNION relation with OR/null-padding semantics.
        if let Some(relation) = union_relation {
            ctx.relations.push(relation);
        }
        ctx.row_sensitive_vars
            .extend(union_context.row_sensitive_vars);
        ctx.unaligned_filter_vars
            .extend(union_context.unaligned_filter_vars);

        Ok(())
    }

    /// Resolves a FIND variable, checking entity bindings first, then predicate bindings.
    ///
    /// Predicate variables (bound via triple patterns like `(?s, ?p, ?o)`) are stored
    /// separately from entity variables. This method handles both cases.
    ///
    /// The cursor is decoded lazily per branch — entity columns use a
    /// `BTree` entity cursor, predicate columns a numeric offset — and an
    /// unparseable token is rejected (`KIP_1001`) instead of silently
    /// replaying from the start with duplicate pages.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn resolve_find_var(
        &self,
        ctx: &QueryContext,
        bindings: &FxHashMap<String, Vec<EntityID>>,
        var: &str,
        fields: &[String],
        order_by: &[OrderByCondition],
        raw_cursor: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<Json>, Option<String>), KipError> {
        if bindings.contains_key(var) {
            let cursor: Option<EntityID> = BTree::from_cursor(
                &raw_cursor.map(|cursor| cursor.to_string()),
            )
            .map_err(|err| KipError::invalid_syntax(format!("Invalid CURSOR token: {err}")))?;
            return self
                .resolve_result(
                    &ctx.cache,
                    bindings,
                    var,
                    fields,
                    order_by,
                    cursor.as_ref(),
                    limit,
                )
                .await;
        }

        // Check if it's a predicate variable
        if let Some(predicates) = ctx.predicates.get(var) {
            let values: Vec<Json> = predicates.iter().map(|p| Json::String(p.clone())).collect();
            let start = parse_offset_cursor(raw_cursor)?.min(values.len());
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

    /// Returns the entity bound to `var` in this row, if `var` is one of the
    /// relation's entity positions. `Some(None)` means the position is
    /// covered but the row is an OPTIONAL-padded row (projects `null`).
    pub(super) fn relation_row_entity<'a>(
        relation: &'a QueryRelationBinding,
        row: &'a QueryRelationRow,
        var: &str,
    ) -> Option<Option<&'a EntityID>> {
        if relation.proposition_var.as_deref() == Some(var) {
            Some(row.proposition.as_ref())
        } else if relation.subject_var.as_deref() == Some(var) {
            Some(row.subject.as_ref())
        } else if relation.object_var.as_deref() == Some(var) {
            Some(row.object.as_ref())
        } else {
            None
        }
    }

    pub(super) fn relation_row_predicate<'a>(
        relation: &'a QueryRelationBinding,
        row: &'a QueryRelationRow,
        var: &str,
    ) -> Option<Option<&'a str>> {
        if relation.predicate_var.as_deref() == Some(var) {
            Some(row.predicate.as_deref())
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
            // Padded (`None`) positions are unconstrained.
            if let Some(allowed) = ctx.entities.get(var)
                && let Some(Some(entity_id)) = Self::relation_row_entity(relation, row, var)
                && !allowed.iter().any(|id| id == entity_id)
            {
                return false;
            }
        }

        if let Some(var) = relation.predicate_var.as_deref()
            && let Some(allowed) = ctx.predicates.get(var)
            && let Some(predicate) = row.predicate.as_deref()
            && !allowed.iter().any(|p| p == predicate)
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
            return match entity_id {
                Some(entity_id) => {
                    self.load_entity_field(cache, entity_id, &dot_path.to_pointer())
                        .await
                }
                // OPTIONAL-padded row: unbound projections yield null.
                None => Ok(Json::Null),
            };
        }

        if let Some(predicate) = Self::relation_row_predicate(relation, row, &dot_path.var) {
            return Ok(match predicate {
                Some(predicate) if dot_path.path.is_empty() => Json::String(predicate.to_string()),
                _ => Json::Null,
            });
        }

        Err(KipError::reference_error(format!(
            "Unbound variable: {:?}",
            dot_path.var
        )))
    }

    /// Row-based multi-variable `FIND`: when a single relation covers every
    /// referenced variable, its rows are the solutions, keeping the columns
    /// index-aligned (KIP §6.2.2).
    ///
    /// Pagination uses a numeric offset cursor over the deterministic row
    /// order (dedup and ORDER BY are deterministic re-executions), the same
    /// convention as [`Self::try_execute_cartesian_row_find`]. Rows are not
    /// required to carry a proposition id (multi-hop paths, OPTIONAL padded
    /// rows and synthetic FILTER relations do not), so the cursor cannot be
    /// anchored to one.
    pub(super) async fn try_execute_relation_row_find(
        &self,
        ctx: &QueryContext,
        clause: &FindClause,
        order_by: &[OrderByCondition],
        raw_cursor: Option<&str>,
        limit: usize,
    ) -> Result<Option<(Vec<Json>, Option<String>)>, KipError> {
        let Some(groups) = Self::collect_find_variable_groups(clause) else {
            return Ok(None);
        };

        // Referenced variables in a stable order: FIND order first, then
        // ORDER BY-only variables. The order defines the projected solution
        // tuple used to join/union rows across relations.
        let mut referenced: Vec<String> = Vec::new();
        for (_, dot_paths) in &groups {
            for dot_path in dot_paths {
                if !referenced.iter().any(|v| v == &dot_path.var) {
                    referenced.push(dot_path.var.clone());
                }
            }
        }
        for cond in order_by {
            if !cond.is_aggregation() && !referenced.iter().any(|v| v == &cond.variable.var) {
                referenced.push(cond.variable.var.clone());
            }
        }

        // Distinct FIND base variables. When a single relation covers two or
        // more of them, its rows are the only representation that keeps the
        // columns index-aligned across solutions (KIP §6.2.2) — projecting
        // each variable's binding set independently would misalign them.
        let distinct_find_vars: FxHashSet<&str> =
            groups.iter().map(|(var, _)| var.as_str()).collect();

        let covering: Vec<&QueryRelationBinding> = ctx
            .relations
            .iter()
            .filter(|relation| {
                let covers_all = referenced
                    .iter()
                    .all(|var| Self::relation_covers_var(relation, var));
                if !covers_all {
                    return false;
                }
                if distinct_find_vars.len() >= 2 {
                    return true;
                }

                // Single-variable projections only need row semantics when a
                // proposition-field sort or filter makes rows distinguishable.
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

                orders_by_proposition_field || filters_by_proposition_field
            })
            .collect();
        if covering.is_empty() {
            return Ok(None);
        }

        // Combining semantics by clause origin (KIP §3.4, §3.4.7):
        // - `Pattern` relations are conjunctive — solutions must satisfy
        //   every one of them (row-level equi-join on the referenced tuple);
        // - `Union` relations contribute a row-wise union of solutions;
        // - `Optional` relations never restrict the outer solution set and
        //   only serve as the row source when nothing else covers the
        //   variables (left-join padded rows).
        let patterns: Vec<&QueryRelationBinding> = covering
            .iter()
            .copied()
            .filter(|r| r.origin == RelationOrigin::Pattern)
            .collect();
        // UNION branches may intentionally omit projected variables; those
        // positions are null-padded. Include every independent UNION
        // relation here, not only relations that cover the whole projection.
        // A full covering relation above still supplies the fixed output slot
        // layout; fully disjoint branches continue through the cartesian
        // union-padded path below.
        let unions: Vec<&QueryRelationBinding> = ctx
            .relations
            .iter()
            .filter(|r| r.origin == RelationOrigin::Union)
            .collect();
        let full_union = covering
            .iter()
            .copied()
            .rev()
            .find(|relation| relation.origin == RelationOrigin::Union);
        let base: &QueryRelationBinding = patterns
            .last()
            .or(full_union.as_ref())
            .copied()
            .unwrap_or_else(|| covering.last().copied().unwrap());

        // The synthesized rows use the base relation's variable→slot layout.
        let relation = QueryRelationBinding {
            proposition_var: base.proposition_var.clone(),
            subject_var: base.subject_var.clone(),
            predicate_var: base.predicate_var.clone(),
            object_var: base.object_var.clone(),
            rows: Vec::new(),
            origin: base.origin,
        };

        // Projects one relation row onto the referenced-variable tuple.
        let project_row =
            |relation: &QueryRelationBinding, row: &QueryRelationRow| -> Vec<FilterBindingValue> {
                referenced
                    .iter()
                    .map(|var| {
                        if let Some(entity) = Self::relation_row_entity(relation, row, var) {
                            match entity {
                                Some(entity) => FilterBindingValue::Entity(entity.clone()),
                                None => FilterBindingValue::Null,
                            }
                        } else if let Some(Some(predicate)) =
                            Self::relation_row_predicate(relation, row, var)
                        {
                            FilterBindingValue::Predicate(predicate.to_string())
                        } else {
                            FilterBindingValue::Null
                        }
                    })
                    .collect()
            };
        // Rebuilds a row in the base slot layout from a projected tuple.
        let synthesize_row = |tuple: &[FilterBindingValue]| -> QueryRelationRow {
            let mut row = QueryRelationRow {
                proposition: None,
                subject: None,
                predicate: None,
                object: None,
            };
            for (var, value) in referenced.iter().zip(tuple) {
                match value {
                    FilterBindingValue::Entity(id) => {
                        if relation.proposition_var.as_deref() == Some(var) {
                            row.proposition = Some(id.clone());
                        } else if relation.subject_var.as_deref() == Some(var) {
                            row.subject = Some(id.clone());
                        } else if relation.object_var.as_deref() == Some(var) {
                            row.object = Some(id.clone());
                        }
                    }
                    FilterBindingValue::Predicate(pred) => {
                        if relation.predicate_var.as_deref() == Some(var) {
                            row.predicate = Some(pred.clone());
                        }
                    }
                    FilterBindingValue::Null => {}
                }
            }
            row
        };

        let mut rows: Vec<QueryRelationRow> = if covering.len() == 1 && unions.is_empty() {
            base.rows
                .iter()
                .filter(|row| Self::relation_row_matches_context(ctx, base, row))
                .cloned()
                .collect()
        } else if patterns.is_empty() && unions.is_empty() {
            // Only OPTIONAL relations cover the variables: project the last
            // one's (left-join padded) rows, as before.
            base.rows
                .iter()
                .filter(|row| Self::relation_row_matches_context(ctx, base, row))
                .cloned()
                .collect()
        } else {
            let mut combined: Vec<QueryRelationRow> = Vec::new();
            if let Some((join_base, others)) = patterns.split_last() {
                let other_sets: Vec<FxHashSet<Vec<FilterBindingValue>>> = others
                    .iter()
                    .map(|relation| {
                        relation
                            .rows
                            .iter()
                            .filter(|row| Self::relation_row_matches_context(ctx, relation, row))
                            .map(|row| project_row(relation, row))
                            .collect()
                    })
                    .collect();
                for row in &join_base.rows {
                    if !Self::relation_row_matches_context(ctx, join_base, row) {
                        continue;
                    }
                    let tuple = project_row(join_base, row);
                    if other_sets.iter().all(|set| set.contains(&tuple)) {
                        combined.push(synthesize_row(&tuple));
                    }
                }
            }
            for relation in &unions {
                for row in &relation.rows {
                    if !Self::relation_row_matches_context(ctx, relation, row) {
                        continue;
                    }
                    combined.push(synthesize_row(&project_row(relation, row)));
                }
            }
            combined
        };

        // Solution deduplication (KIP §3.3): solutions whose bindings agree
        // on every projected variable collapse before ORDER BY and LIMIT.
        // The key is a structured tuple, not a delimiter-joined string —
        // predicates may contain any character (including `|`), so string
        // concatenation would collide distinct solutions and drop rows.
        let projected_vars: Vec<&str> = groups.iter().map(|(var, _)| var.as_str()).collect();
        let mut seen: FxHashSet<Vec<FilterBindingValue>> =
            FxHashSet::with_capacity_and_hasher(rows.len(), Default::default());
        rows.retain(|row| {
            let key: Vec<FilterBindingValue> = projected_vars
                .iter()
                .map(|var| {
                    if let Some(entity) = Self::relation_row_entity(&relation, row, var) {
                        match entity {
                            Some(entity) => FilterBindingValue::Entity(entity.clone()),
                            None => FilterBindingValue::Null,
                        }
                    } else if let Some(Some(predicate)) =
                        Self::relation_row_predicate(&relation, row, var)
                    {
                        FilterBindingValue::Predicate(predicate.to_string())
                    } else {
                        FilterBindingValue::Null
                    }
                })
                .collect();
            seen.insert(key)
        });

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
                    let ordering =
                        compare_order_key(&left_values[idx], &right_values[idx], &cond.direction);
                    if ordering != std::cmp::Ordering::Equal {
                        return ordering;
                    }
                }

                std::cmp::Ordering::Equal
            });

            rows = keyed_rows.into_iter().map(|(row, _)| row).collect();
        }

        // Numeric offset cursor over the deterministic row order (same
        // convention as the cartesian path). An entity-anchored cursor would
        // not work here: rows without a proposition id — multi-hop paths,
        // OPTIONAL padding, synthetic FILTER relations — could neither issue
        // a resumable cursor nor be matched by one, silently truncating the
        // result at the first such page boundary.
        let start = parse_offset_cursor(raw_cursor)?.min(rows.len());
        let mut rows = rows.split_off(start);
        let mut next_cursor: Option<String> = None;
        if limit > 0 && limit < rows.len() {
            rows.truncate(limit);
            next_cursor = Some((start + limit).to_string());
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

    /// Cartesian solution materialization for multi-variable `FIND` when no
    /// single relation covers every referenced variable (KIP §6.2.2 requires
    /// the columns to stay index-aligned across solutions).
    ///
    /// The solution rows are the cross product of:
    /// - the rows of one relation covering all *relation-bound* referenced
    ///   variables (when such variables exist), and
    /// - the binding lists of the remaining "loose" variables (bound only by
    ///   concept clauses).
    ///
    /// Variables partially covered by several relations (a chained join) are
    /// left to the legacy per-variable projection — materializing them would
    /// require an equi-join across relations, which this engine does not
    /// implement yet.
    ///
    /// Pagination uses a numeric offset cursor (row order is deterministic:
    /// relation rows and binding lists preserve insertion order). The row
    /// count is capped at [`MAX_SOLUTION_COMBINATIONS`] (`KIP_4002`).
    pub(super) async fn try_execute_cartesian_row_find(
        &self,
        ctx: &QueryContext,
        clause: &FindClause,
        order_by: &[OrderByCondition],
        raw_cursor: Option<&str>,
        limit: usize,
    ) -> Result<Option<(Vec<Json>, Option<String>)>, KipError> {
        let Some(groups) = Self::collect_find_variable_groups(clause) else {
            return Ok(None);
        };

        // Referenced variables in stable order: FIND order first, then
        // ORDER BY-only variables.
        let mut referenced: Vec<String> = Vec::new();
        for (var, _) in &groups {
            if !referenced.contains(var) {
                referenced.push(var.clone());
            }
        }
        let distinct_find_vars = referenced.len();
        if distinct_find_vars < 2 {
            return Ok(None);
        }
        for cond in order_by {
            if !cond.is_aggregation() && !referenced.contains(&cond.variable.var) {
                referenced.push(cond.variable.var.clone());
            }
        }

        // Every referenced variable must be bound; otherwise let the legacy
        // path report the reference error.
        for var in &referenced {
            if !ctx.entities.contains_key(var) && !ctx.predicates.contains_key(var) {
                return Ok(None);
            }
        }

        // A cross-variable FILTER over more than two entity variables (or
        // more than one predicate variable) could not record its satisfying
        // combinations; projecting two or more of those variables together
        // would re-materialize a full cross product with the filter's
        // alignment lost. Reject with KIP_4002 instead of silently returning
        // misaligned columns (KIP §6.2.2).
        if !ctx.unaligned_filter_vars.is_empty()
            && referenced[..distinct_find_vars]
                .iter()
                .filter(|var| ctx.unaligned_filter_vars.contains(*var))
                .count()
                >= 2
        {
            return Err(KipError::resource_exhausted(
                "FIND projects two or more variables whose cross-variable FILTER exceeded the \
                 recordable slots (more than two entity variables or more than one predicate \
                 variable), so the columns cannot stay solution-aligned; split the FILTER or \
                 project the variables separately"
                    .to_string(),
            ));
        }

        let var_index: FxHashMap<&str, usize> = referenced
            .iter()
            .enumerate()
            .map(|(i, var)| (var.as_str(), i))
            .collect();

        // Row-wise union with null padding (KIP §3.4.7.3): when the
        // referenced variables partition across relations with disjoint
        // variable sets and at least one partition is a UNION branch, the
        // solutions are the concatenation of each branch's rows padded with
        // `null` in the other branches' columns — not the cross product.
        if let Some(union_rows) =
            Self::try_union_padded_rows(ctx, &referenced, &var_index, distinct_find_vars)
        {
            return self
                .finish_cartesian_rows(
                    ctx, groups, order_by, raw_cursor, limit, union_rows, var_index,
                )
                .await
                .map(Some);
        }

        // Split into relation-bound and loose variables.
        let in_relation: Vec<&str> = referenced
            .iter()
            .map(|var| var.as_str())
            .filter(|var| {
                ctx.relations
                    .iter()
                    .any(|relation| Self::relation_covers_var(relation, var))
            })
            .collect();
        let relation = if in_relation.is_empty() {
            None
        } else {
            match ctx.relations.iter().rev().find(|relation| {
                in_relation
                    .iter()
                    .all(|var| Self::relation_covers_var(relation, var))
            }) {
                Some(relation) => Some(relation),
                // Chained join across relations: fall back to the legacy path.
                None => return Ok(None),
            }
        };

        // Base rows from the covering relation (or a single empty row).
        let mut rows: Vec<Vec<FilterBindingValue>> = match relation {
            Some(relation) => {
                let mut seen: FxHashSet<Vec<FilterBindingValue>> = FxHashSet::default();
                let mut base_rows = Vec::with_capacity(relation.rows.len());
                for row in &relation.rows {
                    if !Self::relation_row_matches_context(ctx, relation, row) {
                        continue;
                    }
                    let mut solution = vec![FilterBindingValue::Null; referenced.len()];
                    // Solution dedup (KIP §3.3): collapse rows whose bindings
                    // agree on every projected relation variable. Structured
                    // key — predicates may contain any character, so a
                    // delimiter-joined string would collide distinct rows.
                    let mut key: Vec<FilterBindingValue> = Vec::new();
                    for var in &in_relation {
                        let binding =
                            if let Some(entity) = Self::relation_row_entity(relation, row, var) {
                                match entity {
                                    Some(entity) => FilterBindingValue::Entity(entity.clone()),
                                    None => FilterBindingValue::Null,
                                }
                            } else if let Some(predicate) =
                                Self::relation_row_predicate(relation, row, var)
                            {
                                match predicate {
                                    Some(predicate) => {
                                        FilterBindingValue::Predicate(predicate.to_string())
                                    }
                                    None => FilterBindingValue::Null,
                                }
                            } else {
                                FilterBindingValue::Null
                            };
                        if var_index[*var] < distinct_find_vars {
                            key.push(binding.clone());
                        }
                        solution[var_index[*var]] = binding;
                    }
                    if seen.insert(key) {
                        base_rows.push(solution);
                    }
                }
                base_rows
            }
            None => vec![vec![FilterBindingValue::Null; referenced.len()]],
        };

        // Cross product with the loose variables.
        for var in &referenced {
            if in_relation.iter().any(|v| v == var) {
                continue;
            }
            let candidates: Vec<FilterBindingValue> = if let Some(ids) = ctx.entities.get(var) {
                ids.iter()
                    .map(|id| FilterBindingValue::Entity(id.clone()))
                    .collect()
            } else if let Some(preds) = ctx.predicates.get(var) {
                preds
                    .iter()
                    .map(|pred| FilterBindingValue::Predicate(pred.clone()))
                    .collect()
            } else {
                return Ok(None); // checked above; defensive
            };

            if candidates.is_empty() {
                rows.clear();
                break;
            }
            let projected = rows.len().saturating_mul(candidates.len());
            if projected > MAX_SOLUTION_COMBINATIONS {
                return Err(KipError::resource_exhausted(format!(
                    "FIND materializes {projected} disconnected solution rows, exceeding the \
                     engine cap of {MAX_SOLUTION_COMBINATIONS}; connect the variables through \
                     graph patterns or narrow them before projecting"
                )));
            }
            let slot = var_index[var.as_str()];
            let mut expanded = Vec::with_capacity(projected);
            for row in rows {
                for candidate in &candidates {
                    let mut next = row.clone();
                    next[slot] = candidate.clone();
                    expanded.push(next);
                }
            }
            rows = expanded;
        }

        self.finish_cartesian_rows(ctx, groups, order_by, raw_cursor, limit, rows, var_index)
            .await
            .map(Some)
    }

    /// Detects the "disjoint UNION branches" projection shape (KIP §3.4.7.3
    /// Execution Flow Example 1): every referenced variable is covered by
    /// exactly one non-OPTIONAL relation, those relations partition the
    /// variables into two or more groups, and at least one group comes from
    /// a UNION branch. Returns the row-wise union of the branches' rows,
    /// null-padded in the other branches' columns, deduplicated on the
    /// projected columns (KIP §3.3). Returns `None` when the shape does not
    /// apply (shared or loose variables fall back to the join/cross-product
    /// paths).
    fn try_union_padded_rows(
        ctx: &QueryContext,
        referenced: &[String],
        var_index: &FxHashMap<&str, usize>,
        distinct_find_vars: usize,
    ) -> Option<Vec<Vec<FilterBindingValue>>> {
        // Map each referenced variable to the unique non-OPTIONAL relation
        // covering it.
        let mut assignment: Vec<usize> = Vec::with_capacity(referenced.len());
        for var in referenced {
            let mut covering = ctx.relations.iter().enumerate().filter(|(_, relation)| {
                relation.origin != RelationOrigin::Optional
                    && Self::relation_covers_var(relation, var)
            });
            // Uncovered (loose) variable: not this shape.
            let (idx, _) = covering.next()?;
            if covering.next().is_some() {
                return None; // shared variable: join semantics, not a union
            }
            assignment.push(idx);
        }
        let distinct_relations: FxHashSet<usize> = assignment.iter().copied().collect();
        if distinct_relations.len() < 2
            || !distinct_relations
                .iter()
                .any(|idx| ctx.relations[*idx].origin == RelationOrigin::Union)
        {
            return None;
        }

        // Emit each partition's rows in relation order (main block first),
        // padded with `Null` in the other partitions' columns.
        let mut rows: Vec<Vec<FilterBindingValue>> = Vec::new();
        let mut seen: FxHashSet<Vec<FilterBindingValue>> = FxHashSet::default();
        let mut ordered: Vec<usize> = distinct_relations.into_iter().collect();
        ordered.sort_unstable();
        for rel_idx in ordered {
            let relation = &ctx.relations[rel_idx];
            let vars: Vec<&str> = referenced
                .iter()
                .zip(&assignment)
                .filter(|(_, idx)| **idx == rel_idx)
                .map(|(var, _)| var.as_str())
                .collect();
            for row in &relation.rows {
                if !Self::relation_row_matches_context(ctx, relation, row) {
                    continue;
                }
                let mut solution = vec![FilterBindingValue::Null; referenced.len()];
                for var in &vars {
                    let binding =
                        if let Some(entity) = Self::relation_row_entity(relation, row, var) {
                            match entity {
                                Some(entity) => FilterBindingValue::Entity(entity.clone()),
                                None => FilterBindingValue::Null,
                            }
                        } else if let Some(Some(predicate)) =
                            Self::relation_row_predicate(relation, row, var)
                        {
                            FilterBindingValue::Predicate(predicate.to_string())
                        } else {
                            FilterBindingValue::Null
                        };
                    solution[var_index[*var]] = binding;
                }
                // Structured dedup key (KIP §3.3): predicates may contain
                // any character, so a delimiter-joined string would collide
                // distinct solutions across partitions.
                let key: Vec<FilterBindingValue> =
                    solution[..distinct_find_vars.min(solution.len())].to_vec();
                if seen.insert(key) {
                    rows.push(solution);
                }
            }
        }
        Some(rows)
    }

    /// Shared tail of the cartesian / union-padded `FIND` paths: ORDER BY
    /// over the materialized solution rows, numeric offset cursor, and
    /// index-aligned column projection.
    #[allow(clippy::too_many_arguments)]
    async fn finish_cartesian_rows(
        &self,
        ctx: &QueryContext,
        groups: Vec<(String, Vec<DotPathVar>)>,
        order_by: &[OrderByCondition],
        raw_cursor: Option<&str>,
        limit: usize,
        mut rows: Vec<Vec<FilterBindingValue>>,
        var_index: FxHashMap<&str, usize>,
    ) -> Result<(Vec<Json>, Option<String>), KipError> {
        // ORDER BY over the materialized rows.
        let order_conditions: Vec<&OrderByCondition> = order_by
            .iter()
            .filter(|cond| !cond.is_aggregation())
            .collect();
        if !order_conditions.is_empty() && !rows.is_empty() {
            let mut keyed_rows: Vec<(Vec<FilterBindingValue>, Vec<Json>)> =
                Vec::with_capacity(rows.len());
            for row in rows {
                let mut sort_values = Vec::with_capacity(order_conditions.len());
                for cond in &order_conditions {
                    let binding = &row[var_index[cond.variable.var.as_str()]];
                    sort_values.push(
                        self.load_binding_field(&ctx.cache, binding, &cond.variable)
                            .await?,
                    );
                }
                keyed_rows.push((row, sort_values));
            }
            keyed_rows.sort_by(|(_, left_values), (_, right_values)| {
                for (idx, cond) in order_conditions.iter().enumerate() {
                    let ordering =
                        compare_order_key(&left_values[idx], &right_values[idx], &cond.direction);
                    if ordering != std::cmp::Ordering::Equal {
                        return ordering;
                    }
                }
                std::cmp::Ordering::Equal
            });
            rows = keyed_rows.into_iter().map(|(row, _)| row).collect();
        }

        // Numeric offset cursor over the deterministic row order.
        let start = parse_offset_cursor(raw_cursor)?.min(rows.len());
        let mut rows = rows.split_off(start);
        let mut next_cursor: Option<String> = None;
        if limit > 0 && limit < rows.len() {
            rows.truncate(limit);
            next_cursor = Some((start + limit).to_string());
        }

        // Build the index-aligned columns.
        let mut result: Vec<Json> = Vec::with_capacity(groups.len());
        for (var, dot_paths) in groups {
            let slot = var_index[var.as_str()];
            let mut column = Vec::with_capacity(rows.len());
            for row in &rows {
                if dot_paths.len() == 1 {
                    column.push(
                        self.load_binding_field(&ctx.cache, &row[slot], &dot_paths[0])
                            .await?,
                    );
                } else {
                    let mut values = Vec::with_capacity(dot_paths.len());
                    for dot_path in &dot_paths {
                        values.push(
                            self.load_binding_field(&ctx.cache, &row[slot], dot_path)
                                .await?,
                        );
                    }
                    column.push(Json::Array(values));
                }
            }
            result.push(Json::Array(column));
        }

        Ok((result, next_cursor))
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
        if let Some(row_result) = self
            .try_execute_relation_row_find(ctx, &clause, &order_by, raw_cursor, limit)
            .await?
        {
            return Ok(row_result);
        }

        // 多变量但无单一 relation 全覆盖：物化笛卡尔解行，保证列按解对齐
        // （KIP §6.2.2）。
        if let Some(row_result) = self
            .try_execute_cartesian_row_find(ctx, &clause, &order_by, raw_cursor, limit)
            .await?
        {
            return Ok(row_result);
        }

        // Legacy per-variable projection. Cursor decoding happens inside
        // `resolve_find_var`, per column kind (entity cursor vs numeric
        // offset for predicate columns); an unparseable token is rejected
        // there with KIP_1001 instead of silently replaying from the start.
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
                                    ctx, &bindings, var, fields, &order_by, raw_cursor, limit,
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
                                ctx, &bindings, var, fields, &order_by, raw_cursor, limit,
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
                .resolve_find_var(ctx, &bindings, var, fields, &order_by, raw_cursor, limit)
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

        // 成员收窄：groups 只在建组时写入一次，后续 FILTER/NOT/其它子句
        // 对成员变量的收窄只反映在 bindings 里，因此这里必须与 bindings[mvar]
        // 求交集，否则 COUNT 等聚合会把已被过滤掉的成员计进去。
        let surviving_members: Option<FxHashSet<&EntityID>> =
            bindings.get(mvar).map(|ids| ids.iter().collect());

        // 构造每行数据：(group_entity_id, member_count, member_ids)
        struct GroupRow {
            gid: EntityID,
            member_ids: Vec<EntityID>,
        }
        let mut rows: Vec<GroupRow> = Vec::with_capacity(group_ids.len());
        for gid in &group_ids {
            let mut member_ids = groups_map.get(gid).map(|v| v.to_vec()).unwrap_or_default();
            if let Some(surviving) = &surviving_members {
                member_ids.retain(|id| surviving.contains(id));
            }
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
            // 按 group variable 字段排序：加载各分组实体的排序字段值后按
            // KIP §3.5 比较（数值/布尔/日期感知字符串，null 恒排最后）。
            // 必须在游标切分之前完成，游标基于排序后的行序恢复。
            let order_conditions: Vec<&OrderByCondition> = order_by
                .iter()
                .filter(|o| !o.is_aggregation() && o.variable.var == gvar)
                .collect();
            let mut keyed: Vec<(GroupRow, Vec<Json>)> = Vec::with_capacity(rows.len());
            for row in rows {
                let mut sort_values = Vec::with_capacity(order_conditions.len());
                for cond in &order_conditions {
                    sort_values.push(
                        self.load_entity_field(&ctx.cache, &row.gid, &cond.variable.to_pointer())
                            .await?,
                    );
                }
                keyed.push((row, sort_values));
            }
            keyed.sort_by(|(_, left_values), (_, right_values)| {
                for (idx, cond) in order_conditions.iter().enumerate() {
                    let ordering =
                        compare_order_key(&left_values[idx], &right_values[idx], &cond.direction);
                    if ordering != std::cmp::Ordering::Equal {
                        return ordering;
                    }
                }
                std::cmp::Ordering::Equal
            });
            rows = keyed.into_iter().map(|(row, _)| row).collect();
        }

        // 应用 cursor (基于 group entity ID)；非法游标一律报 KIP_1001，
        // 与其它分页路径一致（静默从头重放会产生重复页）。
        let cursor_id: Option<EntityID> = BTree::from_cursor(cursor)
            .map_err(|err| KipError::invalid_syntax(format!("Invalid CURSOR token: {err}")))?;
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
                        let (col, _) = self
                            .resolve_find_var(
                                ctx,
                                bindings,
                                &dot_path.var,
                                &[dot_path.to_pointer()],
                                order_by,
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

    /// Loads the value a binding yields for a dot-notation path.
    ///
    /// Shared by FILTER evaluation and cartesian FIND materialization:
    /// entity bindings load the entity field, predicate bindings yield the
    /// name for a bare variable (`null` for any dot path), and `Null`
    /// (OPTIONAL-padded) bindings always yield `null`.
    pub(super) async fn load_binding_field(
        &self,
        cache: &QueryCache,
        binding: &FilterBindingValue,
        dot_path: &DotPathVar,
    ) -> Result<Json, KipError> {
        match binding {
            FilterBindingValue::Entity(EntityID::Concept(id)) => {
                self.try_get_concept_with(cache, *id, |concept| {
                    extract_concept_field_value(concept, &dot_path.path)
                })
                .await
            }
            FilterBindingValue::Entity(EntityID::Proposition(id, predicate)) => {
                self.try_get_proposition_with(cache, *id, |proposition| {
                    extract_proposition_field_value(proposition, predicate, &dot_path.path)
                })
                .await
            }
            FilterBindingValue::Predicate(predicate) => Ok(if dot_path.path.is_empty() {
                Json::String(predicate.clone())
            } else {
                Json::Null
            }),
            FilterBindingValue::Null => Ok(Json::Null),
        }
    }

    /// Resolves a FILTER operand against a fixed variable assignment.
    async fn resolve_filter_operand_assigned(
        &self,
        cache: &QueryCache,
        operand: &FilterOperand,
        assign: &FilterAssignment,
    ) -> Result<Json, KipError> {
        match operand {
            FilterOperand::Variable(dot_path) => {
                let binding = assign.get(&dot_path.var).ok_or_else(|| {
                    KipError::reference_error(format!("Unbound variable: {:?}", dot_path.var))
                })?;
                self.load_binding_field(cache, binding, dot_path).await
            }
            FilterOperand::Literal(value) => Ok(value.clone().into()),
            FilterOperand::List(values) => Ok(Json::Array(
                values.iter().cloned().map(Json::from).collect(),
            )),
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

        let has_order_by = order_by
            .iter()
            .any(|v| !v.is_aggregation() && v.variable.var == var);

        // Without ORDER BY the cursor skips entities with `eid <= cursor`,
        // which is only complete over an ascending iteration order. Bindings
        // keep clause insertion order (UNION/OPTIONAL append at the tail),
        // so sort explicitly — otherwise a page boundary can silently skip
        // smaller ids appended by a later branch.
        let mut ordered: Vec<&EntityID> = ids.iter().collect();
        if !has_order_by {
            ordered.sort();
        }

        let mut result = Vec::with_capacity(ordered.len());
        let mut has_more = false;
        for eid in ordered {
            if !has_order_by && cursor.map(|v| eid <= v).unwrap_or(false) {
                continue;
            }
            if !has_order_by && limit > 0 && result.len() >= limit {
                // A further post-cursor entity exists: the page is truly full.
                has_more = true;
                break;
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

        // A cursor is only issued when more rows actually remain (strict
        // `<`), matching the relation/cartesian paths — `<=` would hand the
        // client one extra empty page.
        let mut next_cursor: Option<String> = None;
        if limit > 0 && (has_more || limit < result.len()) {
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

    /// Evaluates a FILTER expression against one fixed variable assignment.
    ///
    /// Evaluation is pure (no binding consumption), so logical operators can
    /// short-circuit without side effects.
    pub(super) async fn eval_filter_assigned(
        &self,
        cache: &QueryCache,
        regex_cache: &mut FxHashMap<String, regex::Regex>,
        expr: &FilterExpression,
        assign: &FilterAssignment,
    ) -> Result<bool, KipError> {
        match expr {
            FilterExpression::Comparison {
                left,
                operator,
                right,
            } => {
                let left_val = self
                    .resolve_filter_operand_assigned(cache, left, assign)
                    .await?;
                let right_val = self
                    .resolve_filter_operand_assigned(cache, right, assign)
                    .await?;
                Ok(operator.compare(&left_val, &right_val))
            }
            FilterExpression::Logical {
                left,
                operator,
                right,
            } => {
                let left_result =
                    Box::pin(self.eval_filter_assigned(cache, regex_cache, left, assign)).await?;
                match operator {
                    LogicalOperator::And if !left_result => Ok(false),
                    LogicalOperator::Or if left_result => Ok(true),
                    _ => {
                        Box::pin(self.eval_filter_assigned(cache, regex_cache, right, assign)).await
                    }
                }
            }
            FilterExpression::Not(inner) => {
                let result =
                    Box::pin(self.eval_filter_assigned(cache, regex_cache, inner, assign)).await?;
                Ok(!result)
            }
            FilterExpression::Function { func, args } => {
                self.eval_filter_function_assigned(cache, regex_cache, func.clone(), args, assign)
                    .await
            }
        }
    }

    async fn eval_filter_function_assigned(
        &self,
        cache: &QueryCache,
        regex_cache: &mut FxHashMap<String, regex::Regex>,
        func: FilterFunction,
        args: &[FilterOperand],
        assign: &FilterAssignment,
    ) -> Result<bool, KipError> {
        match func {
            FilterFunction::IsNull | FilterFunction::IsNotNull => {
                let [arg] = args else {
                    return Err(KipError::invalid_syntax(format!(
                        "{func:?} requires exactly 1 argument"
                    )));
                };
                let val = self
                    .resolve_filter_operand_assigned(cache, arg, assign)
                    .await?;
                Ok(match func {
                    FilterFunction::IsNull => val.is_null(),
                    _ => !val.is_null(),
                })
            }
            FilterFunction::In => {
                let [expr_arg, list_arg] = args else {
                    return Err(KipError::invalid_syntax(
                        "IN requires exactly 2 arguments".to_string(),
                    ));
                };
                let expr_val = self
                    .resolve_filter_operand_assigned(cache, expr_arg, assign)
                    .await?;
                let list_val = self
                    .resolve_filter_operand_assigned(cache, list_arg, assign)
                    .await?;
                match list_val {
                    // Use the same loose equality as `==`/`!=` so
                    // `IN(?x, [v])` and `?x == v` never disagree (e.g. an
                    // attribute stored as `3.0` matches the literal `3` in
                    // both).
                    Json::Array(arr) => Ok(arr.iter().any(|item| loose_equal(item, &expr_val))),
                    _ => Err(KipError::invalid_syntax(
                        "IN second argument must be a list".to_string(),
                    )),
                }
            }
            _ => {
                let [str_arg, pattern_arg] = args else {
                    return Err(KipError::invalid_syntax(
                        "Filter functions require exactly 2 arguments".to_string(),
                    ));
                };
                let str_val = self
                    .resolve_filter_operand_assigned(cache, str_arg, assign)
                    .await?;
                let pattern_val = self
                    .resolve_filter_operand_assigned(cache, pattern_arg, assign)
                    .await?;

                let string = str_val.as_str().unwrap_or("");
                let pattern = pattern_val.as_str().unwrap_or("");

                match func {
                    FilterFunction::Contains => Ok(string.contains(pattern)),
                    FilterFunction::StartsWith => Ok(string.starts_with(pattern)),
                    FilterFunction::EndsWith => Ok(string.ends_with(pattern)),
                    FilterFunction::Regex => {
                        if let Some(compiled) = regex_cache.get(pattern) {
                            return Ok(compiled.is_match(string));
                        }
                        let compiled = regex::Regex::new(pattern).map_err(|e| {
                            KipError::invalid_syntax(format!("Invalid regex: {e:?}"))
                        })?;
                        let rt = compiled.is_match(string);
                        regex_cache.insert(pattern.to_string(), compiled);
                        Ok(rt)
                    }
                    // Defensive: a future FilterFunction variant must fail the
                    // query, not panic the engine.
                    other => Err(KipError::invalid_syntax(format!(
                        "Unsupported filter function: {other:?}"
                    ))),
                }
            }
        }
    }
}
