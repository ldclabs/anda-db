//! KQL execution over the row-oriented solution model.
//!
//! The `WHERE` state is a forest of [`SolutionTable`]s (see
//! [`QueryContext`]); every clause is one relational operator on it:
//!
//! - concept / proposition patterns **natural-join** their match rows in
//!   (KIP §3.4 — clauses are conjunctive),
//! - `FILTER` keeps the satisfying solution rows (§3.4.3),
//! - `NOT` **anti-joins** the block's solutions out (§3.4.7.1),
//! - `OPTIONAL` **left-joins** with `null` padding (§3.4.7.2),
//! - `UNION` merges row-wise with `null` padding and dedup (§3.4.7.3).
//!
//! `FIND` then projects columns from the (joined) tables, so multi-variable
//! results are index-aligned across solutions by construction (§6.2.2),
//! with solution dedup before `ORDER BY` / `LIMIT` (§3.3). Two shortcut
//! paths avoid materializing joins: single-variable projection and pure
//! `COUNT` read one column directly.

use super::*;

/// A concrete assignment of variables to bindings for one `FILTER`
/// evaluation or one solution-row projection.
pub(super) type FilterAssignment = FxHashMap<String, BindingValue>;

/// Parses a numeric offset cursor (issued by the multi-variable row
/// projection and predicate-variable pagination). A token that is not a
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

/// One `FIND` output column: either a group of dot-notation projections of
/// one variable (consecutive same-variable expressions merge into a single
/// column, matching the legacy result shape) or an aggregation.
enum FindItem {
    Column {
        var: String,
        dot_paths: Vec<DotPathVar>,
    },
    Aggregate {
        func: AggregationFunction,
        var: DotPathVar,
        distinct: bool,
    },
}

/// Applies a `FIND` column's dot-notation paths to its loaded root values:
/// a bare variable (or no path at all) projects the root, one path projects
/// that field, several paths project an array of them in clause order.
fn project_dot_paths(roots: Vec<Json>, dot_paths: &[DotPathVar]) -> Vec<Json> {
    let fields: Vec<String> = dot_paths.iter().map(|d| d.to_pointer()).collect();
    match fields.len() {
        0 => roots,
        1 if fields[0].is_empty() => roots,
        1 => roots
            .into_iter()
            .map(|v| v.pointer(&fields[0]).cloned().unwrap_or(Json::Null))
            .collect(),
        _ => roots
            .into_iter()
            .map(|v| {
                Json::Array(
                    fields
                        .iter()
                        .map(|p| v.pointer(p).cloned().unwrap_or(Json::Null))
                        .collect(),
                )
            })
            .collect(),
    }
}

fn collect_find_items(clause: &FindClause) -> Vec<FindItem> {
    let mut items: Vec<FindItem> = Vec::new();
    for expr in &clause.expressions {
        match expr {
            FindExpression::Variable(dot_path) => {
                if let Some(FindItem::Column { var, dot_paths }) = items.last_mut()
                    && var == &dot_path.var
                {
                    dot_paths.push(dot_path.clone());
                    continue;
                }
                items.push(FindItem::Column {
                    var: dot_path.var.clone(),
                    dot_paths: vec![dot_path.clone()],
                });
            }
            FindExpression::Aggregation {
                func,
                var,
                distinct,
            } => {
                items.push(FindItem::Aggregate {
                    func: func.clone(),
                    var: var.clone(),
                    distinct: *distinct,
                });
            }
        }
    }
    items
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
        }
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

        if let Some(table) = ctx
            .tables
            .iter_mut()
            .find(|table| table.covers(&clause.variable))
        {
            // Variable already bound: narrow its rows to the matching
            // concepts. `Null` cells (OPTIONAL / UNION padding) are
            // unconstrained, matching the padded-row semantics.
            let col = table.column(&clause.variable).expect("covered");
            let allowed: FxHashSet<&EntityID> = concept_ids.iter().collect();
            table.rows.retain(|row| match &row[col] {
                BindingValue::Entity(id) => allowed.contains(id),
                BindingValue::Null => true,
                BindingValue::Predicate(_) => false,
            });
        } else {
            ctx.tables.push(SolutionTable::single_column(
                clause.variable,
                concept_ids.into_iter().map(BindingValue::Entity).collect(),
            ));
        }

        Ok(())
    }

    pub(super) async fn execute_proposition_clause(
        &self,
        ctx: &mut QueryContext,
        clause: PropositionClause,
    ) -> Result<(), KipError> {
        match clause.matcher {
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
                let ids = match self
                    .ensure_proposition_link_exists(&ctx.cache, &entity_id)
                    .await
                {
                    Ok(()) => vec![entity_id],
                    Err(err) if ctx.lenient_grounding && err.code == KipErrorCode::NotFound => {
                        Vec::new()
                    }
                    Err(err) => return Err(err),
                };
                if let Some(var) = clause.variable {
                    ctx.merge_table(SolutionTable::single_column(
                        var,
                        ids.into_iter().map(BindingValue::Entity).collect(),
                    ))?;
                }
                Ok(())
            }
            PropositionMatcher::Object {
                subject,
                predicate,
                object,
            } => {
                // `match_propositions` merges the pattern's solution table
                // (including the link variable's column) into the forest.
                self.match_propositions(ctx, subject, predicate, object, clause.variable)
                    .await?;
                Ok(())
            }
        }
    }

    /// Executes a `FILTER` clause with per-solution semantics (KIP §3.4.3):
    /// the tables covering the referenced variables are joined into one
    /// solution table (disconnected variables cross-join under the
    /// [`MAX_SOLUTION_COMBINATIONS`] cap) and each row is kept iff the
    /// expression evaluates to `true` for its assignment. A constant
    /// expression evaluates once: `FILTER(false)` discards every solution.
    pub(super) async fn execute_filter_clause(
        &self,
        ctx: &mut QueryContext,
        clause: FilterClause,
    ) -> Result<(), KipError> {
        let mut filter_vars_set: FxHashSet<String> = FxHashSet::default();
        Self::collect_filter_vars(&clause.expression, &mut filter_vars_set);
        let mut filter_vars: Vec<String> = filter_vars_set.into_iter().collect();
        filter_vars.sort();

        for var in &filter_vars {
            if !ctx.is_bound(var) {
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
                for table in ctx.tables.iter_mut() {
                    table.rows.clear();
                }
            }
            return Ok(());
        }

        let idx = ctx
            .join_tables_covering(filter_vars)?
            .expect("filter variables are bound");
        let cols: Vec<(String, usize)> = filter_vars
            .iter()
            .map(|var| {
                let col = ctx.tables[idx].column(var).expect("covered");
                (var.clone(), col)
            })
            .collect();

        // Evaluate per distinct assignment (rows repeating the same
        // bindings reuse the verdict — one entity load per distinct value).
        let rows = std::mem::take(&mut ctx.tables[idx].rows);
        let mut verdicts: FxHashMap<Vec<BindingValue>, bool> = FxHashMap::default();
        let mut kept: Vec<Vec<BindingValue>> = Vec::with_capacity(rows.len());
        for row in rows {
            let key: Vec<BindingValue> = cols.iter().map(|(_, col)| row[*col].clone()).collect();
            let keep = match verdicts.get(&key) {
                Some(keep) => *keep,
                None => {
                    let mut assign = FilterAssignment::default();
                    for ((var, _), value) in cols.iter().zip(key.iter()) {
                        assign.insert(var.clone(), value.clone());
                    }
                    let keep = self
                        .eval_filter_assigned(cache, regex_cache, expr, &assign)
                        .await?;
                    verdicts.insert(key, keep);
                    keep
                }
            };
            if keep {
                kept.push(row);
            }
        }
        ctx.tables[idx].rows = kept;
        Ok(())
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

    /// Executes a `NOT` clause as a per-solution anti-join (KIP §3.4.7.1:
    /// "exclude solutions that make the internal pattern valid").
    ///
    /// The block runs in a child context seeded with the outer variables'
    /// domains (outer bindings are visible inside `NOT`); its solution rows
    /// projected onto the shared variables form the exclusion set, and the
    /// outer rows whose shared tuple is excluded are removed. Padded
    /// (`null`) positions never contribute an excluded tuple and are never
    /// excluded themselves. Internal variables stay block-local.
    pub(super) async fn execute_not_clause(
        &self,
        ctx: &mut QueryContext,
        clauses: Vec<WhereClause>,
    ) -> Result<(), KipError> {
        // 快速路径: NOT { (?bound_var, "predicate", ?unbound_var) }
        // 通过单次批量查询完成，而不需要对每个 entity 单独查询。
        if clauses.len() == 1
            && let WhereClause::Proposition(prop_clause) = &clauses[0]
            && let PropositionMatcher::Object {
                subject: TargetTerm::Variable(subj_var),
                predicate: PredTerm::Literal(pred),
                object: TargetTerm::Variable(obj_var),
            } = &prop_clause.matcher
            && prop_clause.variable.is_none()
            && ctx.is_bound(subj_var)
            && !ctx.is_bound(obj_var)
        {
            return self
                .execute_not_proposition_fast_path(ctx, subj_var, pred)
                .await;
        }

        let mut not_vars: FxHashSet<String> = FxHashSet::default();
        Self::collect_clause_vars(&clauses, &mut not_vars);
        let mut shared: Vec<String> = not_vars
            .into_iter()
            .filter(|var| ctx.is_bound(var))
            .collect();
        shared.sort_unstable();

        // Grounding is lenient inside the block (KIP §3.4.7.1): a dangling
        // id makes the NOT pattern unmatchable, i.e. the clause succeeds
        // and excludes nothing — it must not abort the query.
        let mut child = ctx.scoped_child(&shared);
        child.lenient_grounding = true;
        for clause in clauses {
            Box::pin(self.execute_where_clause(&mut child, clause)).await?;
        }

        // NOT is a pure filter: with no outer-bound variable referenced it
        // cannot exclude anything.
        if shared.is_empty() {
            return Ok(());
        }
        // The block's clauses are conjunctive: any empty tree means the
        // whole block pattern is unsatisfiable and excludes nothing.
        if child.tables.iter().any(|table| table.rows.is_empty()) {
            return Ok(());
        }

        let Some(bidx) = child.join_tables_covering(&shared)? else {
            return Ok(());
        };
        let btable = &child.tables[bidx];
        let bcols: Vec<usize> = shared
            .iter()
            .map(|var| btable.column(var).expect("seeded"))
            .collect();
        let excluded: FxHashSet<Vec<BindingValue>> = btable
            .rows
            .iter()
            .filter_map(|row| {
                let tuple: Vec<BindingValue> = bcols.iter().map(|&col| row[col].clone()).collect();
                // A padded/unbound position never contributes an exclusion.
                if tuple.iter().any(|value| value.is_null()) {
                    None
                } else {
                    Some(tuple)
                }
            })
            .collect();
        if excluded.is_empty() {
            return Ok(());
        }

        let idx = ctx
            .join_tables_covering(&shared)?
            .expect("shared vars are outer-bound");
        let cols: Vec<usize> = shared
            .iter()
            .map(|var| ctx.tables[idx].column(var).expect("covered"))
            .collect();
        ctx.tables[idx].rows.retain(|row| {
            let tuple: Vec<BindingValue> = cols.iter().map(|&col| row[col].clone()).collect();
            tuple.iter().any(|value| value.is_null()) || !excluded.contains(&tuple)
        });
        Ok(())
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
    /// 3. 从解集中排除这些 subjects 所在的行
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
            .propositions()
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

        // 从解集中排除有此关系的 subject 行；Null（padding）行不受约束。
        if let Some(table) = ctx
            .tables
            .iter_mut()
            .find(|table| table.covers(subject_var))
        {
            let col = table.column(subject_var).expect("covered");
            table.rows.retain(|row| match &row[col] {
                BindingValue::Entity(id) => !subjects_with_relation.contains(id),
                _ => true,
            });
        }

        Ok(())
    }

    /// Executes an `OPTIONAL` clause as a left join (KIP §3.4.7.2): the
    /// block runs in a child context seeded with the outer domains, its
    /// solution set is materialized as one table, and the outer solutions
    /// extend with matching rows or pad the block's new variables with
    /// `null`. `OPTIONAL` never restricts the outer solution set.
    pub(super) async fn execute_optional_clause(
        &self,
        ctx: &mut QueryContext,
        clauses: Vec<WhereClause>,
    ) -> Result<(), KipError> {
        let mut opt_vars: FxHashSet<String> = FxHashSet::default();
        Self::collect_clause_vars(&clauses, &mut opt_vars);
        let mut seed: Vec<String> = opt_vars
            .into_iter()
            .filter(|var| ctx.is_bound(var))
            .collect();
        seed.sort_unstable();

        // Grounding is lenient inside the block (KIP §3.4.7.2): a dangling
        // id makes the optional pattern unmatchable — the outer solutions
        // are kept and the block's variables project `null`; it must not
        // abort the query.
        let mut child = ctx.scoped_child(&seed);
        child.lenient_grounding = true;
        for clause in clauses {
            Box::pin(self.execute_where_clause(&mut child, clause)).await?;
        }
        ctx.group_pairs.extend(child.group_pairs);

        let mut tables = child.tables.into_iter();
        let Some(mut block) = tables.next() else {
            return Ok(());
        };
        for table in tables {
            block = block.natural_join(&table)?;
        }

        let shared: Vec<String> = block
            .vars
            .iter()
            .filter(|var| ctx.is_bound(var))
            .cloned()
            .collect();
        if shared.is_empty() {
            // No outer anchor: new bindings extend every outer solution.
            // With no block solution the new variables project `null`.
            if block.rows.is_empty() {
                block.rows.push(vec![BindingValue::Null; block.vars.len()]);
            }
            ctx.tables.push(block);
            return Ok(());
        }

        let idx = ctx
            .join_tables_covering(&shared)?
            .expect("shared vars are outer-bound");
        let outer = std::mem::take(&mut ctx.tables[idx]);
        ctx.tables[idx] = outer.left_join(&block)?;
        Ok(())
    }

    /// Executes a `UNION` clause (KIP §3.4.7.3): the block runs in a fresh
    /// independent scope (outer bindings invisible), its solution set is
    /// materialized as one table, and the outer solution set becomes the
    /// row-wise union of both sides with `null` padding for the variables
    /// absent on either side, deduplicated (§3.3).
    pub(super) async fn execute_union_clause(
        &self,
        ctx: &mut QueryContext,
        clauses: Vec<WhereClause>,
    ) -> Result<(), KipError> {
        let mut child = QueryContext {
            cache: ctx.cache.clone(),
            // A dangling id inside the branch degrades to an empty match:
            // the branch contributes nothing instead of failing the whole
            // query (KIP §3.4.7.3 — the branch is an independent scope).
            lenient_grounding: true,
            ..Default::default()
        };
        for clause in clauses {
            Box::pin(self.execute_where_clause(&mut child, clause)).await?;
        }
        ctx.group_pairs.extend(child.group_pairs);

        let mut tables = child.tables.into_iter();
        let Some(mut branch) = tables.next() else {
            // The branch bound no variables: it contributes no solutions.
            return Ok(());
        };
        for table in tables {
            branch = branch.natural_join(&table)?;
        }

        if ctx.tables.is_empty() {
            ctx.tables.push(branch);
            return Ok(());
        }
        let mut outer_tables = std::mem::take(&mut ctx.tables).into_iter();
        let mut outer = outer_tables.next().expect("non-empty");
        for table in outer_tables {
            outer = outer.natural_join(&table)?;
        }
        ctx.tables.push(outer.union_padded(&branch)?);
        Ok(())
    }

    pub(super) async fn execute_find_clause(
        &self,
        ctx: &mut QueryContext,
        clause: FindClause,
        order_by: Option<Vec<OrderByCondition>>,
        cursor: Option<String>,
        limit: Option<usize>,
    ) -> Result<(Vec<Json>, Option<String>), KipError> {
        let order_by = order_by.unwrap_or_default();
        let limit = limit.unwrap_or(0);
        let raw_cursor = cursor.as_deref();
        let items = collect_find_items(&clause);

        let mut plain_vars: Vec<&str> = Vec::new();
        let mut agg_vars: Vec<&str> = Vec::new();
        for item in &items {
            match item {
                FindItem::Column { var, .. } => {
                    if !plain_vars.contains(&var.as_str()) {
                        plain_vars.push(var);
                    }
                }
                FindItem::Aggregate { var, .. } => {
                    if !agg_vars.contains(&var.var.as_str()) {
                        agg_vars.push(&var.var);
                    }
                }
            }
        }

        if agg_vars.is_empty() {
            if plain_vars.len() > 1 {
                return self
                    .project_multi_var(ctx, &items, &order_by, raw_cursor, limit)
                    .await;
            }
            // Single-variable projection: one column read straight off the
            // variable's table — no join materialization.
            let mut result = Vec::with_capacity(items.len());
            let mut next_cursor: Option<String> = None;
            for item in &items {
                let FindItem::Column { var, dot_paths } = item else {
                    unreachable!("no aggregates on this path");
                };
                let (col, cur) = self
                    .project_single_column(ctx, var, dot_paths, &order_by, raw_cursor, limit)
                    .await?;
                if cur.is_some() && next_cursor.is_none() {
                    next_cursor = cur;
                }
                result.push(Json::Array(col));
            }
            return Ok((result, next_cursor));
        }

        // Implicit grouping (KIP §3.3): a plain variable and an aggregated
        // variable connected as the endpoints of a matched proposition
        // pattern aggregate per group.
        let group_key = plain_vars.iter().find_map(|g| {
            agg_vars.iter().find_map(|m| {
                if g != m
                    && ctx.group_pairs.contains(&(g.to_string(), m.to_string()))
                    && ctx
                        .table_of(g)
                        .map(|table| table.covers(m))
                        .unwrap_or(false)
                {
                    Some((g.to_string(), m.to_string()))
                } else {
                    None
                }
            })
        });
        if let Some((gvar, mvar)) = group_key {
            return self
                .project_grouped(ctx, &clause, &gvar, &mvar, &order_by, &cursor, limit)
                .await;
        }

        // No group relationship: plain variables project independently and
        // aggregates compute globally (legacy result shape).
        let mut result: Vec<Json> = Vec::with_capacity(items.len());
        let mut next_cursor: Option<String> = None;
        // Only the first plain column paginates. The cursor it issues anchors
        // *its* variable's bindings, so handing the same token to another
        // variable's column would slice that column at an unrelated position;
        // the remaining columns project completely, like the global
        // aggregates beside them.
        let mut pagination_taken = false;
        for item in &items {
            match item {
                FindItem::Column { var, dot_paths } => {
                    let (page_cursor, page_limit) = if pagination_taken {
                        (None, 0)
                    } else {
                        pagination_taken = true;
                        (raw_cursor, limit)
                    };
                    let (col, cur) = self
                        .project_single_column(
                            ctx,
                            var,
                            dot_paths,
                            &order_by,
                            page_cursor,
                            page_limit,
                        )
                        .await?;
                    if cur.is_some() && next_cursor.is_none() {
                        next_cursor = cur;
                    }
                    result.push(Json::Array(col));
                }
                FindItem::Aggregate {
                    func,
                    var,
                    distinct,
                } => {
                    result.push(self.global_aggregate(ctx, func, var, *distinct).await?);
                }
            }
        }
        Ok((result, next_cursor))
    }

    /// Projects one variable as a single column (the non-materializing fast
    /// path). Entity columns keep the legacy semantics: distinct bindings,
    /// ascending `EntityID` order when unordered (deterministic pagination),
    /// `ORDER BY` on this variable's fields (KIP §3.5, nulls last), and an
    /// entity-anchored cursor. Predicate columns paginate with a numeric
    /// offset cursor. A column binding *both* kinds (reachable through
    /// `UNION`) goes to [`project_mixed_column`](Self::project_mixed_column)
    /// — neither kind may be dropped.
    async fn project_single_column(
        &self,
        ctx: &QueryContext,
        var: &str,
        dot_paths: &[DotPathVar],
        order_by: &[OrderByCondition],
        raw_cursor: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<Json>, Option<String>), KipError> {
        let Some(values) = ctx.distinct_values(var) else {
            return Err(KipError::reference_error(format!(
                "Unbound variable: {var:?}"
            )));
        };

        let has_predicates = values
            .iter()
            .any(|value| matches!(value, BindingValue::Predicate(_)));
        let has_entities = values
            .iter()
            .any(|value| matches!(value, BindingValue::Entity(_)));

        if has_predicates && has_entities {
            return self
                .project_mixed_column(ctx, var, dot_paths, order_by, raw_cursor, limit, &values)
                .await;
        }

        if has_predicates {
            // Predicate column: names in binding order, numeric offset
            // cursor. An unparseable token is rejected (KIP_1001) instead
            // of silently replaying from the start with duplicate pages.
            let names: Vec<Json> = values
                .into_iter()
                .filter_map(|value| match value {
                    BindingValue::Predicate(name) => Some(Json::String(name)),
                    _ => None,
                })
                .collect();
            let start = parse_offset_cursor(raw_cursor)?.min(names.len());
            let remaining = &names[start..];
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

        let ids: Vec<EntityID> = values
            .into_iter()
            .filter_map(|value| match value {
                BindingValue::Entity(id) => Some(id),
                _ => None,
            })
            .collect();

        // The cursor is an entity token; an unparseable one is rejected
        // (KIP_1001) instead of silently replaying page one.
        let cursor: Option<EntityID> =
            BTree::from_cursor(&raw_cursor.map(|cursor| cursor.to_string()))
                .map_err(|err| KipError::invalid_syntax(format!("Invalid CURSOR token: {err}")))?;
        let has_order_by = order_by
            .iter()
            .any(|cond| !cond.is_aggregation() && cond.variable.var == var);

        // Without ORDER BY the cursor skips entities with `eid <= cursor`,
        // which is only complete over an ascending iteration order —
        // solution rows keep clause order (UNION appends at the tail), so
        // sort explicitly.
        let mut ordered: Vec<&EntityID> = ids.iter().collect();
        if !has_order_by {
            ordered.sort();
        }

        let mut loaded: Vec<(&EntityID, Json)> = Vec::with_capacity(ordered.len());
        let mut has_more = false;
        for eid in ordered {
            if !has_order_by && cursor.as_ref().map(|v| eid <= v).unwrap_or(false) {
                continue;
            }
            if !has_order_by && limit > 0 && loaded.len() >= limit {
                // A further post-cursor entity exists: the page is truly full.
                has_more = true;
                break;
            }
            let value = self.load_entity_field(&ctx.cache, eid, "").await?;
            loaded.push((eid, value));
        }

        if has_order_by {
            loaded = apply_order_by(loaded, var, order_by);
            if let Some(cursor) = cursor.as_ref() {
                // The cursor anchors the last row of the previous page. When
                // that entity is gone from the ordered set (deleted between
                // pages, or a token minted for another variable), its
                // position is undefined — and leaving the rows unsliced would
                // hand the client page one again labelled "page two". Reject
                // it (KIP_3002), exactly as an unparseable token is rejected
                // above.
                let Some(idx) = loaded.iter().position(|(eid, _)| *eid == cursor) else {
                    return Err(KipError::not_found(format!(
                        "CURSOR token no longer resolves: {cursor} is not among the ordered \
                         results of {var:?} (it may have been deleted); re-run the query \
                         without CURSOR"
                    )));
                };
                loaded = loaded.split_off(idx + 1);
            }
        }

        // A cursor is only issued when more rows actually remain (strict
        // `<`) — `<=` would hand the client one extra empty page.
        let mut next_cursor: Option<String> = None;
        if limit > 0 && (has_more || limit < loaded.len()) {
            loaded.truncate(limit);
            next_cursor = loaded.last().and_then(|(eid, _)| BTree::to_cursor(eid));
        }

        let roots: Vec<Json> = loaded.into_iter().map(|(_, value)| value).collect();
        Ok((project_dot_paths(roots, dot_paths), next_cursor))
    }

    /// Projects a column whose bindings mix entity ids and predicate names.
    ///
    /// Mixed columns are reachable through `UNION`: `union_padded` merges the
    /// branches by variable *name*, and a variable in the predicate position
    /// binds a name — so `?x {type: "Drug"} UNION { (?a, ?x, ?b) }` binds both
    /// kinds. Neither kind may be dropped (a dropped kind would also
    /// contradict `COUNT(?x)`, which counts every distinct binding).
    ///
    /// Because an entity-anchored cursor cannot address a predicate row, this
    /// path paginates with a numeric offset cursor over one deterministic
    /// order: entities by ascending id first, then predicate names in binding
    /// order; `ORDER BY` re-sorts that base order stably.
    #[allow(clippy::too_many_arguments)]
    async fn project_mixed_column(
        &self,
        ctx: &QueryContext,
        var: &str,
        dot_paths: &[DotPathVar],
        order_by: &[OrderByCondition],
        raw_cursor: Option<&str>,
        limit: usize,
        values: &[BindingValue],
    ) -> Result<(Vec<Json>, Option<String>), KipError> {
        let mut ids: Vec<&EntityID> = values
            .iter()
            .filter_map(|value| match value {
                BindingValue::Entity(id) => Some(id),
                _ => None,
            })
            .collect();
        ids.sort();

        let mut roots: Vec<Json> = Vec::with_capacity(values.len());
        for eid in ids {
            roots.push(self.load_entity_field(&ctx.cache, eid, "").await?);
        }
        // A predicate binding is its own root value: a bare variable projects
        // the name and any dot path projects `null` — which is exactly what
        // pointing into a JSON string yields.
        roots.extend(values.iter().filter_map(|value| match value {
            BindingValue::Predicate(name) => Some(Json::String(name.clone())),
            _ => None,
        }));

        if order_by
            .iter()
            .any(|cond| !cond.is_aggregation() && cond.variable.var == var)
        {
            roots.sort_by(|a, b| compare_order_row(a, b, var, order_by));
        }

        // An unparseable token is rejected (KIP_1001) instead of silently
        // replaying from the start with duplicate pages.
        let start = parse_offset_cursor(raw_cursor)?.min(roots.len());
        let mut page = roots.split_off(start);
        let mut next_cursor: Option<String> = None;
        if limit > 0 && limit < page.len() {
            page.truncate(limit);
            next_cursor = Some((start + limit).to_string());
        }

        Ok((project_dot_paths(page, dot_paths), next_cursor))
    }

    /// Multi-variable projection (KIP §6.2.2): the tables covering the
    /// referenced variables are joined into one solution table
    /// (disconnected groups cross-join under the cap), solutions are
    /// deduplicated on the projected variables (§3.3), ordered (§3.5,
    /// nulls last) and paginated with a numeric offset cursor over the
    /// deterministic row order.
    async fn project_multi_var(
        &self,
        ctx: &mut QueryContext,
        items: &[FindItem],
        order_by: &[OrderByCondition],
        raw_cursor: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<Json>, Option<String>), KipError> {
        // Referenced variables in stable order: FIND order first, then
        // ORDER BY-only variables (a sort key on an unbound variable is
        // ignored rather than an error — KIP §3.5).
        let mut referenced: Vec<String> = Vec::new();
        for item in items {
            if let FindItem::Column { var, .. } = item
                && !referenced.contains(var)
            {
                referenced.push(var.clone());
            }
        }
        let find_var_count = referenced.len();
        for var in &referenced {
            if !ctx.is_bound(var) {
                return Err(KipError::reference_error(format!(
                    "Unbound variable: {var:?}"
                )));
            }
        }
        for cond in order_by {
            if !cond.is_aggregation()
                && !referenced.contains(&cond.variable.var)
                && ctx.is_bound(&cond.variable.var)
            {
                referenced.push(cond.variable.var.clone());
            }
        }

        let idx = ctx
            .join_tables_covering(&referenced)?
            .expect("referenced variables are bound");
        let table = &ctx.tables[idx];
        let cols: Vec<usize> = referenced
            .iter()
            .map(|var| table.column(var).expect("covered"))
            .collect();
        let rows: Vec<Vec<BindingValue>> = table
            .rows
            .iter()
            .map(|row| cols.iter().map(|&col| row[col].clone()).collect())
            .collect();

        let var_index: FxHashMap<&str, usize> = referenced
            .iter()
            .enumerate()
            .map(|(pos, var)| (var.as_str(), pos))
            .collect();
        let order_conditions: Vec<&OrderByCondition> = order_by
            .iter()
            .filter(|cond| {
                !cond.is_aggregation() && var_index.contains_key(cond.variable.var.as_str())
            })
            .collect();
        let compare_keys = |left: &[Json], right: &[Json]| -> std::cmp::Ordering {
            for (pos, cond) in order_conditions.iter().enumerate() {
                let ordering = compare_order_key(&left[pos], &right[pos], &cond.direction);
                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
            std::cmp::Ordering::Equal
        };

        // Solution deduplication (KIP §3.3): solutions whose bindings agree
        // on every projected variable collapse before ORDER BY and LIMIT.
        //
        // `ORDER BY` may grade a variable that is *not* projected, and that
        // variable's column differs across the rows collapsing into one
        // solution (an author with three books joins three rows). Taking the
        // key from whichever duplicate happened to survive would make the
        // order depend on join row order — and shift every numeric-offset
        // cursor page when an unrelated link is inserted. Each solution
        // therefore keeps its **best** key tuple for the requested direction:
        // the one that ranks it earliest, i.e. the smallest under `ASC` and
        // the largest under `DESC`, the order an explicit `MIN` / `MAX` sort
        // key would give. Keys graded on projected variables are identical
        // across the duplicates, so this is a no-op for them.
        let mut keyed_rows: Vec<(Vec<BindingValue>, Vec<Json>)> = Vec::with_capacity(rows.len());
        let mut solution_at: FxHashMap<Vec<BindingValue>, usize> =
            FxHashMap::with_capacity_and_hasher(rows.len(), Default::default());
        for row in rows {
            let mut sort_values = Vec::with_capacity(order_conditions.len());
            for cond in &order_conditions {
                let binding = &row[var_index[cond.variable.var.as_str()]];
                sort_values.push(
                    self.load_binding_field(&ctx.cache, binding, &cond.variable)
                        .await?,
                );
            }
            let solution = row[..find_var_count].to_vec();
            match solution_at.get(&solution) {
                Some(&pos) => {
                    if compare_keys(&sort_values, &keyed_rows[pos].1) == std::cmp::Ordering::Less {
                        keyed_rows[pos].1 = sort_values;
                    }
                }
                None => {
                    solution_at.insert(solution, keyed_rows.len());
                    keyed_rows.push((row, sort_values));
                }
            }
        }
        if !order_conditions.is_empty() {
            keyed_rows.sort_by(|(_, left_values), (_, right_values)| {
                compare_keys(left_values, right_values)
            });
        }
        let mut rows: Vec<Vec<BindingValue>> = keyed_rows.into_iter().map(|(row, _)| row).collect();

        // Numeric offset cursor over the deterministic row order. An
        // entity-anchored cursor cannot work here: rows without a
        // proposition binding (multi-hop paths, OPTIONAL padding) could
        // neither issue a resumable cursor nor be matched by one.
        let start = parse_offset_cursor(raw_cursor)?.min(rows.len());
        let mut rows = rows.split_off(start);
        let mut next_cursor: Option<String> = None;
        if limit > 0 && limit < rows.len() {
            rows.truncate(limit);
            next_cursor = Some((start + limit).to_string());
        }

        let mut result: Vec<Json> = Vec::with_capacity(items.len());
        for item in items {
            let FindItem::Column { var, dot_paths } = item else {
                unreachable!("no aggregates on this path");
            };
            let pos = var_index[var.as_str()];
            let mut column = Vec::with_capacity(rows.len());
            for row in &rows {
                if dot_paths.len() == 1 {
                    column.push(
                        self.load_binding_field(&ctx.cache, &row[pos], &dot_paths[0])
                            .await?,
                    );
                } else {
                    let mut values = Vec::with_capacity(dot_paths.len());
                    for dot_path in dot_paths {
                        values.push(
                            self.load_binding_field(&ctx.cache, &row[pos], dot_path)
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

    /// Grouped projection (implicit GROUP BY, KIP §3.3): rows of the table
    /// covering `(gvar, mvar)` group by the group variable in row order;
    /// each group aggregates its distinct non-null member bindings (`null`
    /// members are ignored, so an `OPTIONAL` miss counts 0). Other plain
    /// variables project globally and other aggregates compute globally,
    /// preserving the legacy grouped result shape.
    #[allow(clippy::too_many_arguments)]
    async fn project_grouped(
        &self,
        ctx: &mut QueryContext,
        clause: &FindClause,
        gvar: &str,
        mvar: &str,
        order_by: &[OrderByCondition],
        cursor: &Option<String>,
        limit: usize,
    ) -> Result<(Vec<Json>, Option<String>), KipError> {
        struct GroupRow {
            gid: EntityID,
            members: Vec<BindingValue>,
        }

        let table = ctx.table_of(gvar).expect("group var is bound");
        let gcol = table.column(gvar).expect("covered");
        let mcol = table.column(mvar).expect("group pair shares the table");

        let mut order: Vec<EntityID> = Vec::new();
        let mut members: FxHashMap<EntityID, Vec<BindingValue>> = FxHashMap::default();
        let mut seen: FxHashSet<(EntityID, BindingValue)> = FxHashSet::default();
        for row in &table.rows {
            let BindingValue::Entity(gid) = &row[gcol] else {
                continue;
            };
            if !members.contains_key(gid) {
                order.push(gid.clone());
                members.insert(gid.clone(), Vec::new());
            }
            let member = &row[mcol];
            if !member.is_null() && seen.insert((gid.clone(), member.clone())) {
                members.get_mut(gid).expect("inserted").push(member.clone());
            }
        }
        let mut rows: Vec<GroupRow> = order
            .into_iter()
            .map(|gid| {
                let members = members.remove(&gid).unwrap_or_default();
                GroupRow { gid, members }
            })
            .collect();

        // Ordering: an aggregation sort key (ORDER BY COUNT(?m)) wins,
        // otherwise sort by the group variable's fields (KIP §3.5).
        let has_agg_order = order_by.iter().any(|o| o.is_aggregation());
        let has_var_order = order_by
            .iter()
            .any(|o| !o.is_aggregation() && o.variable.var == gvar);
        if has_agg_order {
            let agg_direction = order_by
                .iter()
                .find(|o| o.is_aggregation())
                .map(|o| &o.direction)
                .unwrap_or(&OrderDirection::Asc);
            rows.sort_by(|a, b| {
                let ord = a.members.len().cmp(&b.members.len());
                match agg_direction {
                    OrderDirection::Asc => ord,
                    OrderDirection::Desc => ord.reverse(),
                }
            });
        } else if has_var_order {
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
                for (pos, cond) in order_conditions.iter().enumerate() {
                    let ordering =
                        compare_order_key(&left_values[pos], &right_values[pos], &cond.direction);
                    if ordering != std::cmp::Ordering::Equal {
                        return ordering;
                    }
                }
                std::cmp::Ordering::Equal
            });
            rows = keyed.into_iter().map(|(row, _)| row).collect();
        }

        // Entity-anchored cursor over the group entity; an invalid token
        // reports KIP_1001 (a silent replay would duplicate pages).
        let cursor_id: Option<EntityID> = BTree::from_cursor(cursor)
            .map_err(|err| KipError::invalid_syntax(format!("Invalid CURSOR token: {err}")))?;
        if let Some(ref cid) = cursor_id {
            // A group whose entity is gone (deleted between pages) leaves the
            // cursor unanchored: reject rather than return page one again
            // labelled "page two", as on the single-column ORDER BY path.
            let Some(pos) = rows.iter().position(|r| &r.gid == cid) else {
                return Err(KipError::not_found(format!(
                    "CURSOR token no longer resolves: {cid} is not among the groups of \
                     {gvar:?} (it may have been deleted); re-run the query without CURSOR"
                )));
            };
            rows = rows.split_off(pos + 1);
        }
        let mut next_cursor: Option<String> = None;
        if limit > 0 && rows.len() > limit {
            rows.truncate(limit);
            next_cursor = rows.last().and_then(|r| BTree::to_cursor(&r.gid));
        }

        // One output column per FIND expression (grouped shape does not
        // merge consecutive same-variable expressions).
        let mut result: Vec<Json> = Vec::with_capacity(clause.expressions.len());
        for expr in &clause.expressions {
            match expr {
                FindExpression::Variable(dot_path) => {
                    if dot_path.var == gvar {
                        let field = dot_path.to_pointer();
                        let mut col: Vec<Json> = Vec::with_capacity(rows.len());
                        for row in &rows {
                            col.push(self.load_entity_field(&ctx.cache, &row.gid, &field).await?);
                        }
                        result.push(Json::Array(col));
                    } else {
                        // Non-group variable: an independent *complete*
                        // global column, like the global aggregates beside it
                        // (a `SUM` over a non-group variable is likewise
                        // computed over its whole binding set).
                        //
                        // It must receive neither the cursor nor the limit:
                        // the cursor anchors a *group* entity, and
                        // `project_single_column` would read it as a position
                        // in this variable's own entity order — silently
                        // dropping every binding whose id sorts below the
                        // group's. The limit likewise belongs to the group
                        // rows. Passing neither keeps this column identical on
                        // every page instead of quietly varying with the group
                        // cursor.
                        let (col, _) = self
                            .project_single_column(
                                ctx,
                                &dot_path.var,
                                std::slice::from_ref(dot_path),
                                order_by,
                                None,
                                0,
                            )
                            .await?;
                        result.push(Json::Array(col));
                    }
                }
                FindExpression::Aggregation {
                    func,
                    var,
                    distinct,
                } => {
                    if var.var == mvar {
                        let mut col: Vec<Json> = Vec::with_capacity(rows.len());
                        for row in &rows {
                            col.push(
                                self.aggregate_bindings(
                                    &ctx.cache,
                                    func,
                                    var,
                                    &row.members,
                                    *distinct,
                                )
                                .await?,
                            );
                        }
                        result.push(Json::Array(col));
                    } else {
                        result.push(self.global_aggregate(ctx, func, var, *distinct).await?);
                    }
                }
            }
        }
        Ok((result, next_cursor))
    }

    /// Computes an aggregate over one variable's whole binding set
    /// (distinct bindings, KIP §3.3 set semantics). `COUNT` counts the
    /// distinct bindings without loading entities; an unbound variable
    /// counts 0 (other functions report the reference error).
    async fn global_aggregate(
        &self,
        ctx: &QueryContext,
        func: &AggregationFunction,
        dot_var: &DotPathVar,
        distinct: bool,
    ) -> Result<Json, KipError> {
        if matches!(func, AggregationFunction::Count) {
            let count = ctx
                .distinct_values(&dot_var.var)
                .map(|values| values.len())
                .unwrap_or(0);
            return Ok(Json::from(count));
        }
        let Some(values) = ctx.distinct_values(&dot_var.var) else {
            return Err(KipError::reference_error(format!(
                "Unbound variable: {:?}",
                dot_var.var
            )));
        };
        self.aggregate_bindings(&ctx.cache, func, dot_var, &values, distinct)
            .await
    }

    /// Aggregates a list of bindings: `COUNT` is a plain count (the inputs
    /// are already distinct, and aggregation ignores `null`); other
    /// functions load each binding's field value and delegate to
    /// [`AggregationFunction::calculate`].
    async fn aggregate_bindings(
        &self,
        cache: &QueryCache,
        func: &AggregationFunction,
        dot_var: &DotPathVar,
        bindings: &[BindingValue],
        distinct: bool,
    ) -> Result<Json, KipError> {
        if matches!(func, AggregationFunction::Count) {
            return Ok(Json::from(bindings.len()));
        }
        let field = dot_var.to_pointer_or("id");
        let mut values: Vec<Json> = Vec::with_capacity(bindings.len());
        for binding in bindings {
            match binding {
                BindingValue::Entity(eid) => {
                    values.push(self.load_entity_field(cache, eid, &field).await?);
                }
                BindingValue::Predicate(name) => values.push(Json::String(name.clone())),
                BindingValue::Null => {}
            }
        }
        Ok(func.calculate(&values, distinct))
    }

    /// 为单变量投影/分组模式加载单个实体的指定字段值
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

    /// Loads the value a binding yields for a dot-notation path.
    ///
    /// Shared by FILTER evaluation and solution-row projection: entity
    /// bindings load the entity field, predicate bindings yield the name
    /// for a bare variable (`null` for any dot path), and `Null` (padded)
    /// bindings always yield `null` (KIP §3.4.7.2).
    pub(super) async fn load_binding_field(
        &self,
        cache: &QueryCache,
        binding: &BindingValue,
        dot_path: &DotPathVar,
    ) -> Result<Json, KipError> {
        match binding {
            BindingValue::Entity(EntityID::Concept(id)) => {
                self.try_get_concept_with(cache, *id, |concept| {
                    extract_concept_field_value(concept, &dot_path.path)
                })
                .await
            }
            BindingValue::Entity(EntityID::Proposition(id, predicate)) => {
                self.try_get_proposition_with(cache, *id, |proposition| {
                    extract_proposition_field_value(proposition, predicate, &dot_path.path)
                })
                .await
            }
            BindingValue::Predicate(predicate) => Ok(if dot_path.path.is_empty() {
                Json::String(predicate.clone())
            } else {
                Json::Null
            }),
            BindingValue::Null => Ok(Json::Null),
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
