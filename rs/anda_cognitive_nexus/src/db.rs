//! # Cognitive Nexus Module
//!
//! This module provides the core database implementation for the cognitive nexus system.
//! It implements the Knowledge Interchange Protocol (KIP) executor interface and manages
//! concepts and propositions in a knowledge graph database.
//!
use anda_db::{
    collection::{Collection, CollectionConfig},
    database::AndaDB,
    error::DBError,
    index::{BTree, extract_json_text, virtual_field_name, virtual_field_value},
    query::{Filter, Query, RangeQuery, Search},
    unix_ms,
};
use anda_db_schema::Fv;
use anda_db_tfs::jieba_tokenizer;
use anda_db_utils::UniqueVec;
use anda_kip::*;
use async_trait::async_trait;
use futures::try_join as try_join_await;
use rustc_hash::{FxHashMap, FxHashSet};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::Arc,
};
use tokio::sync::RwLock;

use crate::{entity::*, helper::*, types::*};

/// Core database structure for the cognitive nexus system.
///
/// `CognitiveNexus` manages a knowledge graph consisting of concepts and propositions,
/// providing high-level operations for querying and manipulating the knowledge base.
/// It implements the Knowledge Interchange Protocol (KIP) executor interface.
///
/// # Architecture
///
/// - **Database Layer**: Built on top of AndaDB for persistent storage
/// - **Collections**: Separate collections for concepts and propositions with optimized indexes
/// - **Caching**: Thread-safe caching for improved query performance
/// - **Protocol Support**: Full KIP implementation with KQL, KML, and Meta commands
///
/// Core database structure for the cognitive nexus system.
///
/// `CognitiveNexus` manages a knowledge graph consisting of concepts and propositions,
/// providing high-level operations for querying and manipulating the knowledge base.
/// It implements the Knowledge Interchange Protocol (KIP) executor interface.
///
/// # Concurrency
///
/// The struct uses an async read-write lock (`RwLock`) to ensure KML execution consistency:
/// - **Read lock**: Acquired for KQL queries and Meta commands (allows concurrent reads)
/// - **Write lock**: Acquired for KML mutations (ensures exclusive access during data modifications)
///
/// This prevents race conditions during complex KML transactions that may involve
/// multiple concept and proposition updates across collections.
#[derive(Clone, Debug)]
pub struct CognitiveNexus {
    db: Arc<AndaDB>,
    concepts: Arc<Collection>,
    propositions: Arc<Collection>,
    /// Read-write lock for KML execution consistency.
    /// KQL/Meta commands acquire read lock; KML commands acquire write lock.
    kml_lock: Arc<RwLock<()>>,
}

/// Implementation of the Knowledge Interchange Protocol (KIP) executor.
///
/// This trait implementation allows the cognitive nexus to process KIP commands,
/// including queries (KQL), markup language statements (KML), and meta commands.
#[async_trait]
impl Executor for CognitiveNexus {
    /// Executes a KIP command and returns the appropriate response.
    ///
    /// # Arguments
    ///
    /// * `command` - The KIP command to execute (KQL, KML, or Meta)
    /// * `dry_run` - Whether to perform a dry run (only applicable to KML commands)
    ///
    /// # Returns
    ///
    /// A `Response` containing the execution result, which may include:
    /// - Query results for KQL commands
    /// - Modification results for KML commands
    /// - Metadata for Meta commands
    ///
    /// # Concurrency
    ///
    /// - KQL and Meta commands acquire a read lock (allows concurrent execution)
    /// - KML commands acquire a write lock (ensures exclusive access during mutations)
    ///
    async fn execute(&self, command: Command, dry_run: bool) -> Response {
        match command {
            Command::Kql(command) => {
                let _guard = self.kml_lock.read().await;
                self.execute_kql(command).await.into()
            }
            Command::Kml(command) => {
                let _guard = self.kml_lock.write().await;
                match self.execute_kml(command, dry_run).await {
                    Ok(result) => Response::Ok {
                        result,
                        next_cursor: None,
                        ignore: Some(true),
                    },
                    Err(error) => Response::err(error),
                }
            }
            Command::Meta(command) => {
                let _guard = self.kml_lock.read().await;
                self.execute_meta(command).await.into()
            }
        }
    }
}

impl CognitiveNexus {
    /// Establishes a connection to the cognitive nexus database.
    ///
    /// This method initializes the database collections, creates necessary indexes,
    /// and sets up the initial schema. It also ensures that essential meta-concepts
    /// are present in the database.
    ///
    /// # Arguments
    ///
    /// * `db` - Reference to the underlying AndaDB database
    /// * `f` - Initialization function called after setup but before returning
    ///
    /// # Returns
    ///
    /// * `Ok(CognitiveNexus)` - Successfully initialized cognitive nexus
    /// * `Err(KipError)` - If initialization fails
    ///
    /// # Database Setup
    ///
    /// The method performs the following initialization steps:
    /// 1. Creates or opens the "concepts" collection with appropriate schema and indexes
    /// 2. Creates or opens the "propositions" collection with appropriate schema and indexes
    /// 3. Sets up text tokenization for full-text search capabilities
    /// 4. Ensures essential meta-concepts exist (creates them if missing)
    /// 5. Calls the provided initialization function
    ///
    /// # Indexes Created
    ///
    /// **Concepts Collection:**
    /// - BTree indexes: ["type", "name"], ["type"], ["name"]
    /// - BM25 index: ["name", "attributes", "metadata"]
    ///
    /// **Propositions Collection:**
    /// - BTree indexes: ["subject", "object"], ["subject"], ["object"], ["predicates"]
    /// - BM25 index: ["predicates", "properties"]
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let db = Arc::new(AndaDB::new("knowledge_base").await?);
    /// let nexus = CognitiveNexus::connect(db, |nexus| async {
    ///     // Custom initialization logic here
    ///     println!("Connected to database: {}", nexus.name());
    ///     Ok(())
    /// }).await?;
    /// ```
    pub async fn connect<F>(db: Arc<AndaDB>, f: F) -> Result<Self, KipError>
    where
        F: AsyncFnOnce(&CognitiveNexus) -> Result<(), KipError>,
    {
        let schema = Concept::schema().map_err(KipError::invalid_syntax)?;
        let concepts = db
            .open_or_create_collection(
                schema,
                CollectionConfig {
                    name: "concepts".to_string(),
                    description: "Concept nodes".to_string(),
                },
                async |collection| {
                    // set tokenizer
                    collection.set_tokenizer(jieba_tokenizer());
                    // create BTree indexes if not exists
                    collection.create_btree_index_nx(&["type", "name"]).await?;
                    collection.create_btree_index_nx(&["type"]).await?;
                    collection.create_btree_index_nx(&["name"]).await?;
                    collection
                        .create_bm25_index_nx(&["name", "attributes", "metadata"])
                        .await?;

                    Ok::<(), DBError>(())
                },
            )
            .await
            .map_err(db_to_kip_error)?;

        let schema = Proposition::schema().map_err(KipError::invalid_syntax)?;
        let propositions = db
            .open_or_create_collection(
                schema,
                CollectionConfig {
                    name: "propositions".to_string(),
                    description: "Proposition links".to_string(),
                },
                async |collection| {
                    // set tokenizer
                    collection.set_tokenizer(jieba_tokenizer());
                    // create BTree indexes if not exists
                    collection
                        .create_btree_index_nx(&["subject", "object"])
                        .await?;
                    collection.create_btree_index_nx(&["subject"]).await?;
                    collection.create_btree_index_nx(&["object"]).await?;
                    collection.create_btree_index_nx(&["predicates"]).await?;
                    collection
                        .create_bm25_index_nx(&["predicates", "properties"])
                        .await?;

                    Ok::<(), DBError>(())
                },
            )
            .await
            .map_err(db_to_kip_error)?;
        let this = Self {
            db,
            concepts,
            propositions,
            kml_lock: Arc::new(RwLock::new(())),
        };

        if !this
            .has_concept(&ConceptPK::Object {
                r#type: META_CONCEPT_TYPE.to_string(),
                name: META_CONCEPT_TYPE.to_string(),
            })
            .await
        {
            this.execute_kml(parse_kml(GENESIS_KIP)?, false).await?;
        }

        if !this
            .has_concept(&ConceptPK::Object {
                r#type: META_CONCEPT_TYPE.to_string(),
                name: PERSON_TYPE.to_string(),
            })
            .await
        {
            this.execute_kml(parse_kml(PERSON_KIP)?, false).await?;
        }

        if !this
            .has_concept(&ConceptPK::Object {
                r#type: META_CONCEPT_TYPE.to_string(),
                name: EVENT_TYPE.to_string(),
            })
            .await
        {
            this.execute_kml(parse_kml(EVENT_KIP)?, false).await?;
        }

        f(&this).await?;
        Ok(this)
    }

    /// Closes the database connection and releases resources.
    ///
    /// This method should be called when the cognitive nexus is no longer needed
    /// to ensure proper cleanup of database resources.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Database closed successfully
    /// * `Err(KipError)` - If closing the database fails
    ///
    pub async fn close(&self) -> Result<(), KipError> {
        self.db.close().await.map_err(db_to_kip_error)
    }

    /// Returns the name of the underlying database.
    pub fn name(&self) -> &str {
        self.db.name()
    }

    /// Checks whether a concept exists in the database.
    ///
    /// This method performs a fast existence check without loading the full concept data.
    /// It supports both ID-based and object-based concept identification.
    ///
    /// # Arguments
    ///
    /// * `pk` - The primary key of the concept to check
    ///
    /// # Returns
    ///
    /// * `true` - If the concept exists
    /// * `false` - If the concept does not exist or cannot be found
    ///
    /// # Performance
    ///
    /// - For ID-based lookups: O(1) existence check
    /// - For object-based lookups: O(log n) index lookup followed by O(1) existence check
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Check by ID
    /// let exists = nexus.has_concept(&ConceptPK::ID(12345)).await;
    ///
    /// // Check by type and name
    /// let exists = nexus.has_concept(&ConceptPK::Object {
    ///     r#type: "Person".to_string(),
    ///     name: "Alice".to_string(),
    /// }).await;
    /// ```
    pub async fn has_concept(&self, pk: &ConceptPK) -> bool {
        let id = match pk {
            ConceptPK::ID(id) => *id,
            ConceptPK::Object { r#type, name } => match self.query_concept_id(r#type, name).await {
                Ok(id) => id,
                Err(_) => return false,
            },
        };

        self.concepts.contains(id)
    }

    /// Retrieves a concept from the database.
    ///
    /// This method loads the complete concept data including all attributes and metadata.
    /// It supports both ID-based and object-based concept identification.
    ///
    /// # Arguments
    ///
    /// * `pk` - The primary key of the concept to retrieve
    ///
    /// # Returns
    ///
    /// * `Ok(Concept)` - The loaded concept with all its data
    /// * `Err(KipError)` - If the concept is not found or loading fails
    ///
    pub async fn get_concept(&self, pk: &ConceptPK) -> Result<Concept, KipError> {
        let id = match pk {
            ConceptPK::ID(id) => *id,
            ConceptPK::Object { r#type, name } => self.query_concept_id(r#type, name).await?,
        };

        self.concepts.get_as(id).await.map_err(db_to_kip_error)
    }

    pub async fn execute_kql(&self, command: KqlQuery) -> Result<(Json, Option<String>), KipError> {
        let mut ctx = QueryContext::default();

        // 执行WHERE子句
        for clause in command.where_clauses {
            self.execute_where_clause(&mut ctx, clause).await?;
        }

        // 执行FIND子句
        let mut result = self
            .execute_find_clause(
                &mut ctx,
                command.find_clause,
                command.order_by,
                command.cursor,
                command.limit,
            )
            .await?;

        if result.0.len() == 1 {
            Ok((result.0.pop().unwrap(), result.1))
        } else {
            Ok((Json::Array(result.0), result.1))
        }
    }

    pub async fn execute_kml(
        &self,
        command: KmlStatement,
        dry_run: bool,
    ) -> Result<Json, KipError> {
        match command {
            KmlStatement::Upsert(upsert_blocks) => {
                self.execute_upsert(upsert_blocks, dry_run).await
            }
            KmlStatement::Delete(delete_statement) => {
                self.execute_delete(delete_statement, dry_run).await
            }
        }
    }

    pub async fn execute_meta(
        &self,
        command: MetaCommand,
    ) -> Result<(Json, Option<String>), KipError> {
        match command {
            MetaCommand::Describe(DescribeTarget::Primer) => {
                self.execute_describe_primer().await.map(|rt| (rt, None))
            }
            MetaCommand::Describe(DescribeTarget::Domains) => {
                self.execute_describe_domains().await.map(|rt| (rt, None))
            }
            MetaCommand::Describe(DescribeTarget::ConceptTypes { limit, cursor }) => {
                self.execute_describe_concept_types(limit, cursor).await
            }
            MetaCommand::Describe(DescribeTarget::ConceptType(name)) => self
                .execute_describe_concept_type(name)
                .await
                .map(|rt| (rt, None)),
            MetaCommand::Describe(DescribeTarget::PropositionTypes { limit, cursor }) => {
                self.execute_describe_proposition_types(limit, cursor).await
            }
            MetaCommand::Describe(DescribeTarget::PropositionType(name)) => self
                .execute_describe_proposition_type(name)
                .await
                .map(|rt| (rt, None)),
            MetaCommand::Search(command) => self.execute_search(command).await.map(|rt| (rt, None)),
        }
    }

    async fn execute_where_clause(
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

    async fn execute_concept_clause(
        &self,
        ctx: &mut QueryContext,
        clause: ConceptClause,
    ) -> Result<(), KipError> {
        if ctx.entities.contains_key(&clause.variable) {
            return Err(KipError::invalid_syntax(format!(
                "Variable '{}' already exists in context",
                clause.variable
            )));
        }

        let concept_ids = self.query_concept_ids(&clause.matcher).await?;
        ctx.entities.insert(
            clause.variable,
            concept_ids.into_iter().map(EntityID::Concept).collect(),
        );

        Ok(())
    }

    async fn execute_proposition_clause(
        &self,
        ctx: &mut QueryContext,
        clause: PropositionClause,
    ) -> Result<(), KipError> {
        if let Some(var) = &clause.variable
            && ctx.entities.contains_key(var)
        {
            return Err(KipError::invalid_syntax(format!(
                "Variable '{var}' already exists in context",
            )));
        }

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
                self.match_propositions(ctx, subject, predicate, object)
                    .await?
            }
        };

        if let TargetEntities::IDs(ids) = result
            && let Some(var) = clause.variable
        {
            ctx.entities.insert(var, ids.into());
        }

        Ok(())
    }

    async fn execute_filter_clause(
        &self,
        ctx: &mut QueryContext,
        clause: FilterClause,
    ) -> Result<(), KipError> {
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

    async fn execute_not_clause(
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

        // 标准路径：对于复杂情况使用原有逻辑
        let mut not_context = ctx.clone();
        for clause in clauses {
            Box::pin(self.execute_where_clause(&mut not_context, clause)).await?;
        }

        for (var, ids) in not_context.entities {
            if ids.is_empty() {
                continue;
            }
            // 如果 NOT 子句中有变量绑定，则从当前上下文中移除这些绑定
            if let Some(existing) = ctx.entities.get_mut(&var) {
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
    /// 相比原始实现的 O(N) 数据库查询有显著提升
    async fn execute_not_proposition_fast_path(
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
                .try_get_proposition_with(&ctx.cache, id, |prop| {
                    if prop.predicates.contains(predicate) {
                        Ok(Some(prop.subject.clone()))
                    } else {
                        Ok(None)
                    }
                })
                .await?;

            if let Some(subj) = subject {
                subjects_with_relation.insert(subj);
            }
        }

        // 从原始绑定中排除有此关系的 subjects
        if let Some(existing) = ctx.entities.get_mut(subject_var) {
            existing.retain(|id| !subjects_with_relation.contains(id));
        }

        Ok(())
    }

    async fn execute_optional_clause(
        &self,
        ctx: &mut QueryContext,
        clauses: Vec<WhereClause>,
    ) -> Result<(), KipError> {
        let mut optional_context = ctx.clone();
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

        Ok(())
    }

    async fn execute_union_clause(
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

        Ok(())
    }

    async fn execute_find_clause(
        &self,
        ctx: &mut QueryContext,
        clause: FindClause,
        order_by: Option<Vec<OrderByCondition>>,
        cursor: Option<String>,
        limit: Option<usize>,
    ) -> Result<(Vec<Json>, Option<String>), KipError> {
        let mut result: Vec<Json> = Vec::with_capacity(clause.expressions.len());
        let bindings: FxHashMap<String, Vec<EntityID>> = ctx
            .entities
            .iter()
            .map(|(var, ids)| (var.clone(), ids.to_vec()))
            .collect();

        let order_by = order_by.unwrap_or_default();
        let limit = limit.unwrap_or(0);
        let cursor: Option<EntityID> = BTree::from_cursor(&cursor).ok().flatten();
        let mut next_cursor: Option<String> = None;
        let mut group_var: Option<(String, Vec<String>)> = None;

        for expr in clause.expressions {
            match expr {
                FindExpression::Variable(dot_path) => {
                    // 如果当前 group_var 存在且变量不同，处理之前的 group_var
                    match &group_var {
                        Some((var, fields)) if var != &dot_path.var => {
                            let (col, cur) = self
                                .resolve_result(
                                    &ctx.cache,
                                    &bindings,
                                    var,
                                    fields,
                                    &order_by,
                                    cursor.as_ref(),
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
                            .resolve_result(
                                &ctx.cache,
                                &bindings,
                                var,
                                fields,
                                &order_by,
                                cursor.as_ref(),
                                limit,
                            )
                            .await?;

                        if cur.is_some() && next_cursor.is_none() {
                            next_cursor = cur;
                        }

                        result.push(Json::Array(col));
                        group_var = None;
                    }

                    let (col, _) = self
                        .resolve_result(
                            &ctx.cache,
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

        // 处理最后的 group_var
        if let Some((var, fields)) = &group_var {
            let (col, cur) = self
                .resolve_result(
                    &ctx.cache,
                    &bindings,
                    var,
                    fields,
                    &order_by,
                    cursor.as_ref(),
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

    async fn execute_describe_primer(&self) -> Result<Json, KipError> {
        let cache = QueryCache::default();
        let matcher = ConceptMatcher::Object {
            r#type: PERSON_TYPE.to_string(),
            name: META_SELF_NAME.to_string(),
        };
        let me = self.query_concept_ids(&matcher).await?;

        let me = me
            .first()
            .ok_or_else(|| KipError::not_found(format!("Concept {matcher} not found")))?;
        let me = self
            .try_get_concept_with(&cache, *me, |concept| {
                extract_concept_field_value(concept, &["attributes".to_string()])
            })
            .await?;

        let domains = self
            .query_concept_ids(&ConceptMatcher::Type(DOMAIN_TYPE.to_string()))
            .await?;
        let mut domain_map: Vec<DomainInfo> = Vec::with_capacity(domains.len());
        for id in domains {
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
                                info.key_concept_types.push(ConceptTypeInfo::from(concept));
                            } else if concept.r#type == META_PROPOSITION_TYPE {
                                info.key_proposition_types
                                    .push(PropositionTypeInfo::from(concept));
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
        }))
    }

    async fn execute_describe_domains(&self) -> Result<Json, KipError> {
        let ids = self
            .query_concept_ids(&ConceptMatcher::Type(DOMAIN_TYPE.to_string()))
            .await?;
        let cache = QueryCache::default();
        let mut result: Vec<Json> = Vec::with_capacity(ids.len());
        for id in ids {
            let concept = self
                .try_get_concept_with(&cache, id, |concept| {
                    extract_concept_field_value(concept, &[])
                })
                .await?;
            result.push(concept);
        }
        Ok(json!(result))
    }

    async fn execute_describe_concept_types(
        &self,
        limit: Option<usize>,
        cursor: Option<String>,
    ) -> Result<(Json, Option<String>), KipError> {
        let index = self
            .concepts
            .get_btree_index(&["type"])
            .map_err(db_to_kip_error)?;

        let result = index.keys(cursor, limit);
        if limit.map(|v| v > 0 && result.len() >= v).unwrap_or(false) {
            let cursor = result.last().and_then(BTree::to_cursor);
            return Ok((json!(result), cursor));
        }
        Ok((json!(result), None))
    }

    async fn execute_describe_concept_type(&self, name: String) -> Result<Json, KipError> {
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
                extract_concept_field_value(concept, &[])
            })
            .await?;
        Ok(result)
    }

    async fn execute_describe_proposition_types(
        &self,
        limit: Option<usize>,
        cursor: Option<String>,
    ) -> Result<(Json, Option<String>), KipError> {
        let index = self
            .propositions
            .get_btree_index(&["predicates"])
            .map_err(db_to_kip_error)?;

        let result = index.keys(cursor, limit);
        if limit.map(|v| v > 0 && result.len() >= v).unwrap_or(false) {
            let cursor = result.last().and_then(BTree::to_cursor);
            return Ok((json!(result), cursor));
        }
        Ok((json!(result), None))
    }

    async fn execute_describe_proposition_type(&self, name: String) -> Result<Json, KipError> {
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
                extract_concept_field_value(concept, &[])
            })
            .await?;
        Ok(result)
    }

    async fn execute_search(&self, command: SearchCommand) -> Result<Json, KipError> {
        match command.target {
            SearchTarget::Concept => {
                let result: Vec<Concept> = self
                    .concepts
                    .search_as(Query {
                        search: Some(Search {
                            text: Some(command.term),
                            logical_search: true,
                            ..Default::default()
                        }),
                        filter: command.in_type.map(|v| {
                            Filter::Field(("type".to_string(), RangeQuery::Eq(Fv::Text(v))))
                        }),
                        limit: command.limit,
                    })
                    .await
                    .map_err(db_to_kip_error)?;

                Ok(json!(
                    result
                        .into_iter()
                        .map(|c| c.into_concept_node())
                        .collect::<Vec<_>>()
                ))
            }
            SearchTarget::Proposition => {
                let tokens = self.propositions.tokenize(&command.term);
                let ids = self
                    .propositions
                    .search_ids(Query {
                        search: Some(Search {
                            text: Some(command.term),
                            logical_search: true,
                            ..Default::default()
                        }),
                        filter: command.in_type.map(|v| {
                            Filter::Field(("predicates".to_string(), RangeQuery::Eq(Fv::Text(v))))
                        }),
                        limit: command.limit,
                    })
                    .await
                    .map_err(db_to_kip_error)?;
                let cache = QueryCache::default();
                let mut result: Vec<Json> = Vec::with_capacity(ids.len());
                for id in ids {
                    let rt = self
                        .try_get_proposition_with(&cache, id, |proposition| {
                            let mut rt: Vec<Json> = Vec::new();
                            for (predicate, prop) in &proposition.properties {
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
                                    && let Ok(val) =
                                        extract_proposition_field_value(proposition, predicate, &[])
                                {
                                    rt.push(val);
                                }
                            }

                            Ok(rt)
                        })
                        .await?;
                    result.extend(rt);
                }
                Ok(json!(result))
            }
        }
    }

    // 处理多跳匹配
    async fn handle_multi_hop_matching(
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
    async fn handle_subject_object_ids_matching(
        &self,
        ctx: &QueryContext,
        subject_ids: Vec<EntityID>,
        object_ids: Vec<EntityID>,
        predicate: PredTerm,
    ) -> Result<PropositionsMatchResult, KipError> {
        let mut result = PropositionsMatchResult::default();

        for subject_id in &subject_ids {
            for object_id in &object_ids {
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
            }
        }

        Ok(result)
    }

    // 处理主体ID和任意对象的匹配
    async fn handle_subject_ids_any_matching(
        &self,
        ctx: &QueryContext,
        subject_ids: Vec<EntityID>,
        predicate: PredTerm,
        any_propositions: bool,
    ) -> Result<PropositionsMatchResult, KipError> {
        let mut result = PropositionsMatchResult::default();

        for subject_id in &subject_ids {
            let ids = self
                .propositions
                .query_ids(
                    Filter::Field((
                        "subject".to_string(),
                        RangeQuery::Eq(Fv::Text(subject_id.to_string())),
                    )),
                    None,
                )
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
        }

        Ok(result)
    }

    // 处理任意主体和对象ID的匹配
    async fn handle_any_to_object_ids_matching(
        &self,
        ctx: &QueryContext,
        object_ids: Vec<EntityID>,
        predicate: PredTerm,
        any_propositions: bool,
    ) -> Result<PropositionsMatchResult, KipError> {
        let mut result = PropositionsMatchResult::default();

        for object_id in &object_ids {
            let ids = self
                .propositions
                .query_ids(
                    Filter::Field((
                        "object".to_string(),
                        RangeQuery::Eq(Fv::Text(object_id.to_string())),
                    )),
                    None,
                )
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
        }

        Ok(result)
    }

    // 处理谓词匹配
    async fn handle_predicate_matching(
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
    async fn bfs_multi_hop(
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

    async fn execute_upsert(
        &self,
        upsert_blocks: Vec<UpsertBlock>,
        dry_run: bool,
    ) -> Result<Json, KipError> {
        let blocks = upsert_blocks.len();
        let mut concept_nodes: Vec<EntityID> = Vec::new();
        let mut proposition_links: Vec<EntityID> = Vec::new();
        for block in upsert_blocks {
            let mut handle_map: FxHashMap<String, EntityID> = FxHashMap::default();
            let mut cached_pks: FxHashMap<EntityPK, EntityID> = FxHashMap::default();
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

        if !dry_run {
            let now_ms = unix_ms();
            try_join_await!(self.concepts.flush(now_ms), self.propositions.flush(now_ms))
                .map_err(db_to_kip_error)?;
        }

        Ok(json!(UpsertResult {
            blocks,
            upsert_concept_nodes: concept_nodes.into_iter().map(|id| id.to_string()).collect(),
            upsert_proposition_links: proposition_links
                .into_iter()
                .map(|id| id.to_string())
                .collect(),
        }))
    }

    async fn execute_concept_block(
        &self,
        concept_block: ConceptBlock,
        default_metadata: &Map<String, Json>,
        handle_map: &mut FxHashMap<String, EntityID>,
        cached_pks: &mut FxHashMap<EntityPK, EntityID>,
        dry_run: bool,
    ) -> Result<Option<EntityID>, KipError> {
        let concept_pk = ConceptPK::try_from(concept_block.concept)?;
        if let Some(propositions) = &concept_block.set_propositions {
            for set_prop in propositions {
                self.check_target_term_for_kml(&set_prop.object, handle_map)?;
            }
        }

        if let ConceptPK::Object { r#type, .. } = &concept_pk {
            // 确保概念类型已经定义
            if r#type != META_CONCEPT_TYPE
                && !self
                    .has_concept(&ConceptPK::Object {
                        r#type: META_CONCEPT_TYPE.to_string(),
                        name: r#type.clone(),
                    })
                    .await
            {
                return Err(KipError::not_found(format!(
                    "Concept type {} not found",
                    r#type
                )));
            }
        }

        if dry_run {
            return Ok(None);
        }

        let attributes = concept_block
            .set_attributes
            .map(|val| val.into_iter().collect())
            .unwrap_or_default();
        let metadata = concept_block
            .metadata
            .map(|val| val.into_iter().collect())
            .unwrap_or_else(|| default_metadata.clone());

        let entity_id = self
            .upsert_concept(concept_pk, attributes, metadata)
            .await?;

        if let Some(handle) = concept_block.handle {
            handle_map.insert(handle, entity_id.clone());
        }

        if let Some(propositions) = concept_block.set_propositions {
            for set_prop in propositions {
                self.execute_set_proposition(
                    &entity_id,
                    set_prop,
                    default_metadata,
                    handle_map,
                    cached_pks,
                )
                .await?;
            }
        }

        Ok(Some(entity_id))
    }

    async fn execute_proposition_block(
        &self,
        proposition_block: PropositionBlock,
        default_metadata: &Map<String, Json>,
        handle_map: &mut FxHashMap<String, EntityID>,
        cached_pks: &mut FxHashMap<EntityPK, EntityID>,
        dry_run: bool,
    ) -> Result<Option<EntityID>, KipError> {
        let proposition_pk = PropositionPK::try_from(proposition_block.proposition)?;
        if let PropositionPK::Object { predicate, .. } = &proposition_pk {
            // 确保命题谓词已经定义
            if !self
                .has_concept(&ConceptPK::Object {
                    r#type: META_PROPOSITION_TYPE.to_string(),
                    name: predicate.clone(),
                })
                .await
            {
                return Err(KipError::not_found(format!(
                    "Proposition type {} not found",
                    r#predicate
                )));
            }
        }

        if dry_run {
            return Ok(None);
        }

        let attributes = proposition_block
            .set_attributes
            .map(|val| val.into_iter().collect())
            .unwrap_or_default();
        let metadata = proposition_block
            .metadata
            .map(|val| val.into_iter().collect())
            .unwrap_or_else(|| default_metadata.clone());

        let entity_id = self
            .upsert_proposition(proposition_pk, attributes, metadata, cached_pks)
            .await?;

        if let Some(handle) = proposition_block.handle {
            handle_map.insert(handle, entity_id.clone());
        }

        Ok(Some(entity_id))
    }

    async fn execute_set_proposition(
        &self,
        subject: &EntityID,
        set_prop: SetProposition,
        default_metadata: &Map<String, Json>,
        handle_map: &FxHashMap<String, EntityID>,
        cached_pks: &mut FxHashMap<EntityPK, EntityID>,
    ) -> Result<EntityID, KipError> {
        let object_id = self
            .resolve_target_term(set_prop.object, handle_map, cached_pks)
            .await?;

        let proposition_pk = PropositionPK::Object {
            subject: Box::new(subject.clone().into()),
            predicate: set_prop.predicate,
            object: Box::new(object_id.clone().into()),
        };
        let metadata = set_prop
            .metadata
            .map(|val| val.into_iter().collect())
            .unwrap_or_else(|| default_metadata.clone());

        let entity_id = self
            .upsert_proposition(proposition_pk, Map::new(), metadata, cached_pks)
            .await?;

        Ok(entity_id)
    }

    async fn execute_delete(
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

        let now_ms = unix_ms();
        try_join_await!(self.concepts.flush(now_ms), self.propositions.flush(now_ms))
            .map_err(db_to_kip_error)?;

        Ok(result)
    }

    async fn execute_delete_attributes(
        &self,
        attributes: Vec<String>,
        target: String,
        where_clauses: Vec<WhereClause>,
        dry_run: bool,
    ) -> Result<Json, KipError> {
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
                        let length = concept.attributes.len();
                        for attr in &attributes {
                            concept.attributes.remove(attr);
                        }
                        if concept.attributes.len() < length
                            && self
                                .concepts
                                .update(
                                    *id,
                                    BTreeMap::from([(
                                        "attributes".to_string(),
                                        concept.attributes.into(),
                                    )]),
                                )
                                .await
                                .is_ok()
                        {
                            updated_concepts += 1;
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

                        if prop.attributes.len() < length
                            && self
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
                            updated_propositions += 1;
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

    async fn execute_delete_metadata(
        &self,
        keys: Vec<String>,
        target: String,
        where_clauses: Vec<WhereClause>,
        dry_run: bool,
    ) -> Result<Json, KipError> {
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
                        if concept.metadata.len() < length
                            && self
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
                            updated_concepts += 1;
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

                        if prop.metadata.len() < length
                            && self
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
                            updated_propositions += 1;
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

    async fn execute_delete_propositions(
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

    async fn execute_delete_concepts(
        &self,
        target: String,
        where_clauses: Vec<WhereClause>,
        dry_run: bool,
    ) -> Result<Json, KipError> {
        if dry_run {
            return Ok(json!({
                "deleted_propositions": 0,
                "deleted_concepts": 0
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
        let mut deleted_concepts: u64 = 0;
        for entity_id in target_entities.as_ref() {
            match &entity_id {
                EntityID::Concept(id) => {
                    let mut propositions_ids: BTreeSet<u64> = BTreeSet::new();
                    let eid: Fv = entity_id.to_string().into();
                    if let Ok(ids) = self
                        .propositions
                        .query_ids(
                            Filter::Or(vec![
                                Box::new(Filter::Field((
                                    "subject".to_string(),
                                    RangeQuery::Eq(eid.clone()),
                                ))),
                                Box::new(Filter::Field((
                                    "object".to_string(),
                                    RangeQuery::Eq(eid),
                                ))),
                            ]),
                            None,
                        )
                        .await
                    {
                        propositions_ids.extend(ids);
                    }

                    for id in propositions_ids {
                        if self.propositions.remove(id).await.is_ok() {
                            deleted_propositions += 1;
                        }
                    }

                    if self.concepts.remove(*id).await.is_ok() {
                        deleted_concepts += 1;
                    }
                }
                EntityID::Proposition(_, _) => {
                    // ignore
                }
            }
        }

        Ok(json!({
            "deleted_propositions": deleted_propositions,
            "deleted_concepts": deleted_concepts
        }))
    }

    async fn upsert_concept(
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

    async fn upsert_proposition(
        &self,
        pk: PropositionPK,
        attributes: Map<String, Json>,
        metadata: Map<String, Json>,
        cached_pks: &mut FxHashMap<EntityPK, EntityID>,
    ) -> Result<EntityID, KipError> {
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

    async fn update_concept(
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
        let mut update_fields: BTreeMap<String, Fv> = BTreeMap::new();
        if !attributes.is_empty() {
            let mut fv = concept.attributes;
            fv.extend(attributes);
            update_fields.insert("attributes".to_string(), fv.into());
        }
        if !metadata.is_empty() {
            let mut fv = concept.metadata;
            fv.extend(metadata);
            update_fields.insert("metadata".to_string(), fv.into());
        }
        self.concepts
            .update(id, update_fields)
            .await
            .map_err(db_to_kip_error)?;

        Ok(())
    }

    async fn update_proposition(
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
        if predicates.insert(predicate.clone()) {
            update_fields.insert("predicates".to_string(), predicates.into());
        }

        if !attributes.is_empty() || !metadata.is_empty() {
            let mut properties = proposition.properties;
            let prop = properties.entry(predicate).or_default();
            prop.attributes.extend(attributes);
            prop.metadata.extend(metadata);

            update_fields.insert("properties".to_string(), properties.into());
        }

        self.propositions
            .update(id, update_fields)
            .await
            .map_err(db_to_kip_error)?;

        Ok(())
    }

    async fn find_propositions(
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

    async fn query_concept_id(&self, ty: &str, name: &str) -> Result<u64, KipError> {
        let virtual_name = virtual_field_name(&["type", "name"]);
        let virtual_val = virtual_field_value(&[
            Some(&Fv::Text(ty.to_string())),
            Some(&Fv::Text(name.to_string())),
        ])
        .unwrap();

        let mut ids = self
            .concepts
            .query_ids(
                Filter::Field((virtual_name, RangeQuery::Eq(virtual_val))),
                None,
            )
            .await
            .map_err(db_to_kip_error)?;
        ids.pop().ok_or(KipError::not_found(format!(
            "Concept {} not found",
            ConceptPK::Object {
                r#type: ty.to_string(),
                name: name.to_string()
            }
        )))
    }

    async fn query_concept_ids(&self, matcher: &ConceptMatcher) -> Result<Vec<u64>, KipError> {
        match matcher {
            ConceptMatcher::ID(id) => {
                let entity_id = EntityID::from_str(id).map_err(KipError::invalid_syntax)?;
                if let EntityID::Concept(concept_id) = entity_id {
                    Ok(vec![concept_id])
                } else {
                    Err(KipError::invalid_syntax(format!(
                        "Invalid concept node ID: {}",
                        id
                    )))
                }
            }
            ConceptMatcher::Type(type_name) => {
                let ids = self
                    .concepts
                    .query_ids(
                        Filter::Field((
                            "type".to_string(),
                            RangeQuery::Eq(Fv::Text(type_name.clone())),
                        )),
                        None,
                    )
                    .await
                    .map_err(db_to_kip_error)?;
                Ok(ids)
            }
            ConceptMatcher::Name(name) => {
                let ids = self
                    .concepts
                    .query_ids(
                        Filter::Field(("name".to_string(), RangeQuery::Eq(Fv::Text(name.clone())))),
                        None,
                    )
                    .await
                    .map_err(db_to_kip_error)?;
                Ok(ids)
            }
            ConceptMatcher::Object { r#type, name } => {
                let id = self.query_concept_id(r#type, name).await?;
                Ok(vec![id])
            }
        }
    }

    async fn evaluate_filter_operand(
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
        }
    }

    async fn match_propositions(
        &self,
        ctx: &mut QueryContext,
        subject: TargetTerm,
        predicate: PredTerm,
        object: TargetTerm,
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

        if let Some(var) = subject_var {
            ctx.entities.insert(var, result.matched_subjects);
        }
        if let Some(var) = predicate_var {
            ctx.predicates.insert(var, result.matched_predicates);
        }
        if let Some(var) = object_var {
            ctx.entities.insert(var, result.matched_objects);
        }

        Ok(TargetEntities::IDs(result.matched_propositions.into()))
    }

    #[allow(clippy::too_many_arguments)]
    async fn resolve_result(
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
        let has_order_by = order_by.iter().any(|v| v.variable.var == var);
        for eid in ids {
            if !has_order_by && cursor.map(|v| v <= eid).unwrap_or(false) {
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

    // 解析目标项为实体ID列表
    async fn resolve_target_term_ids(
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
                let ids = self.query_concept_ids(&concept_matcher).await?;
                Ok(TargetEntities::IDs(
                    ids.into_iter().map(EntityID::Concept).collect(),
                ))
            }
            TargetTerm::Proposition(proposition_matcher) => {
                match *proposition_matcher {
                    PropositionMatcher::ID(id) => {
                        let entity_id =
                            EntityID::from_str(&id).map_err(KipError::invalid_syntax)?;
                        if !matches!(entity_id, EntityID::Proposition(_, _)) {
                            return Err(KipError::invalid_syntax(format!(
                                "Invalid proposition link ID: {id:?}"
                            )));
                        }
                        Ok(TargetEntities::IDs(vec![entity_id]))
                    }
                    PropositionMatcher::Object {
                        subject: TargetTerm::Variable(_),
                        predicate: PredTerm::Variable(_),
                        object: TargetTerm::Variable(_),
                    } => Ok(TargetEntities::AnyPropositions),
                    PropositionMatcher::Object {
                        subject,
                        predicate,
                        object,
                    } => {
                        // 递归查询命题
                        let result =
                            Box::pin(self.match_propositions(ctx, subject, predicate, object))
                                .await?;
                        Ok(result)
                    }
                }
            }
        }
    }

    async fn consume_bindings(
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
                    KipError::reference_error(format!("Unbound variable1: {:?}", dot_path.var))
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

    async fn try_get_concept_with<F, R>(
        &self,
        cache: &QueryCache,
        id: u64,
        f: F,
    ) -> Result<R, KipError>
    where
        F: FnOnce(&Concept) -> Result<R, KipError>,
    {
        if let Some(concept) = cache.concepts.read().get(&id) {
            return f(concept);
        }
        let concept: Concept = self.concepts.get_as(id).await.map_err(db_to_kip_error)?;
        let rt = f(&concept)?;
        cache.concepts.write().insert(id, concept);
        Ok(rt)
    }

    async fn try_get_proposition_with<F, R>(
        &self,
        cache: &QueryCache,
        id: u64,
        f: F,
    ) -> Result<R, KipError>
    where
        F: FnOnce(&Proposition) -> Result<R, KipError>,
    {
        if let Some(proposition) = cache.propositions.read().get(&id) {
            return f(proposition);
        }
        let proposition: Proposition = self
            .propositions
            .get_as(id)
            .await
            .map_err(db_to_kip_error)?;
        let rt = f(&proposition)?;
        cache.propositions.write().insert(id, proposition);
        Ok(rt)
    }

    async fn evaluate_filter_expression(
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

    async fn evaluate_filter_function(
        &self,
        ctx: &mut QueryContext,
        func: FilterFunction,
        mut args: Vec<FilterOperand>,
        bindings_snapshot: &mut FxHashMap<String, Vec<EntityID>>,
        bindings_cursor: &mut FxHashMap<String, EntityID>,
    ) -> Result<Option<bool>, KipError> {
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
                // 简单的正则表达式匹配
                let rt = regex::Regex::new(pattern)
                    .map_err(|e| KipError::invalid_syntax(format!("Invalid regex: {e:?}")))?
                    .is_match(string);
                Ok(Some(rt))
            }
        }
    }

    fn check_target_term_for_kml(
        &self,
        target: &TargetTerm,
        handle_map: &FxHashMap<String, EntityID>,
    ) -> Result<(), KipError> {
        match target {
            TargetTerm::Variable(handle) => {
                if !handle_map.contains_key(handle) {
                    return Err(KipError::reference_error(format!(
                        "Undefined handle: {handle}"
                    )));
                }
            }
            TargetTerm::Concept(concept_matcher) => {
                let _ = ConceptPK::try_from(concept_matcher.clone())?;
            }
            TargetTerm::Proposition(proposition_matcher) => {
                let _ = PropositionPK::try_from(*proposition_matcher.clone())?;
            }
        }

        Ok(())
    }

    async fn resolve_target_term(
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
                let proposition_pk = PropositionPK::try_from(*proposition_matcher)?;
                self.resolve_entity_id(&EntityPK::Proposition(proposition_pk), cached_pks)
                    .await
            }
        }
    }

    // Helper method to resolve EntityPK to EntityID
    async fn resolve_entity_id(
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
                ConceptPK::ID(id) => Ok(EntityID::Concept(*id)),
                ConceptPK::Object { r#type, name } => {
                    let id = self.query_concept_id(r#type, name).await?;
                    Ok(EntityID::Concept(id))
                }
            },
            EntityPK::Proposition(proposition_pk) => match proposition_pk {
                PropositionPK::ID(id, predicate) => {
                    Ok(EntityID::Proposition(*id, predicate.clone()))
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
                        Ok(EntityID::Proposition(*id, predicate.clone()))
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use anda_db::{
        database::{AndaDB, DBConfig},
        storage::StorageConfig,
    };
    use object_store::memory::InMemory;
    use std::sync::Arc;

    async fn setup_test_db<F>(f: F) -> Result<CognitiveNexus, KipError>
    where
        F: AsyncFnOnce(&CognitiveNexus) -> Result<(), KipError>,
    {
        let object_store = Arc::new(InMemory::new());

        let db_config = DBConfig {
            name: "test_anda".to_string(),
            description: "Test Anda Cognitive Nexus".to_string(),
            storage: StorageConfig {
                compress_level: 0,
                ..Default::default()
            },
            lock: None, // no lock for demo
        };

        let db = AndaDB::connect(object_store, db_config)
            .await
            .map_err(db_to_kip_error)?;
        let nexus = CognitiveNexus::connect(Arc::new(db), f).await?;
        Ok(nexus)
    }

    async fn setup_test_data(nexus: &CognitiveNexus) -> Result<(), KipError> {
        // 创建基础概念类型
        let drug_type_kml = r#"
        UPSERT {
            CONCEPT ?drug_type {
                {type: "$ConceptType", name: "Drug"}
                SET ATTRIBUTES {
                    "description": "Pharmaceutical drug concept type"
                }
            }
            WITH METADATA {
                "source": "test_setup",
                "confidence": 1.0
            }
        }
        "#;
        nexus.execute_kml(parse_kml(drug_type_kml)?, false).await?;

        let symptom_type_kml = r#"
        UPSERT {
            CONCEPT ?symptom_type {
                {type: "$ConceptType", name: "Symptom"}
                SET ATTRIBUTES {
                    "description": "Medical symptom concept type"
                }
            }
            WITH METADATA {
                "source": "test_setup",
                "confidence": 1.0
            }
        }
        "#;
        nexus
            .execute_kml(parse_kml(symptom_type_kml)?, false)
            .await?;

        // 创建谓词类型
        let treats_pred_kml = r#"
        UPSERT {
            CONCEPT ?treats_pred {
                {type: "$PropositionType", name: "treats"}
                SET ATTRIBUTES {
                    "description": "Treatment relationship"
                }
            }
            WITH METADATA {
                "source": "test_setup",
                "confidence": 1.0
            }
        }
        "#;
        nexus
            .execute_kml(parse_kml(treats_pred_kml)?, false)
            .await?;

        let headache_kml = r#"
        UPSERT {
            CONCEPT ?headache {
                {type: "Symptom", name: "Headache"}
                SET ATTRIBUTES {
                    "severity": "moderate",
                    "duration": "2-4 hours"
                }
            }
            WITH METADATA {
                "source": "test_data",
                "confidence": 1.0
            }
        }
        "#;
        nexus.execute_kml(parse_kml(headache_kml)?, false).await?;

        let fever_kml = r#"
        UPSERT {
            CONCEPT ?fever {
                {type: "Symptom", name: "Fever"}
                SET ATTRIBUTES {
                    "temperature_range": "38-40°C",
                    "common": true
                }
            }
            WITH METADATA {
                "source": "test_data",
                "confidence": 0.9
            }
        }
        "#;
        nexus.execute_kml(parse_kml(fever_kml)?, false).await?;

        // 创建测试概念
        let aspirin_kml = r#"
        UPSERT {
            CONCEPT ?aspirin {
                {type: "Drug", name: "Aspirin"}
                SET ATTRIBUTES {
                    "molecular_formula": "C9H8O4",
                    "risk_level": 2,
                    "dosage": "325mg"
                }
                SET PROPOSITIONS {
                    ("treats", {type: "Symptom", name: "Headache"})
                    ("treats", {type: "Symptom", name: "Fever"})
                }
            }
        }
        WITH METADATA {
            "source": "test_data",
            "confidence": 0.95
        }
        "#;
        nexus.execute_kml(parse_kml(aspirin_kml)?, false).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_cognitive_nexus_connect() {
        let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
        assert_eq!(nexus.name(), "test_anda");

        // 验证元类型已创建
        assert!(
            nexus
                .has_concept(&ConceptPK::Object {
                    r#type: META_CONCEPT_TYPE.to_string(),
                    name: META_CONCEPT_TYPE.to_string()
                })
                .await
        );

        assert!(
            nexus
                .has_concept(&ConceptPK::Object {
                    r#type: META_CONCEPT_TYPE.to_string(),
                    name: META_PROPOSITION_TYPE.to_string()
                })
                .await
        );
    }

    #[tokio::test]
    async fn test_kml_upsert_concept() {
        let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
        setup_test_data(&nexus).await.unwrap();

        // 验证概念已创建
        let aspirin = nexus
            .get_concept(&ConceptPK::Object {
                r#type: "Drug".to_string(),
                name: "Aspirin".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(aspirin.r#type, "Drug");
        assert_eq!(aspirin.name, "Aspirin");
        assert_eq!(
            aspirin
                .attributes
                .get("molecular_formula")
                .unwrap()
                .as_str()
                .unwrap(),
            "C9H8O4"
        );
        assert_eq!(
            aspirin
                .attributes
                .get("risk_level")
                .unwrap()
                .as_u64()
                .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn test_kql_find_concepts() {
        let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
        setup_test_data(&nexus).await.unwrap();

        // 测试基本概念查询
        let kql = r#"
        FIND(?drug.name, ?drug.attributes.risk_level)
        WHERE {
            ?drug {type: "Drug"}
        }
        "#;

        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        assert_eq!(result, json!([["Aspirin", 2]]));

        let kql = r#"
        FIND(?drug) // return concept object
        WHERE {
            ?drug {type: "Drug"}
        }
        "#;

        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        assert_eq!(
            result,
            json!([{
                "_type":"ConceptNode",
                "id":"C:13",
                "type":"Drug",
                "name":"Aspirin",
                "attributes":{"dosage":"325mg","molecular_formula":"C9H8O4","risk_level":2},
                "metadata":{"source":"test_data","confidence":0.95}
            }])
        );
    }

    #[tokio::test]
    async fn test_kql_proposition_matching() {
        let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
        setup_test_data(&nexus).await.unwrap();

        // 测试命题匹配
        let kql = r#"
        FIND(?drug.name, ?symptom.name)
        WHERE {
            ?drug {type: "Drug"}
            ?symptom {type: "Symptom"}
            (?drug, "treats", ?symptom)
        }
        "#;

        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        assert_eq!(result, json!([["Aspirin"], ["Headache", "Fever"]]));

        let kql = r#"
        FIND(?drug.name, ?symptom.name)
        WHERE {
            ?drug {type: "Drug"}
            (?drug, "treats", ?symptom) // find symptom by proposition matching
        }
        "#;

        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        assert_eq!(result, json!([["Aspirin"], ["Headache", "Fever"]]));

        let kql = r#"
        FIND(?drug.name, ?symptom.name)
        WHERE {
            ?drug {type: "Drug"}
            ?symptom {type: "Symptom"}
            (?drug, "treats1", ?symptom) // when predicate not exists
        }
        "#;

        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        assert_eq!(result, json!([[], []]));
    }

    #[tokio::test]
    async fn test_kql_multi_hop_bidirectional_matching() {
        let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
        setup_test_data(&nexus).await.unwrap();

        // 创建多层级的测试数据用于多跳查询
        let multi_hop_data_kml = r#"
            UPSERT {
                // 创建新的概念类型
                CONCEPT ?category_type {
                    {type: "$ConceptType", name: "Category"}
                }
                CONCEPT ?person_type {
                    {type: "$ConceptType", name: "Person"}
                }

                // 创建新的谓词类型
                CONCEPT ?is_subclass_of_pred {
                    {type: "$PropositionType", name: "is_subclass_of"}
                }
                CONCEPT ?belongs_to_pred {
                    {type: "$PropositionType", name: "belongs_to"}
                }
                CONCEPT ?knows_pred {
                    {type: "$PropositionType", name: "knows"}
                }

                // 创建分类层次结构
                CONCEPT ?medicine {
                    {type: "Category", name: "Medicine"}
                }
                CONCEPT ?pain_reliever {
                    {type: "Category", name: "PainReliever"}
                    SET PROPOSITIONS {
                        ("is_subclass_of", {type: "Category", name: "Medicine"})
                    }
                }
                CONCEPT ?nsaid {
                    {type: "Category", name: "NSAID"}
                    SET PROPOSITIONS {
                        ("is_subclass_of", {type: "Category", name: "PainReliever"})
                    }
                }

                // 让阿司匹林属于NSAID类别
                CONCEPT ?aspirin_category {
                    {type: "Drug", name: "Aspirin"}
                    SET PROPOSITIONS {
                        ("belongs_to", {type: "Category", name: "NSAID"})
                    }
                }

                // 创建人员和关系网络
                CONCEPT ?alice {
                    {type: "Person", name: "Alice"}
                }
                CONCEPT ?bob {
                    {type: "Person", name: "Bob"}
                    SET PROPOSITIONS {
                        ("knows", {type: "Person", name: "Alice"})
                    }
                }
                CONCEPT ?charlie {
                    {type: "Person", name: "Charlie"}
                    SET PROPOSITIONS {
                        ("knows", {type: "Person", name: "Bob"})
                    }
                }
                CONCEPT ?david {
                    {type: "Person", name: "David"}
                    SET PROPOSITIONS {
                        ("knows", {type: "Person", name: "Charlie"})
                    }
                }
            }
        "#;
        nexus
            .execute_kml(parse_kml(multi_hop_data_kml).unwrap(), false)
            .await
            .unwrap();

        // 测试1: 正向多跳查询 - 查找阿司匹林的所有上级分类（1-3跳）
        let kql = r#"
            FIND(?drug.name, ?category.name, ?parent_category.name)
            WHERE {
                ?drug {type: "Drug", name: "Aspirin"}
                (?drug, "belongs_to", ?category)
                (?category, "is_subclass_of"{1,3}, ?parent_category)
            }
            "#;
        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        assert_eq!(
            result,
            json!([["Aspirin"], ["NSAID"], ["PainReliever", "Medicine"]])
        );

        // 测试2: 反向多跳查询 - 从Medicine分类查找所有下级药物（1-3跳）
        // 反向查询：从Medicine通过is_subclass_of关系找到药物
        let kql = r#"
            FIND(?category.name)
            WHERE {
                (?category, "is_subclass_of"{1,3}, {type: "Category", name: "Medicine"})
            }
            "#;
        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        assert_eq!(result, json!(["PainReliever", "NSAID"]));

        let kql = r#"
            FIND(?category.name, ?drug.name)
            WHERE {
                (?category, "is_subclass_of"{1,3}, {type: "Category", name: "Medicine"})
                (?drug, "belongs_to", ?category)
            }
            "#;
        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        assert_eq!(result, json!([["NSAID"], ["Aspirin"]]));

        // 测试3: 精确跳数查询 - 查找恰好2跳的关系
        let kql = r#"
            FIND(?drug.name, ?parent_category.name)
            WHERE {
                ?drug {type: "Drug", name: "Aspirin"}
                (?drug, "belongs_to", ?category)
                (?category, "is_subclass_of"{2}, ?parent_category)
            }
            "#;
        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        // 应该只找到PainReliever（2跳：Aspirin->NSAID, NSAID->PainReliever->Medicine）
        assert_eq!(result, json!([["Aspirin"], ["Medicine"]]));

        // 测试4: 人际关系网络的多跳查询
        let kql = r#"
            FIND(?person1.name, ?person2.name)
            WHERE {
                ?person1 {type: "Person", name: "David"}
                ?person2 {type: "Person", name: "Alice"}
                (?person1, "knows"{1,3}, ?person2)
            }
        "#;
        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        // David通过3跳关系认识Alice: David->Charlie->Bob->Alice
        assert_eq!(result, json!([["David"], ["Alice"]]));

        // 测试5: 反向人际关系查询
        let kql = r#"
            FIND(?person1.name, ?person2.name)
            WHERE {
                ?person1 {type: "Person", name: "Alice"}
                ?person2 {type: "Person", name: "David"}
                (?person1, "knows"{1,3}, ?person2)
            }
        "#;
        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        // 反向查询应该为空，因为knows关系是单向的
        assert_eq!(result, json!([[], []]));

        // 测试6: 边界条件 - 0跳查询（自身）
        let kql = r#"
            FIND(?drug.name)
            WHERE {
                ?drug {type: "Drug", name: "Aspirin"}
                (?drug, "belongs_to"{0}, ?drug)
            }
        "#;
        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        // 0跳应该匹配自身
        assert_eq!(result, json!(["Aspirin"]));

        // 测试7: 超出范围的查询
        let kql = r#"
            FIND(?drug.name, ?category.name)
            WHERE {
                ?drug {type: "Drug", name: "Aspirin"}
                (?drug, "belongs_to", ?category)
                (?category, "is_subclass_of"{1,}, ?o)
            }
        "#;
        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        assert_eq!(result, json!([["Aspirin"], ["NSAID"]]));

        let kql = r#"
            FIND(?drug.name, ?category.name)
            WHERE {
                ?drug {type: "Drug", name: "Aspirin"}
                (?drug, "belongs_to", ?category)
                (?category, "is_subclass_of"{5,10}, ?o)
            }
        "#;
        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        // 超出实际路径长度，应该为空
        assert_eq!(result, json!([["Aspirin"], []]));
    }

    #[tokio::test]
    async fn test_multi_hop_error_handling() {
        let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
        setup_test_data(&nexus).await.unwrap();

        // 测试错误情况：主语和宾语都是变量的多跳查询
        let kql = r#"
            FIND(?a.name, ?b.name)
            WHERE {
                (?a, "treats"{1,3}, ?b)
            }
            "#;
        let query = parse_kql(kql).unwrap();
        let result = nexus.execute_kql(query).await;
        // 应该返回错误，因为多跳查询要求主语或宾语至少有一个是具体的ID
        assert!(result.is_err());
        if let Err(err) = result {
            assert!(matches!(err.code, KipErrorCode::InvalidSyntax));
            assert!(
                err.message
                    .contains("cannot both be variables in multi-hop matching")
            );
        } else {
            panic!("Expected InvalidSyntax error");
        }
    }

    #[tokio::test]
    async fn test_kql_filter_clause() {
        let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
        setup_test_data(&nexus).await.unwrap();

        // 测试过滤器
        let kql = r#"
        FIND(?drug.name)
        WHERE {
            ?drug {type: "Drug"}
            FILTER(?drug.attributes.risk_level < 3)
        }
        "#;

        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        assert_eq!(result, json!(["Aspirin"]));

        let kql = r#"
        FIND(?drug.name)
        WHERE {
            ?drug {type: "Drug"}
            FILTER(?drug.attributes.risk_level < 1)
        }
        "#;

        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        assert_eq!(result, json!([]));
    }

    #[tokio::test]
    async fn test_kql_aggregation() {
        let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
        setup_test_data(&nexus).await.unwrap();

        // 测试聚合函数
        let kql = r#"
        FIND(COUNT(?drug))
        WHERE {
            ?drug {type: "Drug"}
        }
        "#;

        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        assert_eq!(result, json!(1));

        let kql = r#"
        FIND(COUNT(?drug), COUNT(DISTINCT ?symptom))
        WHERE {
            ?drug {type: "Drug"}
            ?symptom {type: "Symptom"}
        }
        "#;

        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        assert_eq!(result, json!([1, 2]));
    }

    #[tokio::test]
    async fn test_kql_optional_clause() {
        let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
        setup_test_data(&nexus).await.unwrap();

        // 测试可选子句
        let kql = r#"
        FIND(?symptom.name, ?drug.name)
        WHERE {
            ?symptom {type: "Symptom"}
            OPTIONAL {
                (?drug, "treats", ?symptom)
            }
        }
        "#;

        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        assert_eq!(result, json!([["Headache", "Fever"], ["Aspirin"]]));

        let kql = r#"
        FIND(?symptom.name, ?drug.name)
        WHERE {
            ?symptom {type: "Symptom"}
            OPTIONAL {
                (?drug, "treats1", ?symptom)
            }
        }
        "#;

        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        assert_eq!(result, json!([["Headache", "Fever"], []]));

        let kql = r#"
        FIND(?symptom.name, ?drug.name)
        WHERE {
            ?symptom {type: "Symptom"}
            (?drug, "treats1", ?symptom)  // when predicate not exists
        }
        "#;

        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        assert_eq!(result, json!([[], []]));
    }

    #[tokio::test]
    async fn test_kql_not_clause() {
        let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
        setup_test_data(&nexus).await.unwrap();

        // 添加另一个药物用于测试
        let ibuprofen_kml = r#"
        UPSERT {
            CONCEPT ?ibuprofen {
                {type: "Drug", name: "Ibuprofen"}
                SET ATTRIBUTES {
                    "risk_level": 4
                }
            }
        }
        "#;
        nexus
            .execute_kml(parse_kml(ibuprofen_kml).unwrap(), false)
            .await
            .unwrap();

        // 测试NOT子句
        let kql = r#"
        FIND(?drug.name)
        WHERE {
            ?drug {type: "Drug"}
            NOT {
                FILTER(?drug.attributes.risk_level > 3)
            }
        }
        "#;

        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        assert_eq!(result, json!(["Aspirin".to_string()]));

        // 测试NOT子句
        let kql = r#"
        FIND(?drug.name)
        WHERE {
            ?drug {type: "Drug"}
            NOT {
                FILTER(?drug.attributes.risk_level > 4)
            }
        }
        "#;

        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();

        assert_eq!(
            result,
            json!(["Aspirin".to_string(), "Ibuprofen".to_string()])
        );
    }

    #[tokio::test]
    async fn test_kql_not_clause_fast_path_orphan_concepts() {
        let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
        setup_test_data(&nexus).await.unwrap();

        // 设置测试数据：创建一个 Domain 和一些概念，部分概念有 belongs_to_domain 关系
        let setup_kml = r#"
        UPSERT {
            CONCEPT ?domain {
                {type: "Domain", name: "TestDomain"}
                SET ATTRIBUTES {
                    "description": "Test domain for orphan detection"
                }
            }
            CONCEPT ?belongs_to_domain_type {
                {type: "$PropositionType", name: "belongs_to_domain"}
            }

            // Drug 类型中，只有 Aspirin 属于 TestDomain，其他不属于任何 domain
            CONCEPT ?aspirin_with_domain {
                {type: "Drug", name: "Aspirin"}
                SET PROPOSITIONS {
                    ("belongs_to_domain", {type: "Domain", name: "TestDomain"})
                }
            }

            // 创建一个孤儿药物（不属于任何 domain）
            CONCEPT ?orphan_drug {
                {type: "Drug", name: "OrphanDrug"}
                SET ATTRIBUTES {
                    "description": "A drug without domain"
                }
            }
        }
        "#;
        nexus
            .execute_kml(parse_kml(setup_kml).unwrap(), false)
            .await
            .unwrap();

        // 测试：查找没有 belongs_to_domain 关系的 Drug 概念（孤儿概念）
        // 这个查询应该使用快速路径优化
        let kql = r#"
        FIND(?n.name)
        WHERE {
            ?n {type: "Drug"}
            NOT {
                (?n, "belongs_to_domain", ?d)
            }
        }
        "#;

        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();

        // OrphanDrug 没有 belongs_to_domain 关系，应该被返回
        // Aspirin 有 belongs_to_domain 关系，不应该被返回
        assert_eq!(result, json!(["OrphanDrug".to_string()]));

        // 测试：查找没有 treats 关系的 Drug 概念
        let kql = r#"
        FIND(?n.name)
        WHERE {
            ?n {type: "Drug"}
            NOT {
                (?n, "treats", ?s)
            }
        }
        "#;

        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();

        // OrphanDrug 没有 treats 关系，应该被返回
        // Aspirin 有 treats 关系（treats Headache 和 Fever），不应该被返回
        assert_eq!(result, json!(["OrphanDrug".to_string()]));

        // 测试：查找没有任何关系的 Symptom 概念
        // Headache 和 Fever 都被 Aspirin treats，所以不会被返回
        let kql = r#"
        FIND(?n.name)
        WHERE {
            ?n {type: "Symptom"}
            NOT {
                (?d, "treats", ?n)
            }
        }
        "#;

        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();

        // 所有 Symptom 都被 treats，应该返回空
        assert_eq!(result, json!([]));
    }

    #[tokio::test]
    async fn test_kql_union_clause() {
        let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
        setup_test_data(&nexus).await.unwrap();

        // 测试UNION子句
        let kql = r#"
        FIND(?concept.name)
        WHERE {
            ?concept {type: "Drug"}
            ?concept {type: "Symptom"}
        }
        "#;

        let query = parse_kql(kql).unwrap();
        let res = nexus.execute_kql(query).await;
        assert!(res.is_err());
        assert!(matches!(res.unwrap_err().code, KipErrorCode::InvalidSyntax));

        // 测试UNION子句
        let kql = r#"
        FIND(?concept.name)
        WHERE {
            ?concept {type: "Drug"}
            UNION {
                ?concept {type: "Symptom"}
            }
        }
        "#;

        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        assert_eq!(
            result,
            json!([
                "Aspirin".to_string(),
                "Headache".to_string(),
                "Fever".to_string(),
            ])
        );
    }

    #[tokio::test]
    async fn test_kql_order_by_and_limit() {
        let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
        setup_test_data(&nexus).await.unwrap();

        // 添加更多药物用于测试排序
        let drugs_kml = r#"
        UPSERT {
            CONCEPT ?drug1 {
                {type: "Drug", name: "Ibuprofen"}
                SET ATTRIBUTES {
                    "risk_level": 3
                }
            }
            CONCEPT ?drug2 {
                {type: "Drug", name: "Acetaminophen"}
                SET ATTRIBUTES {
                    "risk_level": 1
                }
            }
        }
        "#;
        nexus
            .execute_kml(parse_kml(drugs_kml).unwrap(), false)
            .await
            .unwrap();

        // 测试排序和限制
        let kql = r#"
        FIND(?drug.name, ?drug.attributes.risk_level)
        WHERE {
            ?drug {type: "Drug"}
        }
        ORDER BY ?drug.attributes.risk_level ASC
        LIMIT 2
        "#;

        let query = parse_kql(kql).unwrap();
        let (result, cursor) = nexus.execute_kql(query).await.unwrap();
        assert!(cursor.is_some());
        assert_eq!(
            result,
            json!([["Acetaminophen".to_string(), 1], ["Aspirin".to_string(), 2]])
        );

        let kql = r#"
        FIND(?drug.name, ?drug.attributes.risk_level)
        WHERE {
            ?drug {type: "Drug"}
        }
        ORDER BY ?drug.attributes.risk_level ASC
        LIMIT 2 CURSOR "$cursor"
        "#;

        let query = parse_kql(&kql.replace("$cursor", cursor.unwrap().as_str())).unwrap();
        let (result, cursor) = nexus.execute_kql(query).await.unwrap();
        assert!(cursor.is_none());
        assert_eq!(result, json!([["Ibuprofen".to_string(), 3]]));

        let kql = r#"
        FIND(?drug.name, ?drug.attributes.risk_level)
        WHERE {
            ?drug {type: "Drug"}
        }
        ORDER BY ?drug.attributes.risk_level DESC
        LIMIT 2
        "#;

        let query = parse_kql(kql).unwrap();
        let (result, cursor) = nexus.execute_kql(query).await.unwrap();
        assert!(cursor.is_some());
        assert_eq!(
            result,
            json!([["Ibuprofen".to_string(), 3], ["Aspirin".to_string(), 2]])
        );

        let kql = r#"
        FIND(?drug.name, ?drug.attributes.risk_level)
        WHERE {
            ?drug {type: "Drug"}
        }
        ORDER BY ?drug.attributes.risk_level DESC
        LIMIT 2
        CURSOR "$cursor"
        "#;

        let query = parse_kql(&kql.replace("$cursor", cursor.unwrap().as_str())).unwrap();
        let (result, cursor) = nexus.execute_kql(query).await.unwrap();
        assert!(cursor.is_none());
        assert_eq!(result, json!([["Acetaminophen".to_string(), 1]]));
    }

    #[tokio::test]
    async fn test_kml_upsert_proposition() {
        let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
        setup_test_data(&nexus).await.unwrap();

        let kql = r#"
        FIND(?link, ?drug.name, ?symptom.name)
        WHERE {
            ?link (?drug, "treats", ?symptom)
        }
        "#;

        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        let result = result.as_array().unwrap();
        assert_eq!(
            json!(result[1..]),
            json!([
                ["Aspirin".to_string()],
                ["Headache".to_string(), "Fever".to_string()]
            ])
        );
        let props: Vec<PropositionLink> = serde_json::from_value(result[0].clone()).unwrap();
        // println!("{:#?}", props);
        assert_eq!(props.len(), 2);
        assert!(props[0].attributes.is_empty());
        assert!(props[1].attributes.is_empty());
        assert_eq!(
            json!(props[0].metadata),
            json!({
                "source": "test_data",
                "confidence": 0.95
            })
        );
        assert_eq!(
            json!(props[1].metadata),
            json!({
                "source": "test_data",
                "confidence": 0.95
            })
        );

        // 测试独立命题创建
        let prop_kml = r#"
        UPSERT {
            PROPOSITION ?treatment {
                ({type: "Drug", name: "Aspirin"}, "treats", {type: "Symptom", name: "Headache"})
                SET ATTRIBUTES {
                    "effectiveness": 0.85,
                    "onset_time": "30 minutes"
                }
            }
            WITH METADATA {
                "source": "clinical_trial",
                "study_id": "CT-2024-001"
            }
        }
        "#;

        let result = nexus
            .execute_kml(parse_kml(prop_kml).unwrap(), false)
            .await
            .unwrap();
        let result: UpsertResult = serde_json::from_value(result).unwrap();
        assert_eq!(result.blocks, 1);
        assert!(result.upsert_concept_nodes.is_empty());
        assert_eq!(result.upsert_proposition_links.len(), 1);

        let kql = r#"
        FIND(?link)
        WHERE {
            ?link (?drug, "treats", ?symptom)
        }
        "#;

        let query = parse_kql(kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        let props: Vec<PropositionLink> = serde_json::from_value(result).unwrap();
        // println!("{:#?}", props);
        assert_eq!(props.len(), 2);
        assert_eq!(
            json!(props[0].attributes),
            json!({
                "effectiveness": 0.85,
                "onset_time": "30 minutes"
            })
        );
        assert_eq!(
            json!(props[0].metadata),
            json!({
                "source": "clinical_trial",
                "confidence": 0.95,
                "study_id": "CT-2024-001"
            })
        );
    }

    #[tokio::test]
    async fn test_kml_dry_run() {
        let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
        setup_test_data(&nexus).await.unwrap();

        let test_kml = r#"
        UPSERT {
            CONCEPT ?test_drug {
                {type: "Drug", name: "TestDrug"}
                SET ATTRIBUTES {
                    "test": true
                }
            }
        }
        "#;

        // 干运行不应该实际创建概念
        let result = nexus
            .execute_kml(parse_kml(test_kml).unwrap(), true)
            .await
            .unwrap();
        let result: UpsertResult = serde_json::from_value(result).unwrap();
        assert_eq!(result.blocks, 1);
        assert!(result.upsert_concept_nodes.is_empty());
        assert_eq!(result.upsert_proposition_links.len(), 0);

        // 验证概念没有被创建
        assert!(
            !nexus
                .has_concept(&ConceptPK::Object {
                    r#type: "Drug".to_string(),
                    name: "TestDrug".to_string(),
                })
                .await
        );
    }

    #[tokio::test]
    async fn test_meta_describe_primer() {
        let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
        setup_test_data(&nexus).await.unwrap();

        let meta_cmd = MetaCommand::Describe(DescribeTarget::Primer);
        let result = nexus.execute_meta(meta_cmd).await;
        assert!(result.is_err());
        assert!(matches!(
            result.as_ref().unwrap_err().code,
            KipErrorCode::NotFound
        ));
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains(r#"{type: "Person", name: "$self"}"#)
        );

        let kml = PERSON_SELF_KIP.replace(
            "$self_reserved_principal_id",
            "gcxml-rtxjo-ib7ov-5si5r-5jluv-zek7y-hvody-nneuz-hcg5i-6notx-aae",
        );

        let result = nexus
            .execute_kml(parse_kml(&kml).unwrap(), false)
            .await
            .unwrap();
        assert!(result.is_object());

        let (result, _) = nexus
            .execute_meta(parse_meta("DESCRIBE PRIMER").unwrap())
            .await
            .unwrap();
        assert!(result.is_object());

        let primer = result.as_object().unwrap();
        assert!(primer.contains_key("identity"));
        assert!(primer.contains_key("domain_map"));
    }

    #[tokio::test]
    async fn test_meta_describe_domains() {
        let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
        setup_test_data(&nexus).await.unwrap();

        let (result, _) = nexus
            .execute_meta(parse_meta("DESCRIBE DOMAINS").unwrap())
            .await
            .unwrap();
        let domains: Vec<ConceptNode> = serde_json::from_value(result).unwrap();
        // println!("{:#?}", domains);
        assert_eq!(domains.len(), 1);
        assert_eq!(domains[0].r#type, "Domain");
        assert_eq!(domains[0].name, "CoreSchema");
    }

    #[tokio::test]
    async fn test_meta_describe_concept_types() {
        let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
        setup_test_data(&nexus).await.unwrap();

        let (result, _) = nexus
            .execute_meta(parse_meta("DESCRIBE CONCEPT TYPES").unwrap())
            .await
            .unwrap();

        assert_eq!(
            result,
            json!([
                "$ConceptType",
                "$PropositionType",
                "Domain",
                "Drug",
                "Symptom",
            ])
        );

        let (result, _) = nexus
            .execute_meta(parse_meta("DESCRIBE CONCEPT TYPE \"Drug\"").unwrap())
            .await
            .unwrap();
        let concept: ConceptNode = serde_json::from_value(result).unwrap();
        assert_eq!(concept.r#type, "$ConceptType");
        assert_eq!(concept.name, "Drug");

        let res = nexus
            .execute_meta(parse_meta("DESCRIBE CONCEPT TYPE \"drug\"").unwrap())
            .await;
        assert!(res.is_err());
        assert!(matches!(res.unwrap_err().code, KipErrorCode::NotFound));
    }

    #[tokio::test]
    async fn test_meta_describe_proposition_types() {
        let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
        setup_test_data(&nexus).await.unwrap();

        let (result, _) = nexus
            .execute_meta(parse_meta("DESCRIBE PROPOSITION TYPES").unwrap())
            .await
            .unwrap();

        // println!("{:#?}", result);
        assert_eq!(result, json!(["belongs_to_domain", "treats",]));

        let (result, _) = nexus
            .execute_meta(parse_meta("DESCRIBE PROPOSITION TYPE \"belongs_to_domain\"").unwrap())
            .await
            .unwrap();
        let concept: ConceptNode = serde_json::from_value(result).unwrap();
        assert_eq!(concept.r#type, "$PropositionType");
        assert_eq!(concept.name, "belongs_to_domain");

        let res = nexus
            .execute_meta(parse_meta("DESCRIBE PROPOSITION TYPE \"treats1\"").unwrap())
            .await;
        assert!(res.is_err());
        assert!(matches!(res.unwrap_err().code, KipErrorCode::NotFound));
    }

    #[tokio::test]
    async fn test_meta_search() {
        let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
        setup_test_data(&nexus).await.unwrap();

        let (result, _) = nexus
            .execute_meta(parse_meta(r#"SEARCH CONCEPT "aspirin""#).unwrap())
            .await
            .unwrap();
        let result: Vec<ConceptNode> = serde_json::from_value(result).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "Aspirin");

        let (result, _) = nexus
            .execute_meta(parse_meta(r#"SEARCH CONCEPT "C9H8O4""#).unwrap())
            .await
            .unwrap();
        let result: Vec<ConceptNode> = serde_json::from_value(result).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "Aspirin");

        let (result, _) = nexus
            .execute_meta(parse_meta(r#"SEARCH CONCEPT "test_data""#).unwrap())
            .await
            .unwrap();
        let result: Vec<ConceptNode> = serde_json::from_value(result).unwrap();
        assert_eq!(result.len(), 10);

        let (result, _) = nexus
            .execute_meta(parse_meta(r#"SEARCH CONCEPT "test_data" LIMIT 5"#).unwrap())
            .await
            .unwrap();
        let result: Vec<ConceptNode> = serde_json::from_value(result).unwrap();
        assert_eq!(result.len(), 5);

        let (result, _) = nexus
            .execute_meta(
                parse_meta(r#"SEARCH CONCEPT "test_data" WITH TYPE "$PropositionType""#).unwrap(),
            )
            .await
            .unwrap();
        let result: Vec<ConceptNode> = serde_json::from_value(result).unwrap();
        assert_eq!(result.len(), 2);

        let (result, _) = nexus
            .execute_meta(parse_meta(r#"SEARCH PROPOSITION "test_data""#).unwrap())
            .await
            .unwrap();
        let result: Vec<PropositionLink> = serde_json::from_value(result).unwrap();
        assert_eq!(result.len(), 8);

        let (result, _) = nexus
            .execute_meta(parse_meta(r#"SEARCH PROPOSITION "test_data" LIMIT 5"#).unwrap())
            .await
            .unwrap();
        let result: Vec<PropositionLink> = serde_json::from_value(result).unwrap();
        assert_eq!(result.len(), 5);

        let (result, _) = nexus
            .execute_meta(
                parse_meta(r#"SEARCH PROPOSITION "test_data" WITH TYPE "treats""#).unwrap(),
            )
            .await
            .unwrap();
        let result: Vec<PropositionLink> = serde_json::from_value(result).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn test_error_handling() {
        let nexus = setup_test_db(async |_| Ok(())).await.unwrap();

        // 测试查询不存在的概念
        let result = nexus
            .get_concept(&ConceptPK::Object {
                r#type: "NonExistent".to_string(),
                name: "Test".to_string(),
            })
            .await;
        assert!(result.is_err());

        // 测试无效的KQL
        let invalid_kql = r#"
        FIND(?invalid)
        WHERE {
            ?invalid {invalid_field: "test"}
        }
        "#;

        let parse_result = parse_kql(invalid_kql);
        assert!(parse_result.is_err());
    }

    #[tokio::test]
    async fn test_complex_query_scenario() {
        let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
        setup_test_data(&nexus).await.unwrap();

        // 创建更复杂的测试数据
        let complex_data_kml = r#"
        UPSERT {
            CONCEPT ?drug_class_type {
                {type: "$ConceptType", name: "DrugClass"}
            }
            CONCEPT ?belongs_to_pred {
                {type: "$PropositionType", name: "belongs_to_class"}
            }
            CONCEPT ?nsaid_class {
                {type: "DrugClass", name: "NSAID"}
                SET ATTRIBUTES {
                    "description": "Non-steroidal anti-inflammatory drugs"
                }
            }
            PROPOSITION ?aspirin_nsaid {
                ({type: "Drug", name: "Aspirin"}, "belongs_to_class", {type: "DrugClass", name: "NSAID"})
                SET ATTRIBUTES {
                    "classification_confidence": 0.99
                }
            }
        }
        "#;
        nexus
            .execute_kml(parse_kml(complex_data_kml).unwrap(), false)
            .await
            .unwrap();

        // 复杂查询：找到所有NSAID类药物及其治疗的症状
        let complex_kql = r#"
        FIND(?drug.name, ?symptom.name, ?treatment.metadata)
        WHERE {
            ?drug {type: "Drug"}
            ?nsaid_class {type: "DrugClass", name: "NSAID"}
            ?symptom {type: "Symptom"}

            (?drug, "belongs_to_class", ?nsaid_class)
            ?treatment (?drug, "treats", ?symptom)

            FILTER(?drug.attributes.risk_level <= 3)
        }
        ORDER BY ?drug.name ASC
        "#;

        let query = parse_kql(complex_kql).unwrap();
        let (result, _) = nexus.execute_kql(query).await.unwrap();
        // println!("{:#?}", result);
        let result = result.as_array().unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], json!(["Aspirin".to_string()]));
        assert_eq!(
            result[1],
            json!(["Headache".to_string(), "Fever".to_string()])
        );
    }

    #[tokio::test]
    async fn test_concurrent_operations() {
        let nexus = Arc::new(setup_test_db(async |_| Ok(())).await.unwrap());
        setup_test_data(&nexus).await.unwrap();

        // 测试并发查询
        let nexus1 = nexus.clone();
        let nexus2 = nexus.clone();

        let task1 = tokio::spawn(async move {
            let kql = r#"
            FIND(?drug.name)
            WHERE {
                ?drug {type: "Drug"}
            }
            "#;
            nexus1.execute_kql(parse_kql(kql).unwrap()).await
        });

        let task2 = tokio::spawn(async move {
            let kql = r#"
            FIND(?symptom.name)
            WHERE {
                ?symptom {type: "Symptom"}
            }
            "#;
            nexus2.execute_kql(parse_kql(kql).unwrap()).await
        });

        let (result1, result2) = tokio::try_join!(task1, task2).unwrap();
        assert!(result1.is_ok());
        assert!(result2.is_ok());
    }
}
