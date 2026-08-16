//! # The storage layer
//!
//! Nine `anda_db` collections: one per Core element kind (Spec §6.1), plus the
//! MemorySpace registry, the transaction journal, and the Schema Package and
//! Schema Environment registries.
//!
//! ## Why the element kinds do not share a collection
//!
//! They have genuinely different columns and genuinely different hot paths. An
//! Epistemic Projection starts by fetching every Assertion about one
//! Proposition; a grounding SEARCH looks only at Concept names. Merging them
//! would put both behind the same index and make every scan pay for the kinds
//! it is not reading.
//!
//! ## Why every index here is single-field
//!
//! An `anda_db` composite B-Tree index is a **unique** key: the virtual field
//! it is built on is created `with_unique()`. So a composite index is a
//! uniqueness constraint that happens to also answer queries, and declaring
//! one over `(space, state)` would assert that a Space contains at most one
//! active element.
//!
//! Only three combinations in this schema really are unique — a Proposition's
//! `tuple_key`, a Space's `space_id`, a transaction's `tx_id` — and each is
//! already one column, marked `#[unique]`. Everything else is indexed per
//! column and intersected with [`Filter::And`] at query time, which costs an
//! intersection and buys the ability to have two active elements.
//!
//! ## Why the handles live in swappable slots
//!
//! A cancelled mutating future — or any failed flush — poisons an `anda_db`
//! collection handle, and a poisoned handle rejects every later mutation.
//! Recovery is [`AndaDB::open_collection`], which reloads from storage; a
//! handle captured once at startup could never reach it, so the process would
//! stay bricked until restart. Each handle therefore lives behind a slot that
//! [`Store::reopen`] can replace, and every mutating entry point checks
//! [`Store::has_poisoned_handle`] first.

pub mod rows;
pub mod schema;
pub mod space;
pub mod write;

use anda_db::{
    collection::{Collection, CollectionConfig},
    database::AndaDB,
    error::DBError,
    query::{Filter, RangeQuery},
};
use anda_db_schema::Fv;
use anda_db_tfs::jieba_tokenizer;
use anda_kip::{ElementKind, KipError};
use std::sync::Arc;

use crate::error::{db_error, reopen_error, schema_error};
use crate::id::ElementId;
use rows::*;

/// The collection names, in one place so a rename cannot half-happen.
pub const CONCEPTS: &str = "concepts";
/// The Proposition collection name.
pub const PROPOSITIONS: &str = "propositions";
/// The Assertion collection name.
pub const ASSERTIONS: &str = "assertions";
/// The Evidence collection name.
pub const EVIDENCE: &str = "evidence";
/// The Activity collection name.
pub const ACTIVITIES: &str = "activities";
/// The MemorySpace registry collection name.
pub const SPACES: &str = "spaces";
/// The transaction journal collection name.
pub const TRANSACTIONS: &str = "transactions";
/// The installed Schema Package collection name.
pub const SCHEMA_PACKAGES: &str = "schema_packages";
/// The Schema Environment version collection name.
pub const SCHEMA_ENVS: &str = "schema_envs";

/// A collection handle that survives poisoning.
#[derive(Clone, Debug)]
struct Slot(Arc<parking_lot::RwLock<Arc<Collection>>>);

impl Slot {
    fn new(collection: Arc<Collection>) -> Self {
        Self(Arc::new(parking_lot::RwLock::new(collection)))
    }

    fn get(&self) -> Arc<Collection> {
        self.0.read().clone()
    }

    fn set(&self, collection: Arc<Collection>) {
        *self.0.write() = collection;
    }
}

/// The persistent home of one Cognitive Nexus.
#[derive(Clone, Debug)]
pub struct Store {
    /// The underlying database, shared with whatever else the host registered.
    pub db: Arc<AndaDB>,
    concepts: Slot,
    propositions: Slot,
    assertions: Slot,
    evidence: Slot,
    activities: Slot,
    spaces: Slot,
    transactions: Slot,
    schema_packages: Slot,
    schema_envs: Slot,
}

/// The columns every element kind is indexed on.
///
/// `space` and `state` are the two predicates almost every query carries, and
/// `seq` and `expires_at` are what the `CHANGES` cursor and the retention
/// sweep range over.
///
/// The per-kind setups below are named functions rather than closures inlined
/// at the open site because every *re*-open must run exactly the same setup:
/// `create_*_nx` is a no-op once the index exists, but a freshly loaded handle
/// starts with the default tokenizer and needs the jieba chain reinstalled.
async fn init_envelope(c: &mut Collection) -> Result<(), DBError> {
    c.create_btree_index_nx(&["space"]).await?;
    c.create_btree_index_nx(&["state"]).await?;
    c.create_btree_index_nx(&["seq"]).await?;
    c.create_btree_index_nx(&["expires_at"]).await?;
    Ok(())
}

async fn init_concepts(c: &mut Collection) -> Result<(), DBError> {
    c.set_tokenizer(jieba_tokenizer());
    init_envelope(c).await?;
    // `key` is the logical identity `UPSERT ... MATCH {key: ...}` resolves. It
    // is Space-local, so the lookup intersects this with `space`; it cannot be
    // a unique composite because most Concepts carry no logical key at all.
    c.create_btree_index_nx(&["key"]).await?;
    c.create_btree_index_nx(&["client_key"]).await?;
    c.create_btree_index_nx(&["schema_ref"]).await?;
    c.create_btree_index_nx(&["name"]).await?;
    c.create_btree_index_nx(&["canonical_id"]).await?;
    c.create_btree_index_nx(&["merged_into"]).await?;
    // Grounding search: names and aliases are what a model has to work with
    // before it knows an id. Attributes join them because a Concept's
    // representation-local state is often the only text it carries.
    c.create_bm25_index_nx(&["name", "aliases", "attributes"])
        .await?;
    Ok(())
}

async fn init_propositions(c: &mut Collection) -> Result<(), DBError> {
    c.set_tokenizer(jieba_tokenizer());
    init_envelope(c).await?;
    // Declared `#[unique]` by the schema: this is the constraint that keeps
    // one canonical Proposition per semantic tuple in a Space (§93.6).
    c.create_btree_index_nx(&["tuple_key"]).await?;
    // Traversal, in both directions. A conflict set — same subject, same
    // predicate, competing objects (§58) — is the intersection of the first
    // and the third.
    c.create_btree_index_nx(&["subject_key"]).await?;
    c.create_btree_index_nx(&["object_key"]).await?;
    c.create_btree_index_nx(&["predicate_ref"]).await?;
    c.create_bm25_index_nx(&["predicate_ref", "attributes"])
        .await?;
    Ok(())
}

async fn init_assertions(c: &mut Collection) -> Result<(), DBError> {
    c.set_tokenizer(jieba_tokenizer());
    init_envelope(c).await?;
    // Projection's first move is always "every Assertion about this
    // Proposition", so this index is the one that has to be fast.
    c.create_btree_index_nx(&["proposition_id"]).await?;
    c.create_btree_index_nx(&["asserted_by_key"]).await?;
    c.create_btree_index_nx(&["client_key"]).await?;
    c.create_btree_index_nx(&["status"]).await?;
    c.create_btree_index_nx(&["mode"]).await?;
    c.create_btree_index_nx(&["stance"]).await?;
    c.create_btree_index_nx(&["evidence_ids"]).await?;
    c.create_btree_index_nx(&["superseded_by"]).await?;
    // Temporal eligibility ranges over these (§60). They are normalized UTC
    // text, so lexicographic range *is* chronological range.
    c.create_btree_index_nx(&["valid_from"]).await?;
    c.create_btree_index_nx(&["valid_until"]).await?;
    Ok(())
}

async fn init_evidence(c: &mut Collection) -> Result<(), DBError> {
    c.set_tokenizer(jieba_tokenizer());
    init_envelope(c).await?;
    c.create_btree_index_nx(&["client_key"]).await?;
    c.create_btree_index_nx(&["evidence_class"]).await?;
    // Indexed for lookup, never for identity: two independent observations of
    // the same bytes are two observations (§73).
    c.create_btree_index_nx(&["content_digest"]).await?;
    c.create_btree_index_nx(&["generated_by"]).await?;
    c.create_btree_index_nx(&["corrected_by"]).await?;
    c.create_btree_index_nx(&["observed_at"]).await?;
    c.create_bm25_index_nx(&["payload_inline"]).await?;
    Ok(())
}

async fn init_activities(c: &mut Collection) -> Result<(), DBError> {
    c.set_tokenizer(jieba_tokenizer());
    init_envelope(c).await?;
    c.create_btree_index_nx(&["client_key"]).await?;
    c.create_btree_index_nx(&["activity_class"]).await?;
    c.create_btree_index_nx(&["status"]).await?;
    // The provenance DAG is walked backward from outputs to inputs (§62).
    c.create_btree_index_nx(&["input_keys"]).await?;
    c.create_btree_index_nx(&["output_keys"]).await?;
    Ok(())
}

async fn init_spaces(c: &mut Collection) -> Result<(), DBError> {
    c.create_btree_index_nx(&["space_id"]).await?;
    Ok(())
}

async fn init_schema_packages(c: &mut Collection) -> Result<(), DBError> {
    c.create_btree_index_nx(&["package_ref"]).await?;
    c.create_btree_index_nx(&["package_id"]).await?;
    Ok(())
}

async fn init_schema_envs(c: &mut Collection) -> Result<(), DBError> {
    c.create_btree_index_nx(&["space"]).await?;
    c.create_btree_index_nx(&["version"]).await?;
    Ok(())
}

async fn init_transactions(c: &mut Collection) -> Result<(), DBError> {
    c.create_btree_index_nx(&["tx_id"]).await?;
    c.create_btree_index_nx(&["space"]).await?;
    // Idempotency is per Space — two Spaces may reuse a key — so the lookup
    // intersects this with `space` (§80.4). It cannot be a unique composite:
    // the empty string stands for "no key was supplied", and most
    // transactions carry it.
    c.create_btree_index_nx(&["idempotency_key"]).await?;
    c.create_btree_index_nx(&["seq"]).await?;
    c.create_btree_index_nx(&["changed_ids"]).await?;
    Ok(())
}

impl Store {
    /// Opens — creating if absent — every collection the engine needs.
    pub async fn open(db: Arc<AndaDB>) -> Result<Self, KipError> {
        let concepts = db
            .open_or_create_collection(
                ConceptRow::schema().map_err(schema_error)?,
                collection_config(CONCEPTS, "Concepts — units of meaning"),
                init_concepts,
            )
            .await
            .map_err(db_error)?;
        let propositions = db
            .open_or_create_collection(
                PropositionRow::schema().map_err(schema_error)?,
                collection_config(PROPOSITIONS, "Propositions — truth-neutral tuples"),
                init_propositions,
            )
            .await
            .map_err(db_error)?;
        let assertions = db
            .open_or_create_collection(
                AssertionRow::schema().map_err(schema_error)?,
                collection_config(ASSERTIONS, "Assertions — actors' epistemic commitments"),
                init_assertions,
            )
            .await
            .map_err(db_error)?;
        let evidence = db
            .open_or_create_collection(
                EvidenceRow::schema().map_err(schema_error)?,
                collection_config(EVIDENCE, "Evidence — observation records"),
                init_evidence,
            )
            .await
            .map_err(db_error)?;
        let activities = db
            .open_or_create_collection(
                ActivityRow::schema().map_err(schema_error)?,
                collection_config(ACTIVITIES, "Activities — provenance records"),
                init_activities,
            )
            .await
            .map_err(db_error)?;
        let spaces = db
            .open_or_create_collection(
                SpaceRow::schema().map_err(schema_error)?,
                collection_config(SPACES, "MemorySpaces — governance containers"),
                init_spaces,
            )
            .await
            .map_err(db_error)?;
        let transactions = db
            .open_or_create_collection(
                TransactionRow::schema().map_err(schema_error)?,
                collection_config(TRANSACTIONS, "The transaction journal"),
                init_transactions,
            )
            .await
            .map_err(db_error)?;

        let schema_packages = db
            .open_or_create_collection(
                SchemaPackageRow::schema().map_err(schema_error)?,
                collection_config(SCHEMA_PACKAGES, "Installed Schema Package artifacts"),
                init_schema_packages,
            )
            .await
            .map_err(db_error)?;
        let schema_envs = db
            .open_or_create_collection(
                SchemaEnvRow::schema().map_err(schema_error)?,
                collection_config(SCHEMA_ENVS, "Schema Environment versions"),
                init_schema_envs,
            )
            .await
            .map_err(db_error)?;

        Ok(Self {
            db,
            concepts: Slot::new(concepts),
            propositions: Slot::new(propositions),
            assertions: Slot::new(assertions),
            evidence: Slot::new(evidence),
            activities: Slot::new(activities),
            spaces: Slot::new(spaces),
            transactions: Slot::new(transactions),
            schema_packages: Slot::new(schema_packages),
            schema_envs: Slot::new(schema_envs),
        })
    }

    /// The Concept collection handle.
    pub fn concepts(&self) -> Arc<Collection> {
        self.concepts.get()
    }

    /// The Proposition collection handle.
    pub fn propositions(&self) -> Arc<Collection> {
        self.propositions.get()
    }

    /// The Assertion collection handle.
    pub fn assertions(&self) -> Arc<Collection> {
        self.assertions.get()
    }

    /// The Evidence collection handle.
    pub fn evidence(&self) -> Arc<Collection> {
        self.evidence.get()
    }

    /// The Activity collection handle.
    pub fn activities(&self) -> Arc<Collection> {
        self.activities.get()
    }

    /// The MemorySpace registry handle.
    pub fn spaces(&self) -> Arc<Collection> {
        self.spaces.get()
    }

    /// The transaction journal handle.
    pub fn transactions(&self) -> Arc<Collection> {
        self.transactions.get()
    }

    /// The installed Schema Package handle.
    pub fn schema_packages(&self) -> Arc<Collection> {
        self.schema_packages.get()
    }

    /// The Schema Environment version handle.
    pub fn schema_envs(&self) -> Arc<Collection> {
        self.schema_envs.get()
    }

    /// The collection holding one Core element kind.
    pub fn elements(&self, kind: ElementKind) -> Arc<Collection> {
        match kind {
            ElementKind::Concept => self.concepts(),
            ElementKind::Proposition => self.propositions(),
            ElementKind::Assertion => self.assertions(),
            ElementKind::Evidence => self.evidence(),
            ElementKind::Activity => self.activities(),
        }
    }

    /// Whether any handle has been poisoned and needs reopening.
    pub fn has_poisoned_handle(&self) -> bool {
        [
            self.concepts(),
            self.propositions(),
            self.assertions(),
            self.evidence(),
            self.activities(),
            self.spaces(),
            self.transactions(),
            self.schema_packages(),
            self.schema_envs(),
        ]
        .iter()
        .any(|c| c.is_poisoned())
    }

    /// Reloads every collection handle from storage.
    ///
    /// Idempotent, and safe to call when nothing is poisoned: reopening a
    /// healthy handle costs a reload and changes no state. Each setup closure
    /// runs again, which is what reinstalls the jieba tokenizer a freshly
    /// loaded handle does not carry.
    pub async fn reopen(&self) -> Result<(), KipError> {
        self.concepts
            .set(self.reload(CONCEPTS, init_concepts).await?);
        self.propositions
            .set(self.reload(PROPOSITIONS, init_propositions).await?);
        self.assertions
            .set(self.reload(ASSERTIONS, init_assertions).await?);
        self.evidence
            .set(self.reload(EVIDENCE, init_evidence).await?);
        self.activities
            .set(self.reload(ACTIVITIES, init_activities).await?);
        self.spaces.set(self.reload(SPACES, init_spaces).await?);
        self.transactions
            .set(self.reload(TRANSACTIONS, init_transactions).await?);
        self.schema_packages
            .set(self.reload(SCHEMA_PACKAGES, init_schema_packages).await?);
        self.schema_envs
            .set(self.reload(SCHEMA_ENVS, init_schema_envs).await?);
        Ok(())
    }

    async fn reload<F>(&self, name: &str, init: F) -> Result<Arc<Collection>, KipError>
    where
        F: AsyncFnOnce(&mut Collection) -> Result<(), DBError>,
    {
        self.db
            .open_collection(name.to_string(), init)
            .await
            .map_err(reopen_error)
    }

    /// Reopens only when something is actually poisoned.
    pub async fn reopen_if_poisoned(&self) -> Result<(), KipError> {
        if self.has_poisoned_handle() {
            self.reopen().await?;
        }
        Ok(())
    }

    /// Flushes every collection, making the transaction's writes durable.
    pub async fn flush(&self, now_ms: u64) -> Result<(), KipError> {
        for collection in [
            self.concepts(),
            self.propositions(),
            self.assertions(),
            self.evidence(),
            self.activities(),
            self.spaces(),
            self.transactions(),
            self.schema_packages(),
            self.schema_envs(),
        ] {
            collection.flush(now_ms).await.map_err(db_error)?;
        }
        Ok(())
    }

    /// Looks one element up by id, whatever kind it is.
    ///
    /// The id carries its kind, so this is a single row read rather than five
    /// speculative ones.
    pub async fn get_element(&self, id: ElementId) -> Result<Element, KipError> {
        let collection = self.elements(id.kind);
        let missing = || {
            KipError::not_found_or_not_visible(format!(
                "{id} does not exist in this Nexus, or policy hides it"
            ))
        };
        Ok(match id.kind {
            ElementKind::Concept => Element::Concept(Box::new(
                collection.get_as(id.seq).await.map_err(|_| missing())?,
            )),
            ElementKind::Proposition => Element::Proposition(Box::new(
                collection.get_as(id.seq).await.map_err(|_| missing())?,
            )),
            ElementKind::Assertion => Element::Assertion(Box::new(
                collection.get_as(id.seq).await.map_err(|_| missing())?,
            )),
            ElementKind::Evidence => Element::Evidence(Box::new(
                collection.get_as(id.seq).await.map_err(|_| missing())?,
            )),
            ElementKind::Activity => Element::Activity(Box::new(
                collection.get_as(id.seq).await.map_err(|_| missing())?,
            )),
        })
    }

    /// Whether an element exists at all.
    pub async fn contains(&self, id: ElementId) -> bool {
        self.elements(id.kind).contains(id.seq)
    }
}

fn collection_config(name: &str, description: &str) -> CollectionConfig {
    CollectionConfig {
        name: name.to_string(),
        description: description.to_string(),
    }
}

/// A filter matching one exact value of a single-field index.
pub fn eq_field(field: &str, value: Fv) -> Filter {
    Filter::Field((field.to_string(), RangeQuery::Eq(value)))
}

/// A filter matching several columns at once, by intersecting their indexes.
///
/// This is the composite lookup, spelled as a conjunction rather than as a
/// composite index, because a composite index in this database is also a
/// uniqueness constraint and none of these combinations is unique.
pub fn eq_fields(pairs: &[(&str, Fv)]) -> Filter {
    match pairs {
        // An empty conjunction is a caller mistake. Matching nothing is the
        // safe reading of it: a broken filter must never widen a query.
        [] => Filter::Or(vec![]),
        [(field, value)] => eq_field(field, value.clone()),
        _ => Filter::And(
            pairs
                .iter()
                .map(|(field, value)| Box::new(eq_field(field, value.clone())))
                .collect(),
        ),
    }
}

/// One loaded Cognitive Element, whatever kind it is.
///
/// Boxed variants: the rows differ in size by several hundred bytes, and an
/// unboxed enum would make every `Element` as large as the widest one.
#[derive(Clone, Debug)]
pub enum Element {
    /// A Concept.
    Concept(Box<ConceptRow>),
    /// A Proposition.
    Proposition(Box<PropositionRow>),
    /// An Assertion.
    Assertion(Box<AssertionRow>),
    /// An Evidence record.
    Evidence(Box<EvidenceRow>),
    /// An Activity.
    Activity(Box<ActivityRow>),
}

/// Reads one envelope column out of whichever row this is.
macro_rules! envelope {
    ($self:ident, $field:ident) => {
        match $self {
            Element::Concept(row) => &row.$field,
            Element::Proposition(row) => &row.$field,
            Element::Assertion(row) => &row.$field,
            Element::Evidence(row) => &row.$field,
            Element::Activity(row) => &row.$field,
        }
    };
}

impl Element {
    /// Which Core kind this is.
    pub fn kind(&self) -> ElementKind {
        match self {
            Element::Concept(_) => ElementKind::Concept,
            Element::Proposition(_) => ElementKind::Proposition,
            Element::Assertion(_) => ElementKind::Assertion,
            Element::Evidence(_) => ElementKind::Evidence,
            Element::Activity(_) => ElementKind::Activity,
        }
    }

    /// The element's Nexus-local id.
    pub fn id(&self) -> ElementId {
        ElementId::new(self.kind(), *envelope!(self, _id))
    }

    /// The element's home MemorySpace.
    pub fn space(&self) -> &str {
        envelope!(self, space)
    }

    /// The engine-level state.
    pub fn state(&self) -> &str {
        envelope!(self, state)
    }

    /// The mutation counter `EXPECT VERSION` compares against.
    pub fn version(&self) -> u64 {
        *envelope!(self, version)
    }

    /// The Space sequence of the last state change.
    pub fn seq(&self) -> u64 {
        *envelope!(self, seq)
    }

    /// Whether this element is in ordinary recall.
    ///
    /// Archived and tombstoned elements still exist and still resolve as
    /// references — deletion preserves reference integrity (§93.33) — so this
    /// is a recall question, never an existence one.
    pub fn is_active(&self) -> bool {
        self.state() == state::ACTIVE
    }
}

impl Store {
    /// Looks a Proposition up by its tuple identity.
    ///
    /// This is what makes `ENSURE PROPOSITION` resolve-or-create rather than
    /// create-or-collide: the same semantic tuple in the same Space is the
    /// same Proposition (§93.6).
    pub async fn find_proposition(
        &self,
        tuple_key: &str,
    ) -> Result<Option<rows::PropositionRow>, KipError> {
        let collection = self.propositions();
        let ids = collection
            .query_all_ids(eq_field("tuple_key", Fv::Text(tuple_key.to_string())))
            .await
            .map_err(crate::error::db_error)?;
        match ids.first() {
            None => Ok(None),
            Some(id) => Ok(Some(
                collection
                    .get_as(*id)
                    .await
                    .map_err(crate::error::db_error)?,
            )),
        }
    }

    /// Looks a Concept up by its Space-local logical key.
    ///
    /// The key is immutable identity, unlike `name`, which is why `UPSERT`
    /// resolves through it (§54).
    pub async fn find_concept_by_key(
        &self,
        space: &str,
        key: &str,
    ) -> Result<Option<rows::ConceptRow>, KipError> {
        if key.is_empty() {
            // The empty string stores "no logical key", so it must never
            // match — otherwise every keyless Concept in the Space would
            // answer an upsert meant for one of them.
            return Ok(None);
        }
        let collection = self.concepts();
        let ids = collection
            .query_all_ids(eq_fields(&[
                ("space", Fv::Text(space.to_string())),
                ("key", Fv::Text(key.to_string())),
            ]))
            .await
            .map_err(crate::error::db_error)?;
        match ids.first() {
            None => Ok(None),
            Some(id) => Ok(Some(
                collection
                    .get_as(*id)
                    .await
                    .map_err(crate::error::db_error)?,
            )),
        }
    }

    /// Loads one Concept row by id.
    pub async fn find_concept(&self, id: ElementId) -> Result<rows::ConceptRow, KipError> {
        self.concepts()
            .get_as(id.seq)
            .await
            .map_err(crate::error::db_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_element_kind_reaches_its_own_collection() {
        // A name typo here would silently split one kind across two
        // collections on the next reopen.
        let names = [
            CONCEPTS,
            PROPOSITIONS,
            ASSERTIONS,
            EVIDENCE,
            ACTIVITIES,
            SPACES,
            TRANSACTIONS,
            SCHEMA_PACKAGES,
            SCHEMA_ENVS,
        ];
        let mut sorted = names.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), names.len());
    }

    #[test]
    fn the_envelope_reader_agrees_with_the_row() {
        let element = Element::Assertion(Box::new(AssertionRow {
            _id: 7,
            space: "space-1".into(),
            state: state::ACTIVE.into(),
            version: 3,
            seq: 11,
            ..Default::default()
        }));
        assert_eq!(element.id().to_string(), "A-7");
        assert_eq!(element.kind(), ElementKind::Assertion);
        assert_eq!(element.space(), "space-1");
        assert_eq!(element.version(), 3);
        assert_eq!(element.seq(), 11);
        assert!(element.is_active());
    }

    #[test]
    fn an_archived_element_still_exists() {
        // Spec §41.2: archive is not purge, and a reference to an archived
        // element must keep resolving.
        let element = Element::Concept(Box::new(ConceptRow {
            _id: 1,
            state: state::ARCHIVED.into(),
            ..Default::default()
        }));
        assert!(!element.is_active());
        assert_eq!(element.id().to_string(), "C-1");
    }
}
