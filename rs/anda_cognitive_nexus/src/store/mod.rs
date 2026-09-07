//! # The storage layer
//!
//! Ten `anda_db` collections: one per Core element kind (Spec §6.1), plus the
//! MemorySpace registry, the transaction journal, the Schema Package and Schema
//! Environment registries, and the element version log. The Governance Control
//! Plane's eight collections live beside them under
//! [`governance`](crate::governance::store), in the same database and behind
//! the same flush — but semantically a different plane, and reachable from no
//! cognitive write path.
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

pub mod control;
pub mod history;
pub mod planes;
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
use anda_kip::{ElementKind, KipError, KipErrorCode};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::error::{db_error, reopen_error, schema_error};
use crate::id::ElementId;
use crate::schema::SchemaEnvironment;
use rows::*;
use write::Row;

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
/// The collection holding one row per element version.
pub const ELEMENT_VERSIONS: &str = "element_versions";
pub const CONTROL_RECORDS: &str = "kip_control_records";
pub const COMMIT_LOG: &str = "kip_commit_log";

/// A collection handle that survives poisoning.
#[derive(Clone, Debug)]
pub(crate) struct Slot(Arc<parking_lot::RwLock<Arc<Collection>>>);

impl Slot {
    pub(crate) fn new(collection: Arc<Collection>) -> Self {
        Self(Arc::new(parking_lot::RwLock::new(collection)))
    }

    pub(crate) fn get(&self) -> Arc<Collection> {
        self.0.read().clone()
    }

    pub(crate) fn set(&self, collection: Arc<Collection>) {
        *self.0.write() = collection;
    }
}

/// Declares the `Store`'s collection handles and everything that iterates them.
///
/// The `(name, row type, setup)` triple used to be written out three times —
/// once in `open`, once in `reopen`, once in the flush and poison lists — and
/// only the first was load-bearing for a fresh database, so a collection
/// missing from the second or third failed nowhere until recovery needed it.
macro_rules! collections {
    ($($(#[$doc:meta])* $field:ident: $row:ty = ($name:ident, $init:ident, $description:literal),)*) => {
        /// The persistent home of one Cognitive Nexus.
        #[derive(Clone, Debug)]
        pub struct Store {
            /// The underlying database, shared with whatever else the host registered.
            pub db: Arc<AndaDB>,
            /// The Governance Control Plane's own collections.
            ///
            /// Reached from here so that the two planes share one database handle, one
            /// flush and one poison recovery — and from nowhere in [`kml`](crate::kml),
            /// which is what keeps an ordinary cognitive write off the control plane.
            pub governance: crate::governance::store::GovernanceStore,
            pub evaluation_rules: crate::evaluation::EvaluationRules,
            $($field: Slot,)*
            /// Resolved Schema Environments, keyed by Space and version.
            ///
            /// Safe to keep forever, and that is a property of the data rather than a
            /// bet: activation only ever mints a *new* version (§20.8), and an installed
            /// package is immutable by reference (§20.4). So one `(space, version)`
            /// resolves to one environment for the life of the database. Without this,
            /// every KQL, KML and META command re-read every installed artifact and
            /// re-parsed it — tens of KB of JSON before the command looked at any data.
            environments: Arc<parking_lot::RwLock<BTreeMap<(String, u64), SchemaEnvironment>>>,
        }

        impl Store {
            /// Opens — creating if absent — every collection the engine needs.
            pub async fn open(db: Arc<AndaDB>) -> Result<Self, KipError> {
                $(
                    let $field = Slot::new(
                        db.open_or_create_collection(
                            <$row>::schema().map_err(schema_error)?,
                            collection_config($name, $description),
                            $init,
                        )
                        .await
                        .map_err(db_error)?,
                    );
                )*
                let governance = crate::governance::store::GovernanceStore::open(db.clone()).await?;
                let opened = Self {
                    db,
                    governance,
                    evaluation_rules: crate::evaluation::EvaluationRules::default(),
                    $($field,)*
                    environments: Arc::new(parking_lot::RwLock::new(BTreeMap::new())),
                };
                opened.attach_control_notifications();
                Ok(opened)
            }

            /// Reloads every collection handle from storage.
            ///
            /// Idempotent, and safe to call when nothing is poisoned: reopening a
            /// healthy handle costs a reload and changes no state. Each setup closure
            /// runs again, which is what reinstalls the jieba tokenizer a freshly
            /// loaded handle does not carry.
            pub async fn reopen(&self) -> Result<(), KipError> {
                $(self.$field.set(self.reload($name, $init).await?);)*
                self.governance.reopen().await
            }

            /// Every collection handle, for the passes that touch all of them.
            fn all(&self) -> impl Iterator<Item = Arc<Collection>> {
                [$(self.$field.get(),)*].into_iter()
            }

            $(
                $(#[$doc])*
                pub fn $field(&self) -> Arc<Collection> {
                    self.$field.get()
                }
            )*
        }
    };
}

// The engine's ten collections, declared once.
//
// Each row names the accessor, the row type whose schema it is opened with,
// the collection name, the index setup, and the description. Opening,
// reopening, flushing and poison detection are all generated from this list,
// so a collection added here is reached by every one of them — the failure
// this replaces was a handle that `open` created and `reopen` forgot, which
// no compiler could have caught and which surfaces only as a Nexus that stays
// bricked after a poisoned flush.
collections! {
    /// The Concept collection handle.
    concepts: ConceptRow = (CONCEPTS, init_concepts, "Concepts — units of meaning"),
    /// The Proposition collection handle.
    propositions: PropositionRow =
        (PROPOSITIONS, init_propositions, "Propositions — truth-neutral tuples"),
    /// The Assertion collection handle.
    assertions: AssertionRow =
        (ASSERTIONS, init_assertions, "Assertions — actors' epistemic commitments"),
    /// The Evidence collection handle.
    evidence: EvidenceRow = (EVIDENCE, init_evidence, "Evidence — observation records"),
    /// The Activity collection handle.
    activities: ActivityRow = (ACTIVITIES, init_activities, "Activities — provenance records"),
    /// The MemorySpace registry handle.
    spaces: SpaceRow = (SPACES, init_spaces, "MemorySpaces — governance containers"),
    /// The transaction journal handle.
    transactions: TransactionRow = (TRANSACTIONS, init_transactions, "The transaction journal"),
    /// The installed Schema Package handle.
    schema_packages: SchemaPackageRow =
        (SCHEMA_PACKAGES, init_schema_packages, "Installed Schema Package artifacts"),
    /// The Schema Environment version handle.
    schema_envs: SchemaEnvRow = (SCHEMA_ENVS, init_schema_envs, "Schema Environment versions"),
    /// The element version log handle.
    element_versions: ElementVersionRow =
        (ELEMENT_VERSIONS, init_element_versions, "One row per element version"),
    control_records: ControlRecordRow =
        (CONTROL_RECORDS, init_control, "Protected, versioned Nexus control records"),
    commit_log: CommitLogRow = (COMMIT_LOG, init_commit_log, "Recoverable multi-collection commits"),
}

async fn init_control(c: &mut Collection) -> Result<(), DBError> {
    c.create_btree_index_nx(&["space"]).await?;
    c.create_btree_index_nx(&["key"]).await?;
    c.create_btree_index_nx(&["seq"]).await?;
    c.create_btree_index_nx(&["record_id"]).await?;
    Ok(())
}

async fn init_commit_log(c: &mut Collection) -> Result<(), DBError> {
    c.create_btree_index_nx(&["tx_id"]).await?;
    Ok(())
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
    // A Proposition's whole content is its tuple (§12.2), so the only text it
    // has of its own is the predicate it was written under. The endpoints
    // carry the words, and they are Concepts and Literals a search reaches on
    // their own terms.
    c.create_bm25_index_nx(&["predicate_ref"]).await?;
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

async fn init_element_versions(c: &mut Collection) -> Result<(), DBError> {
    c.create_btree_index_nx(&["space"]).await?;
    // The historical read is "the greatest version of this element at or
    // before this sequence", so both columns are ranged over.
    c.create_btree_index_nx(&["element"]).await?;
    c.create_btree_index_nx(&["seq"]).await?;
    c.create_btree_index_nx(&["kind"]).await?;
    c.create_btree_index_nx(&["tx_id"]).await?;
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
    fn attach_control_notifications(&self) {
        self.governance.attach_notifications(
            self.spaces.clone(),
            self.transactions.clone(),
            self.control_records.clone(),
        );
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
        self.all().any(|c| c.is_poisoned()) || self.governance.has_poisoned_handle()
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
        self.recover_commits().await?;
        if self.governance.control_recovery_needed() {
            self.governance.recover_control_delivery().await?;
        }
        Ok(())
    }

    /// Flushes every collection, making the transaction's writes durable.
    pub async fn flush(&self, now_ms: u64) -> Result<(), KipError> {
        for collection in self.all() {
            collection.flush(now_ms).await.map_err(db_error)?;
        }
        self.governance.flush(now_ms).await?;
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
        // One arm per variant because `get_as` picks the row type from the
        // binding: the kind decides which struct is deserialized, so the
        // dispatch cannot be hoisted behind a value.
        macro_rules! read {
            ($variant:ident) => {
                Element::$variant(Box::new(
                    collection.get_as(id.seq).await.map_err(|_| missing())?,
                ))
            };
        }
        Ok(match id.kind {
            ElementKind::Concept => read!(Concept),
            ElementKind::Proposition => read!(Proposition),
            ElementKind::Assertion => read!(Assertion),
            ElementKind::Evidence => read!(Evidence),
            ElementKind::Activity => read!(Activity),
        })
    }

    /// Every element in a Space that points at one element.
    ///
    /// A full scan of the Space rather than an index intersection, because the
    /// reference paths are not all indexed — Profile structural fields and an
    /// Assertion's context have no key column — and an incomplete answer here
    /// would let a destructive operation leave a dangling reference behind.
    /// Purge is documented as exceptional; paying a scan for it is the right
    /// trade against getting it wrong.
    pub async fn referrers(
        &self,
        space_id: &str,
        target: ElementId,
    ) -> Result<Vec<ElementId>, KipError> {
        let mut found = Vec::new();
        for kind in [
            ElementKind::Concept,
            ElementKind::Proposition,
            ElementKind::Assertion,
            ElementKind::Evidence,
            ElementKind::Activity,
        ] {
            let collection = self.elements(kind);
            let ids = collection
                .query_all_ids(eq_field("space", Fv::Text(space_id.to_string())))
                .await
                .map_err(db_error)?;
            for row_id in ids {
                let id = ElementId::new(kind, row_id);
                if id == target {
                    continue;
                }
                let Ok(element) = self.get_element(id).await else {
                    continue;
                };
                if element.references().contains(&target) {
                    found.push(id);
                }
            }
        }
        Ok(found)
    }

    /// Every Activity in a Space that names one endpoint key among its inputs.
    ///
    /// The reverse of the provenance edge `Activity.inputs`, which is what
    /// `LIST DEPENDENTS` walks (§63.5). An index lookup rather than a scan:
    /// `input_keys` is indexed precisely so the DAG can be traversed in the
    /// derived direction as cheaply as in the source direction (§62).
    pub async fn activities_with_input(
        &self,
        space_id: &str,
        input_key: &str,
    ) -> Result<Vec<ElementId>, KipError> {
        let ids = self
            .elements(ElementKind::Activity)
            .query_all_ids(eq_fields(&[
                ("space", Fv::Text(space_id.to_string())),
                ("input_keys", Fv::Text(input_key.to_string())),
            ]))
            .await
            .map_err(db_error)?;
        Ok(ids
            .into_iter()
            .map(|row_id| ElementId::new(ElementKind::Activity, row_id))
            .collect())
    }

    /// Whether an element exists at all.
    pub async fn contains(&self, id: ElementId) -> bool {
        self.elements(id.kind).contains(id.seq)
    }
}

/// The full-row assignment map an `anda_db` update takes.
///
/// Whole rows rather than a computed delta, for the same reason
/// [`Store::update`](write) rewrites everything: a delta is a second place
/// where the column list is enumerated, and a column missing from it stops
/// being persisted without anything failing.
pub(crate) fn full_row_fields<T: serde::Serialize>(
    schema: Arc<anda_db_schema::Schema>,
    row: &T,
) -> Result<std::collections::BTreeMap<String, Fv>, KipError> {
    let mut document =
        anda_db_schema::Document::try_from(schema.clone(), row).map_err(schema_error)?;
    let mut fields = std::collections::BTreeMap::new();
    for entry in schema.iter() {
        // `_id` is the update's target, not one of its assignments.
        if entry.name() == "_id" {
            continue;
        }
        // Moved out rather than cloned: the document is dropped at the end of
        // this function, and a row carries whole JSON blobs.
        if let Some(value) = document.remove_field(entry.name()) {
            fields.insert(entry.name().to_string(), value);
        }
    }
    Ok(fields)
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
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
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

impl Element {
    /// The shared envelope columns, whichever kind this is.
    ///
    /// The one place the five variants are told apart for a shared column.
    /// Everything below reads a field off this rather than matching again, so
    /// adding a column to `envelope_columns!` in [`write`](mod@write) reaches every
    /// accessor without any of them being touched.
    #[inline]
    pub(crate) fn envelope(&self) -> write::Envelope<'_> {
        match self {
            Element::Concept(row) => row.envelope(),
            Element::Proposition(row) => row.envelope(),
            Element::Assertion(row) => row.envelope(),
            Element::Evidence(row) => row.envelope(),
            Element::Activity(row) => row.envelope(),
        }
    }

    /// The same columns, for the authorized paths that change them.
    #[inline]
    pub(crate) fn envelope_mut(&mut self) -> write::EnvelopeMut<'_> {
        match self {
            Element::Concept(row) => row.envelope_mut(),
            Element::Proposition(row) => row.envelope_mut(),
            Element::Assertion(row) => row.envelope_mut(),
            Element::Evidence(row) => row.envelope_mut(),
            Element::Activity(row) => row.envelope_mut(),
        }
    }

    /// Which Core kind this is.
    #[inline]
    pub fn kind(&self) -> ElementKind {
        self.envelope().kind
    }

    /// The element's Nexus-local id.
    #[inline]
    pub fn id(&self) -> ElementId {
        let envelope = self.envelope();
        ElementId::new(envelope.kind, envelope.id)
    }

    /// The element's home MemorySpace.
    #[inline]
    pub fn space(&self) -> &str {
        self.envelope().space
    }

    /// The engine-level state.
    #[inline]
    pub fn state(&self) -> &str {
        self.envelope().state
    }

    /// The mutation counter `EXPECT VERSION` compares against.
    #[inline]
    pub fn version(&self) -> u64 {
        *self.envelope().version
    }

    /// The Space sequence of the last state change.
    #[inline]
    pub fn seq(&self) -> u64 {
        *self.envelope().seq
    }

    /// The per-plane counters `EXPECT VERSION ... OF` compares against (§6.3).
    pub fn plane_versions(&self) -> anda_kip::PlaneVersions {
        planes::decode(self.envelope().plane_versions)
    }

    /// Writes the per-plane counters; the transaction commit is the one caller.
    pub(crate) fn set_plane_versions(&mut self, planes: &anda_kip::PlaneVersions) {
        *self.envelope_mut().plane_versions = planes::encode(planes);
    }

    /// Whether this element is in ordinary recall.
    ///
    /// Archived and tombstoned elements still exist and still resolve as
    /// references — deletion preserves reference integrity (§93.33) — so this
    /// is a recall question, never an existence one.
    pub fn is_active(&self) -> bool {
        self.state() == state::ACTIVE
    }

    /// The element's own Governance members (Spec §6.2).
    pub fn governance(&self) -> &anda_kip::Json {
        self.envelope().governance
    }

    /// The storage-lifecycle hook (Spec §19.1).
    pub fn retention(&self) -> &anda_kip::Json {
        self.envelope().retention
    }

    /// The storage-lifecycle hook and the sweep column that mirrors it.
    pub(crate) fn retention_mut(&mut self) -> (&mut anda_kip::Json, &mut String) {
        let envelope = self.envelope_mut();
        (envelope.retention, envelope.expires_at)
    }

    /// `retention.expires_at`, the column the retention sweep ranges over.
    pub(crate) fn expires_at(&self) -> &str {
        self.envelope().expires_at
    }

    /// The element's Facets, keyed by facet symbol (§35).
    pub(crate) fn facets(&self) -> &anda_db_schema::Map<String, anda_kip::Json> {
        self.envelope().facets
    }

    /// The same map, for the authorized paths that change it.
    pub(crate) fn facets_mut(&mut self) -> &mut anda_db_schema::Map<String, anda_kip::Json> {
        self.envelope_mut().facets
    }

    /// The Profile structural fields this element carries (§8.2).
    pub(crate) fn structural(&self) -> &anda_db_schema::Map<String, anda_kip::Json> {
        self.envelope().structural
    }

    /// Every local element this one points at.
    ///
    /// The complete set, including the reference paths no index covers —
    /// Profile structural fields and an Assertion's context. A purge planner
    /// and a Capsule closure both need *all* of them: a walker that missed one
    /// would let a destructive operation leave a dangling reference behind, or
    /// let an export ship a graph with a broken edge.
    pub fn references(&self) -> Vec<ElementId> {
        fn local(value: &anda_kip::Json) -> Option<ElementId> {
            match crate::term::Endpoint::from_json(value) {
                Ok(crate::term::Endpoint::Local(id)) => Some(id),
                _ => None,
            }
        }
        // The Profile structural fields are a shared column, so they are
        // walked once here rather than per kind; only the typed Core edges
        // below differ between the five.
        let mut out: Vec<ElementId> = self
            .structural()
            .values()
            .filter_map(anda_kip::Json::as_array)
            .flatten()
            .filter_map(local)
            .collect();
        match self {
            Element::Concept(_) => {}
            Element::Proposition(row) => {
                out.extend(local(&row.subject));
                out.extend(local(&row.object));
            }
            Element::Assertion(row) => {
                if let Ok(id) = row.proposition_id.parse() {
                    out.push(id);
                }
                out.extend(local(&row.asserted_by));
                out.extend(
                    row.evidence_ids
                        .iter()
                        .filter_map(|id| id.parse::<ElementId>().ok()),
                );
                out.extend(row.context_refs.iter().filter_map(local));
            }
            Element::Evidence(row) => {
                if let Ok(id) = row.generated_by.parse() {
                    out.push(id);
                }
                out.extend(row.source_refs.iter().filter_map(local));
                out.extend(
                    row.corrects
                        .iter()
                        .chain(row.corrected_by.iter())
                        .filter_map(|id| id.parse::<ElementId>().ok()),
                );
            }
            Element::Activity(row) => {
                out.extend(row.inputs.iter().filter_map(local));
                out.extend(row.outputs.iter().filter_map(local));
                out.extend(row.associated_actors.iter().filter_map(local));
            }
        }
        out
    }

    /// Rebuilds an element of one kind from a stored row's JSON.
    ///
    /// The version log keeps whole rows as JSON, so replaying history is a
    /// decode rather than a read; the kind comes from the log entry beside it.
    pub(crate) fn from_json(
        kind: ElementKind,
        value: anda_kip::Json,
    ) -> Result<Self, serde_json::Error> {
        macro_rules! decode {
            ($variant:ident) => {
                Element::$variant(Box::new(serde_json::from_value(value)?))
            };
        }
        Ok(match kind {
            ElementKind::Concept => decode!(Concept),
            ElementKind::Proposition => decode!(Proposition),
            ElementKind::Assertion => decode!(Assertion),
            ElementKind::Evidence => decode!(Evidence),
            ElementKind::Activity => decode!(Activity),
        })
    }

    /// The engine-level state, for the authorized paths that change it.
    pub(crate) fn state_mut(&mut self) -> &mut String {
        self.envelope_mut().state
    }

    /// The same block, for the authorized paths that change it.
    pub(crate) fn governance_mut(&mut self) -> &mut anda_kip::Json {
        self.envelope_mut().governance
    }

    /// The classification label this element carries, if it carries one.
    ///
    /// Empty means the element states none, which is **not** `public`: the
    /// Space's default applies instead (§95). Resolving that default is the
    /// authorization layer's job, because only it knows which Space the read
    /// is running in.
    pub fn classification(&self) -> &str {
        self.governance()
            .get("classification")
            .and_then(anda_kip::Json::as_str)
            .unwrap_or_default()
    }

    /// The exact Schema symbol this element is typed by, where it has one.
    ///
    /// A Proposition's predicate and an Evidence record's class play the same
    /// role for authorization — they are what a Grant scoped to a schema
    /// reference is scoped to — so they answer here rather than forcing every
    /// caller to match on the kind first.
    pub fn schema_ref(&self) -> &str {
        match self {
            Element::Concept(row) => &row.schema_ref,
            Element::Proposition(row) => &row.predicate_ref,
            Element::Evidence(row) => &row.evidence_class,
            Element::Activity(row) => &row.activity_class,
            // An Assertion is typed by the Proposition it is about, not by a
            // symbol of its own.
            Element::Assertion(_) => "",
        }
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

    /// Every active Assertion in a Space whose validity window has closed.
    ///
    /// Ordered by id, so a bounded pass is repeatable.
    pub async fn lapsed_assertions(
        &self,
        space: &str,
        now: &str,
    ) -> Result<Vec<ElementId>, KipError> {
        let ids = self
            .elements(ElementKind::Assertion)
            .query_all_ids(anda_db::query::Filter::And(vec![
                Box::new(eq_field("space", Fv::Text(space.to_string()))),
                Box::new(eq_field("state", Fv::Text("active".to_string()))),
                Box::new(eq_field("status", Fv::Text("active".to_string()))),
                // The empty string stores "no window", and sorts below every
                // timestamp, so the range starts just above it rather than
                // sweeping every claim that never declared one.
                Box::new(anda_db::query::Filter::Field((
                    "valid_until".to_string(),
                    anda_db::query::RangeQuery::Between(
                        Fv::Text("0".to_string()),
                        Fv::Text(now.to_string()),
                    ),
                ))),
            ]))
            .await
            .map_err(crate::error::db_error)?;
        let mut out: Vec<ElementId> = ids
            .into_iter()
            .map(|seq| ElementId::new(ElementKind::Assertion, seq))
            .collect();
        out.sort();
        Ok(out)
    }

    /// Every active element in a Space whose retention has lapsed (§19.1).
    ///
    /// Sorted by id so a bounded sweep is repeatable: the same `limit` over
    /// the same state acts on the same elements, which is what lets a host run
    /// one in slices without wondering what it skipped.
    pub async fn expired_elements(
        &self,
        space: &str,
        now: &str,
    ) -> Result<Vec<ElementId>, KipError> {
        let mut out = Vec::new();
        for kind in [
            ElementKind::Concept,
            ElementKind::Proposition,
            ElementKind::Assertion,
            ElementKind::Evidence,
            ElementKind::Activity,
        ] {
            let ids = self
                .elements(kind)
                .query_all_ids(anda_db::query::Filter::And(vec![
                    Box::new(eq_field("space", Fv::Text(space.to_string()))),
                    Box::new(eq_field("state", Fv::Text("active".to_string()))),
                    // The empty string stores "no expiry", and it sorts below
                    // every timestamp — so the range starts just above it
                    // rather than sweeping every element that never had one.
                    Box::new(anda_db::query::Filter::Field((
                        "expires_at".to_string(),
                        anda_db::query::RangeQuery::Between(
                            Fv::Text("0".to_string()),
                            Fv::Text(now.to_string()),
                        ),
                    ))),
                ]))
                .await
                .map_err(crate::error::db_error)?;
            out.extend(ids.into_iter().map(|seq| ElementId::new(kind, seq)));
        }
        out.sort();
        Ok(out)
    }

    /// Resolves a Concept by validated `canonical_id` (§8.2).
    ///
    /// Same-Space only: a canonical id is a cross-system identity claim, and
    /// resolving one outside the Space would be a foreign reference (§8.3),
    /// which never grants read authority or triggers traversal on its own.
    pub async fn find_concept_by_canonical_id(
        &self,
        space: &str,
        canonical_id: &str,
    ) -> Result<Option<ElementId>, KipError> {
        if canonical_id.is_empty() {
            return Ok(None);
        }
        let ids = self
            .concepts()
            .query_all_ids(eq_fields(&[
                ("space", Fv::Text(space.to_string())),
                ("canonical_id", Fv::Text(canonical_id.to_string())),
            ]))
            .await
            .map_err(crate::error::db_error)?;
        Ok(ids
            .iter()
            .min()
            .map(|seq| ElementId::new(ElementKind::Concept, *seq)))
    }

    /// Resolves the element a `CLIENT KEY` names, when this Space has one.
    ///
    /// §52.1 makes a `CREATE` create "a historically distinct element unless a
    /// `client_key` proves a retry of the same logical creation". Without this
    /// lookup the key is written and never read, so a client that lost its
    /// response and re-sent the same command gets a second element — the exact
    /// duplicate the key exists to prevent, and the one a caller is least able
    /// to detect afterwards.
    ///
    /// A Proposition has no client key: its identity is its tuple (§12.3),
    /// which is what `ENSURE` resolves through instead.
    pub async fn find_by_client_key(
        &self,
        space: &str,
        kind: ElementKind,
        key: &str,
    ) -> Result<Option<ElementId>, KipError> {
        // The empty string stores "no client key", so it must never match —
        // otherwise every keyless element in the Space would answer for one
        // another.
        if key.is_empty() || kind == ElementKind::Proposition {
            return Ok(None);
        }
        let ids = self
            .elements(kind)
            .query_all_ids(eq_fields(&[
                ("space", Fv::Text(space.to_string())),
                ("client_key", Fv::Text(key.to_string())),
            ]))
            .await
            .map_err(crate::error::db_error)?;
        // Lowest id wins, deterministically: a database written before this
        // lookup existed may hold more than one, and a retry that resolved to
        // a different one each time would be worse than not resolving at all.
        Ok(ids.iter().min().map(|seq| ElementId::new(kind, *seq)))
    }

    /// Looks a Concept up by its Space-local logical key.
    ///
    /// The key is immutable identity, unlike `name`, which is why `UPSERT`
    /// resolves through it (§54).
    ///
    /// `schema_ref` narrows the lookup to the type's **lineage** (§7.3,
    /// §20.14): key uniqueness is scoped to `(space_id, type lineage, key)`,
    /// so a Person and a Preference both keyed `"alice"` are two identities,
    /// not a collision — which is also what makes the 1.x migration of
    /// `(type, name)` identity into a key collision-free — while a Person
    /// written under an earlier version of the same package is the same
    /// Person, and an upsert by `key` after an upgrade finds it rather than
    /// minting a duplicate.
    ///
    /// Without a declared type the key alone must still land on one Concept.
    /// Returning the first of several would be the arbitrary winner §54.2
    /// forbids for names, arriving through `key` instead.
    pub async fn find_concept_by_key(
        &self,
        space: &str,
        schema_ref: Option<&str>,
        key: &str,
    ) -> Result<Option<rows::ConceptRow>, KipError> {
        if key.is_empty() {
            // The empty string stores "no logical key", so it must never
            // match — otherwise every keyless Concept in the Space would
            // answer an upsert meant for one of them.
            return Ok(None);
        }
        let collection = self.concepts();
        // The declared type narrows the *index* over its whole lineage, and
        // `same_lineage` settles the symbol on the rows that come back — the
        // same two-step `predicate_ref` matching uses (§20.14). Filtering
        // without the range would fetch every Concept in the Space that
        // carries the key, however many unrelated types share it.
        let mut filter = eq_fields(&[
            ("space", Fv::Text(space.to_string())),
            ("key", Fv::Text(key.to_string())),
        ]);
        if let Some((low, high)) = schema_ref.and_then(crate::schema::lineage_range) {
            filter = Filter::And(vec![
                Box::new(filter),
                Box::new(Filter::Field((
                    "schema_ref".to_string(),
                    RangeQuery::Between(Fv::Text(low), Fv::Text(high)),
                ))),
            ]);
        }
        let ids = collection
            .query_all_ids(filter)
            .await
            .map_err(crate::error::db_error)?;
        let mut found: Vec<rows::ConceptRow> = Vec::with_capacity(ids.len());
        for id in ids {
            let row: rows::ConceptRow = collection
                .get_as(id)
                .await
                .map_err(crate::error::db_error)?;
            if let Some(schema_ref) = schema_ref
                && !crate::schema::same_lineage(&row.schema_ref, schema_ref)
            {
                continue;
            }
            found.push(row);
        }
        match found.len() {
            0 => Ok(None),
            1 => Ok(found.pop()),
            n => Err(KipError::new(
                KipErrorCode::IdentityConflict,
                format!(
                    "the key {key:?} is carried by {n} Concepts in this Space, so it does not name \
                     one on its own; add the type — MATCH {{type: ..., key: ...}} — rather than \
                     letting the engine pick among them"
                ),
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
