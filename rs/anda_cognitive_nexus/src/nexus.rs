//! # The Cognitive Nexus
//!
//! The engine behind [`anda_kip::Executor`]. It owns three things the layers
//! below deliberately do not: which MemorySpace a request runs against, the
//! lock that makes a transaction atomically visible, and the poison recovery
//! that keeps a cancelled write from bricking the process.
//!
//! ## The lock is the atomicity
//!
//! `anda_db` has no multi-collection transaction, so a KML statement writes
//! several rows one at a time. What stops a reader from seeing half of that is
//! this struct's `RwLock`: mutations take it exclusively, reads take it shared.
//! Within one process that is genuine atomic visibility (Spec §29). Across
//! processes there is nothing to coordinate — `anda_db` allows one live writer
//! per database — so the guarantee is not weaker than the storage underneath
//! it.
//!
//! What neither provides is crash atomicity mid-commit. That is handled by
//! construction instead: elements are minted `pending` and swept on open.

use anda_kip::{
    Command, CommandType, Executor, Json, KipError, Operation, Request, Response, SpaceSelector,
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

use crate::governance::approval::Approved;
use crate::governance::rows::principal_class;
use crate::governance::rows::{
    ActorBindingRow, ApprovalRow, DelegationRow, GovernancePolicyRow, GrantRow, PrincipalGroupRow,
    PrincipalRow,
};
use crate::governance::store::{
    ActorBindingDraft, DelegationDraft, GrantDraft, GroupDraft, PolicyDraft, PrincipalDraft,
};
use crate::governance::{
    ANONYMOUS_PRINCIPAL, AuthContext, Authorization, EffectiveAuthority, Permission,
    ResourceContext, SYSTEM_PRINCIPAL, gate,
};
use crate::schema::{SchemaEnvironment, SchemaPackage};
use crate::store::Store;
use crate::store::space::SpaceDraft;

/// The default MemorySpace a request runs against when it names none.
pub const DEFAULT_SPACE: &str = "kip:space:default";

/// A KIP 2.0 Cognitive Nexus backed by Anda DB.
#[derive(Clone)]
pub struct CognitiveNexus {
    /// The storage layer, exposed so a host can reach the raw Core view.
    pub store: Store,
    /// The Space a request runs against when its envelope names none.
    default_space: String,
    lock: Arc<RwLock<()>>,
    approval_lock: Arc<Mutex<()>>,
}

impl std::fmt::Debug for CognitiveNexus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CognitiveNexus")
            .field("default_space", &self.default_space)
            .finish_non_exhaustive()
    }
}

impl CognitiveNexus {
    /// Opens a Nexus on an existing database.
    ///
    /// Sweeps any element left `pending` by a run that crashed mid-commit
    /// before returning: such an element belongs to no committed transaction
    /// and was never visible, so removing it is the whole of the recovery.
    pub async fn connect(db: Arc<anda_db::database::AndaDB>) -> Result<Self, KipError> {
        // A KIP 1.x database occupies the two collection names this engine is
        // about to open, with schemas that mean something else. Extract and
        // clear it first, or `Store::open` fails building an index on a field
        // the old schema never had — safe, but unreadable as a diagnosis.
        crate::migrate::prepare(&db).await?;
        let store = Store::open(db).await?;
        store.sweep_pending().await?;
        store.install_core_package().await?;
        store
            .governance
            .ensure_principal(PrincipalDraft {
                principal_id: SYSTEM_PRINCIPAL.to_string(),
                principal_class: principal_class::SYSTEM.to_string(),
                display_name: "The Nexus itself".to_string(),
                auth_provider: "engine".to_string(),
                auth_subject: SYSTEM_PRINCIPAL.to_string(),
            })
            .await?;
        // Registered rather than special-cased, so that "unauthenticated" is a
        // Principal a policy can name — which is how a Space becomes publicly
        // readable on purpose (§28.2) instead of by an absent check.
        store
            .governance
            .ensure_principal(PrincipalDraft {
                principal_id: ANONYMOUS_PRINCIPAL.to_string(),
                principal_class: principal_class::ANONYMOUS.to_string(),
                display_name: "An unauthenticated caller".to_string(),
                auth_provider: "engine".to_string(),
                auth_subject: String::new(),
            })
            .await?;
        store
            .open_or_create_space(SpaceDraft {
                space_id: DEFAULT_SPACE.to_string(),
                name: "Default MemorySpace".to_string(),
                description: "The Space a request runs against when it names none.".to_string(),
                owner_principal: SYSTEM_PRINCIPAL.to_string(),
                ..Default::default()
            })
            .await?;
        store.adopt_unowned_spaces(SYSTEM_PRINCIPAL).await?;
        let nexus = Self {
            store,
            default_space: DEFAULT_SPACE.to_string(),
            lock: Arc::new(RwLock::new(())),
            approval_lock: Arc::new(Mutex::new(())),
        };
        // A staged 1.x migration is finished here only when this Space already
        // has a Schema Environment — meaning a previous run activated the
        // host's packages and this is a restart, possibly one resuming an
        // interrupted load.
        //
        // On a *first* start there is nothing but Core, and finishing now would
        // decide the migration's vocabulary before the host has said what its
        // vocabulary is: every legacy type would be minted as a duplicate
        // symbol, and the host's `Person` and the migrated `Person` would
        // become two names for one word that no query can tell apart. So it
        // waits for `ensure_schema` instead.
        //
        // A failure here fails `connect`: a half-migrated brain that answers
        // queries is worse than one that refuses to start, because the answers
        // look ordinary.
        if nexus
            .store
            .get_space(DEFAULT_SPACE)
            .await?
            .schema_environment_version
            > 0
        {
            crate::migrate::load(&nexus).await?;
        }
        Ok(nexus)
    }

    /// Finishes a staged KIP 1.x migration against the vocabulary now in force.
    ///
    /// A host that activates packages gets this for free from
    /// [`Self::ensure_schema`]. This is for one that activates nothing and still
    /// wants its 1.x rows converted — they will be, against a generated legacy
    /// package, which is the best mapping available when nothing better has been
    /// declared.
    ///
    /// Idempotent: a completed migration is a no-op, and an interrupted one
    /// resumes from where it stopped.
    pub async fn finish_migration(&self) -> Result<(), KipError> {
        crate::migrate::load(self).await
    }

    /// Wraps an already-open store, for a caller that has one.
    ///
    /// Takes no lock of its own beyond a fresh one, so this is for read paths
    /// that are already holding the caller's lock.
    pub(crate) fn attach(store: Store) -> Self {
        Self {
            store,
            default_space: DEFAULT_SPACE.to_string(),
            lock: Arc::new(RwLock::new(())),
            approval_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Opens a session for an authenticated caller.
    ///
    /// The [`AuthContext`] must come from what the host observed about the
    /// connection, never from the request body — the envelope's own
    /// documentation calls its context non-authoritative, because an Agent
    /// under prompt injection can write anything into it (§10).
    pub fn session(&self, auth: AuthContext) -> Session {
        Session {
            nexus: self.clone(),
            auth: Arc::new(auth),
        }
    }

    /// A session as the engine's own Principal (§28.2).
    pub fn system_session(&self) -> Session {
        self.session(AuthContext::system())
    }

    /// The Governance Control Plane.
    ///
    /// A **host** handle, and deliberately not an authorized one: whoever holds
    /// a `&CognitiveNexus` is the process that opened the database, and asking
    /// that process to prove to itself that it may configure its own Space
    /// would be theatre. Authorization is what happens to *callers*, and a
    /// caller reaches the engine through a session, never through this.
    ///
    /// Which is also why Governance mutation lives here rather than in KML: a
    /// language a model writes must not be a language that can change who
    /// controls the Space (§20.10).
    pub fn governance(&self) -> &crate::governance::store::GovernanceStore {
        &self.store.governance
    }

    /// Installs a Schema Package artifact. Installing does not activate it.
    pub async fn install_package(
        &self,
        package: &SchemaPackage,
        source: &str,
    ) -> Result<crate::schema::PackageRef, KipError> {
        let _guard = self.lock.write().await;
        self.store.install_package(package, source).await
    }

    /// Activates a Schema Lock in a Space, minting the next environment version.
    pub async fn activate_schema(
        &self,
        space_id: &str,
        lock: crate::schema::SchemaLock,
    ) -> Result<SchemaEnvironment, KipError> {
        let _guard = self.lock.write().await;
        self.store.activate_schema(space_id, lock).await
    }

    /// Installs each artifact and puts exactly those packages in force in a
    /// Space.
    ///
    /// This is the bootstrap a host runs on start: `artifacts` is `(source,
    /// JSON)`, where the source is recorded on the installed package row so
    /// `LIST PACKAGES` can say where an artifact entered. The resulting Schema
    /// Lock names exactly the packages given — a host owns its Space's lock, so
    /// dropping an artifact from the list deactivates it — and is activated
    /// only when it differs from the one already in force.
    ///
    /// Installing is still not activating (§20.12): this activates because the
    /// caller said which packages to activate, not because they were installed.
    pub async fn install_and_activate(
        &self,
        artifacts: &[(&str, &str)],
        space_id: &str,
    ) -> Result<SchemaEnvironment, KipError> {
        let mut lock = crate::schema::SchemaLock::default();
        for (source, artifact) in artifacts {
            let package = SchemaPackage::parse(artifact).map_err(|err| {
                KipError::new(
                    err.code,
                    format!("schema package from {source}: {}", err.message),
                )
            })?;
            let package_ref = self.install_package(&package, source).await?;
            lock.packages.insert(
                package_ref.package_id.clone(),
                package_ref.version.to_string(),
            );
            lock.states
                .insert(package_ref.package_id, crate::schema::PackageState::Active);
        }
        self.ensure_schema(space_id, lock).await
    }

    /// Activates `lock` in a Space, but only when it differs from the one
    /// already in force.
    ///
    /// This is what a host calls on every start. Every activation mints a new
    /// environment version (§20.8), so a host that unconditionally re-activated
    /// its baseline lock would walk the version forward on each restart —
    /// invalidating clients' `preconditions.schema_environment_version` and
    /// filling `HISTORY` with schema changes that changed nothing.
    pub async fn ensure_schema(
        &self,
        space_id: &str,
        lock: crate::schema::SchemaLock,
    ) -> Result<SchemaEnvironment, KipError> {
        self.activate_if_changed(space_id, lock).await?;
        // A 1.x migration staged by `connect` waits for exactly this moment: the
        // host has now said what its vocabulary is, so a legacy `Person` can be
        // carried onto the host's `Person` instead of becoming a second symbol
        // spelled the same way. Outside the write guard, because the load runs
        // its KML through the ordinary engine — which takes that guard itself.
        crate::migrate::load(self).await?;
        self.store.schema_environment(space_id).await
    }

    /// Activates `lock` when it differs from the one in force, and nothing else.
    ///
    /// Separate from [`Self::ensure_schema`] because the migration load calls
    /// it: going through `ensure_schema` there would re-enter the load that is
    /// already running.
    pub(crate) async fn activate_if_changed(
        &self,
        space_id: &str,
        lock: crate::schema::SchemaLock,
    ) -> Result<SchemaEnvironment, KipError> {
        let _guard = self.lock.write().await;
        let current = self.store.schema_environment(space_id).await?;
        let mut lock = lock;
        crate::migrate::retain_legacy_package(&current.lock, &mut lock);
        if current.lock == lock {
            return Ok(current);
        }
        self.store.activate_schema(space_id, lock).await
    }

    /// Imports a Cognitive Capsule into a Space (§39.3, the `merge` mode).
    ///
    /// A host operation, not a KIP command: KML has no import clause and META
    /// is read-only, so the only thing an Agent can do through the protocol is
    /// `PREVIEW IMPORT CAPSULE`. Deciding that this Space accepts another
    /// Brain's cognition is the host's call, and leaving it outside the command
    /// surface keeps a prompt from making it.
    ///
    /// Re-importing the same artifact is idempotent: every record resolves back
    /// to the element the first import created.
    pub async fn import_capsule(
        &self,
        capsule: &anda_kip::Capsule,
        space_id: &str,
    ) -> Result<crate::capsule::ImportReport, KipError> {
        let _guard = self.lock.write().await;
        self.store.reopen_if_poisoned().await?;
        crate::capsule::import(self, capsule, space_id, false, AuthContext::system(), false).await
    }

    /// Imports a Capsule into quarantine rather than into recall (§39.2).
    ///
    /// The `isolate` mode, for cognition whose sender, schema or contents have
    /// not been reviewed: the records land durably and auditably, a reviewer
    /// with the right permission can read them, and nothing recalls, projects
    /// or acts on them until somebody releases each one. That is the honest
    /// answer to "should I accept this?" — accept it where it cannot do
    /// anything, and decide afterwards.
    pub async fn import_capsule_isolated(
        &self,
        capsule: &anda_kip::Capsule,
        space_id: &str,
    ) -> Result<crate::capsule::ImportReport, KipError> {
        let _guard = self.lock.write().await;
        self.store.reopen_if_poisoned().await?;
        crate::capsule::import(self, capsule, space_id, false, AuthContext::system(), true).await
    }

    /// The Space a request runs against.
    ///
    /// A Space named in the envelope must exist: creating one implicitly would
    /// let a typo silently start a second, empty memory rather than failing.
    async fn space_of(&self, request: &Request) -> Result<String, KipError> {
        let named = match &request.space {
            Some(SpaceSelector { id: Some(id), .. }) => id.clone(),
            Some(SpaceSelector { uri: Some(uri), .. }) => uri.clone(),
            _ => return Ok(self.default_space.clone()),
        };
        self.store.get_space(&named).await?;
        Ok(named)
    }

    /// Flushes and closes the underlying database.
    pub async fn close(&self) -> Result<(), KipError> {
        let _guard = self.lock.write().await;
        self.store.db.close().await.map_err(crate::error::db_error)
    }
}

/// One authenticated caller's view of a Nexus.
///
/// This is the type a multi-tenant host executes through: it authenticates the
/// caller itself, builds an [`AuthContext`] from what it observed, and every
/// command run here is authorized against the control plane before it touches
/// anything.
///
/// A session holds identity, not authority. Authority is resolved from the
/// control plane on each request, so a session that has been running since
/// January does not still hold what January's Grants said (§28.6).
#[derive(Clone)]
pub struct Session {
    nexus: CognitiveNexus,
    auth: Arc<AuthContext>,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("principal_id", &self.auth.principal_id)
            .finish_non_exhaustive()
    }
}

impl Session {
    /// The identity this session runs as.
    pub fn auth(&self) -> &AuthContext {
        &self.auth
    }

    /// The Nexus underneath.
    pub fn nexus(&self) -> &CognitiveNexus {
        &self.nexus
    }

    /// What this Principal may do in a Space, resolved fresh.
    pub async fn effective_authority(
        &self,
        space_id: &str,
    ) -> Result<EffectiveAuthority, KipError> {
        EffectiveAuthority::resolve(&self.nexus.store, space_id, &self.auth).await
    }

    /// Reads the Governance audit for a Space (§29).
    ///
    /// Its own permission, because the audit says what everyone else did: a
    /// caller who may read a Space's cognition has not thereby earned the right
    /// to read who has been reading it.
    pub async fn read_audit(
        &self,
        space_id: &str,
        limit: usize,
    ) -> Result<Vec<crate::governance::rows::GovernanceAuditRow>, KipError> {
        let _guard = self.nexus.lock.read().await;
        let authority = self.authority(space_id, &self.auth).await?;
        authority
            .authorize(
                Permission::ReadAudit,
                &ResourceContext::default(),
                &self.auth,
            )
            .into_result()?;
        self.nexus
            .store
            .governance
            .read_audit(space_id, limit)
            .await
    }

    /// What this Principal could do in a Space at a past instant (§48.5).
    ///
    /// A historical answer, and nothing more: that a Principal could read
    /// something in January says nothing about whether it can today (§48.5).
    /// Reading it needs `read_governance_history`, which is separate from
    /// `read_audit` — one is what the control plane *was*, the other is what
    /// people *did*.
    pub async fn access_as_of(
        &self,
        space_id: &str,
        at: &str,
    ) -> Result<EffectiveAuthority, KipError> {
        let _guard = self.nexus.lock.read().await;
        let now = self.authority(space_id, &self.auth).await?;
        now.authorize(
            Permission::ReadGovernanceHistory,
            &ResourceContext::default(),
            &self.auth,
        )
        .into_result()?;
        let at = crate::time::normalize(at, "AS OF")?;
        EffectiveAuthority::resolve_at(&self.nexus.store, space_id, &self.auth, &at).await
    }

    /// Raises or lowers how strongly one element may influence action.
    ///
    /// Raising is bounded by the element's authority lineage, so no chain of
    /// summarizing turns a descriptive note into an executable one (§31.5).
    /// Lowering is deliberately as easy as the permission itself: an incident
    /// response that had to wait for an approval would arrive late (§31.5).
    ///
    /// Returns the ceiling the element carried before.
    pub async fn elevate_authority(
        &self,
        space_id: &str,
        element: crate::id::ElementId,
        class: &str,
    ) -> Result<String, KipError> {
        let _guard = self.nexus.lock.write().await;
        self.nexus.store.reopen_if_poisoned().await?;
        let authority = self.authority(space_id, &self.auth).await?;
        crate::governance::element::elevate_authority(
            &self.nexus.store,
            space_id,
            element,
            class,
            &authority,
            &self.auth,
        )
        .await
    }

    /// Holds an element out of ordinary use, pending review (§39.2).
    ///
    /// Not a retraction: it says this Brain does not currently allow ordinary
    /// use of the element, which is a statement about this Brain and not about
    /// whoever wrote it (§39.2).
    pub async fn quarantine(
        &self,
        space_id: &str,
        element: crate::id::ElementId,
        reason: &str,
    ) -> Result<(), KipError> {
        let _guard = self.nexus.lock.write().await;
        self.nexus.store.reopen_if_poisoned().await?;
        let authority = self.authority(space_id, &self.auth).await?;
        crate::governance::element::quarantine(
            &self.nexus.store,
            space_id,
            element,
            reason,
            &authority,
            &self.auth,
        )
        .await
    }

    /// Returns a quarantined element to ordinary use.
    pub async fn release_quarantine(
        &self,
        space_id: &str,
        element: crate::id::ElementId,
    ) -> Result<(), KipError> {
        let _guard = self.nexus.lock.write().await;
        self.nexus.store.reopen_if_poisoned().await?;
        let authority = self.authority(space_id, &self.auth).await?;
        crate::governance::element::release(
            &self.nexus.store,
            space_id,
            element,
            &authority,
            &self.auth,
        )
        .await
    }

    /// Acts on the elements whose retention has lapsed (§19.1, §19.2).
    ///
    /// `retention.expires_at` says when the *record* stops being kept. It is not
    /// `valid_time.until`, which says when the claim stops applying, and it is not
    /// archival, which says the element is out of ordinary recall while still
    /// being kept. Until this existed the field was written, indexed, and never
    /// read — a caller could set a 90-day expiry and the engine would keep the
    /// record forever without ever saying it would not honor it.
    ///
    /// This is an explicit sweep rather than a background timer, and the
    /// capability answer says so. A Nexus is a library inside somebody's process:
    /// a thread that deleted memory on its own schedule would act while no request
    /// was in flight and no Principal was accountable for it. The host decides
    /// when forgetting happens; the engine decides what may be forgotten.
    ///
    /// Four gates, in this order:
    ///
    /// 1. `manage_retention` at Space scope — reaching the whole Space's lifecycle
    ///    is not something an element-scoped Grant should confer;
    /// 2. the action's own permission per element (`archive`, `tombstone`,
    ///    `purge`), because expiry is not an exemption from what those cost;
    /// 3. `legal_hold`, which stops erasure for everyone (§60.3);
    /// 4. per-element authorization, so a sweep cannot reach what the caller
    ///    cannot see.
    ///
    /// A held or unauthorized element is **skipped and counted**, not silently
    /// dropped: the answer says how many were left and why, because "swept 4"
    /// when 9 expired is the shape of a compliance failure nobody notices.
    pub async fn sweep_expired(
        &self,
        space_id: &str,
        action: RetentionAction,
        limit: usize,
    ) -> Result<RetentionSweep, KipError> {
        let _guard = self.nexus.lock.write().await;
        self.nexus.store.reopen_if_poisoned().await?;
        let authority = self.authority(space_id, &self.auth).await?;
        let resource = ResourceContext::default();
        Approved::require(
            crate::governance::approval::resolve(
                &self.nexus.store,
                space_id,
                &resource,
                authority.authorize(Permission::ManageRetention, &resource, &self.auth),
                &self.auth,
            )
            .await?,
        )?
        .spend(&self.nexus.store)
        .await?;

        let now = crate::time::now();
        let mut report = RetentionSweep::default();
        let expired = self.nexus.store.expired_elements(space_id, &now).await?;
        for id in expired {
            if report.swept.len() >= limit {
                report.remaining += 1;
                continue;
            }
            let element = match self.nexus.store.get_element(id).await {
                Ok(element) => element,
                Err(_) => continue,
            };
            // §60.3: a hold blocks removal for everyone, including a sweep the
            // holder authorized. Reported as held rather than as failed, because
            // nothing went wrong — the record is being kept on purpose.
            if element
                .retention()
                .get("legal_hold")
                .and_then(anda_kip::Json::as_bool)
                .unwrap_or(false)
            {
                report.held += 1;
                continue;
            }
            let outcome = match action {
                RetentionAction::Archive => {
                    crate::governance::element::archive_expired(
                        &self.nexus.store,
                        space_id,
                        id,
                        &authority,
                        &self.auth,
                    )
                    .await
                }
                RetentionAction::Tombstone => {
                    crate::governance::element::tombstone_expired(
                        &self.nexus.store,
                        space_id,
                        id,
                        &authority,
                        &self.auth,
                    )
                    .await
                }
            };
            match outcome {
                Ok(()) => report.swept.push(id.to_string()),
                Err(_) => report.refused += 1,
            }
        }
        Ok(report)
    }

    /// Marks the Assertions whose validity windows have closed as `expired`.
    ///
    /// §14.3's lifecycle state, reached explicitly. The alternative — deriving
    /// it on every read and never recording it — leaves `expired` as a state
    /// the model names and nothing ever produces, and leaves a caller unable
    /// to ask which claims have lapsed without recomputing the answer itself.
    ///
    /// Not retraction and not supersession (§14.1, §14.2): nobody withdrew
    /// these and nothing replaced them; their own stated windows ran out.
    pub async fn expire_lapsed_assertions(
        &self,
        space_id: &str,
        limit: usize,
    ) -> Result<Vec<String>, KipError> {
        let _guard = self.nexus.lock.write().await;
        self.nexus.store.reopen_if_poisoned().await?;
        let authority = self.authority(space_id, &self.auth).await?;
        let now = crate::time::now();
        let mut expired = Vec::new();
        for id in self.nexus.store.lapsed_assertions(space_id, &now).await? {
            if expired.len() >= limit {
                break;
            }
            if crate::governance::element::expire_assertion(
                &self.nexus.store,
                space_id,
                id,
                &authority,
                &self.auth,
            )
            .await
            .unwrap_or(false)
            {
                expired.push(id.to_string());
            }
        }
        Ok(expired)
    }

    /// Designates the Concept this Space treats as its semantic `$self` (§5.6).
    ///
    /// A Governance operation and not a KML clause, because §5.6 makes the
    /// designation protected Space configuration: "ordinary KML MUST NOT
    /// create or change it". Cognitive content that could name the Brain's own
    /// identity would be content deciding who the Brain is, which is the
    /// laundering §88.8 is about.
    ///
    /// Every Capsule rule about source and destination `$self` (§38.4, §38.5)
    /// refers to this designation, and a Space that has designated none has no
    /// `$self` for those rules to map onto.
    ///
    /// Pass `None` to clear it.
    pub async fn designate_self(
        &self,
        space_id: &str,
        concept: Option<crate::id::ElementId>,
    ) -> Result<(), KipError> {
        let _guard = self.nexus.lock.write().await;
        self.nexus.store.reopen_if_poisoned().await?;
        let authority = self.authority(space_id, &self.auth).await?;
        let decision = authority.authorize(
            Permission::ManagePolicy,
            &ResourceContext::default(),
            &self.auth,
        );
        Approved::require(
            crate::governance::approval::resolve(
                &self.nexus.store,
                space_id,
                &ResourceContext::default(),
                decision,
                &self.auth,
            )
            .await?,
        )?
        .spend(&self.nexus.store)
        .await?;

        let mut row = self.nexus.store.get_space(space_id).await?;
        row.self_concept = match concept {
            Some(id) => {
                // Refused rather than stored as a name nothing resolves: every
                // `$self` rule downstream dereferences it, and a dangling one
                // would make the Space's own identity a broken link.
                let element = self.nexus.store.get_element(id).await?;
                if element.kind() != anda_kip::ElementKind::Concept {
                    return Err(KipError::structural_reference_invalid(format!(
                        "{id} is a {:?}; a Space's self identity is a Concept (§5.6)",
                        element.kind()
                    )));
                }
                if element.space() != space_id {
                    return Err(KipError::structural_reference_invalid(format!(
                        "{id} belongs to another Space; a self identity is Space-local (§5.3)"
                    )));
                }
                id.to_string()
            }
            None => String::new(),
        };
        self.nexus.store.put_space(&row).await
    }

    // --- the governed control plane -------------------------------------
    //
    // §29 registers a name for each control-plane operation, and until these
    // existed no gate asked for any of them: a Grant listing `manage_grants`
    // conferred nothing, which is the failure mode the registry exists to
    // prevent — authority that looks conferred and is not, discovered during
    // an incident.
    //
    // These do not put the control plane in reach of cognition. No KML clause
    // and no META command resolves to any of them, which is what keeps a
    // prompt injection off the plane; they are host calls, and what changed is
    // that a host call made *as a Principal* is now authorized as that
    // Principal. `nexus.governance()` remains the host's own unguarded path,
    // for the bootstrap that has to happen before any Grant exists.

    /// Takes the one approval a control-plane operation needs, at Space scope.
    async fn gate_control_plane(
        &self,
        space_id: &str,
        permission: Permission,
    ) -> Result<Vec<Approved>, KipError> {
        let authority = self.authority(space_id, &self.auth).await?;
        let resource = ResourceContext::default();
        let decision = authority.authorize(permission, &resource, &self.auth);
        self.gate(&authority, &self.auth, vec![decision]).await
    }

    /// Spends approvals only after the operation they authorized succeeded.
    async fn spend(&self, approvals: Vec<Approved>) -> Result<(), KipError> {
        for approved in approvals {
            approved.spend(&self.nexus.store).await?;
        }
        Ok(())
    }

    /// Creates a Grant in this Space (§29, `manage_grants`).
    ///
    /// The actions are checked against the registry before the record is
    /// written: a Grant naming a permission this engine does not implement
    /// confers nothing, and the holder must learn that here rather than during
    /// an incident.
    pub async fn create_grant(
        &self,
        space_id: &str,
        draft: GrantDraft,
    ) -> Result<GrantRow, KipError> {
        let _guard = self.nexus.lock.write().await;
        self.nexus.store.reopen_if_poisoned().await?;
        let approvals = self
            .gate_control_plane(space_id, Permission::ManageGrants)
            .await?;
        for action in &draft.actions {
            Permission::parse(action)?;
        }
        let row = self
            .nexus
            .governance()
            .create_grant(
                GrantDraft {
                    space_id: space_id.to_string(),
                    ..draft
                },
                &self.auth.principal_id,
            )
            .await?;
        self.spend(approvals).await?;
        Ok(row)
    }

    /// Revokes a Grant (§29, `manage_grants`). Revoked, never deleted.
    pub async fn revoke_grant(&self, space_id: &str, id: u64) -> Result<(), KipError> {
        let _guard = self.nexus.lock.write().await;
        self.nexus.store.reopen_if_poisoned().await?;
        let approvals = self
            .gate_control_plane(space_id, Permission::ManageGrants)
            .await?;
        self.nexus
            .governance()
            .revoke_grant(id, &self.auth.principal_id)
            .await?;
        self.spend(approvals).await
    }

    /// Creates a Delegation (§29).
    ///
    /// Which permission this asks for depends on whose authority is being
    /// passed on, and the distinction is the whole reason both names exist:
    /// conferring part of *one's own* authority is `delegate`, and
    /// administering a Delegation between two other Principals is
    /// `manage_delegation`. Collapsing them would let anyone who may delegate
    /// their own authority hand out somebody else's.
    pub async fn create_delegation(
        &self,
        space_id: &str,
        draft: DelegationDraft,
    ) -> Result<DelegationRow, KipError> {
        let _guard = self.nexus.lock.write().await;
        self.nexus.store.reopen_if_poisoned().await?;
        let permission = if draft.delegator_principal == self.auth.principal_id {
            Permission::Delegate
        } else {
            Permission::ManageDelegation
        };
        let approvals = self.gate_control_plane(space_id, permission).await?;
        for action in &draft.actions {
            Permission::parse(action)?;
        }
        let row = self
            .nexus
            .governance()
            .create_delegation(
                DelegationDraft {
                    space_id: space_id.to_string(),
                    ..draft
                },
                &self.auth.principal_id,
            )
            .await?;
        self.spend(approvals).await?;
        Ok(row)
    }

    /// Revokes a Delegation (§29, `manage_delegation`).
    ///
    /// Revoking one's own asks for the same thing as revoking another's:
    /// unlike conferring, withdrawing authority is never the more dangerous
    /// direction, and a caller who could not reach the record could not
    /// withdraw at all.
    pub async fn revoke_delegation(&self, space_id: &str, id: u64) -> Result<(), KipError> {
        let _guard = self.nexus.lock.write().await;
        self.nexus.store.reopen_if_poisoned().await?;
        let approvals = self
            .gate_control_plane(space_id, Permission::ManageDelegation)
            .await?;
        self.nexus
            .governance()
            .revoke_delegation(id, &self.auth.principal_id)
            .await?;
        self.spend(approvals).await
    }

    /// Creates or replaces a Principal group (§29, `manage_membership`).
    pub async fn put_group(
        &self,
        space_id: &str,
        draft: GroupDraft,
    ) -> Result<PrincipalGroupRow, KipError> {
        let _guard = self.nexus.lock.write().await;
        self.nexus.store.reopen_if_poisoned().await?;
        let approvals = self
            .gate_control_plane(space_id, Permission::ManageMembership)
            .await?;
        let row = self
            .nexus
            .governance()
            .put_group(draft, &self.auth.principal_id)
            .await?;
        self.spend(approvals).await?;
        Ok(row)
    }

    /// Suspends or restores a Principal (§29, `manage_membership`).
    pub async fn set_principal_status(
        &self,
        space_id: &str,
        principal_id: &str,
        status: &str,
    ) -> Result<PrincipalRow, KipError> {
        let _guard = self.nexus.lock.write().await;
        self.nexus.store.reopen_if_poisoned().await?;
        let approvals = self
            .gate_control_plane(space_id, Permission::ManageMembership)
            .await?;
        let row = self
            .nexus
            .governance()
            .set_principal_status(principal_id, status, &self.auth.principal_id)
            .await?;
        self.spend(approvals).await?;
        Ok(row)
    }

    /// Binds a Principal to a semantic actor (§17, `manage_actor_binding`).
    ///
    /// The record that decides whether writing `asserted_by: ?alice` is
    /// attributed recording or speaking as Alice, so writing one is more
    /// authority than either — a writer who could bind itself could authorize
    /// its own impersonation.
    pub async fn create_binding(
        &self,
        space_id: &str,
        draft: ActorBindingDraft,
    ) -> Result<ActorBindingRow, KipError> {
        let _guard = self.nexus.lock.write().await;
        self.nexus.store.reopen_if_poisoned().await?;
        let approvals = self
            .gate_control_plane(space_id, Permission::ManageActorBinding)
            .await?;
        let row = self
            .nexus
            .governance()
            .create_binding(draft, &self.auth.principal_id)
            .await?;
        self.spend(approvals).await?;
        Ok(row)
    }

    /// Revokes an ActorBinding (§17, `manage_actor_binding`).
    pub async fn revoke_binding(&self, space_id: &str, id: u64) -> Result<(), KipError> {
        let _guard = self.nexus.lock.write().await;
        self.nexus.store.reopen_if_poisoned().await?;
        let approvals = self
            .gate_control_plane(space_id, Permission::ManageActorBinding)
            .await?;
        self.nexus
            .governance()
            .revoke_binding(id, &self.auth.principal_id)
            .await?;
        self.spend(approvals).await
    }

    /// Publishes a Governance Policy version (§29, `manage_policy`).
    pub async fn publish_policy(
        &self,
        space_id: &str,
        draft: PolicyDraft,
    ) -> Result<GovernancePolicyRow, KipError> {
        let _guard = self.nexus.lock.write().await;
        self.nexus.store.reopen_if_poisoned().await?;
        let approvals = self
            .gate_control_plane(space_id, Permission::ManagePolicy)
            .await?;
        let row = self
            .nexus
            .governance()
            .publish_policy(draft, &self.auth.principal_id)
            .await?;
        self.spend(approvals).await?;
        Ok(row)
    }

    /// Supplies one of the independent approvals a high-risk operation needs
    /// (§40, `approve_high_risk`).
    ///
    /// Its own permission rather than the operation's: the point of an
    /// independent approval is that the approver is not the one asking, so the
    /// authority to approve cannot be the authority to act.
    pub async fn approve(
        &self,
        space_id: &str,
        id: u64,
        note: &str,
    ) -> Result<ApprovalRow, KipError> {
        let _guard = self.nexus.lock.write().await;
        self.nexus.store.reopen_if_poisoned().await?;
        let approvals = self
            .gate_control_plane(space_id, Permission::ApproveHighRisk)
            .await?;
        let row = self
            .nexus
            .governance()
            .approve(id, &self.auth.principal_id, note)
            .await?;
        self.spend(approvals).await?;
        Ok(row)
    }

    /// Installs a Schema Package artifact (§20, `manage_schema`).
    ///
    /// Installing does not activate: what a symbol means in this Space is
    /// decided by the Schema Lock, and this only makes an artifact available
    /// to be locked onto.
    pub async fn install_package(
        &self,
        space_id: &str,
        artifact: &SchemaPackage,
        source: &str,
    ) -> Result<crate::schema::PackageRef, KipError> {
        let approvals = self
            .gate_control_plane(space_id, Permission::ManageSchema)
            .await?;
        let package_ref = self.nexus.install_package(artifact, source).await?;
        self.spend(approvals).await?;
        Ok(package_ref)
    }

    /// Activates a Schema Lock over the installed artifacts (§20,
    /// `manage_schema`).
    ///
    /// The operation that changes what every stored symbol resolves to, which
    /// is why it is gated rather than treated as configuration: a package
    /// swapped underneath a Space rewrites the meaning of cognition already
    /// written.
    pub async fn activate_schema(
        &self,
        space_id: &str,
        lock: crate::schema::SchemaLock,
    ) -> Result<SchemaEnvironment, KipError> {
        let approvals = self
            .gate_control_plane(space_id, Permission::ManageSchema)
            .await?;
        let env = self.nexus.activate_schema(space_id, lock).await?;
        self.spend(approvals).await?;
        Ok(env)
    }

    /// Accepts another Brain's cognition into a Space (§29, `import`).
    ///
    /// Its own permission, and not `create`: the difference between writing
    /// what this Brain concluded and admitting what another one did is the
    /// whole of §78, and an importer running under a writer's Grant would
    /// erase it. `isolate` lands the records in quarantine (§39.2), which is
    /// the honest answer to "should I accept this?" — accept it where it
    /// cannot do anything, and decide afterwards.
    pub async fn import_capsule(
        &self,
        space_id: &str,
        capsule: &anda_kip::Capsule,
        isolate: bool,
    ) -> Result<crate::capsule::ImportReport, KipError> {
        let approvals = self
            .gate_control_plane(space_id, Permission::Import)
            .await?;
        let guard = self.nexus.lock.write().await;
        self.nexus.store.reopen_if_poisoned().await?;
        let report = crate::capsule::import(
            &self.nexus,
            capsule,
            space_id,
            false,
            (*self.auth).clone(),
            isolate,
        )
        .await?;
        drop(guard);
        self.spend(approvals).await?;
        Ok(report)
    }

    /// Sets one element's classification (§93, §100).
    ///
    /// A Governance operation rather than a KML clause, because an element's
    /// `governance` block is not author-writable: the protocol's own parser
    /// refuses it in every assignment. Raising a label needs `update` and
    /// lowering one needs `declassify` — it is disclosure that requires
    /// authority, not caution.
    ///
    /// Returns the label the element carried before.
    pub async fn classify(
        &self,
        space_id: &str,
        element: crate::id::ElementId,
        classification: &str,
    ) -> Result<String, KipError> {
        let _guard = self.nexus.lock.write().await;
        self.nexus.store.reopen_if_poisoned().await?;
        let authority = self.authority(space_id, &self.auth).await?;
        crate::governance::element::classify(
            &self.nexus.store,
            space_id,
            element,
            classification,
            &authority,
            &self.auth,
        )
        .await
    }
}

#[async_trait]
impl Executor for Session {
    async fn execute(
        &self,
        command: Command,
        request: &Request,
        operation: &Operation,
    ) -> Response {
        let space = match self.nexus.space_of(request).await {
            Ok(space) => space,
            Err(err) => return Response::from(err),
        };
        if let Err(err) = self.check_envelope(&space, request).await {
            return Response::from(err);
        }
        // The envelope contributes a purpose and a client label and nothing
        // else. Identity, strength and delegation come from the host (§10).
        let auth = self.auth.merged_with_request(request);

        match command {
            Command::Kml(statement) => {
                // Exclusive: readers must not observe a partly-written
                // transaction, and `anda_db` cannot make the multi-row write
                // atomic on its own.
                let _guard = self.nexus.lock.write().await;
                if let Err(err) = self.nexus.store.reopen_if_poisoned().await {
                    return Response::from(err);
                }
                // Resolved under the write lock, so a Grant revoked while this
                // request was queued is already gone when it is read (§28.6).
                let authority = match self.authority(&space, &auth).await {
                    Ok(authority) => authority,
                    Err(err) => return Response::from(err),
                };
                let permissions = gate::kml_permissions(&statement);

                // §26, §33: a timeout is not an abort. A client that lost its
                // response resends the same key and gets the outcome its first
                // attempt produced, rather than writing a second time or being
                // told its own write is a conflict.
                match self
                    .replay(
                        &space,
                        &statement,
                        request,
                        operation,
                        &authority,
                        &auth,
                        &permissions,
                    )
                    .await
                {
                    Ok(Some(response)) => return response,
                    Ok(None) => {}
                    Err(err) => return Response::from(err),
                }

                // No approval guard here: the exclusive write lock above
                // already serializes everything that could spend an approval.
                let base = base_authorizations(&authority, &auth, permissions);
                let decisions = match self.gate(&authority, &auth, base).await {
                    Ok(decisions) => decisions,
                    Err(err) => return Response::from(err),
                };
                let response = crate::kml::execute(
                    &self.nexus.store,
                    &space,
                    &statement,
                    request,
                    operation,
                    &authority,
                    &auth,
                )
                .await;
                let response = self.settle(response, decisions).await;
                // A poison event costs no further command: the next mutation
                // would be rejected outright, so recovery happens here rather
                // than being deferred to the caller's next attempt.
                if self.nexus.store.has_poisoned_handle() {
                    let _ = self.nexus.store.reopen().await;
                }
                response
            }
            Command::Kql(query) => {
                // Shared: readers may run concurrently, but none of them
                // overlaps a commit.
                let _guard = self.nexus.lock.read().await;
                let authority = match self.authority(&space, &auth).await {
                    Ok(authority) => authority,
                    Err(err) => return Response::from(err),
                };
                let base = base_authorizations(&authority, &auth, gate::kql_permissions(&query));
                let _approval_guard = self.approval_guard(&base).await;
                let decisions = match self.gate(&authority, &auth, base).await {
                    Ok(decisions) => decisions,
                    Err(err) => return Response::from(err),
                };
                let response = crate::kql::execute(
                    &self.nexus.store,
                    &space,
                    &query,
                    request,
                    operation,
                    &authority,
                    &auth,
                )
                .await;
                self.settle(response, decisions).await
            }
            Command::Meta(command) => {
                // META is semantically read-only (§63.2), so it shares the
                // lock with KQL rather than taking it exclusively.
                let _guard = self.nexus.lock.read().await;
                let authority = match self.authority(&space, &auth).await {
                    Ok(authority) => authority,
                    Err(err) => return Response::from(err),
                };
                let base = base_authorizations(&authority, &auth, gate::meta_permissions(&command));
                let _approval_guard = self.approval_guard(&base).await;
                let decisions = match self.gate(&authority, &auth, base).await {
                    Ok(decisions) => decisions,
                    Err(err) => return Response::from(err),
                };
                let response = crate::meta::execute(
                    &self.nexus.store,
                    &space,
                    &command,
                    request,
                    operation,
                    &authority,
                    &auth,
                )
                .await;
                self.settle(response, decisions).await
            }
        }
    }
}

/// What a retention sweep does with an element whose retention has lapsed.
///
/// Purge is deliberately absent. §19.3 makes physical erasure a high-impact
/// operation with its own reference policy, its own confirmation and its own
/// destruction of the version log; running it over a set the caller never
/// enumerated would be the largest irreversible action this engine can take,
/// reached by a maintenance call. A host that means to erase expired records
/// sweeps them to tombstones first and purges those it has looked at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetentionAction {
    /// Out of ordinary recall, still readable and still referenced.
    Archive,
    /// Withdrawn from use, identity and references intact.
    Tombstone,
}

/// What one retention sweep did, and what it left alone.
#[derive(Clone, Debug, Default)]
pub struct RetentionSweep {
    /// The elements it acted on.
    pub swept: Vec<String>,
    /// How many were kept because a legal hold blocks removal (§60.3).
    pub held: usize,
    /// How many the caller was not authorized to act on.
    pub refused: usize,
    /// How many were left for the next sweep by `limit`.
    pub remaining: usize,
}

/// The Space-scope decision for each permission a command needs.
///
/// Resolved once and then read twice — by the approval guard and by the gate.
/// `EffectiveAuthority::authorize` re-parses every statement of the governing
/// policy on each call, so asking it the same question twice is not free.
fn base_authorizations(
    authority: &EffectiveAuthority,
    auth: &AuthContext,
    permissions: Vec<Permission>,
) -> Vec<Authorization> {
    let resource = ResourceContext::default();
    permissions
        .into_iter()
        .map(|permission| authority.authorize(permission, &resource, auth))
        .collect()
}

impl Session {
    /// The recorded outcome of a write this key already committed (§26, §33).
    ///
    /// `None` when there is nothing to replay — no key, a dry run, or a key
    /// this Space has not seen. A dry run is excluded in both directions: a
    /// preview establishes no durable commit (§69.3), so there is nothing to
    /// replay and nothing to record, and answering one from an earlier real
    /// commit would report a write as a preview of itself.
    ///
    /// The permission is checked exactly as it would be for the write itself,
    /// so a caller who could not have run the command cannot learn what it did.
    /// An outstanding *approval* obligation deliberately does not block it: an
    /// approval authorizes the work, and on a replay the work already happened
    /// — demanding a second one to learn the outcome of the first is what would
    /// make a lost response unrecoverable.
    #[allow(clippy::too_many_arguments)]
    async fn replay(
        &self,
        space: &str,
        statement: &anda_kip::KmlStatement,
        request: &Request,
        operation: &Operation,
        authority: &EffectiveAuthority,
        auth: &AuthContext,
        permissions: &[Permission],
    ) -> Result<Option<Response>, KipError> {
        if request.is_dry_run() {
            return Ok(None);
        }
        let key = crate::kml::idempotency_key(request, operation);
        if key.is_empty() {
            return Ok(None);
        }
        let Some(row) = self
            .nexus
            .store
            .find_transaction_by_idempotency_key(space, &key)
            .await?
        else {
            return Ok(None);
        };

        // Authorized before it answers — a replay is still a read of what
        // this Space did — and before the conflict below, whose refusal names
        // the transaction the key already committed: a caller that may not
        // make this write may not learn that either. `ts/kip-do` orders the
        // two the same way.
        let resource = ResourceContext::default();
        for permission in permissions {
            let decision = authority.authorize(*permission, &resource, auth);
            if decision.decision == crate::governance::Decision::RequireApproval {
                continue;
            }
            decision.into_result()?;
        }

        // §34.4: the same key on different work is a caller bug, not a retry.
        // Replaying the first outcome would tell the second write it
        // succeeded, hand back a receipt for a transaction that did something
        // else, and leave the work it asked for undone — silently, since the
        // response looks ordinary. An empty stored digest is a transaction
        // journalled before this check existed; those replay as they did.
        let digest = crate::kml::request_digest(statement, request, operation);
        if !row.request_digest.is_empty() && row.request_digest != digest {
            return Err(anda_kip::KipError::new(
                anda_kip::KipErrorCode::IdempotencyConflict,
                format!(
                    "idempotency key {key:?} already committed transaction {} for a different \
                     request; a key names one piece of work, and reusing it for another would \
                     answer this one with that one's receipt",
                    row.tx_id
                ),
            ));
        }

        Ok(Some(crate::kml::replay(&row)))
    }

    /// Checks the envelope fields that decide whether a command may run at all.
    ///
    /// `anda_kip::Executor` asks an implementation to honor every applicable
    /// request field "or fail explicitly", and these three are the ones where
    /// ignoring them changes what the caller gets rather than merely what it is
    /// told:
    ///
    /// - **`preconditions`** (§35.4) is the caller's optimistic-concurrency guard.
    ///   Executing past a stale `space_seq` commits the write the guard existed to
    ///   stop, and the caller has no way to notice.
    /// - **`requires`** (§67) is a fail-fast capability check. Running a command
    ///   that needed semantic search and answering it with keyword results is a
    ///   wrong answer wearing a success status.
    /// - **`options.deadline_ms`** (§80.1) is the caller's execution window.
    ///   Accepting a deadline this engine cannot enforce would be a promise, and
    ///   §80.2 is explicit that a client timeout is not an abort — so the honest
    ///   move is to say so rather than to imply a cancellation that will not
    ///   happen.
    async fn check_envelope(&self, space: &str, request: &Request) -> Result<(), KipError> {
        if let Some(preconditions) = &request.preconditions {
            let row = self.nexus.store.get_space(space).await?;
            if let Some(expected) = preconditions.space_seq
                && row.seq != expected
            {
                return Err(KipError::precondition_failed(format!(
                    "this request expects {space} at sequence {expected}, and it is at {}",
                    row.seq
                )));
            }
            if let Some(expected) = preconditions.schema_environment_version
                && row.schema_environment_version != expected
            {
                return Err(KipError::precondition_failed(format!(
                    "this request expects Schema Environment version {expected}, and {space} is on \
                     version {}",
                    row.schema_environment_version
                )));
            }
        }

        for (name, wanted) in request.requires.iter().flatten() {
            // A requirement is satisfied only by a capability this engine can
            // name. An unknown one is refused rather than assumed present: a
            // fail-fast check that passes because nobody recognized it is worse
            // than no check, because the caller believes it ran.
            let satisfied = crate::meta::capability_state(name);
            match (satisfied, wanted) {
                (Some(true), Json::Bool(true)) | (Some(false), Json::Bool(false)) => {}
                (Some(have), wanted) => {
                    return Err(KipError::unsupported_capability(format!(
                        "this request requires the capability {name:?} to be {wanted}, and this \
                         engine reports {have}; DESCRIBE CAPABILITIES lists what it does and does \
                         not implement"
                    )));
                }
                (None, _) => {
                    return Err(KipError::unsupported_capability(format!(
                        "this request requires the capability {name:?}, which this engine does not \
                         recognize; it will not report an unknown requirement as satisfied"
                    )));
                }
            }
        }

        // §32.2: weaker isolation must be capability-declared, and a request
        // for stronger isolation must never be silently satisfied. This engine
        // serializes every mutation behind one write lock, which is
        // `serializable` and nothing else — so a name it cannot map onto that
        // is refused rather than echoed back as if it had been honoured.
        if let Some(execution) = &request.execution
            && let Some(isolation) = &execution.isolation
            && isolation != "serializable"
        {
            return Err(anda_kip::KipError::new(
                anda_kip::KipErrorCode::UnsupportedIsolation,
                format!(
                    "this engine commits every mutation under one exclusive write lock, which is \
                     `serializable`, and offers no other isolation; it will not accept \
                     {isolation:?} by ignoring it"
                ),
            ));
        }

        if let Some(options) = &request.options
            && options.deadline_ms.is_some()
        {
            return Err(KipError::unsupported_capability(
                "this engine does not enforce `options.deadline_ms`: a KML statement runs under one \
                 exclusive lock and is not cancellable mid-commit, and §80.2 is explicit that a \
                 client timeout is not an abort. Accepting the deadline would promise a cancellation \
                 that never happens",
            ));
        }

        Ok(())
    }

    async fn authority(
        &self,
        space: &str,
        auth: &AuthContext,
    ) -> Result<EffectiveAuthority, KipError> {
        EffectiveAuthority::resolve(&self.nexus.store, space, auth).await
    }

    /// Requires every permission a command asks for, at Space scope.
    ///
    /// Space scope rather than element scope, because at this point no element
    /// has been read yet — and reading one to decide whether it may be read
    /// would be the disclosure the check exists to prevent. Per-element
    /// authorization happens where the elements are.
    async fn gate(
        &self,
        authority: &EffectiveAuthority,
        auth: &AuthContext,
        needed: Vec<Authorization>,
    ) -> Result<Vec<Approved>, KipError> {
        let resource = ResourceContext::default();
        let mut decisions = Vec::with_capacity(needed.len());
        for base in needed {
            // A policy may require independent approval for a whole command
            // family — declassification, elevation, export — and a satisfied
            // approval is what turns that into an allow. An unsatisfied one
            // stays a refusal: `require_approval` is not a soft yes (§40).
            let decision = crate::governance::approval::resolve(
                &self.nexus.store,
                &authority.space.space_id,
                &resource,
                base,
                auth,
            )
            .await?;
            if !decision.is_permitted() {
                self.audit(authority, auth, &decision).await;
            }
            let approved = Approved::require(decision)?;
            if approved.decision().obligations.audit {
                self.audit(authority, auth, approved.decision()).await;
            }
            decisions.push(approved);
        }
        Ok(decisions)
    }

    /// Serializes the commands that could otherwise spend one approval twice.
    ///
    /// Only the read paths need it: a KML statement already runs under the
    /// exclusive write lock, and taking a second lock under it would only add
    /// a way to deadlock.
    async fn approval_guard(
        &self,
        base: &[Authorization],
    ) -> Option<tokio::sync::MutexGuard<'_, ()>> {
        if base
            .iter()
            .all(|decision| decision.obligations.approvals_required == 0)
        {
            return None;
        }
        Some(self.nexus.approval_lock.lock().await)
    }

    /// Spends the approvals a command carried, once it has actually succeeded.
    ///
    /// A failed attempt leaves them unspent: an approval buys one completed
    /// operation, not one try at it.
    async fn settle(&self, response: Response, approvals: Vec<Approved>) -> Response {
        if response.status != anda_kip::TopLevelStatus::Succeeded {
            return response;
        }
        for approved in approvals {
            if let Err(err) = approved.spend(&self.nexus.store).await {
                return Response::from(err);
            }
        }
        response
    }

    /// Writes one decision to the Governance audit.
    ///
    /// Best effort by design at this layer: a denial that could not be logged
    /// is still a denial, and failing the request a second time over the log
    /// would turn an audit outage into an availability outage. An obligation
    /// that genuinely must not proceed unlogged is the caller's to enforce
    /// (§86.1), and those paths check the write.
    async fn audit(
        &self,
        authority: &EffectiveAuthority,
        auth: &AuthContext,
        decision: &Authorization,
    ) {
        let _ = self
            .nexus
            .store
            .governance
            .record_decision(crate::governance::rows::GovernanceAuditRow {
                at: crate::time::now(),
                space_id: authority.space.space_id.clone(),
                principal_id: auth.principal_id.clone(),
                delegation_chain: auth.delegation_chain.clone(),
                operation: decision.permission.as_str().to_string(),
                decision: decision.decision.as_str().to_string(),
                reason: decision.reason.clone(),
                policy_id: decision.policy_id.clone(),
                policy_version: decision.policy_version,
                authorities_used: decision.authorities_used.clone(),
                ..Default::default()
            })
            .await;
    }
}

#[async_trait]
impl Executor for CognitiveNexus {
    /// Runs a command as the system Principal.
    ///
    /// This is the embedded case: one process, one owner, and the process *is*
    /// the owner. It is a real authorization — the system Principal owns the
    /// default Space and the decision goes through the same path as anyone
    /// else's — rather than a bypass, so a Space whose policy denies something
    /// denies it here too.
    ///
    /// A host serving more than one caller must not use this. Authenticate and
    /// go through [`CognitiveNexus::session`], or every caller is the owner.
    async fn execute(
        &self,
        command: Command,
        request: &Request,
        operation: &Operation,
    ) -> Response {
        self.system_session()
            .execute(command, request, operation)
            .await
    }
}

/// The command families this engine can currently run.
///
/// Stated as data rather than prose so a caller can branch on it instead of
/// discovering the gap through an error.
pub fn supported_command_types() -> &'static [CommandType] {
    &[CommandType::Kml, CommandType::Kql, CommandType::Meta]
}
