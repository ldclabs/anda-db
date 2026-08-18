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
    Command, CommandType, Executor, KipError, Operation, Request, Response, SpaceSelector,
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

use crate::governance::approval::Approved;
use crate::governance::rows::principal_class;
use crate::governance::store::PrincipalDraft;
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
        // readable on purpose (§214) instead of by an absent check.
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
        // Loading writes through the engine, so it has to come after Governance
        // and Core are up. A failure here fails `connect`: a half-migrated
        // brain that answers queries is worse than one that refuses to start,
        // because the answers look ordinary.
        crate::migrate::load(&nexus).await?;
        Ok(nexus)
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

    /// A session as the engine's own Principal (§212).
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
    /// controls the Space (§264).
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
    /// Installing is still not activating (§240.18): this activates because the
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
    /// environment version (§143), so a host that unconditionally re-activated
    /// its baseline lock would walk the version forward on each restart —
    /// invalidating clients' `preconditions.schema_environment_version` and
    /// filling `HISTORY` with schema changes that changed nothing.
    pub async fn ensure_schema(
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
/// January does not still hold what January's Grants said (§188, §245).
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

    /// Reads the Governance audit for a Space (§89, §172).
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

    /// What this Principal could do in a Space at a past instant (§176, §177).
    ///
    /// A historical answer, and nothing more: that a Principal could read
    /// something in January says nothing about whether it can today (§179).
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
    /// summarizing turns a descriptive note into an executable one (§127).
    /// Lowering is deliberately as easy as the permission itself: an incident
    /// response that had to wait for an approval would arrive late (§132).
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

    /// Holds an element out of ordinary use, pending review (§133).
    ///
    /// Not a retraction: it says this Brain does not currently allow ordinary
    /// use of the element, which is a statement about this Brain and not about
    /// whoever wrote it (§134).
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
                // No approval guard here: the exclusive write lock above
                // already serializes everything that could spend an approval.
                let base =
                    base_authorizations(&authority, &auth, gate::kml_permissions(&statement));
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
    /// (§184), and those paths check the write.
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
