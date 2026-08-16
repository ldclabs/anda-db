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
use tokio::sync::RwLock;

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
        let store = Store::open(db).await?;
        store.sweep_pending().await?;
        store.install_core_package().await?;
        store
            .open_or_create_space(SpaceDraft {
                space_id: DEFAULT_SPACE.to_string(),
                name: "Default MemorySpace".to_string(),
                description: "The Space a request runs against when it names none.".to_string(),
                ..Default::default()
            })
            .await?;
        Ok(Self {
            store,
            default_space: DEFAULT_SPACE.to_string(),
            lock: Arc::new(RwLock::new(())),
        })
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
        }
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
        self.store
            .activate_schema(space_id, lock, "governance")
            .await
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
        if current.lock == lock {
            return Ok(current);
        }
        self.store
            .activate_schema(space_id, lock, "governance")
            .await
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

#[async_trait]
impl Executor for CognitiveNexus {
    async fn execute(
        &self,
        command: Command,
        request: &Request,
        operation: &Operation,
    ) -> Response {
        let space = match self.space_of(request).await {
            Ok(space) => space,
            Err(err) => return Response::from(err),
        };

        match command {
            Command::Kml(statement) => {
                // Exclusive: readers must not observe a partly-written
                // transaction, and `anda_db` cannot make the multi-row write
                // atomic on its own.
                let _guard = self.lock.write().await;
                if let Err(err) = self.store.reopen_if_poisoned().await {
                    return Response::from(err);
                }
                let response =
                    crate::kml::execute(&self.store, &space, &statement, request, operation).await;
                // A poison event costs no further command: the next mutation
                // would be rejected outright, so recovery happens here rather
                // than being deferred to the caller's next attempt.
                if self.store.has_poisoned_handle() {
                    let _ = self.store.reopen().await;
                }
                response
            }
            Command::Kql(query) => {
                // Shared: readers may run concurrently, but none of them
                // overlaps a commit.
                let _guard = self.lock.read().await;
                crate::kql::execute(&self.store, &space, &query, request, operation).await
            }
            Command::Meta(command) => {
                // META is semantically read-only (§63.2), so it shares the
                // lock with KQL rather than taking it exclusively.
                let _guard = self.lock.read().await;
                crate::meta::execute(&self.store, &space, &command, request, operation).await
            }
        }
    }
}

/// The command families this engine can currently run.
///
/// Stated as data rather than prose so a caller can branch on it instead of
/// discovering the gap through an error.
pub fn supported_command_types() -> &'static [CommandType] {
    &[CommandType::Kml, CommandType::Kql, CommandType::Meta]
}
