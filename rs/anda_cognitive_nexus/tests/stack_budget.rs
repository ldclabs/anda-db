//! Host flows stay within a bounded stack in debug builds.
//!
//! A debug build keeps every awaited future in its caller's stack frame, so
//! futures inlined into one another cost the sum of their sizes at every level
//! of a call chain. Before the heavy Nexus and AndaDB futures were boxed,
//! opening a Nexus took about 0.9 MiB of stack and a KIP write about 1 MiB,
//! which overflowed the 2 MiB test threads of hosts that add their own layers.
//! These flows run on a thread with a quarter of that, so a heavy future that
//! is inlined again fails here rather than in a host.

use anda_cognitive_nexus::{
    CognitiveNexus, SpaceDraft,
    governance::{rows::principal_class, store::PrincipalDraft},
    nexus::DEFAULT_SPACE,
    profiles::COGNITIVE_MEMORY,
    schema::SchemaLock,
};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{Request, TopLevelStatus, execute_request};
use object_store::memory::InMemory;
use std::{future::Future, sync::Arc};

/// A quarter of the 2 MiB stack a spawned Rust thread gets by default.
const STACK_BUDGET: usize = 512 * 1024;

/// Runs `flow` on a current-thread runtime whose only thread has
/// [`STACK_BUDGET`] of stack. Overflowing it aborts the test binary.
fn within_stack_budget<F, Fut>(flow: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()>,
{
    std::thread::Builder::new()
        .name("stack-budget".into())
        .stack_size(STACK_BUDGET)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(flow())
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn database(store: Arc<InMemory>) -> Arc<AndaDB> {
    Arc::new(
        AndaDB::connect(
            store,
            DBConfig {
                name: "stack_budget".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap(),
    )
}

/// What a host does on every start: open the Nexus and activate its vocabulary.
async fn start(store: Arc<InMemory>) -> CognitiveNexus {
    let nexus = CognitiveNexus::connect(database(store).await)
        .await
        .unwrap();
    nexus
        .install_and_activate(&[("stack_budget", COGNITIVE_MEMORY)], DEFAULT_SPACE)
        .await
        .unwrap();
    nexus
}

async fn run(nexus: &CognitiveNexus, command: &str) {
    let request = Request::single(command);
    let response = execute_request(&nexus.system_session(), &request).await;
    assert_eq!(response.status, TopLevelStatus::Succeeded, "{response:?}");
}

/// A boxed future is a pointer; anything larger was left inline.
fn assert_boxed<F: Future>(name: &str, future: F) {
    assert_eq!(
        size_of_val(&future),
        size_of::<usize>(),
        "{name} returns its state machine inline instead of boxed"
    );
}

#[test]
fn a_host_start_fits_the_stack_budget() {
    within_stack_budget(|| async {
        start(Arc::new(InMemory::new())).await;
    });
}

#[test]
fn provisioning_a_caller_fits_the_stack_budget() {
    within_stack_budget(|| async {
        let nexus = start(Arc::new(InMemory::new())).await;
        nexus
            .governance()
            .ensure_principal(PrincipalDraft {
                principal_id: "tenant".into(),
                principal_class: principal_class::HUMAN.to_string(),
                display_name: "Tenant".into(),
                auth_provider: "test".into(),
                auth_subject: "tenant".into(),
            })
            .await
            .unwrap();
        nexus
            .store
            .open_or_create_space(SpaceDraft {
                space_id: "tenant_space".into(),
                name: "Tenant".into(),
                owner_principal: "tenant".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        let environment = nexus.store.schema_environment(DEFAULT_SPACE).await.unwrap();
        nexus
            .ensure_schema("tenant_space", environment.lock)
            .await
            .unwrap();
    });
}

#[test]
fn kip_writes_and_reads_fit_the_stack_budget() {
    within_stack_budget(|| async {
        let nexus = start(Arc::new(InMemory::new())).await;
        run(
            &nexus,
            r#"CREATE CONCEPT ?e { TYPE "Event" NAME "budgeted" SET ATTRIBUTES { summary: "budgeted" } }"#,
        )
        .await;
        run(&nexus, r#"FIND(?e) WHERE { ?e {type: "Event"} }"#).await;
    });
}

#[test]
fn reopening_a_database_fits_the_stack_budget() {
    within_stack_budget(|| async {
        let store = Arc::new(InMemory::new());
        let nexus = start(store.clone()).await;
        run(
            &nexus,
            r#"CREATE CONCEPT ?e { TYPE "Event" NAME "kept" SET ATTRIBUTES { summary: "kept" } }"#,
        )
        .await;
        nexus.close().await.unwrap();
        let nexus = start(store).await;
        run(&nexus, r#"FIND(?e) WHERE { ?e {type: "Event"} }"#).await;
    });
}

#[tokio::test]
async fn host_facing_futures_are_boxed() {
    let store = Arc::new(InMemory::new());
    let db = database(store.clone()).await;
    assert_boxed(
        "CognitiveNexus::connect",
        CognitiveNexus::connect(db.clone()),
    );
    let nexus = CognitiveNexus::connect(db).await.unwrap();
    assert_boxed(
        "CognitiveNexus::install_and_activate",
        nexus.install_and_activate(&[("stack_budget", COGNITIVE_MEMORY)], DEFAULT_SPACE),
    );
    assert_boxed(
        "CognitiveNexus::ensure_schema",
        nexus.ensure_schema(DEFAULT_SPACE, SchemaLock::default()),
    );
    assert_boxed("CognitiveNexus::recover", nexus.recover());
    assert_boxed(
        "GovernanceStore::ensure_principal",
        nexus.governance().ensure_principal(PrincipalDraft {
            principal_id: "tenant".into(),
            principal_class: principal_class::HUMAN.to_string(),
            display_name: String::new(),
            auth_provider: String::new(),
            auth_subject: String::new(),
        }),
    );
    assert_boxed(
        "Store::open_or_create_space",
        nexus.store.open_or_create_space(SpaceDraft::default()),
    );
}
