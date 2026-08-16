//! Schema Package lifecycle tests against a real Anda DB.
//!
//! The unit tests check resolution rules in isolation; these check the part
//! that only exists once state is durable — that a published version really is
//! immutable, that installing is not activating, and that a historical
//! environment version is still reconstructible after the Space has moved on.

use anda_cognitive_nexus::{
    Store,
    schema::{
        Intent, PackageState, SchemaLock, SchemaPackage, env::CORE_PACKAGE_REF, symbol::SymbolKind,
    },
    store::space::SpaceDraft,
};
use anda_db::database::{AndaDB, DBConfig};
use object_store::memory::InMemory;
use std::sync::Arc;

const SPACE: &str = "space-test";
const COGNITIVE_MEMORY: &str = include_str!("fixtures/cognitive-memory-2.0.0.json");
const PROFILE_ID: &str = "kip://profiles/cognitive-memory";

const ACME_HR: &str = r#"{
    "format": "KIP-Schema-Package",
    "manifest": {"package_id": "kip://acme/hr", "version": "1.0.0"},
    "definitions": {
        "concept_types": {
            "Person": {"kind": "ConceptType", "description": "An employee."}
        }
    }
}"#;

async fn fresh_store(name: &str) -> Store {
    let db = AndaDB::connect(
        Arc::new(InMemory::new()),
        DBConfig {
            name: name.to_string(),
            description: "schema tests".to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let store = Store::open(Arc::new(db)).await.unwrap();
    store
        .open_or_create_space(SpaceDraft {
            space_id: SPACE.to_string(),
            owner_principal: "principal-1".to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
    store
}

fn lock(entries: &[(&str, &str, PackageState)]) -> SchemaLock {
    let mut lock = SchemaLock::default();
    for (id, version, state) in entries {
        lock.packages.insert(id.to_string(), version.to_string());
        lock.states.insert(id.to_string(), *state);
    }
    lock
}

#[tokio::test]
async fn installing_a_package_does_not_activate_it() {
    // Spec §240.18 and §88: activation is a Governance operation. If arriving
    // data could activate its own schema, it could redefine what the Space's
    // existing data means.
    let store = fresh_store("install_is_not_activate").await;
    let profile = SchemaPackage::parse(COGNITIVE_MEMORY).unwrap();
    let package_ref = store.install_package(&profile, "test").await.unwrap();
    assert_eq!(
        package_ref.to_string(),
        "kip://profiles/cognitive-memory@2.0.0"
    );

    // Installed, and inert: the Space still resolves only Core.
    let before = store.schema_environment(SPACE).await.unwrap();
    assert_eq!(before.version, 0);
    assert!(
        before
            .resolve_symbol(SymbolKind::ConceptType, "Person", Intent::Write)
            .is_err()
    );

    let after = store
        .activate_schema(
            SPACE,
            lock(&[(PROFILE_ID, "2.0.0", PackageState::Active)]),
            "tx-1",
        )
        .await
        .unwrap();
    assert_eq!(after.version, 1);
    assert_eq!(
        after
            .resolve_symbol(SymbolKind::ConceptType, "Person", Intent::Write)
            .unwrap()
            .to_string(),
        "kip://profiles/cognitive-memory@2.0.0/Person"
    );

    // And the Space now reports it as current.
    let current = store.schema_environment(SPACE).await.unwrap();
    assert_eq!(current.version, 1);
    assert_eq!(current.state(PROFILE_ID), PackageState::Active);
}

#[tokio::test]
async fn a_published_version_cannot_be_replaced_with_different_content() {
    // Spec §150, §240.5: the same-version replacement attack. Every element
    // bound to `@2.0.0` would change meaning with no transaction recording it.
    let store = fresh_store("immutable_versions").await;
    let profile = SchemaPackage::parse(COGNITIVE_MEMORY).unwrap();
    store.install_package(&profile, "test").await.unwrap();

    // Re-installing byte-identical content is fine — installation is
    // idempotent, because startup paths run more than once.
    store.install_package(&profile, "test").await.unwrap();

    let mut tampered = SchemaPackage::parse(COGNITIVE_MEMORY).unwrap();
    tampered
        .definitions
        .concept_types
        .remove("Person")
        .expect("the profile defines Person");
    let err = store.install_package(&tampered, "test").await.unwrap_err();
    assert_eq!(err.name(), "DigestMismatch");
    assert!(err.message.contains("immutable"));

    // The original content survived the attempt.
    let installed = store.installed_packages().await.unwrap();
    assert!(
        installed["kip://profiles/cognitive-memory@2.0.0"]
            .defines(SymbolKind::ConceptType, "Person")
    );
}

#[tokio::test]
async fn a_historical_environment_version_stays_reconstructible() {
    // Spec §144, §240.44: a transaction records the environment it ran under,
    // and rolling defaults forward must not change what past reads meant.
    let store = fresh_store("historical_env").await;
    store
        .install_package(&SchemaPackage::parse(COGNITIVE_MEMORY).unwrap(), "test")
        .await
        .unwrap();
    store
        .install_package(&SchemaPackage::parse(ACME_HR).unwrap(), "test")
        .await
        .unwrap();

    store
        .activate_schema(
            SPACE,
            lock(&[(PROFILE_ID, "2.0.0", PackageState::Active)]),
            "tx-1",
        )
        .await
        .unwrap();
    store
        .activate_schema(
            SPACE,
            lock(&[
                (PROFILE_ID, "2.0.0", PackageState::Active),
                ("kip://acme/hr", "1.0.0", PackageState::Active),
            ]),
            "tx-2",
        )
        .await
        .unwrap();

    // Today, `Person` is ambiguous.
    let current = store.schema_environment(SPACE).await.unwrap();
    assert_eq!(current.version, 2);
    assert_eq!(
        current
            .resolve_symbol(SymbolKind::ConceptType, "Person", Intent::Write)
            .unwrap_err()
            .name(),
        "SchemaSymbolAmbiguous"
    );

    // At version 1 it was not, and that answer is still recoverable.
    let historical = store.schema_environment_at(SPACE, 1).await.unwrap();
    assert_eq!(
        historical
            .resolve_symbol(SymbolKind::ConceptType, "Person", Intent::Write)
            .unwrap()
            .to_string(),
        "kip://profiles/cognitive-memory@2.0.0/Person"
    );

    // Version 0 is the pre-activation environment: Core, and nothing else.
    let genesis = store.schema_environment_at(SPACE, 0).await.unwrap();
    assert!(
        genesis
            .resolve_symbol(SymbolKind::ConceptType, "Person", Intent::Write)
            .is_err()
    );

    let missing = store.schema_environment_at(SPACE, 99).await.unwrap_err();
    assert_eq!(missing.name(), "HistoricalSchemaUnavailable");
}

#[tokio::test]
async fn activating_an_uninstalled_package_leaves_the_space_untouched() {
    // Spec §240.43: activation is atomic at the environment boundary. A
    // half-applied upgrade would surface later as a missing symbol somewhere
    // unrelated to the upgrade.
    let store = fresh_store("atomic_activation").await;
    store
        .install_package(&SchemaPackage::parse(COGNITIVE_MEMORY).unwrap(), "test")
        .await
        .unwrap();
    store
        .activate_schema(
            SPACE,
            lock(&[(PROFILE_ID, "2.0.0", PackageState::Active)]),
            "tx-1",
        )
        .await
        .unwrap();

    let err = store
        .activate_schema(
            SPACE,
            lock(&[
                (PROFILE_ID, "2.0.0", PackageState::Active),
                ("kip://acme/nowhere", "1.0.0", PackageState::Active),
            ]),
            "tx-2",
        )
        .await
        .unwrap_err();
    assert_eq!(err.name(), "SchemaPackageUnavailable");

    let current = store.schema_environment(SPACE).await.unwrap();
    assert_eq!(
        current.version, 1,
        "the failed activation minted no version"
    );
    assert!(
        current
            .resolve_symbol(SymbolKind::ConceptType, "Person", Intent::Write)
            .is_ok()
    );
}

#[tokio::test]
async fn core_is_installable_and_introspectable_without_being_activated() {
    // Spec §158: conformance to Core does not depend on package activation,
    // but having the artifact installed is what lets META describe it beside
    // every other package.
    let store = fresh_store("core_package").await;
    let installed = store.install_core_package().await.unwrap();
    assert_eq!(installed.to_string(), CORE_PACKAGE_REF.to_string());
    // Idempotent.
    store.install_core_package().await.unwrap();

    let packages = store.installed_packages().await.unwrap();
    assert_eq!(packages.len(), 1);
    let core = &packages[&CORE_PACKAGE_REF.to_string()];
    assert!(
        core.definitions
            .registry_extensions
            .contains_key("evidence_classes")
    );
    // Core types nothing: Concept types are schema-defined (Core Data Model
    // §49), so a Space with only Core cannot type a Concept.
    assert!(core.definitions.concept_types.is_empty());
}

#[tokio::test]
async fn the_schema_registries_survive_a_reopen() {
    let store = fresh_store("schema_reopen").await;
    store
        .install_package(&SchemaPackage::parse(COGNITIVE_MEMORY).unwrap(), "test")
        .await
        .unwrap();
    store
        .activate_schema(
            SPACE,
            lock(&[(PROFILE_ID, "2.0.0", PackageState::Active)]),
            "tx-1",
        )
        .await
        .unwrap();
    store.flush(1_755_000_000_000).await.unwrap();
    store.reopen().await.unwrap();

    let current = store.schema_environment(SPACE).await.unwrap();
    assert_eq!(current.version, 1);
    assert!(
        current
            .resolve_symbol(SymbolKind::PredicateType, "prefers", Intent::Write)
            .is_ok()
    );
    // The unique constraint on `package_ref` is index state; if the reopen
    // dropped it, a replacement would be accepted rather than refused.
    let mut tampered = SchemaPackage::parse(COGNITIVE_MEMORY).unwrap();
    tampered.definitions.predicates.clear();
    assert!(store.install_package(&tampered, "test").await.is_err());
}
