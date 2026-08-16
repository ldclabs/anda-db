//! Governance Control Plane tests: the records, their lifecycle, and the
//! things they refuse.
//!
//! Every fixture runs against a real Nexus and a real database, because the
//! properties under test are storage properties: that a revoked Grant is still
//! *there*, that a policy version is never overwritten, that an approval count
//! cannot be reached by one Principal twice.

use anda_cognitive_nexus::{
    CognitiveNexus,
    governance::{
        AuthContext, SYSTEM_PRINCIPAL, classification,
        rows::{
            ActorBindingRow, AuthorityConditions, AuthorityConstraints, AuthorityScope, GrantRow,
            PolicyStatement, assurance, auth_strength, binding_class, principal_class,
            purpose_assurance, status,
        },
        store::{
            ActorBindingDraft, ApprovalDraft, DelegationDraft, GovernanceStore, GrantDraft,
            GroupDraft, PolicyDraft, PrincipalDraft, grant_id,
        },
    },
    nexus::DEFAULT_SPACE,
    nexus::Session,
    schema::{PackageState, SchemaLock, SchemaPackage},
};
use anda_db::database::{AndaDB, DBConfig};
use anda_kip::{Executor, Request, Response, TopLevelStatus};
use object_store::memory::InMemory;
use std::sync::Arc;

async fn fresh(name: &str) -> CognitiveNexus {
    let db = AndaDB::connect(
        Arc::new(InMemory::new()),
        DBConfig {
            name: name.to_string(),
            description: "governance tests".to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    CognitiveNexus::connect(Arc::new(db)).await.unwrap()
}

async fn agent(gov: &GovernanceStore, id: &str) -> String {
    gov.ensure_principal(PrincipalDraft {
        principal_id: id.to_string(),
        principal_class: principal_class::AGENT.to_string(),
        display_name: id.to_string(),
        auth_provider: "test".to_string(),
        auth_subject: id.to_string(),
    })
    .await
    .unwrap()
    .principal_id
}

#[tokio::test]
async fn opening_a_nexus_bootstraps_an_owner_rather_than_an_ownerless_space() {
    // Default deny (§41) makes an unowned Space one nobody can administer —
    // including to give it an owner. The bootstrap is what keeps an embedded
    // host working *through* the authorization path instead of around it.
    let nexus = fresh("bootstrap").await;
    let system = nexus
        .governance()
        .find_principal(SYSTEM_PRINCIPAL)
        .await
        .unwrap()
        .expect("the system Principal exists after connect");
    assert_eq!(system.principal_class, principal_class::SYSTEM);
    assert_eq!(system.status, status::ACTIVE);

    let space = nexus.store.get_space(DEFAULT_SPACE).await.unwrap();
    assert_eq!(space.owner_principal, SYSTEM_PRINCIPAL);
    assert!(space.owners.contains(&SYSTEM_PRINCIPAL.to_string()));
    // §95: a Space that says nothing about sensitivity has not said "public".
    assert_eq!(space.default_classification, classification::DEFAULT);
    assert_ne!(space.default_classification, classification::PUBLIC);
}

#[tokio::test]
async fn the_bootstrap_does_not_reset_what_a_deployment_configured() {
    // Opening runs on every start. A bootstrap that recreated its records
    // would silently undo a deployment's own edits on the next restart.
    let db = AndaDB::connect(
        Arc::new(InMemory::new()),
        DBConfig {
            name: "reopen".to_string(),
            description: "governance tests".to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let db = Arc::new(db);

    let nexus = CognitiveNexus::connect(db.clone()).await.unwrap();
    nexus
        .governance()
        .set_principal_status(SYSTEM_PRINCIPAL, status::SUSPENDED, SYSTEM_PRINCIPAL)
        .await
        .unwrap();

    let reopened = CognitiveNexus::connect(db).await.unwrap();
    let system = reopened
        .governance()
        .find_principal(SYSTEM_PRINCIPAL)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(system.status, status::SUSPENDED);
}

#[tokio::test]
async fn revoking_a_principal_does_not_erase_it() {
    // §9: revocation affects future authority. A historical write by a
    // later-revoked Principal stays attributable to it, which needs the record
    // to still be there.
    let nexus = fresh("revoke_principal").await;
    let gov = nexus.governance();
    let id = agent(gov, "kip:principal:agent-42").await;

    let revoked = gov
        .set_principal_status(&id, status::REVOKED, SYSTEM_PRINCIPAL)
        .await
        .unwrap();
    assert_eq!(revoked.status, status::REVOKED);
    assert!(!revoked.revoked_at.is_empty());
    assert_eq!(revoked.version, 2);

    let found = gov.find_principal(&id).await.unwrap();
    assert!(found.is_some(), "a revoked Principal is still on record");
}

#[tokio::test]
async fn a_revoked_grant_stops_authorizing_but_stays_readable() {
    let nexus = fresh("revoke_grant").await;
    let gov = nexus.governance();
    let reader = agent(gov, "kip:principal:reader").await;

    let grant = gov
        .create_grant(
            GrantDraft {
                space_id: DEFAULT_SPACE.to_string(),
                grantee_principal: reader.clone(),
                actions: vec!["read".into(), "search".into()],
                ..Default::default()
            },
            SYSTEM_PRINCIPAL,
        )
        .await
        .unwrap();

    let live = gov.grants_for(DEFAULT_SPACE, &reader, &[]).await.unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].actions, vec!["read".to_string(), "search".into()]);

    gov.revoke_grant(grant._id, SYSTEM_PRINCIPAL).await.unwrap();
    let live = gov.grants_for(DEFAULT_SPACE, &reader, &[]).await.unwrap();
    assert!(live.is_empty(), "a revoked Grant confers nothing");

    // §36: the audit still says it was valid, and names it.
    let audit = gov.read_audit(DEFAULT_SPACE, 10).await.unwrap();
    let revocation = audit
        .iter()
        .find(|entry| entry.operation == "revoke_grant")
        .expect("the revocation is audited");
    assert_eq!(revocation.resource, grant_id(grant._id));
    assert!(
        audit.iter().any(|entry| entry.operation == "create_grant"),
        "and so is the creation it revoked"
    );
}

#[tokio::test]
async fn a_group_grant_reaches_its_members() {
    let nexus = fresh("group_grant").await;
    let gov = nexus.governance();
    let member = agent(gov, "kip:principal:maintainer-1").await;
    agent(gov, "kip:principal:outsider").await;

    gov.put_group(
        GroupDraft {
            group_id: "kip:group:maintainers".into(),
            name: "Maintainers".into(),
            description: "Custodial agents".into(),
            members: vec![member.clone()],
        },
        SYSTEM_PRINCIPAL,
    )
    .await
    .unwrap();

    gov.create_grant(
        GrantDraft {
            space_id: DEFAULT_SPACE.to_string(),
            grantee_group: "kip:group:maintainers".into(),
            actions: vec!["maintain".into()],
            ..Default::default()
        },
        SYSTEM_PRINCIPAL,
    )
    .await
    .unwrap();

    let groups = gov.groups_of(&member).await.unwrap();
    assert_eq!(groups, vec!["kip:group:maintainers".to_string()]);
    let grants: Vec<GrantRow> = gov
        .grants_for(DEFAULT_SPACE, &member, &groups)
        .await
        .unwrap();
    assert_eq!(grants.len(), 1);

    // §25: membership is Governance state, so an outsider gets nothing —
    // there is no Proposition anyone could write to join.
    let outsider_groups = gov.groups_of("kip:principal:outsider").await.unwrap();
    assert!(outsider_groups.is_empty());
    assert!(
        gov.grants_for(DEFAULT_SPACE, "kip:principal:outsider", &outsider_groups)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn a_space_scoped_binding_does_not_leak_into_another_space() {
    let nexus = fresh("binding_scope").await;
    let gov = nexus.governance();
    let principal = agent(gov, "kip:principal:assistant").await;

    gov.create_binding(
        ActorBindingDraft {
            principal_id: principal.clone(),
            actor_key: "C-1".into(),
            binding_class: binding_class::REPRESENTS.into(),
            assurance: assurance::VERIFIED.into(),
            scope: DEFAULT_SPACE.to_string(),
        },
        SYSTEM_PRINCIPAL,
    )
    .await
    .unwrap();

    let here: Vec<ActorBindingRow> = gov.bindings_of(&principal, DEFAULT_SPACE).await.unwrap();
    assert_eq!(here.len(), 1);
    assert_eq!(here[0].binding_class, binding_class::REPRESENTS);

    let elsewhere = gov
        .bindings_of(&principal, "kip:space:someone-elses-brain")
        .await
        .unwrap();
    assert!(
        elsewhere.is_empty(),
        "representation in one Space is not representation everywhere"
    );
}

#[tokio::test]
async fn a_revoked_binding_stops_binding() {
    let nexus = fresh("binding_revoke").await;
    let gov = nexus.governance();
    let principal = agent(gov, "kip:principal:assistant").await;

    let binding = gov
        .create_binding(
            ActorBindingDraft {
                principal_id: principal.clone(),
                actor_key: "C-1".into(),
                binding_class: binding_class::SELF.into(),
                assurance: assurance::VERIFIED.into(),
                scope: "*".into(),
            },
            SYSTEM_PRINCIPAL,
        )
        .await
        .unwrap();

    gov.revoke_binding(binding._id, SYSTEM_PRINCIPAL)
        .await
        .unwrap();
    assert!(
        gov.bindings_of(&principal, DEFAULT_SPACE)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn a_policy_version_is_never_overwritten() {
    // §46 and the §247 fixture: an audit must be able to name the policy
    // version that authorized a past operation, which a rewrite would destroy.
    let nexus = fresh("policy_versions").await;
    let gov = nexus.governance();

    let first = gov
        .publish_policy(
            PolicyDraft {
                policy_id: "kip:policy:space-default".into(),
                space_id: DEFAULT_SPACE.into(),
                description: "Broad internal sharing".into(),
                statements: vec![PolicyStatement {
                    effect: "allow".into(),
                    actions: vec!["read".into()],
                    ..Default::default()
                }],
            },
            SYSTEM_PRINCIPAL,
        )
        .await
        .unwrap();
    assert_eq!(first.version, 1);
    assert_eq!(first.policy_ref, "kip:policy:space-default@1");

    // The historical coordinate is a timestamp with millisecond resolution, so
    // two publishes in the same millisecond genuinely share one coordinate and
    // "the version in force at T" is then the later one. Separating them is
    // what makes the assertion below about history rather than about clocks.
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;

    let second = gov
        .publish_policy(
            PolicyDraft {
                policy_id: "kip:policy:space-default".into(),
                space_id: DEFAULT_SPACE.into(),
                description: "Strict compartmentalization".into(),
                statements: vec![PolicyStatement {
                    effect: "deny".into(),
                    actions: vec!["read".into()],
                    ..Default::default()
                }],
            },
            SYSTEM_PRINCIPAL,
        )
        .await
        .unwrap();
    assert_eq!(second.version, 2);

    let versions = gov
        .policy_versions("kip:policy:space-default")
        .await
        .unwrap();
    assert_eq!(versions.len(), 2, "publishing appends, it does not replace");
    assert_eq!(versions[0].description, "Broad internal sharing");

    // §177: the version in force at a past coordinate is still reconstructable.
    let then = gov
        .policy_at("kip:policy:space-default", &first.created_at)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(then.version, 1);
    assert_eq!(
        gov.active_policy("kip:policy:space-default")
            .await
            .unwrap()
            .unwrap()
            .version,
        2
    );
}

#[tokio::test]
async fn a_policy_that_predates_the_coordinate_is_the_one_that_applied() {
    let nexus = fresh("policy_as_of").await;
    let gov = nexus.governance();
    gov.publish_policy(
        PolicyDraft {
            policy_id: "kip:policy:p".into(),
            space_id: DEFAULT_SPACE.into(),
            ..Default::default()
        },
        SYSTEM_PRINCIPAL,
    )
    .await
    .unwrap();

    // Before the first version existed there was no policy — which is not the
    // same as an empty policy, and the store says so by answering None.
    assert!(
        gov.policy_at("kip:policy:p", "2020-01-01T00:00:00.000Z")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn one_principal_cannot_satisfy_a_two_party_approval() {
    // The §246 fixture, and §170's separation of duties. Both reduce to the
    // same rule: the approvals have to be independent, or the count is decoration.
    let nexus = fresh("approvals").await;
    let gov = nexus.governance();
    let proposer = agent(gov, "kip:principal:proposer").await;
    let auditor = agent(gov, "kip:principal:auditor").await;

    let approval = gov
        .request_approval(
            ApprovalDraft {
                space_id: DEFAULT_SPACE.into(),
                operation: "elevate_authority".into(),
                resource: "C-7".into(),
                subject_digest: "digest-of-this-one-request".into(),
                required: 2,
                ..Default::default()
            },
            &proposer,
        )
        .await
        .unwrap();
    assert_eq!(approval.status, "pending");

    // The requester is not an independent approver.
    let err = gov.approve(approval._id, &proposer, "").await.unwrap_err();
    assert_eq!(err.name(), "NotAuthorized");

    let after_one = gov
        .approve(approval._id, &auditor, "looks fine")
        .await
        .unwrap();
    assert_eq!(
        after_one.status, "pending",
        "one of two is not partial activation"
    );
    assert!(
        gov.granted_approvals(DEFAULT_SPACE, "digest-of-this-one-request")
            .await
            .unwrap()
            .is_empty()
    );

    // And the same identity cannot count twice.
    let err = gov
        .approve(approval._id, &auditor, "again")
        .await
        .unwrap_err();
    assert_eq!(err.name(), "NotAuthorized");

    let security = agent(gov, "kip:principal:security").await;
    let granted = gov.approve(approval._id, &security, "").await.unwrap();
    assert_eq!(granted.status, "granted");
    assert_eq!(
        gov.granted_approvals(DEFAULT_SPACE, "digest-of-this-one-request")
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn an_approval_is_bound_to_one_operation() {
    // Without the subject digest, an approval for purging one Evidence record
    // would authorize purging anything.
    let nexus = fresh("approval_binding").await;
    let gov = nexus.governance();
    let proposer = agent(gov, "kip:principal:proposer").await;
    let approver = agent(gov, "kip:principal:approver").await;

    let approval = gov
        .request_approval(
            ApprovalDraft {
                space_id: DEFAULT_SPACE.into(),
                operation: "purge".into(),
                resource: "E-1".into(),
                subject_digest: "purge-E-1".into(),
                required: 1,
                ..Default::default()
            },
            &proposer,
        )
        .await
        .unwrap();
    gov.approve(approval._id, &approver, "").await.unwrap();

    assert_eq!(
        gov.granted_approvals(DEFAULT_SPACE, "purge-E-1")
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        gov.granted_approvals(DEFAULT_SPACE, "purge-E-2")
            .await
            .unwrap()
            .is_empty(),
        "an approval for one operation does not travel to another"
    );
}

#[tokio::test]
async fn a_delegation_records_its_bounds_and_its_parent() {
    let nexus = fresh("delegation").await;
    let gov = nexus.governance();
    let owner = agent(gov, "kip:principal:owner").await;
    let worker = agent(gov, "kip:principal:sub-agent").await;

    let delegation = gov
        .create_delegation(
            DelegationDraft {
                space_id: DEFAULT_SPACE.into(),
                delegator_principal: owner.clone(),
                delegate_principal: worker.clone(),
                actions: vec!["read".into()],
                scope: AuthorityScope {
                    kinds: vec!["evidence".into()],
                    ..Default::default()
                },
                conditions: AuthorityConditions {
                    valid_until: "2026-08-17T00:00:00.000Z".into(),
                    min_auth_strength: auth_strength::STANDARD.into(),
                    ..Default::default()
                },
                constraints: AuthorityConstraints {
                    export: false,
                    ..Default::default()
                },
                ..Default::default()
            },
            &owner,
        )
        .await
        .unwrap();

    // §34: re-delegation is off unless somebody says otherwise.
    assert!(!delegation.may_redelegate);

    let live = gov.delegations_to(DEFAULT_SPACE, &worker).await.unwrap();
    assert_eq!(live.len(), 1);
    let stored: AuthorityScope = serde_json::from_value(live[0].scope.clone()).unwrap();
    assert_eq!(stored.kinds, vec!["evidence".to_string()]);

    gov.revoke_delegation(delegation._id, &owner).await.unwrap();
    assert!(
        gov.delegations_to(DEFAULT_SPACE, &worker)
            .await
            .unwrap()
            .is_empty(),
        "§245: revocation is effective for future operations"
    );
}

#[tokio::test]
async fn every_control_plane_change_lands_in_the_audit() {
    // §172 enumerates what must be auditable. The point of checking it here is
    // that the audit is written by the store rather than by each caller, so a
    // new Governance operation cannot forget to record itself.
    let nexus = fresh("audit").await;
    let gov = nexus.governance();
    let principal = agent(gov, "kip:principal:p").await;

    gov.put_group(
        GroupDraft {
            group_id: "kip:group:g".into(),
            members: vec![principal.clone()],
            ..Default::default()
        },
        SYSTEM_PRINCIPAL,
    )
    .await
    .unwrap();
    gov.create_grant(
        GrantDraft {
            space_id: DEFAULT_SPACE.into(),
            grantee_principal: principal.clone(),
            actions: vec!["read".into()],
            ..Default::default()
        },
        SYSTEM_PRINCIPAL,
    )
    .await
    .unwrap();
    gov.publish_policy(
        PolicyDraft {
            policy_id: "kip:policy:p".into(),
            space_id: DEFAULT_SPACE.into(),
            ..Default::default()
        },
        SYSTEM_PRINCIPAL,
    )
    .await
    .unwrap();

    let audit = gov.read_audit(DEFAULT_SPACE, 100).await.unwrap();
    let operations: Vec<&str> = audit.iter().map(|e| e.operation.as_str()).collect();
    for expected in ["create_grant", "publish_policy"] {
        assert!(operations.contains(&expected), "{expected} is audited");
    }
    assert!(
        audit.iter().all(|entry| entry.entry_class == "mutation"),
        "these are control-plane changes, not authorization decisions"
    );
    // The whole new record travels with the entry, so reconstructing past
    // state never depends on replaying a diff chain.
    let grant_entry = audit
        .iter()
        .find(|e| e.operation == "create_grant")
        .unwrap();
    assert_eq!(
        grant_entry.record.get("grantee_principal").unwrap(),
        &serde_json::json!(principal)
    );

    // Principal and group changes are Nexus-wide, not Space-scoped.
    let global = gov.read_audit("*", 100).await.unwrap();
    let global_ops: Vec<&str> = global.iter().map(|e| e.operation.as_str()).collect();
    assert!(global_ops.contains(&"create_principal"));
    assert!(global_ops.contains(&"put_group"));
}

// ---------------------------------------------------------------------------
// Sessions: what a caller can actually do
// ---------------------------------------------------------------------------

/// A Nexus with the bundled profile in force, so KML has types to write.
async fn stocked(name: &str) -> CognitiveNexus {
    let nexus = fresh(name).await;
    nexus
        .install_package(
            &SchemaPackage::parse(anda_cognitive_nexus::profiles::COGNITIVE_MEMORY).unwrap(),
            "test",
        )
        .await
        .unwrap();
    let mut lock = SchemaLock::default();
    lock.packages
        .insert("kip://profiles/cognitive-memory".into(), "2.0.0".into());
    lock.states.insert(
        "kip://profiles/cognitive-memory".into(),
        PackageState::Active,
    );
    nexus.activate_schema(DEFAULT_SPACE, lock).await.unwrap();
    nexus
}

async fn run_as(session: &Session, command: &str) -> Response {
    let request = Request::single(command);
    let parsed = anda_kip::parse_kip(command).unwrap_or_else(|err| panic!("{command}\n{err}"));
    session
        .execute(parsed, &request, &request.operations[0])
        .await
}

fn error_code(response: &Response) -> &str {
    response
        .error
        .as_ref()
        .or_else(|| response.results.first().and_then(|r| r.error.as_ref()))
        .map(|error| error.code.as_str())
        .unwrap_or("")
}

#[tokio::test]
async fn a_caller_with_no_grants_is_denied_rather_than_shown_an_empty_space() {
    // §41: default deny. A missing policy must not become public access — and
    // an empty result set would tell the caller something about the world.
    let nexus = stocked("default_deny").await;
    agent(nexus.governance(), "kip:principal:stranger").await;
    let stranger = nexus.session(AuthContext::principal("kip:principal:stranger"));

    let response = run_as(
        &stranger,
        r#"FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }"#,
    )
    .await;
    assert_eq!(error_code(&response), "NotAuthorized");
    assert!(
        response.error.as_ref().unwrap().message.contains("read"),
        "the denial names the permission, and nothing else"
    );
}

#[tokio::test]
async fn an_unregistered_principal_is_a_configuration_bug_not_a_denial() {
    // A host that asserts an identity the control plane never heard of has a
    // bug. Resolving it to "a caller with no Grants" would hide that bug behind
    // something that reads like policy.
    let nexus = stocked("unregistered").await;
    let ghost = nexus.session(AuthContext::principal("kip:principal:ghost"));
    let response = run_as(&ghost, "DESCRIBE PRIMER").await;
    assert_eq!(error_code(&response), "Unauthenticated");
}

#[tokio::test]
async fn a_read_grant_permits_reading_and_not_exporting() {
    // §271: Read ≠ Export. A caller who may see every element in a Space still
    // may not package them and take them away.
    let nexus = stocked("read_not_export").await;
    let gov = nexus.governance();
    let reader = agent(gov, "kip:principal:reader").await;
    gov.create_grant(
        GrantDraft {
            space_id: DEFAULT_SPACE.into(),
            grantee_principal: reader.clone(),
            actions: vec!["read".into(), "discover".into()],
            ..Default::default()
        },
        SYSTEM_PRINCIPAL,
    )
    .await
    .unwrap();
    let session = nexus.session(AuthContext::principal(&reader));

    let query = run_as(
        &session,
        r#"FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }"#,
    )
    .await;
    assert_eq!(query.status, TopLevelStatus::Succeeded);

    let export = run_as(
        &session,
        r#"EXPORT CAPSULE :out WHERE { ?c CONCEPT {type: "Person"} }"#,
    )
    .await;
    assert_eq!(error_code(&export), "NotAuthorized");
    assert!(export.error.as_ref().unwrap().message.contains("export"));
}

#[tokio::test]
async fn reading_the_past_is_a_permission_of_its_own() {
    let nexus = stocked("read_history").await;
    let gov = nexus.governance();
    let reader = agent(gov, "kip:principal:reader").await;
    gov.create_grant(
        GrantDraft {
            space_id: DEFAULT_SPACE.into(),
            grantee_principal: reader.clone(),
            actions: vec!["read".into()],
            ..Default::default()
        },
        SYSTEM_PRINCIPAL,
    )
    .await
    .unwrap();
    let session = nexus.session(AuthContext::principal(&reader));

    assert_eq!(
        run_as(
            &session,
            r#"FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }"#
        )
        .await
        .status,
        TopLevelStatus::Succeeded
    );
    let historical = run_as(
        &session,
        r#"FIND(?c) WHERE { ?c CONCEPT {type: "Person"} } AS OF SEQ 1"#,
    )
    .await;
    assert_eq!(error_code(&historical), "NotAuthorized");
}

#[tokio::test]
async fn a_writer_without_the_clause_permission_is_refused() {
    let nexus = stocked("clause_permissions").await;
    let gov = nexus.governance();
    let writer = agent(gov, "kip:principal:writer").await;
    gov.create_grant(
        GrantDraft {
            space_id: DEFAULT_SPACE.into(),
            grantee_principal: writer.clone(),
            actions: vec!["read".into(), "create".into()],
            ..Default::default()
        },
        SYSTEM_PRINCIPAL,
    )
    .await
    .unwrap();
    let session = nexus.session(AuthContext::principal(&writer));

    let created = run_as(
        &session,
        r#"CREATE CONCEPT ?p { TYPE "Person" NAME "Alice" }"#,
    )
    .await;
    assert_eq!(created.status, TopLevelStatus::Succeeded);

    // Creating is not erasing, and it is not removing either.
    for (command, code) in [
        (r#"TOMBSTONE "C-1""#, "NotAuthorized"),
        (r#"PURGE "C-1" CONFIRM "PURGE""#, "NotAuthorized"),
    ] {
        assert_eq!(
            error_code(&run_as(&session, command).await),
            code,
            "{command}"
        );
    }
}

#[tokio::test]
async fn what_a_principal_wrote_is_stamped_on_the_element() {
    // §26: engine origin is what the runtime observed. It comes from the
    // authenticated session, never from the command's content.
    let nexus = stocked("origin").await;
    let gov = nexus.governance();
    let writer = agent(gov, "kip:principal:writer").await;
    gov.create_grant(
        GrantDraft {
            space_id: DEFAULT_SPACE.into(),
            grantee_principal: writer.clone(),
            actions: vec!["read".into(), "create".into()],
            ..Default::default()
        },
        SYSTEM_PRINCIPAL,
    )
    .await
    .unwrap();
    let session = nexus.session(AuthContext::principal(&writer));
    run_as(
        &session,
        r#"CREATE CONCEPT ?p { TYPE "Person" NAME "Alice" }"#,
    )
    .await;

    let element = nexus
        .store
        .get_element(anda_cognitive_nexus::ElementId::new(
            anda_kip::ElementKind::Concept,
            1,
        ))
        .await
        .unwrap();
    let view = anda_cognitive_nexus::view::render(&element);
    assert_eq!(view["_system"]["origin"]["principal_id"], writer);
}

#[tokio::test]
async fn a_revoked_grant_stops_a_session_that_was_already_running() {
    // §188, §245: a session must not assume its startup permissions hold
    // forever. Authority is re-resolved on every request.
    let nexus = stocked("revocation_live").await;
    let gov = nexus.governance();
    let reader = agent(gov, "kip:principal:reader").await;
    let grant = gov
        .create_grant(
            GrantDraft {
                space_id: DEFAULT_SPACE.into(),
                grantee_principal: reader.clone(),
                actions: vec!["read".into()],
                ..Default::default()
            },
            SYSTEM_PRINCIPAL,
        )
        .await
        .unwrap();
    let session = nexus.session(AuthContext::principal(&reader));
    let query = r#"FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }"#;
    assert_eq!(
        run_as(&session, query).await.status,
        TopLevelStatus::Succeeded
    );

    gov.revoke_grant(grant._id, SYSTEM_PRINCIPAL).await.unwrap();
    assert_eq!(error_code(&run_as(&session, query).await), "NotAuthorized");
}

#[tokio::test]
async fn suspending_a_principal_stops_it_without_touching_its_grants() {
    let nexus = stocked("suspend").await;
    let gov = nexus.governance();
    let reader = agent(gov, "kip:principal:reader").await;
    gov.create_grant(
        GrantDraft {
            space_id: DEFAULT_SPACE.into(),
            grantee_principal: reader.clone(),
            actions: vec!["read".into()],
            ..Default::default()
        },
        SYSTEM_PRINCIPAL,
    )
    .await
    .unwrap();
    let session = nexus.session(AuthContext::principal(&reader));
    let query = r#"FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }"#;
    assert_eq!(
        run_as(&session, query).await.status,
        TopLevelStatus::Succeeded
    );

    gov.set_principal_status(&reader, status::SUSPENDED, SYSTEM_PRINCIPAL)
        .await
        .unwrap();
    assert_eq!(error_code(&run_as(&session, query).await), "NotAuthorized");
    assert_eq!(
        gov.grants_for(DEFAULT_SPACE, &reader, &[])
            .await
            .unwrap()
            .len(),
        1,
        "the Grant is untouched; the Principal is what changed"
    );
}

#[tokio::test]
async fn an_explicit_policy_deny_overrides_a_grant() {
    // §42, and the reason exceptions are expressed by narrowing a deny rather
    // than by a priority rule nobody can see.
    let nexus = stocked("deny_overrides").await;
    let gov = nexus.governance();
    let reader = agent(gov, "kip:principal:reader").await;
    gov.create_grant(
        GrantDraft {
            space_id: DEFAULT_SPACE.into(),
            grantee_principal: reader.clone(),
            actions: vec!["read".into(), "search".into()],
            ..Default::default()
        },
        SYSTEM_PRINCIPAL,
    )
    .await
    .unwrap();
    gov.publish_policy(
        PolicyDraft {
            policy_id: "kip:policy:space".into(),
            space_id: DEFAULT_SPACE.into(),
            description: "No reading for this one".into(),
            statements: vec![PolicyStatement {
                effect: "deny".into(),
                principals: vec![reader.clone()],
                actions: vec!["read".into()],
                ..Default::default()
            }],
        },
        SYSTEM_PRINCIPAL,
    )
    .await
    .unwrap();
    let mut space = nexus.store.get_space(DEFAULT_SPACE).await.unwrap();
    space.default_policy_id = "kip:policy:space".into();
    nexus.store.put_space(&space).await.unwrap();

    let session = nexus.session(AuthContext::principal(&reader));
    let query = r#"FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }"#;
    assert_eq!(error_code(&run_as(&session, query).await), "NotAuthorized");
}

#[tokio::test]
async fn a_policy_can_open_a_space_to_unauthenticated_readers() {
    // §214: a public Space is one whose policy says so, never one whose check
    // was missing.
    let nexus = stocked("public_space").await;
    let gov = nexus.governance();
    gov.publish_policy(
        PolicyDraft {
            policy_id: "kip:policy:public".into(),
            space_id: DEFAULT_SPACE.into(),
            description: "Read-only public knowledge".into(),
            statements: vec![PolicyStatement {
                effect: "allow".into(),
                principals: vec!["kip:principal:anonymous".into()],
                actions: vec!["read".into(), "discover".into()],
                ..Default::default()
            }],
        },
        SYSTEM_PRINCIPAL,
    )
    .await
    .unwrap();
    let mut space = nexus.store.get_space(DEFAULT_SPACE).await.unwrap();
    space.default_policy_id = "kip:policy:public".into();
    nexus.store.put_space(&space).await.unwrap();

    let anonymous = nexus.session(AuthContext::anonymous());
    let query = r#"FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }"#;
    assert_eq!(
        run_as(&anonymous, query).await.status,
        TopLevelStatus::Succeeded
    );
    // Reading is not writing, even in a public Space.
    assert_eq!(
        error_code(&run_as(&anonymous, r#"CREATE CONCEPT ?p { TYPE "Person" }"#).await),
        "NotAuthorized"
    );
}

#[tokio::test]
async fn a_delegation_cannot_confer_what_its_delegator_never_held() {
    // §238, the delegation amplification fixture.
    let nexus = stocked("amplification").await;
    let gov = nexus.governance();
    let owner_agent = agent(gov, "kip:principal:team-lead").await;
    let sub_agent = agent(gov, "kip:principal:research-bot").await;
    gov.create_grant(
        GrantDraft {
            space_id: DEFAULT_SPACE.into(),
            grantee_principal: owner_agent.clone(),
            actions: vec!["read".into()],
            delegation_allowed: true,
            ..Default::default()
        },
        SYSTEM_PRINCIPAL,
    )
    .await
    .unwrap();
    gov.create_delegation(
        DelegationDraft {
            space_id: DEFAULT_SPACE.into(),
            delegator_principal: owner_agent.clone(),
            delegate_principal: sub_agent.clone(),
            // The delegate asks for more than the delegator holds.
            actions: vec!["read".into(), "export".into()],
            ..Default::default()
        },
        &owner_agent,
    )
    .await
    .unwrap();

    let session = nexus.session(AuthContext::principal(&sub_agent));
    assert_eq!(
        run_as(
            &session,
            r#"FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }"#
        )
        .await
        .status,
        TopLevelStatus::Succeeded,
        "the part the delegator held comes through"
    );
    assert_eq!(
        error_code(
            &run_as(
                &session,
                r#"EXPORT CAPSULE :out WHERE { ?c CONCEPT {type: "Person"} }"#,
            )
            .await
        ),
        "NotAuthorized",
        "the part it never held does not"
    );
}

#[tokio::test]
async fn revoking_the_delegator_disables_the_delegation_it_made() {
    // §35: a child Delegation cannot outlive the authority that created it,
    // even though its own record still says active.
    let nexus = stocked("delegation_parent").await;
    let gov = nexus.governance();
    let lead = agent(gov, "kip:principal:team-lead").await;
    let bot = agent(gov, "kip:principal:research-bot").await;
    let parent = gov
        .create_grant(
            GrantDraft {
                space_id: DEFAULT_SPACE.into(),
                grantee_principal: lead.clone(),
                actions: vec!["read".into()],
                delegation_allowed: true,
                ..Default::default()
            },
            SYSTEM_PRINCIPAL,
        )
        .await
        .unwrap();
    let child = gov
        .create_delegation(
            DelegationDraft {
                space_id: DEFAULT_SPACE.into(),
                delegator_principal: lead.clone(),
                delegate_principal: bot.clone(),
                actions: vec!["read".into()],
                ..Default::default()
            },
            &lead,
        )
        .await
        .unwrap();

    let session = nexus.session(AuthContext::principal(&bot));
    let query = r#"FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }"#;
    assert_eq!(
        run_as(&session, query).await.status,
        TopLevelStatus::Succeeded
    );

    gov.revoke_grant(parent._id, SYSTEM_PRINCIPAL)
        .await
        .unwrap();
    assert_eq!(error_code(&run_as(&session, query).await), "NotAuthorized");
    assert_eq!(
        gov.delegation(child._id).await.unwrap().unwrap().status,
        status::ACTIVE,
        "the Delegation record is untouched; what it rests on is gone"
    );
}

#[tokio::test]
async fn describe_access_answers_without_needing_any_permission() {
    // §266: an Agent must be able to learn what it may do without already
    // being allowed to do it. Otherwise a denied caller cannot find out why.
    let nexus = stocked("describe_access").await;
    let gov = nexus.governance();
    let reader = agent(gov, "kip:principal:reader").await;
    gov.create_grant(
        GrantDraft {
            space_id: DEFAULT_SPACE.into(),
            grantee_principal: reader.clone(),
            actions: vec!["read".into()],
            constraints: AuthorityConstraints {
                fields: vec!["name".into()],
                ..Default::default()
            },
            ..Default::default()
        },
        SYSTEM_PRINCIPAL,
    )
    .await
    .unwrap();
    let session = nexus.session(AuthContext::principal(&reader));

    let response = run_as(&session, "DESCRIBE ACCESS").await;
    let report = response.first_result().unwrap().clone();
    assert_eq!(report["principal"]["id"], reader);
    assert_eq!(report["is_owner"], false);
    let permissions = report["permissions"].as_array().unwrap();
    assert!(permissions.iter().any(|p| p == "read"));
    assert!(
        !permissions.iter().any(|p| p == "export"),
        "and it does not claim what the caller does not hold"
    );

    // A caller with nothing at all still gets an answer.
    agent(gov, "kip:principal:stranger").await;
    let stranger = nexus.session(AuthContext::principal("kip:principal:stranger"));
    let response = run_as(&stranger, "DESCRIBE ACCESS").await;
    assert_eq!(response.status, TopLevelStatus::Succeeded);
    assert!(
        response.first_result().unwrap()["permissions"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn describe_access_can_be_asked_about_one_operation() {
    let nexus = stocked("describe_access_with").await;
    let gov = nexus.governance();
    let reader = agent(gov, "kip:principal:reader").await;
    gov.create_grant(
        GrantDraft {
            space_id: DEFAULT_SPACE.into(),
            grantee_principal: reader.clone(),
            actions: vec!["read".into()],
            scope: AuthorityScope {
                kinds: vec!["concept".into()],
                ..Default::default()
            },
            ..Default::default()
        },
        SYSTEM_PRINCIPAL,
    )
    .await
    .unwrap();
    let session = nexus.session(AuthContext::principal(&reader));

    let allowed = run_as(
        &session,
        r#"DESCRIBE ACCESS WITH {operation: "read", kind: "concept"}"#,
    )
    .await;
    assert_eq!(
        allowed.first_result().unwrap()["decision"]["decision"],
        "allow"
    );

    let denied = run_as(
        &session,
        r#"DESCRIBE ACCESS WITH {operation: "read", kind: "evidence"}"#,
    )
    .await;
    assert_eq!(
        denied.first_result().unwrap()["decision"]["decision"],
        "deny"
    );
}

#[tokio::test]
async fn describing_the_engine_works_before_anyone_is_authorized() {
    let nexus = stocked("engine_description").await;
    agent(nexus.governance(), "kip:principal:stranger").await;
    let stranger = nexus.session(AuthContext::principal("kip:principal:stranger"));

    for command in ["DESCRIBE PROTOCOL", "DESCRIBE CAPABILITIES"] {
        assert_eq!(
            run_as(&stranger, command).await.status,
            TopLevelStatus::Succeeded,
            "{command}"
        );
    }
    // But the Space itself still needs discovery.
    assert_eq!(
        error_code(&run_as(&stranger, "DESCRIBE PRIMER").await),
        "NotAuthorized"
    );
}

#[tokio::test]
async fn a_grant_that_expired_stops_working_without_being_revoked() {
    let nexus = stocked("expiry").await;
    let gov = nexus.governance();
    let reader = agent(gov, "kip:principal:reader").await;
    gov.create_grant(
        GrantDraft {
            space_id: DEFAULT_SPACE.into(),
            grantee_principal: reader.clone(),
            actions: vec!["read".into()],
            conditions: AuthorityConditions {
                valid_until: "2020-01-01T00:00:00.000Z".into(),
                ..Default::default()
            },
            ..Default::default()
        },
        SYSTEM_PRINCIPAL,
    )
    .await
    .unwrap();
    let session = nexus.session(AuthContext::principal(&reader));
    assert_eq!(
        error_code(
            &run_as(
                &session,
                r#"FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }"#
            )
            .await
        ),
        "NotAuthorized"
    );
}

#[tokio::test]
async fn a_grant_can_require_stronger_authentication_than_the_session_has() {
    let nexus = stocked("auth_strength").await;
    let gov = nexus.governance();
    let reader = agent(gov, "kip:principal:reader").await;
    gov.create_grant(
        GrantDraft {
            space_id: DEFAULT_SPACE.into(),
            grantee_principal: reader.clone(),
            actions: vec!["read".into()],
            conditions: AuthorityConditions {
                min_auth_strength: auth_strength::STRONG.into(),
                ..Default::default()
            },
            ..Default::default()
        },
        SYSTEM_PRINCIPAL,
    )
    .await
    .unwrap();
    let query = r#"FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }"#;

    let ordinary = nexus.session(AuthContext::principal(&reader));
    assert_eq!(error_code(&run_as(&ordinary, query).await), "NotAuthorized");

    let strong =
        nexus.session(AuthContext::principal(&reader).with_auth_strength(auth_strength::STRONG));
    assert_eq!(
        run_as(&strong, query).await.status,
        TopLevelStatus::Succeeded
    );
}

#[tokio::test]
async fn a_declared_purpose_does_not_unlock_a_purpose_bound_grant() {
    // §12 end to end: writing purpose into the request envelope gets nothing.
    let nexus = stocked("purpose").await;
    let gov = nexus.governance();
    let bot = agent(gov, "kip:principal:maintenance-bot").await;
    gov.create_grant(
        GrantDraft {
            space_id: DEFAULT_SPACE.into(),
            grantee_principal: bot.clone(),
            actions: vec!["read".into()],
            conditions: AuthorityConditions {
                purpose: vec!["maintenance".into()],
                min_purpose_assurance: purpose_assurance::SESSION_BOUND.into(),
                ..Default::default()
            },
            ..Default::default()
        },
        SYSTEM_PRINCIPAL,
    )
    .await
    .unwrap();
    let query = r#"FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }"#;

    // Declared in the envelope by the caller.
    let session = nexus.session(AuthContext::principal(&bot));
    let mut request = Request::single(query);
    request.context = Some(anda_kip::RequestContext {
        purpose: Some("maintenance".into()),
        ..Default::default()
    });
    let parsed = anda_kip::parse_kip(query).unwrap();
    let response = session
        .execute(parsed, &request, &request.operations[0])
        .await;
    assert_eq!(error_code(&response), "NotAuthorized");

    // Bound to the session by the host.
    let bound = nexus.session(
        AuthContext::principal(&bot).with_purpose("maintenance", purpose_assurance::SESSION_BOUND),
    );
    assert_eq!(
        run_as(&bound, query).await.status,
        TopLevelStatus::Succeeded
    );
}

#[tokio::test]
async fn a_denied_operation_is_recorded_in_the_audit() {
    let nexus = stocked("decision_audit").await;
    let gov = nexus.governance();
    agent(gov, "kip:principal:stranger").await;
    let stranger = nexus.session(AuthContext::principal("kip:principal:stranger"));
    run_as(
        &stranger,
        r#"FIND(?c) WHERE { ?c CONCEPT {type: "Person"} }"#,
    )
    .await;

    let audit = gov.read_audit(DEFAULT_SPACE, 50).await.unwrap();
    let denial = audit
        .iter()
        .find(|entry| entry.entry_class == "decision" && entry.decision == "deny")
        .expect("the denial is on record");
    assert_eq!(denial.principal_id, "kip:principal:stranger");
    assert_eq!(denial.operation, "read");
}
