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
        SYSTEM_PRINCIPAL, classification,
        rows::{
            ActorBindingRow, AuthorityConditions, AuthorityConstraints, AuthorityScope, GrantRow,
            PolicyStatement, assurance, auth_strength, binding_class, principal_class, status,
        },
        store::{
            ActorBindingDraft, ApprovalDraft, DelegationDraft, GovernanceStore, GrantDraft,
            GroupDraft, PolicyDraft, PrincipalDraft, grant_id,
        },
    },
    nexus::DEFAULT_SPACE,
};
use anda_db::database::{AndaDB, DBConfig};
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
