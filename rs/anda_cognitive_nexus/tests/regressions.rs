// Regression cases from the September 2026 Nexus review.
#[cfg(test)]
mod tests {
    use anda_cognitive_nexus::{
        CognitiveNexus,
        governance::{
            AuthContext, SYSTEM_PRINCIPAL,
            rows::{AuthorityConstraints, principal_class},
            store::{GrantDraft, PrincipalDraft},
        },
        nexus::{DEFAULT_SPACE, Session},
        schema::{PackageState, SchemaLock, SchemaPackage},
        store::space::SpaceDraft,
    };
    use anda_db::database::{AndaDB, DBConfig};
    use anda_kip::{Executor, Request, Response, TopLevelStatus};
    use std::sync::Arc;
    async fn fresh(name: &str) -> CognitiveNexus {
        let db = AndaDB::connect(
            Arc::new(object_store::memory::InMemory::new()),
            DBConfig {
                name: name.into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let n = CognitiveNexus::connect(Arc::new(db)).await.unwrap();
        n.install_package(
            &SchemaPackage::parse(anda_cognitive_nexus::profiles::COGNITIVE_MEMORY).unwrap(),
            "test",
        )
        .await
        .unwrap();
        let mut lock = SchemaLock::default();
        lock.packages
            .insert("kip://profiles/cognitive-memory".into(), "2.1.0".into());
        lock.states.insert(
            "kip://profiles/cognitive-memory".into(),
            PackageState::Active,
        );
        n.activate_schema(DEFAULT_SPACE, lock).await.unwrap();
        n
    }
    async fn agent(n: &CognitiveNexus, id: &str) {
        n.governance()
            .ensure_principal(PrincipalDraft {
                principal_id: id.into(),
                principal_class: principal_class::AGENT.into(),
                auth_provider: "test".into(),
                auth_subject: id.into(),
                display_name: id.into(),
            })
            .await
            .unwrap();
    }
    async fn run(s: &Session, cmd: &str, dry: bool) -> Response {
        let mut r = Request::single(cmd);
        if dry {
            r.options = Some(anda_kip::RequestOptions {
                dry_run: Some(true),
                ..Default::default()
            });
        }
        s.execute(anda_kip::parse_kip(cmd).unwrap(), &r, &r.operations[0])
            .await
    }
    #[tokio::test]
    async fn cross_space_revoke() {
        let n = fresh("cross_space").await;
        agent(&n, "admin-a").await;
        agent(&n, "reader-b").await;
        n.store
            .open_or_create_space(SpaceDraft {
                space_id: "space-b".into(),
                owner_principal: SYSTEM_PRINCIPAL.into(),
                ..Default::default()
            })
            .await
            .unwrap();
        n.governance()
            .create_grant(
                GrantDraft {
                    space_id: DEFAULT_SPACE.into(),
                    grantee_principal: "admin-a".into(),
                    actions: vec!["manage_grants".into()],
                    ..Default::default()
                },
                SYSTEM_PRINCIPAL,
            )
            .await
            .unwrap();
        let target = n
            .governance()
            .create_grant(
                GrantDraft {
                    space_id: "space-b".into(),
                    grantee_principal: "reader-b".into(),
                    actions: vec!["read".into()],
                    ..Default::default()
                },
                SYSTEM_PRINCIPAL,
            )
            .await
            .unwrap();
        let result = n
            .session(AuthContext::principal("admin-a"))
            .revoke_grant(DEFAULT_SPACE, target._id)
            .await;
        let saved = n.governance().grant(target._id).await.unwrap().unwrap();
        println!(
            "cross_space_revoke: result={result:?}, target_space={}, target_status={}",
            saved.space_id, saved.status
        );
        assert!(
            result.is_err(),
            "a Space-A admin must not revoke Space-B grants"
        );
    }
    #[tokio::test]
    async fn masked_proposition_endpoints() {
        let n = fresh("masked").await;
        agent(&n, "reader").await;
        let seed=run(&n.system_session(),r#"MUTATE { CREATE CONCEPT ?a { TYPE "Person" NAME "Alice" } CREATE CONCEPT ?b { TYPE "Preference" NAME "Secret" } ENSURE PROPOSITION ?p (?a, "prefers", ?b) }"#,false).await;
        assert_eq!(seed.status, TopLevelStatus::Succeeded, "{seed:?}");
        n.governance()
            .create_grant(
                GrantDraft {
                    space_id: DEFAULT_SPACE.into(),
                    grantee_principal: "reader".into(),
                    actions: vec!["read".into()],
                    constraints: AuthorityConstraints {
                        fields: vec!["id".into()],
                        ..Default::default()
                    },
                    ..Default::default()
                },
                SYSTEM_PRINCIPAL,
            )
            .await
            .unwrap();
        let r = run(
            &n.session(AuthContext::principal("reader")),
            r#"FIND(?p) WHERE { ?p PROPOSITION (?s, ?predicate, ?o) }"#,
            false,
        )
        .await;
        println!("masked_proposition: {}", serde_json::to_string(&r).unwrap());
        let rows = r.first_result().unwrap().as_array().unwrap();
        assert!(
            rows.is_empty(),
            "tuple matching must not bind masked endpoints"
        );
    }
    #[tokio::test]
    async fn duplicate_key_dry_run() {
        let n = fresh("duplicate").await;
        let cmd = r#"MUTATE { CREATE CONCEPT ?a { TYPE "Person" NAME "A" SET FIELDS {key: "same"} } CREATE CONCEPT ?b { TYPE "Person" NAME "B" SET FIELDS {key: "same"} } }"#;
        let preview = run(&n.system_session(), cmd, true).await;
        let actual = run(&n.system_session(), cmd, false).await;
        println!("dry_run={preview:?}\nreal={actual:?}");
        assert_eq!(
            preview.status, actual.status,
            "dry run must run the same semantic validation"
        );
    }
    #[tokio::test]
    async fn join_exceeds_budget() {
        let n = fresh("joins").await;
        // Use the documented host Store API to seed cheap, valid current rows.
        let cx = n
            .store
            .begin_transaction(DEFAULT_SPACE, serde_json::json!({}))
            .await
            .unwrap();
        for i in 0..317 {
            n.store
                .insert(
                    &cx,
                    &mut anda_cognitive_nexus::rows::ConceptRow {
                        name: format!("row-{i}"),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
        }
        let started = std::time::Instant::now();
        let r = run(
            &n.system_session(),
            "FIND(COUNT(?a)) WHERE { ?a CONCEPT {} ?b CONCEPT {} }",
            false,
        )
        .await;
        println!(
            "join_budget: elapsed={:?} response={}",
            started.elapsed(),
            serde_json::to_string(&r).unwrap()
        );
        assert_ne!(
            r.status,
            TopLevelStatus::Succeeded,
            "317 x 317 join work exceeds the intermediate budget"
        );
    }
    #[tokio::test]
    async fn cross_space_transaction_description() {
        let n = fresh("describe_leak").await;
        agent(&n, "historian-a").await;
        n.governance()
            .create_grant(
                GrantDraft {
                    space_id: DEFAULT_SPACE.into(),
                    grantee_principal: "historian-a".into(),
                    actions: vec!["read_history".into()],
                    ..Default::default()
                },
                SYSTEM_PRINCIPAL,
            )
            .await
            .unwrap();
        n.store
            .open_or_create_space(SpaceDraft {
                space_id: "space-b".into(),
                owner_principal: SYSTEM_PRINCIPAL.into(),
                ..Default::default()
            })
            .await
            .unwrap();
        let cx = n
            .store
            .begin_transaction("space-b", serde_json::json!({}))
            .await
            .unwrap();
        n.store.journal(&cx,anda_cognitive_nexus::store::space::JournalEntry{status:"committed".into(),transaction_class:"cognitive".into(),changes:vec![serde_json::json!({"op":"create","kind":"concept","id":"C-999","new_version":1})],..Default::default()}).await.unwrap();
        let r = run(
            &n.session(AuthContext::principal("historian-a")),
            &format!("DESCRIBE TRANSACTION {:?}", cx.tx_id),
            false,
        )
        .await;
        println!(
            "transaction_cross_space={}",
            serde_json::to_string(&r).unwrap()
        );
        let missing = run(
            &n.session(AuthContext::principal("historian-a")),
            "DESCRIBE TRANSACTION \"missing#1\"",
            false,
        )
        .await;
        assert_eq!(
            r.error.as_ref().unwrap().message,
            missing.error.unwrap().message
        );
        assert_ne!(
            r.status,
            TopLevelStatus::Succeeded,
            "Space-A read_history must not disclose Space-B transactions"
        );
    }
    #[tokio::test]
    async fn precondition_idempotency_retry() {
        let n = fresh("precondition_retry").await;
        let seq = n.store.get_space(DEFAULT_SPACE).await.unwrap().seq;
        let cmd = r#"CREATE CONCEPT ?c {TYPE "Person" NAME "Alice"}"#;
        let r:Request=serde_json::from_value(serde_json::json!({"kip":"2.0","operations":[{"command":cmd}],"preconditions":{"space_seq":seq},"execution":{"mode":"independent","idempotency_key":"retry-key"}})).unwrap();
        let first = n
            .execute(anda_kip::parse_kip(cmd).unwrap(), &r, &r.operations[0])
            .await;
        let retry = n
            .execute(anda_kip::parse_kip(cmd).unwrap(), &r, &r.operations[0])
            .await;
        println!("first={:?}, retry={retry:?}", first.status);
        assert_eq!(
            first.status, retry.status,
            "same committed request must replay even with its original precondition"
        );
    }
    #[tokio::test]
    async fn capsule_unknown_type() {
        let n = fresh("capsule_source").await;
        let d = fresh("capsule_destination").await;
        let s = run(
            &n.system_session(),
            r#"CREATE CONCEPT ?a {TYPE "Person" NAME "Alice"}"#,
            false,
        )
        .await;
        assert_eq!(s.status, TopLevelStatus::Succeeded);
        let out = run(
            &n.system_session(),
            "EXPORT CAPSULE ?c WHERE { ?c CONCEPT {} }",
            false,
        )
        .await;
        let mut cap: anda_kip::Capsule =
            serde_json::from_value(out.first_result().unwrap().clone()).unwrap();
        cap.payload.records.0[0]["schema_ref"] =
            serde_json::json!("kip://profiles/cognitive-memory@2.1.0/NonexistentType");
        cap.integrity.content_digest =
            anda_cognitive_nexus::capsule::payload_digest(&cap.payload).unwrap();
        let result = d.import_capsule(&cap, DEFAULT_SPACE).await;
        println!("capsule_unknown_type={result:?}");
        assert!(
            result.is_err(),
            "an unavailable Concept type must not be imported"
        );
    }
    #[tokio::test]
    async fn history_cursor_snapshot() {
        let n = fresh("history_pages").await;
        for name in ["A", "B"] {
            assert_eq!(
                run(
                    &n.system_session(),
                    &format!("CREATE CONCEPT ?c {{ TYPE \"Person\" NAME {name:?} }}"),
                    false
                )
                .await
                .status,
                TopLevelStatus::Succeeded
            );
        }
        let seq = n.store.get_space(DEFAULT_SPACE).await.unwrap().seq;
        let first = run(&n.system_session(), "HISTORY SPACE LIMIT 1", false).await;
        let cursor = first.next_cursor.unwrap();
        assert_eq!(
            run(
                &n.system_session(),
                r#"CREATE CONCEPT ?c { TYPE "Person" NAME "C" }"#,
                false
            )
            .await
            .status,
            TopLevelStatus::Succeeded
        );
        let second = run(
            &n.system_session(),
            &format!("HISTORY SPACE LIMIT 100 CURSOR {cursor:?}"),
            false,
        )
        .await;
        println!(
            "history_pinned_seq={seq}, second={}",
            serde_json::to_string(&second).unwrap()
        );
        assert!(
            second
                .first_result()
                .unwrap()
                .as_array()
                .unwrap()
                .iter()
                .all(|r| r["space_seq"].as_u64().unwrap() <= seq),
            "cursor must retain first page snapshot"
        );
    }
    #[tokio::test]
    async fn historical_token_without_read_history() {
        let n = fresh("token_history").await;
        agent(&n, "reader").await;
        n.governance()
            .create_grant(
                GrantDraft {
                    space_id: DEFAULT_SPACE.into(),
                    grantee_principal: "reader".into(),
                    actions: vec!["read".into()],
                    ..Default::default()
                },
                SYSTEM_PRINCIPAL,
            )
            .await
            .unwrap();
        let seed = run(
            &n.system_session(),
            r#"CREATE CONCEPT ?c { TYPE "Person" NAME "Before" }"#,
            false,
        )
        .await;
        let id = seed.first_result().unwrap()["handles"]["c"]
            .as_str()
            .unwrap();
        let seq = n.store.get_space(DEFAULT_SPACE).await.unwrap().seq;
        let change = run(
            &n.system_session(),
            &format!("UPDATE {id:?} SET FIELDS {{name: \"After\"}}"),
            false,
        )
        .await;
        assert_eq!(change.status, TopLevelStatus::Succeeded, "{change:?}");
        let session = n.session(AuthContext::principal("reader"));
        let query = "FIND(?c.name) WHERE { ?c CONCEPT {} }";
        let direct = run(&session, &format!("{query} AS OF SEQ {seq}"), false).await;
        let token =
            anda_cognitive_nexus::store::history::Coordinate { seq }.to_token(DEFAULT_SPACE);
        let r:Request=serde_json::from_value(serde_json::json!({"kip":"2.0","operations":[{"command":query}],"read":{"snapshot_token":token}})).unwrap();
        let via_token = session
            .execute(anda_kip::parse_kip(query).unwrap(), &r, &r.operations[0])
            .await;
        println!(
            "historical_direct={:?}, via_token={}",
            direct.status,
            serde_json::to_string(&via_token).unwrap()
        );
        assert_eq!(
            direct.status, via_token.status,
            "snapshot token must enforce the same history permission"
        );
    }
    #[tokio::test]
    async fn rejected_commit_cleans_shells() {
        let n = fresh("shell_cleanup").await;
        let cmd = r#"MUTATE { CREATE CONCEPT ?a { TYPE "Person" NAME "A" SET FIELDS {key: "same"} } CREATE CONCEPT ?b { TYPE "Person" NAME "B" SET FIELDS {key: "same"} } }"#;
        let r = run(&n.system_session(), cmd, false).await;
        assert_eq!(r.status, TopLevelStatus::Failed);
        println!(
            "rejected_commit remaining_concept_rows={}",
            n.store.concepts().len()
        );
        assert_eq!(
            n.store.concepts().len(),
            0,
            "semantic rejection must discard allocated pending rows"
        );
    }
    #[tokio::test]
    async fn repeated_client_key_in_one_transaction() {
        let n = fresh("client_key_in_batch").await;
        let cmd = r#"MUTATE { CREATE CONCEPT ?a { TYPE "Person" NAME "Alice" CLIENT KEY "same-creation" } CREATE CONCEPT ?b { TYPE "Person" NAME "Alice" CLIENT KEY "same-creation" } }"#;
        let r = run(&n.system_session(), cmd, false).await;
        println!(
            "duplicate_client_key={}",
            serde_json::to_string(&r).unwrap()
        );
        if r.status == TopLevelStatus::Succeeded {
            let v = r.first_result().unwrap();
            assert_eq!(
                v["handles"]["a"], v["handles"]["b"],
                "one client key must identify one logical creation inside a transaction too"
            );
        }
    }
    #[tokio::test]
    async fn session_control_targets_and_global_identity_are_isolated() {
        use anda_cognitive_nexus::governance::store::{
            ActorBindingDraft, ApprovalDraft, DelegationDraft, GroupDraft, PolicyDraft,
        };
        let n = fresh("all_control_targets").await;
        agent(&n, "admin-a").await;
        n.store
            .open_or_create_space(SpaceDraft {
                space_id: "space-b".into(),
                owner_principal: SYSTEM_PRINCIPAL.into(),
                ..Default::default()
            })
            .await
            .unwrap();
        n.governance()
            .create_grant(
                GrantDraft {
                    space_id: DEFAULT_SPACE.into(),
                    grantee_principal: "admin-a".into(),
                    actions: [
                        "manage_grants",
                        "manage_delegation",
                        "manage_actor_binding",
                        "manage_policy",
                        "approve_high_risk",
                        "manage_membership",
                    ]
                    .map(str::to_string)
                    .to_vec(),
                    ..Default::default()
                },
                SYSTEM_PRINCIPAL,
            )
            .await
            .unwrap();
        let session = n.session(AuthContext::principal("admin-a"));
        let delegation = n
            .governance()
            .create_delegation(
                DelegationDraft {
                    space_id: "space-b".into(),
                    delegator_principal: SYSTEM_PRINCIPAL.into(),
                    delegate_principal: "admin-a".into(),
                    actions: vec!["read".into()],
                    ..Default::default()
                },
                SYSTEM_PRINCIPAL,
            )
            .await
            .unwrap();
        assert!(
            session
                .revoke_delegation(DEFAULT_SPACE, delegation._id)
                .await
                .is_err()
        );
        assert_eq!(
            n.governance()
                .delegation(delegation._id)
                .await
                .unwrap()
                .unwrap()
                .status,
            "active"
        );
        let binding = n
            .governance()
            .create_binding(
                ActorBindingDraft {
                    principal_id: "admin-a".into(),
                    actor_key: "C-1".into(),
                    scope: "space-b".into(),
                    ..Default::default()
                },
                SYSTEM_PRINCIPAL,
            )
            .await
            .unwrap();
        assert!(
            session
                .revoke_binding(DEFAULT_SPACE, binding._id)
                .await
                .is_err()
        );
        for scope in ["space-b", "*"] {
            assert!(
                session
                    .create_binding(
                        DEFAULT_SPACE,
                        ActorBindingDraft {
                            principal_id: "admin-a".into(),
                            actor_key: "C-1".into(),
                            scope: scope.into(),
                            ..Default::default()
                        }
                    )
                    .await
                    .is_err()
            );
        }
        let own_binding = session
            .create_binding(
                DEFAULT_SPACE,
                ActorBindingDraft {
                    principal_id: "admin-a".into(),
                    actor_key: "C-1".into(),
                    scope: DEFAULT_SPACE.into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        session
            .revoke_binding(DEFAULT_SPACE, own_binding._id)
            .await
            .unwrap();
        let approval = n
            .governance()
            .request_approval(
                ApprovalDraft {
                    space_id: "space-b".into(),
                    operation: "purge".into(),
                    subject_digest: "foreign".into(),
                    ..Default::default()
                },
                SYSTEM_PRINCIPAL,
            )
            .await
            .unwrap();
        assert!(
            session
                .approve(DEFAULT_SPACE, approval._id, "no")
                .await
                .is_err()
        );
        assert_eq!(
            n.governance()
                .find_approval(approval._id)
                .await
                .unwrap()
                .unwrap()
                .status,
            "pending"
        );
        n.governance()
            .publish_policy(
                PolicyDraft {
                    policy_id: "foreign-policy".into(),
                    space_id: "space-b".into(),
                    ..Default::default()
                },
                SYSTEM_PRINCIPAL,
            )
            .await
            .unwrap();
        for space in [DEFAULT_SPACE, "space-b"] {
            assert!(
                session
                    .publish_policy(
                        DEFAULT_SPACE,
                        PolicyDraft {
                            policy_id: "foreign-policy".into(),
                            space_id: space.into(),
                            ..Default::default()
                        }
                    )
                    .await
                    .is_err()
            );
        }
        assert!(
            session
                .put_group(
                    DEFAULT_SPACE,
                    GroupDraft {
                        group_id: "global".into(),
                        members: vec!["admin-a".into()],
                        ..Default::default()
                    }
                )
                .await
                .is_err()
        );
        assert!(
            session
                .set_principal_status(DEFAULT_SPACE, SYSTEM_PRINCIPAL, "suspended")
                .await
                .is_err()
        );
        // The trusted embedded host remains able to manage global identities.
        n.system_session()
            .put_group(
                DEFAULT_SPACE,
                GroupDraft {
                    group_id: "global".into(),
                    members: vec!["admin-a".into()],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let own = session
            .create_grant(
                DEFAULT_SPACE,
                GrantDraft {
                    grantee_principal: "admin-a".into(),
                    actions: vec!["read".into()],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        session.revoke_grant(DEFAULT_SPACE, own._id).await.unwrap();
    }

    #[tokio::test]
    async fn masked_tuple_cannot_be_read_by_id_or_fixed_endpoint() {
        let n = fresh("tuple_masks").await;
        agent(&n, "masked").await;
        assert_eq!(run(&n.system_session(),r#"MUTATE {CREATE CONCEPT ?a {TYPE "Person" NAME "Alice"} CREATE CONCEPT ?b {TYPE "Preference" NAME "Secret"} ENSURE PROPOSITION ?p (?a,"prefers",?b)}"#,false).await.status,TopLevelStatus::Succeeded);
        n.governance()
            .create_grant(
                GrantDraft {
                    space_id: DEFAULT_SPACE.into(),
                    grantee_principal: "masked".into(),
                    actions: vec!["read".into()],
                    constraints: AuthorityConstraints {
                        fields: vec!["id".into()],
                        ..Default::default()
                    },
                    ..Default::default()
                },
                SYSTEM_PRINCIPAL,
            )
            .await
            .unwrap();
        let s = n.session(AuthContext::principal("masked"));
        let direct = run(&s, r#"FIND(?p) WHERE {?p PROPOSITION (id:"P-1")}"#, false).await;
        let rows = direct.first_result().unwrap().as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].get("canonical_subject").is_none());
        assert!(rows[0].get("canonical_object").is_none());
        for query in [
            r#"FIND(?s,?pred,?o) WHERE {(?s,?pred,?o)}"#,
            r#"FIND(?p) WHERE {?p PROPOSITION ({id:"C-1"},"prefers",{id:"C-2"})}"#,
            r#"FIND(?o) WHERE {({id:"C-1"},"prefers"{1,2},?o)}"#,
        ] {
            let r = run(&s, query, false).await;
            assert_eq!(
                r.first_result(),
                Some(&serde_json::json!([])),
                "{query}: {r:?}"
            );
        }
    }

    #[tokio::test]
    async fn concurrent_space_preconditions_admit_one_new_write() {
        let n = fresh("precondition_race").await;
        let seq = n.store.get_space(DEFAULT_SPACE).await.unwrap().seq;
        let command = r#"CREATE CONCEPT ?c {TYPE "Person" NAME "CAS"}"#;
        let r:Request=serde_json::from_value(serde_json::json!({"kip":"2.0","operations":[{"command":command}],"preconditions":{"space_seq":seq}})).unwrap();
        let (one, two) = tokio::join!(
            n.execute(anda_kip::parse_kip(command).unwrap(), &r, &r.operations[0]),
            n.execute(anda_kip::parse_kip(command).unwrap(), &r, &r.operations[0])
        );
        assert_eq!(
            [one, two]
                .iter()
                .filter(|r| r.status == TopLevelStatus::Succeeded)
                .count(),
            1
        );
        assert_eq!(n.store.concepts().len(), 1);
    }

    #[tokio::test]
    async fn capsule_facet_contract_is_checked_before_commit() {
        let n = fresh("capsule_facet_source").await;
        let d = fresh("capsule_facet_destination").await;
        run(
            &n.system_session(),
            r#"CREATE CONCEPT ?a {TYPE "Person" NAME "Alice"}"#,
            false,
        )
        .await;
        let out = run(
            &n.system_session(),
            "EXPORT CAPSULE ?c WHERE {?c CONCEPT {}}",
            false,
        )
        .await;
        let mut cap: anda_kip::Capsule =
            serde_json::from_value(out.first_result().unwrap().clone()).unwrap();
        cap.payload.records.0[0]["facets"] = serde_json::json!({"kip://profiles/cognitive-memory@2.1.0/MnemonicState":{"memory_strength":"strong"}});
        cap.integrity.content_digest =
            anda_cognitive_nexus::capsule::payload_digest(&cap.payload).unwrap();
        assert!(d.import_capsule(&cap, DEFAULT_SPACE).await.is_err());
        assert_eq!(d.store.concepts().len(), 0);
    }

    #[tokio::test]
    async fn change_pages_order_sequences_instead_of_physical_ids() {
        use anda_cognitive_nexus::rows::TransactionRow;
        let n = fresh("ordered_pages").await;
        let table = n.store.transactions();
        for seq in (1..=1100).rev() {
            table
                .add_from(&TransactionRow {
                    _id: 0,
                    tx_id: format!("{DEFAULT_SPACE}#{seq}"),
                    space: DEFAULT_SPACE.into(),
                    seq,
                    committed_at: anda_cognitive_nexus::time::now(),
                    status: "committed".into(),
                    ..Default::default()
                })
                .await
                .unwrap();
        }
        let mut space = n.store.get_space(DEFAULT_SPACE).await.unwrap();
        space.seq = 1100;
        // Use the raw host table so no additional governance envelope changes the head.
        n.store
            .spaces()
            .update(
                space._id,
                std::collections::BTreeMap::from([(
                    "seq".into(),
                    anda_db::schema::Fv::U64(space.seq),
                )]),
            )
            .await
            .unwrap();
        let page = n
            .system_session()
            .change_page(DEFAULT_SPACE, 0, 1001)
            .await
            .unwrap();
        let changes = page["changes"].as_array().unwrap();
        assert_eq!(changes.len(), 1001);
        assert_eq!(changes[0]["space_seq"], 1);
        assert_eq!(changes[1000]["space_seq"], 1001);
        assert_eq!(page["coverage"]["complete"], false);
        let next = n
            .system_session()
            .change_page(DEFAULT_SPACE, 1001, 1001)
            .await
            .unwrap();
        assert_eq!(next["changes"].as_array().unwrap().len(), 99);
        assert_eq!(next["coverage"]["complete"], true);
    }

    #[tokio::test]
    async fn output_index_backfills_legacy_rows_from_actual_references() {
        use anda_cognitive_nexus::rows::ActivityRow;
        use anda_db::collection::CollectionConfig;
        let db = Arc::new(
            AndaDB::connect(
                Arc::new(object_store::memory::InMemory::new()),
                DBConfig {
                    name: "legacy_output_index".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
        );
        let activities = db
            .open_or_create_collection(
                ActivityRow::schema().unwrap(),
                CollectionConfig {
                    name: "activities".into(),
                    description: String::new(),
                },
                async |_| Ok(()),
            )
            .await
            .unwrap();
        let id = activities
            .add_from(&ActivityRow {
                space: DEFAULT_SPACE.into(),
                state: "active".into(),
                outputs: vec![serde_json::json!({"id":"C-1"})],
                output_keys: vec![],
                ..Default::default()
            })
            .await
            .unwrap();
        db.close_collection("activities").await.unwrap();
        let n = CognitiveNexus::connect(db).await.unwrap();
        let endpoint = anda_cognitive_nexus::Endpoint::Local("C-1".parse().unwrap()).key();
        let ids = n
            .store
            .activities()
            .query_all_ids(anda_cognitive_nexus::store::eq_field(
                "output_keys",
                anda_db::schema::Fv::Text(endpoint.clone()),
            ))
            .await
            .unwrap();
        assert_eq!(ids, vec![id]);
        // Updating outputs must remove the derived old posting even if the old column was empty.
        let cx = n
            .store
            .begin_transaction(DEFAULT_SPACE, serde_json::json!({}))
            .await
            .unwrap();
        let mut row: ActivityRow = n.store.activities().get_as(id).await.unwrap();
        row.outputs = vec![serde_json::json!({"id":"C-2"})];
        n.store.update(&cx, &mut row).await.unwrap();
        assert!(
            n.store
                .activities()
                .query_all_ids(anda_cognitive_nexus::store::eq_field(
                    "output_keys",
                    anda_db::schema::Fv::Text(endpoint)
                ))
                .await
                .unwrap()
                .is_empty()
        );
    }
    #[tokio::test]
    async fn read_only_paging_does_not_allow_forged_historical_coordinates() {
        use anda_cognitive_nexus::store::history::{CursorFamily, PageCursor, traversal_of};
        let n = fresh("issued_pages").await;
        agent(&n, "pager").await;
        n.governance()
            .create_grant(
                GrantDraft {
                    space_id: DEFAULT_SPACE.into(),
                    grantee_principal: "pager".into(),
                    actions: vec!["read".into()],
                    ..Default::default()
                },
                SYSTEM_PRINCIPAL,
            )
            .await
            .unwrap();
        run(&n.system_session(),r#"MUTATE {CREATE CONCEPT ?a {TYPE "Person" NAME "A"} CREATE CONCEPT ?b {TYPE "Person" NAME "B"}}"#,false).await;
        let s = n.session(AuthContext::principal("pager"));
        let query = "FIND(?c.name) WHERE {?c CONCEPT {}} ORDER BY ?c.name LIMIT 1";
        let first = run(&s, query, false).await;
        let token = first.next_cursor.unwrap();
        run(
            &n.system_session(),
            r#"UPDATE "C-2" SET FIELDS {name:"Changed"}"#,
            false,
        )
        .await;
        let second = run(&s, &format!("{query} CURSOR {token:?}"), false).await;
        assert_eq!(second.first_result(), Some(&serde_json::json!(["B"])));
        let anda_kip::Command::Kql(ast) = anda_kip::parse_kip(query).unwrap() else {
            unreachable!()
        };
        let forged = PageCursor {
            family: CursorFamily::Query,
            snapshot_seq: 0,
            offset: 0,
            traversal: traversal_of(&ast, None, None),
        }
        .to_token(DEFAULT_SPACE);
        let response = run(&s, &format!("{query} CURSOR {forged:?}"), false).await;
        assert_eq!(response.error.unwrap().code, "CursorExpired");
    }
    #[tokio::test]
    async fn validation_only_imports_remain_available_without_allowing_local_creation() {
        let source = fresh("validation_source").await;
        let dest = fresh("validation_destination").await;
        run(&source.system_session(),r#"CREATE CONCEPT ?a {TYPE "Person" NAME "Imported" SET FACET "MnemonicState" {memory_strength:0.5}}"#,false).await;
        let exported = run(
            &source.system_session(),
            "EXPORT CAPSULE ?c WHERE {?c CONCEPT {}}",
            false,
        )
        .await;
        let capsule = serde_json::from_value(exported.first_result().unwrap().clone()).unwrap();
        let mut lock = dest
            .store
            .schema_environment(DEFAULT_SPACE)
            .await
            .unwrap()
            .lock;
        lock.states.insert(
            "kip://profiles/cognitive-memory".into(),
            PackageState::ValidationOnly,
        );
        dest.activate_schema(DEFAULT_SPACE, lock).await.unwrap();
        dest.import_capsule_isolated(&capsule, DEFAULT_SPACE)
            .await
            .unwrap();
        assert_eq!(dest.store.concepts().len(), 1);
        assert_eq!(run(&dest.system_session(),r#"CREATE CONCEPT ?a {TYPE "kip://profiles/cognitive-memory@2.1.0/Person" NAME "Local"}"#,false).await.status,TopLevelStatus::Failed);
    }
}
