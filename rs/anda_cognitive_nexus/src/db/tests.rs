
use super::*;
use anda_db::{
    database::{AndaDB, DBConfig},
    storage::StorageConfig,
};
use object_store::memory::InMemory;
use std::sync::Arc;

async fn setup_test_db<F>(f: F) -> Result<CognitiveNexus, KipError>
where
    F: AsyncFnOnce(&CognitiveNexus) -> Result<(), KipError>,
{
    let object_store = Arc::new(InMemory::new());

    let db_config = DBConfig {
        name: "test_anda".to_string(),
        description: "Test Anda Cognitive Nexus".to_string(),
        storage: StorageConfig {
            compress_level: 0,
            ..Default::default()
        },
        lock: None, // no lock for demo
    };

    let db = AndaDB::connect(object_store, db_config)
        .await
        .map_err(db_to_kip_error)?;
    let nexus = CognitiveNexus::connect(Arc::new(db), f).await?;
    Ok(nexus)
}

#[tokio::test]
async fn test_connect_skips_bootstrap_when_capsules_are_current() {
    let object_store = Arc::new(InMemory::new());
    let db_config = DBConfig {
        name: "test_bootstrap_skip".to_string(),
        description: "Test Anda Cognitive Nexus bootstrap reuse".to_string(),
        storage: StorageConfig {
            compress_level: 0,
            ..Default::default()
        },
        lock: None,
    };
    let db = Arc::new(
        AndaDB::connect(object_store, db_config)
            .await
            .map_err(db_to_kip_error)
            .unwrap(),
    );

    let first = CognitiveNexus::connect(Arc::clone(&db), async |_| Ok(()))
        .await
        .unwrap();
    assert_eq!(first.capsule_version(), 2);
    for name in [
        META_CONCEPT_TYPE,
        PERSON_TYPE,
        PREFERENCE_TYPE,
        EVENT_TYPE,
        SLEEP_TASK_TYPE,
        INSIGHT_TYPE,
        COMMITMENT_TYPE,
    ] {
        assert!(
            first
                .has_concept(&ConceptPK::Object {
                    r#type: META_CONCEPT_TYPE.to_string(),
                    name: name.to_string(),
                })
                .await
        );
    }
    drop(first);

    let second = CognitiveNexus::connect(Arc::clone(&db), async |nexus| {
        assert_eq!(nexus.capsule_version(), 2);
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(second.capsule_version(), 2);
}

#[tokio::test]
async fn test_connect_syncs_bundled_capsules_by_content_hash() {
    let object_store = Arc::new(InMemory::new());
    let db_config = DBConfig {
        name: "test_capsule_sync".to_string(),
        description: "Test bundled capsule hash sync".to_string(),
        storage: StorageConfig {
            compress_level: 0,
            ..Default::default()
        },
        lock: None,
    };
    let db = Arc::new(
        AndaDB::connect(object_store, db_config)
            .await
            .map_err(db_to_kip_error)
            .unwrap(),
    );

    // A fresh connect records a content hash for every bundled capsule.
    let first = CognitiveNexus::connect(Arc::clone(&db), async |_| Ok(()))
        .await
        .unwrap();
    for (name, source, _) in BUNDLED_CAPSULES {
        assert_eq!(
            first
                .concepts
                .get_extension_as::<String>(&format!("capsule_hash:{name}")),
            Some(capsule_hash(source)),
            "missing hash for capsule {name}"
        );
    }

    // Simulate an upgrade shipping a revised Person capsule: drift a key
    // the capsule owns, then invalidate the stored hash (a changed .kip
    // source would do exactly this).
    first
        .execute_kml(
            parse_kml(
                r#"UPSERT {
                        CONCEPT ?c {
                            {type: "$ConceptType", name: "Person"}
                            SET ATTRIBUTES { "display_hint": "drifted" }
                        }
                    }"#,
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap();
    first
        .concepts
        .save_extension(
            "capsule_hash:person".to_string(),
            Fv::Text("stale".to_string()),
        )
        .await
        .unwrap();
    drop(first);

    // Reconnect: the Person capsule is re-applied (shallow merge restores
    // the keys it owns) and the hash is repaired.
    let second = CognitiveNexus::connect(Arc::clone(&db), async |_| Ok(()))
        .await
        .unwrap();
    let person_def = second
        .get_concept(&ConceptPK::Object {
            r#type: META_CONCEPT_TYPE.to_string(),
            name: PERSON_TYPE.to_string(),
        })
        .await
        .unwrap();
    assert_eq!(person_def.attributes["display_hint"], json!("👤"));
    assert_eq!(
        second
            .concepts
            .get_extension_as::<String>("capsule_hash:person"),
        Some(capsule_hash(PERSON_KIP))
    );

    // Self-healing: deleting an anchor definition (hash still current)
    // re-applies that capsule on the next connect.
    second
        .execute_kml(
            parse_kml(
                r#"DELETE CONCEPT ?c DETACH
                    WHERE { ?c {type: "$ConceptType", name: "Insight"} }"#,
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap();
    assert!(
        !second
            .has_concept(&ConceptPK::Object {
                r#type: META_CONCEPT_TYPE.to_string(),
                name: INSIGHT_TYPE.to_string(),
            })
            .await
    );
    drop(second);

    let third = CognitiveNexus::connect(Arc::clone(&db), async |_| Ok(()))
        .await
        .unwrap();
    assert!(
        third
            .has_concept(&ConceptPK::Object {
                r#type: META_CONCEPT_TYPE.to_string(),
                name: INSIGHT_TYPE.to_string(),
            })
            .await
    );
}

async fn setup_test_data(nexus: &CognitiveNexus) -> Result<(), KipError> {
    // 创建基础概念类型
    let drug_type_kml = r#"
        UPSERT {
            CONCEPT ?drug_type {
                {type: "$ConceptType", name: "Drug"}
                SET ATTRIBUTES {
                    "description": "Pharmaceutical drug concept type"
                }
            }
            WITH METADATA {
                "source": "test_setup",
                "confidence": 1.0
            }
        }
        "#;
    nexus.execute_kml(parse_kml(drug_type_kml)?, false).await?;

    let symptom_type_kml = r#"
        UPSERT {
            CONCEPT ?symptom_type {
                {type: "$ConceptType", name: "Symptom"}
                SET ATTRIBUTES {
                    "description": "Medical symptom concept type"
                }
            }
            WITH METADATA {
                "source": "test_setup",
                "confidence": 1.0
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(symptom_type_kml)?, false)
        .await?;

    // 创建谓词类型
    let treats_pred_kml = r#"
        UPSERT {
            CONCEPT ?treats_pred {
                {type: "$PropositionType", name: "treats"}
                SET ATTRIBUTES {
                    "description": "Treatment relationship"
                }
            }
            WITH METADATA {
                "source": "test_setup",
                "confidence": 1.0
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(treats_pred_kml)?, false)
        .await?;

    let headache_kml = r#"
        UPSERT {
            CONCEPT ?headache {
                {type: "Symptom", name: "Headache"}
                SET ATTRIBUTES {
                    "severity": "moderate",
                    "duration": "2-4 hours"
                }
            }
            WITH METADATA {
                "source": "test_data",
                "confidence": 1.0
            }
        }
        "#;
    nexus.execute_kml(parse_kml(headache_kml)?, false).await?;

    let fever_kml = r#"
        UPSERT {
            CONCEPT ?fever {
                {type: "Symptom", name: "Fever"}
                SET ATTRIBUTES {
                    "temperature_range": "38-40°C",
                    "common": true
                }
            }
            WITH METADATA {
                "source": "test_data",
                "confidence": 0.9
            }
        }
        "#;
    nexus.execute_kml(parse_kml(fever_kml)?, false).await?;

    // 创建测试概念
    let aspirin_kml = r#"
        UPSERT {
            CONCEPT ?aspirin {
                {type: "Drug", name: "Aspirin"}
                SET ATTRIBUTES {
                    "molecular_formula": "C9H8O4",
                    "risk_level": 2,
                    "dosage": "325mg"
                }
                SET PROPOSITIONS {
                    ("treats", {type: "Symptom", name: "Headache"})
                    ("treats", {type: "Symptom", name: "Fever"})
                }
            }
        }
        WITH METADATA {
            "source": "test_data",
            "confidence": 0.95
        }
        "#;
    nexus.execute_kml(parse_kml(aspirin_kml)?, false).await?;

    Ok(())
}

#[tokio::test]
async fn test_cognitive_nexus_connect() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    assert_eq!(nexus.name(), "test_anda");

    // 验证元类型已创建
    assert!(
        nexus
            .has_concept(&ConceptPK::Object {
                r#type: META_CONCEPT_TYPE.to_string(),
                name: META_CONCEPT_TYPE.to_string()
            })
            .await
    );

    assert!(
        nexus
            .has_concept(&ConceptPK::Object {
                r#type: META_CONCEPT_TYPE.to_string(),
                name: META_PROPOSITION_TYPE.to_string()
            })
            .await
    );
}

#[tokio::test]
async fn test_kml_upsert_concept() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // 验证概念已创建
    let aspirin = nexus
        .get_concept(&ConceptPK::Object {
            r#type: "Drug".to_string(),
            name: "Aspirin".to_string(),
        })
        .await
        .unwrap();

    assert_eq!(aspirin.r#type, "Drug");
    assert_eq!(aspirin.name, "Aspirin");
    assert_eq!(
        aspirin
            .attributes
            .get("molecular_formula")
            .unwrap()
            .as_str()
            .unwrap(),
        "C9H8O4"
    );
    assert_eq!(
        aspirin
            .attributes
            .get("risk_level")
            .unwrap()
            .as_u64()
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn test_public_concept_id_helpers_get_or_init_and_close() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let aspirin = nexus
        .get_concept(&ConceptPK::Object {
            r#type: "Drug".to_string(),
            name: "Aspirin".to_string(),
        })
        .await
        .unwrap();
    let aspirin_id = aspirin._id;

    assert!(nexus.has_concept(&ConceptPK::ID(aspirin_id)).await);
    assert_eq!(
        nexus
            .get_concept(&ConceptPK::ID(aspirin_id))
            .await
            .unwrap()
            .name,
        "Aspirin"
    );
    assert!(!nexus.has_concept(&ConceptPK::ID(u64::MAX)).await);
    assert!(nexus.get_concept(&ConceptPK::ID(u64::MAX)).await.is_err());

    let created = nexus
        .get_or_init_concept(
            "Drug".to_string(),
            "UnitOnlyDrug".to_string(),
            Map::from_iter([("risk_level".to_string(), json!(1))]),
            Map::from_iter([("source".to_string(), json!("unit"))]),
        )
        .await
        .unwrap();
    assert_ne!(created._id, 0);
    assert_eq!(created.attributes["risk_level"], json!(1));

    let existing = nexus
        .get_or_init_concept(
            "Drug".to_string(),
            "UnitOnlyDrug".to_string(),
            Map::from_iter([("risk_level".to_string(), json!(9))]),
            Map::new(),
        )
        .await
        .unwrap();
    assert_eq!(existing._id, created._id);
    assert_eq!(existing.attributes["risk_level"], json!(1));

    nexus.close().await.unwrap();
}

#[tokio::test]
async fn test_kml_concept_id_matcher_updates_existing_and_rejects_missing_id() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let aspirin = nexus
        .get_concept(&ConceptPK::Object {
            r#type: "Drug".to_string(),
            name: "Aspirin".to_string(),
        })
        .await
        .unwrap();
    let aspirin_id = aspirin.entity_id().to_string();

    let kml = format!(
        r#"
            UPSERT {{
                CONCEPT ?aspirin {{
                    {{id: "{aspirin_id}"}}
                    SET ATTRIBUTES {{
                        "risk_level": 5,
                        "dosage": "100mg"
                    }}
                }}
            }}
            "#
    );
    let result = nexus
        .execute_kml(parse_kml(&kml).unwrap(), false)
        .await
        .unwrap();
    let result: UpsertResult = serde_json::from_value(result).unwrap();
    assert_eq!(result.upsert_concept_nodes, vec![aspirin_id.clone()]);

    let updated = nexus
        .get_concept(&ConceptPK::ID(aspirin._id))
        .await
        .unwrap();
    assert_eq!(updated.attributes["risk_level"], json!(5));
    assert_eq!(updated.attributes["dosage"], json!("100mg"));

    let missing = r#"
            UPSERT {
                CONCEPT ?missing {
                    {id: "C:18446744073709551615"}
                    SET ATTRIBUTES { "risk_level": 1 }
                }
            }
            "#;
    let err = nexus
        .execute_kml(parse_kml(missing).unwrap(), false)
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::NotFound));
}

#[tokio::test]
async fn test_kql_find_concepts() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // 测试基本概念查询
    let kql = r#"
        FIND(?drug.name, ?drug.attributes.risk_level)
        WHERE {
            ?drug {type: "Drug"}
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!([["Aspirin", 2]]));

    let kql = r#"
        FIND(?drug) // return concept object
        WHERE {
            ?drug {type: "Drug"}
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (mut result, _) = nexus.execute_kql(query).await.unwrap();
    // The engine maintains `_version` / `_updated_at` in metadata
    // (KIP §2.11.1); check them separately from the author metadata.
    let metadata = result[0]["metadata"].as_object_mut().unwrap();
    assert_eq!(metadata.remove("_version"), Some(json!(1)));
    assert!(
        metadata
            .remove("_updated_at")
            .and_then(|v| v.as_str().map(|s| s.ends_with('Z')))
            .unwrap_or(false)
    );
    assert_eq!(
        result,
        json!([{
            "_type":"ConceptNode",
            "id":"C:28",
            "type":"Drug",
            "name":"Aspirin",
            "attributes":{"dosage":"325mg","molecular_formula":"C9H8O4","risk_level":2},
            "metadata":{"source":"test_data","confidence":0.95}
        }])
    );
}

#[tokio::test]
async fn test_kql_filter_regex() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let kql = r#"
        FIND(?drug.name)
        WHERE {
            ?drug {type: "Drug"}
            FILTER(REGEX(?drug.name, "^Asp.*"))
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!(["Aspirin"]));
}

#[tokio::test]
async fn test_kql_proposition_matching() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // 测试命题匹配
    let kql = r#"
        FIND(?drug.name, ?symptom.name)
        WHERE {
            ?drug {type: "Drug"}
            ?symptom {type: "Symptom"}
            (?drug, "treats", ?symptom)
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!([["Aspirin"], ["Headache", "Fever"]]));

    let kql = r#"
        FIND(?drug.name, ?symptom.name)
        WHERE {
            ?drug {type: "Drug"}
            (?drug, "treats", ?symptom) // find symptom by proposition matching
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!([["Aspirin"], ["Headache", "Fever"]]));

    let kql = r#"
        FIND(?drug.name, ?symptom.name)
        WHERE {
            ?drug {type: "Drug"}
            ?symptom {type: "Symptom"}
            (?drug, "treats1", ?symptom) // when predicate not exists
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!([[], []]));

    let kql = r#"
        FIND(?symptom.name, COUNT(?link))
        WHERE {
            ?symptom {type: "Symptom"}
            OPTIONAL {
                ?link (?drug, "treats", ?symptom)
            }
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!([["Headache", "Fever"], 2]));
}

#[tokio::test]
async fn test_kql_proposition_id_matcher_success_and_invalid_id() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let query = parse_kql(
        r#"
            FIND(?link)
            WHERE {
                ?link (?drug, "treats", ?symptom)
            }
            LIMIT 1
            "#,
    )
    .unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    let links: Vec<PropositionLink> = serde_json::from_value(result).unwrap();
    let link_id = links[0].id.clone();

    let query = parse_kql(&format!(
        r#"
            FIND(?link)
            WHERE {{
                ?link (id: "{link_id}")
            }}
            "#
    ))
    .unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    let links: Vec<PropositionLink> = serde_json::from_value(result).unwrap();
    assert_eq!(links.len(), 1);
    assert_eq!(links[0].id, link_id);

    let query = parse_kql(
        r#"
            FIND(?link)
            WHERE {
                ?link (id: "C:1")
            }
            "#,
    )
    .unwrap();
    let err = nexus.execute_kql(query).await.unwrap_err();
    assert!(matches!(err.code, KipErrorCode::InvalidSyntax));
    assert!(err.message.contains("Invalid proposition link ID"));
}

#[tokio::test]
async fn test_kml_proposition_id_matcher_and_object_error_paths() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let query = parse_kql(
        r#"
            FIND(?link)
            WHERE {
                ?link (?drug, "treats", ?symptom)
            }
            LIMIT 1
            "#,
    )
    .unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    let links: Vec<PropositionLink> = serde_json::from_value(result).unwrap();
    let link_id = links[0].id.clone();

    let update = format!(
        r#"
            UPSERT {{
                PROPOSITION ?link {{
                    (id: "{link_id}")
                    SET ATTRIBUTES {{ "source": "kml-id" }}
                }}
            }}
            "#
    );
    let result = nexus
        .execute_kml(parse_kml(&update).unwrap(), false)
        .await
        .unwrap();
    let result: UpsertResult = serde_json::from_value(result).unwrap();
    assert_eq!(result.upsert_proposition_links, vec![link_id.clone()]);

    let bad_concept_id = r#"
            UPSERT {
                PROPOSITION ?link {
                    (id: "C:1")
                }
            }
            "#;
    let err = nexus
        .execute_kml(parse_kml(bad_concept_id).unwrap(), false)
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::InvalidSyntax));
    assert!(err.message.contains("must be a Proposition ID"));

    let missing_prop_id = r#"
            UPSERT {
                PROPOSITION ?link {
                    (id: "P:18446744073709551615:treats")
                }
            }
            "#;
    let err = nexus
        .execute_kml(parse_kml(missing_prop_id).unwrap(), false)
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::NotFound));

    let variable_predicate = r#"
            UPSERT {
                PROPOSITION ?link {
                    ({type: "Drug", name: "Aspirin"}, ?p, {type: "Symptom", name: "Headache"})
                }
            }
            "#;
    let err = nexus
        .execute_kml(parse_kml(variable_predicate).unwrap(), false)
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::InvalidSyntax));
    assert!(err.message.contains("predicate must be a literal string"));

    let same_target = r#"
            UPSERT {
                PROPOSITION ?link {
                    ({type: "Drug", name: "Aspirin"}, "treats", {type: "Drug", name: "Aspirin"})
                }
            }
            "#;
    let err = nexus
        .execute_kml(parse_kml(same_target).unwrap(), false)
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::InvalidSyntax));
    assert!(
        err.message
            .contains("Subject and object cannot be the same")
    );
}

#[tokio::test]
async fn test_private_entity_id_resolution_error_paths() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();
    let mut cached_pks = FxHashMap::default();

    let missing_concept = EntityPK::Concept(ConceptPK::ID(u64::MAX));
    let err = nexus
        .resolve_entity_id(&missing_concept, &mut cached_pks)
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::NotFound));
    assert!(err.message.contains("Concept"));

    let query = parse_kql(
            r#"
            FIND(?link)
            WHERE {
                ?link ({type: "Drug", name: "Aspirin"}, "treats", {type: "Symptom", name: "Headache"})
            }
            LIMIT 1
            "#,
        )
        .unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    let links: Vec<PropositionLink> = serde_json::from_value(result).unwrap();
    let EntityID::Proposition(prop_id, predicate) = links[0].id.parse().unwrap() else {
        panic!("expected proposition link id");
    };

    let missing_proposition = EntityPK::Proposition(PropositionPK::ID(u64::MAX, predicate.clone()));
    let err = nexus
        .resolve_entity_id(&missing_proposition, &mut cached_pks)
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::NotFound));
    assert!(err.message.contains("Proposition"));

    let wrong_id_predicate =
        EntityPK::Proposition(PropositionPK::ID(prop_id, "wrong_predicate".to_string()));
    let err = nexus
        .resolve_entity_id(&wrong_id_predicate, &mut cached_pks)
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::NotFound));
    assert!(err.message.contains("proposition link not found"));

    let aspirin = EntityPK::Concept(ConceptPK::Object {
        r#type: "Drug".to_string(),
        name: "Aspirin".to_string(),
    });
    let headache = EntityPK::Concept(ConceptPK::Object {
        r#type: "Symptom".to_string(),
        name: "Headache".to_string(),
    });

    let wrong_object_predicate = EntityPK::Proposition(PropositionPK::Object {
        subject: Box::new(aspirin.clone()),
        predicate: "wrong_predicate".to_string(),
        object: Box::new(headache.clone()),
    });
    let err = nexus
        .resolve_entity_id(&wrong_object_predicate, &mut cached_pks)
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::NotFound));
    assert!(err.message.contains("proposition link not found"));

    let missing_link = EntityPK::Proposition(PropositionPK::Object {
        subject: Box::new(aspirin.clone()),
        predicate,
        object: Box::new(aspirin),
    });
    let err = nexus
        .resolve_entity_id(&missing_link, &mut cached_pks)
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::NotFound));
    assert!(err.message.contains("proposition link not found"));
}

#[tokio::test]
async fn test_kql_embedded_endpoint_clauses() {
    // Per the KIP spec, embedded endpoint clauses must be unnamed: a
    // variable is bound in its own clause first, then referenced in the
    // proposition pattern.
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // Named embedded endpoints were removed from the protocol and must
    // be rejected by the parser.
    assert!(
        parse_kql(
            r#"
                FIND(?drug.name, ?symptom.name)
                WHERE {
                    ?drug {type: "Drug"}
                    (?drug, "treats", ?symptom {type: "Symptom", name: "Headache"})
                }
                "#
        )
        .is_err()
    );

    // The equivalent separate-clause form binds AND constrains.
    let kql = r#"
        FIND(?drug.name, ?symptom.name)
        WHERE {
            ?drug {type: "Drug"}
            ?symptom {type: "Symptom", name: "Headache"}
            (?drug, "treats", ?symptom)
        }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!([["Aspirin"], ["Headache"]]));

    // Unnamed embedded endpoint clauses remain valid in both positions.
    let kql = r#"
        FIND(?d.name)
        WHERE {
            ?d {type: "Drug"}
            (?d, "treats", {type: "Symptom", name: "Fever"})
        }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!(["Aspirin"]));

    // An unnamed endpoint referencing a non-existent concept reports
    // KIP_3002 (NotFound).
    let kql = r#"
        FIND(?drug.name)
        WHERE {
            ?drug {type: "Drug"}
            (?drug, "treats", {type: "Symptom", name: "Nonexistent"})
        }
        "#;
    let query = parse_kql(kql).unwrap();
    let err = nexus.execute_kql(query).await.unwrap_err();
    assert!(matches!(err.code, KipErrorCode::NotFound));
}

#[tokio::test]
async fn test_kql_nested_proposition_variable_binding() {
    // A proposition is bound to a variable in its own clause
    // (`?link (?s, "p", ?o)`) and then referenced as an endpoint;
    // naming an embedded endpoint clause inline is no longer legal.
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // Create a higher-order proposition: Alice stated that
    // (Aspirin treats Headache).
    let higher_order_kml = r#"
        UPSERT {
            CONCEPT ?person_type {
                {type: "$ConceptType", name: "Person"}
            }
            CONCEPT ?stated_pred {
                {type: "$PropositionType", name: "stated"}
            }
            CONCEPT ?alice {
                {type: "Person", name: "Alice"}
            }
            PROPOSITION ?fact {
                ({type: "Person", name: "Alice"},
                 "stated",
                 ({type: "Drug", name: "Aspirin"},
                  "treats",
                  {type: "Symptom", name: "Headache"})
                )
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(higher_order_kml).unwrap(), false)
        .await
        .unwrap();

    // The removed inline-binding form must be rejected by the parser.
    assert!(
        parse_kql(
            r#"
                FIND(COUNT(?person))
                WHERE {
                    (?person, "stated", ?inner (?drug, "treats", ?symptom))
                }
                "#
        )
        .is_err()
    );

    // Bind the inner proposition in its own clause, then use it as the
    // object endpoint. We just count to keep the assertion stable
    // regardless of internal IDs.
    let kql = r#"
        FIND(COUNT(?person))
        WHERE {
            ?person {type: "Person", name: "Alice"}
            ?inner (?drug, "treats", ?symptom)
            (?person, "stated", ?inner)
        }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!(1));

    // ?inner remains a normal variable across clauses; the bound link
    // participates exactly once.
    let kql = r#"
        FIND(COUNT(?inner))
        WHERE {
            ?drug {type: "Drug", name: "Aspirin"}
            ?inner (?drug, "treats", ?symptom)
            ({type: "Person", name: "Alice"}, "stated", ?inner)
        }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!(1));
}

#[tokio::test]
async fn test_kql_multi_hop_bidirectional_matching() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // 创建多层级的测试数据用于多跳查询
    let multi_hop_data_kml = r#"
            UPSERT {
                // 创建新的概念类型
                CONCEPT ?category_type {
                    {type: "$ConceptType", name: "Category"}
                }
                CONCEPT ?person_type {
                    {type: "$ConceptType", name: "Person"}
                }

                // 创建新的谓词类型
                CONCEPT ?is_subclass_of_pred {
                    {type: "$PropositionType", name: "is_subclass_of"}
                }
                CONCEPT ?belongs_to_pred {
                    {type: "$PropositionType", name: "belongs_to"}
                }
                CONCEPT ?knows_pred {
                    {type: "$PropositionType", name: "knows"}
                }

                // 创建分类层次结构
                CONCEPT ?medicine {
                    {type: "Category", name: "Medicine"}
                }
                CONCEPT ?pain_reliever {
                    {type: "Category", name: "PainReliever"}
                    SET PROPOSITIONS {
                        ("is_subclass_of", {type: "Category", name: "Medicine"})
                    }
                }
                CONCEPT ?nsaid {
                    {type: "Category", name: "NSAID"}
                    SET PROPOSITIONS {
                        ("is_subclass_of", {type: "Category", name: "PainReliever"})
                    }
                }

                // 让阿司匹林属于NSAID类别
                CONCEPT ?aspirin_category {
                    {type: "Drug", name: "Aspirin"}
                    SET PROPOSITIONS {
                        ("belongs_to", {type: "Category", name: "NSAID"})
                    }
                }

                // 创建人员和关系网络
                CONCEPT ?alice {
                    {type: "Person", name: "Alice"}
                }
                CONCEPT ?bob {
                    {type: "Person", name: "Bob"}
                    SET PROPOSITIONS {
                        ("knows", {type: "Person", name: "Alice"})
                    }
                }
                CONCEPT ?charlie {
                    {type: "Person", name: "Charlie"}
                    SET PROPOSITIONS {
                        ("knows", {type: "Person", name: "Bob"})
                    }
                }
                CONCEPT ?david {
                    {type: "Person", name: "David"}
                    SET PROPOSITIONS {
                        ("knows", {type: "Person", name: "Charlie"})
                    }
                }
            }
        "#;
    nexus
        .execute_kml(parse_kml(multi_hop_data_kml).unwrap(), false)
        .await
        .unwrap();

    // 测试1: 正向多跳查询 - 查找阿司匹林的所有上级分类（1-3跳）
    let kql = r#"
            FIND(?drug.name, ?category.name, ?parent_category.name)
            WHERE {
                ?drug {type: "Drug", name: "Aspirin"}
                (?drug, "belongs_to", ?category)
                (?category, "is_subclass_of"{1,3}, ?parent_category)
            }
            "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(
        result,
        json!([["Aspirin"], ["NSAID"], ["PainReliever", "Medicine"]])
    );

    // 测试2: 反向多跳查询 - 从Medicine分类查找所有下级药物（1-3跳）
    // 反向查询：从Medicine通过is_subclass_of关系找到药物
    let kql = r#"
            FIND(?category.name)
            WHERE {
                (?category, "is_subclass_of"{1,3}, {type: "Category", name: "Medicine"})
            }
            "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!(["PainReliever", "NSAID"]));

    let kql = r#"
            FIND(?category.name, ?drug.name)
            WHERE {
                (?category, "is_subclass_of"{1,3}, {type: "Category", name: "Medicine"})
                (?drug, "belongs_to", ?category)
            }
            "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!([["NSAID"], ["Aspirin"]]));

    // 测试3: 精确跳数查询 - 查找恰好2跳的关系
    let kql = r#"
            FIND(?drug.name, ?parent_category.name)
            WHERE {
                ?drug {type: "Drug", name: "Aspirin"}
                (?drug, "belongs_to", ?category)
                (?category, "is_subclass_of"{2}, ?parent_category)
            }
            "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    // 应该只找到PainReliever（2跳：Aspirin->NSAID, NSAID->PainReliever->Medicine）
    assert_eq!(result, json!([["Aspirin"], ["Medicine"]]));

    // 测试4: 人际关系网络的多跳查询
    let kql = r#"
            FIND(?person1.name, ?person2.name)
            WHERE {
                ?person1 {type: "Person", name: "David"}
                ?person2 {type: "Person", name: "Alice"}
                (?person1, "knows"{1,3}, ?person2)
            }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    // David通过3跳关系认识Alice: David->Charlie->Bob->Alice
    assert_eq!(result, json!([["David"], ["Alice"]]));

    // 测试5: 反向人际关系查询
    let kql = r#"
            FIND(?person1.name, ?person2.name)
            WHERE {
                ?person1 {type: "Person", name: "Alice"}
                ?person2 {type: "Person", name: "David"}
                (?person1, "knows"{1,3}, ?person2)
            }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    // 反向查询应该为空，因为knows关系是单向的
    assert_eq!(result, json!([[], []]));

    // 测试6: 边界条件 - 0跳查询（自身）
    let kql = r#"
            FIND(?drug.name)
            WHERE {
                ?drug {type: "Drug", name: "Aspirin"}
                (?drug, "belongs_to"{0}, ?drug)
            }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    // 0跳应该匹配自身
    assert_eq!(result, json!(["Aspirin"]));

    // 测试7: 超出范围的查询
    let kql = r#"
            FIND(?drug.name, ?category.name)
            WHERE {
                ?drug {type: "Drug", name: "Aspirin"}
                (?drug, "belongs_to", ?category)
                (?category, "is_subclass_of"{1,}, ?o)
            }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!([["Aspirin"], ["NSAID"]]));

    let kql = r#"
            FIND(?drug.name, ?category.name)
            WHERE {
                ?drug {type: "Drug", name: "Aspirin"}
                (?drug, "belongs_to", ?category)
                (?category, "is_subclass_of"{5,10}, ?o)
            }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    // 超出实际路径长度，应该为空
    assert_eq!(result, json!([["Aspirin"], []]));
}

#[tokio::test]
async fn test_multi_hop_error_handling() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // 测试错误情况：主语和宾语都是变量的多跳查询
    let kql = r#"
            FIND(?a.name, ?b.name)
            WHERE {
                (?a, "treats"{1,3}, ?b)
            }
            "#;
    let query = parse_kql(kql).unwrap();
    let result = nexus.execute_kql(query).await;
    // 应该返回错误，因为多跳查询要求主语或宾语至少有一个是具体的ID
    assert!(result.is_err());
    if let Err(err) = result {
        assert!(matches!(err.code, KipErrorCode::InvalidSyntax));
        assert!(
            err.message
                .contains("cannot both be variables in multi-hop matching")
        );
    } else {
        panic!("Expected InvalidSyntax error");
    }
}

#[tokio::test]
async fn test_kql_filter_clause() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // 测试过滤器
    let kql = r#"
        FIND(?drug.name)
        WHERE {
            ?drug {type: "Drug"}
            FILTER(?drug.attributes.risk_level < 3)
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!(["Aspirin"]));

    let kql = r#"
        FIND(?drug.name)
        WHERE {
            ?drug {type: "Drug"}
            FILTER(?drug.attributes.risk_level < 1)
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!([]));
}

#[tokio::test]
async fn test_kql_aggregation() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // 测试聚合函数
    let kql = r#"
        FIND(COUNT(?drug))
        WHERE {
            ?drug {type: "Drug"}
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!(1));

    let kql = r#"
        FIND(COUNT(?drug), COUNT(DISTINCT ?symptom))
        WHERE {
            ?drug {type: "Drug"}
            ?symptom {type: "Symptom"}
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!([1, 2]));

    let kql = r#"
        FIND(
            ?drug.name,
            SUM(?drug.attributes.risk_level),
            AVG(?drug.attributes.risk_level),
            MIN(?drug.attributes.risk_level),
            MAX(?drug.attributes.risk_level)
        )
        WHERE {
            ?drug {type: "Drug"}
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!([["Aspirin"], 2.0, 2.0, 2.0, 2.0]));
}

#[tokio::test]
async fn test_kql_optional_clause() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // 测试可选子句
    let kql = r#"
        FIND(?symptom.name, ?drug.name)
        WHERE {
            ?symptom {type: "Symptom"}
            OPTIONAL {
                (?drug, "treats", ?symptom)
            }
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!([["Headache", "Fever"], ["Aspirin"]]));

    let kql = r#"
        FIND(?symptom.name, ?drug.name)
        WHERE {
            ?symptom {type: "Symptom"}
            OPTIONAL {
                (?drug, "treats1", ?symptom)
            }
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!([["Headache", "Fever"], []]));

    let kql = r#"
        FIND(?symptom.name, ?drug.name)
        WHERE {
            ?symptom {type: "Symptom"}
            (?drug, "treats1", ?symptom)  // when predicate not exists
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!([[], []]));
}

#[tokio::test]
async fn test_kql_not_clause() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // 添加另一个药物用于测试
    let ibuprofen_kml = r#"
        UPSERT {
            CONCEPT ?ibuprofen {
                {type: "Drug", name: "Ibuprofen"}
                SET ATTRIBUTES {
                    "risk_level": 4
                }
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(ibuprofen_kml).unwrap(), false)
        .await
        .unwrap();

    // 测试NOT子句
    let kql = r#"
        FIND(?drug.name)
        WHERE {
            ?drug {type: "Drug"}
            NOT {
                FILTER(?drug.attributes.risk_level > 3)
            }
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!(["Aspirin".to_string()]));

    // 测试NOT子句
    let kql = r#"
        FIND(?drug.name)
        WHERE {
            ?drug {type: "Drug"}
            NOT {
                FILTER(?drug.attributes.risk_level > 4)
            }
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();

    assert_eq!(
        result,
        json!(["Aspirin".to_string(), "Ibuprofen".to_string()])
    );
}

#[tokio::test]
async fn test_kql_not_clause_fast_path_orphan_concepts() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // 设置测试数据：创建一个 Domain 和一些概念，部分概念有 belongs_to_domain 关系
    let setup_kml = r#"
        UPSERT {
            CONCEPT ?domain {
                {type: "Domain", name: "TestDomain"}
                SET ATTRIBUTES {
                    "description": "Test domain for orphan detection"
                }
            }
            CONCEPT ?belongs_to_domain_type {
                {type: "$PropositionType", name: "belongs_to_domain"}
            }

            // Drug 类型中，只有 Aspirin 属于 TestDomain，其他不属于任何 domain
            CONCEPT ?aspirin_with_domain {
                {type: "Drug", name: "Aspirin"}
                SET PROPOSITIONS {
                    ("belongs_to_domain", {type: "Domain", name: "TestDomain"})
                }
            }

            // 创建一个孤儿药物（不属于任何 domain）
            CONCEPT ?orphan_drug {
                {type: "Drug", name: "OrphanDrug"}
                SET ATTRIBUTES {
                    "description": "A drug without domain"
                }
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(setup_kml).unwrap(), false)
        .await
        .unwrap();

    // 测试：查找没有 belongs_to_domain 关系的 Drug 概念（孤儿概念）
    // 这个查询应该使用快速路径优化
    let kql = r#"
        FIND(?n.name)
        WHERE {
            ?n {type: "Drug"}
            NOT {
                (?n, "belongs_to_domain", ?d)
            }
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();

    // OrphanDrug 没有 belongs_to_domain 关系，应该被返回
    // Aspirin 有 belongs_to_domain 关系，不应该被返回
    assert_eq!(result, json!(["OrphanDrug".to_string()]));

    // 测试：查找没有 treats 关系的 Drug 概念
    let kql = r#"
        FIND(?n.name)
        WHERE {
            ?n {type: "Drug"}
            NOT {
                (?n, "treats", ?s)
            }
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();

    // OrphanDrug 没有 treats 关系，应该被返回
    // Aspirin 有 treats 关系（treats Headache 和 Fever），不应该被返回
    assert_eq!(result, json!(["OrphanDrug".to_string()]));

    // 测试：查找没有任何关系的 Symptom 概念
    // Headache 和 Fever 都被 Aspirin treats，所以不会被返回
    let kql = r#"
        FIND(?n.name)
        WHERE {
            ?n {type: "Symptom"}
            NOT {
                (?d, "treats", ?n)
            }
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();

    // 所有 Symptom 都被 treats，应该返回空
    assert_eq!(result, json!([]));
}

#[tokio::test]
async fn test_kql_union_clause() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // 测试UNION子句
    let kql = r#"
        FIND(?concept.name)
        WHERE {
            ?concept {type: "Drug"}
            ?concept {type: "Symptom"} // filter by multiple types, should return empty
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert!(result.as_array().unwrap().is_empty());

    // 测试UNION子句
    let kql = r#"
        FIND(?concept.name)
        WHERE {
            ?concept {type: "Drug"}
            UNION {
                ?concept {type: "Symptom"}
            }
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(
        result,
        json!([
            "Aspirin".to_string(),
            "Headache".to_string(),
            "Fever".to_string(),
        ])
    );

    let kql = r#"
        FIND(?link)
        WHERE {
            ?link ({type: "Drug", name: "Aspirin"}, "treats", {type: "Symptom", name: "Headache"})
            UNION {
                ?link ({type: "Drug", name: "Aspirin"}, "treats", {type: "Symptom", name: "Fever"})
            }
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    let links: Vec<PropositionLink> = serde_json::from_value(result).unwrap();
    assert_eq!(links.len(), 2);
}

#[tokio::test]
async fn test_kql_order_by_and_limit() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // 添加更多药物用于测试排序
    let drugs_kml = r#"
        UPSERT {
            CONCEPT ?drug1 {
                {type: "Drug", name: "Ibuprofen"}
                SET ATTRIBUTES {
                    "risk_level": 3
                }
            }
            CONCEPT ?drug2 {
                {type: "Drug", name: "Acetaminophen"}
                SET ATTRIBUTES {
                    "risk_level": 1
                }
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(drugs_kml).unwrap(), false)
        .await
        .unwrap();

    // 测试排序和限制
    let kql = r#"
        FIND(?drug.name, ?drug.attributes.risk_level)
        WHERE {
            ?drug {type: "Drug"}
        }
        ORDER BY ?drug.attributes.risk_level ASC
        LIMIT 2
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, cursor) = nexus.execute_kql(query).await.unwrap();
    assert!(cursor.is_some());
    assert_eq!(
        result,
        json!([["Acetaminophen".to_string(), 1], ["Aspirin".to_string(), 2]])
    );

    let kql = r#"
        FIND(?drug.name, ?drug.attributes.risk_level)
        WHERE {
            ?drug {type: "Drug"}
        }
        ORDER BY ?drug.attributes.risk_level ASC
        LIMIT 2 CURSOR "$cursor"
        "#;

    let query = parse_kql(&kql.replace("$cursor", cursor.unwrap().as_str())).unwrap();
    let (result, cursor) = nexus.execute_kql(query).await.unwrap();
    assert!(cursor.is_none());
    assert_eq!(result, json!([["Ibuprofen".to_string(), 3]]));

    let kql = r#"
        FIND(?drug.name, ?drug.attributes.risk_level)
        WHERE {
            ?drug {type: "Drug"}
        }
        ORDER BY ?drug.attributes.risk_level DESC
        LIMIT 2
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, cursor) = nexus.execute_kql(query).await.unwrap();
    assert!(cursor.is_some());
    assert_eq!(
        result,
        json!([["Ibuprofen".to_string(), 3], ["Aspirin".to_string(), 2]])
    );

    let kql = r#"
        FIND(?drug.name, ?drug.attributes.risk_level)
        WHERE {
            ?drug {type: "Drug"}
        }
        ORDER BY ?drug.attributes.risk_level DESC
        LIMIT 2
        CURSOR "$cursor"
        "#;

    let query = parse_kql(&kql.replace("$cursor", cursor.unwrap().as_str())).unwrap();
    let (result, cursor) = nexus.execute_kql(query).await.unwrap();
    assert!(cursor.is_none());
    assert_eq!(result, json!([["Acetaminophen".to_string(), 1]]));
}

#[tokio::test]
async fn test_kml_upsert_proposition() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let kql = r#"
        FIND(?link, ?drug.name, ?symptom.name)
        WHERE {
            ?link (?drug, "treats", ?symptom)
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    let result = result.as_array().unwrap();
    assert_eq!(
        json!(result[1..]),
        json!([
            ["Aspirin".to_string()],
            ["Headache".to_string(), "Fever".to_string()]
        ])
    );
    let mut props: Vec<PropositionLink> = serde_json::from_value(result[0].clone()).unwrap();
    // println!("{:#?}", props);
    assert_eq!(props.len(), 2);
    assert!(props[0].attributes.is_empty());
    assert!(props[1].attributes.is_empty());
    for prop in props.iter_mut() {
        // Engine-maintained bookkeeping (KIP §2.11.1) accompanies the
        // author metadata on every link element.
        assert_eq!(prop.metadata.remove("_version"), Some(json!(1)));
        assert!(prop.metadata.remove("_updated_at").is_some());
        assert_eq!(
            json!(prop.metadata),
            json!({
                "source": "test_data",
                "confidence": 0.95
            })
        );
    }

    // 测试独立命题创建
    let prop_kml = r#"
        UPSERT {
            PROPOSITION ?treatment {
                ({type: "Drug", name: "Aspirin"}, "treats", {type: "Symptom", name: "Headache"})
                SET ATTRIBUTES {
                    "effectiveness": 0.85,
                    "onset_time": "30 minutes"
                }
            }
            WITH METADATA {
                "source": "clinical_trial",
                "study_id": "CT-2024-001"
            }
        }
        "#;

    let result = nexus
        .execute_kml(parse_kml(prop_kml).unwrap(), false)
        .await
        .unwrap();
    let result: UpsertResult = serde_json::from_value(result).unwrap();
    assert_eq!(result.blocks, 1);
    assert!(result.upsert_concept_nodes.is_empty());
    assert_eq!(result.upsert_proposition_links.len(), 1);

    let kql = r#"
        FIND(?link)
        WHERE {
            ?link (?drug, "treats", ?symptom)
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    let mut props: Vec<PropositionLink> = serde_json::from_value(result).unwrap();
    // println!("{:#?}", props);
    assert_eq!(props.len(), 2);
    assert_eq!(
        json!(props[0].attributes),
        json!({
            "effectiveness": 0.85,
            "onset_time": "30 minutes"
        })
    );
    // The second UPSERT mutated the existing link, so its engine-tracked
    // `_version` advanced to 2.
    assert_eq!(props[0].metadata.remove("_version"), Some(json!(2)));
    assert!(props[0].metadata.remove("_updated_at").is_some());
    assert_eq!(
        json!(props[0].metadata),
        json!({
            "source": "clinical_trial",
            "confidence": 0.95,
            "study_id": "CT-2024-001"
        })
    );
}

#[tokio::test]
async fn test_kml_dry_run() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let test_kml = r#"
        UPSERT {
            CONCEPT ?test_drug {
                {type: "Drug", name: "TestDrug"}
                SET ATTRIBUTES {
                    "test": true
                }
            }
        }
        "#;

    // 干运行不应该实际创建概念
    let result = nexus
        .execute_kml(parse_kml(test_kml).unwrap(), true)
        .await
        .unwrap();
    let result: UpsertResult = serde_json::from_value(result).unwrap();
    assert_eq!(result.blocks, 1);
    assert!(result.upsert_concept_nodes.is_empty());
    assert_eq!(result.upsert_proposition_links.len(), 0);

    // 验证概念没有被创建
    assert!(
        !nexus
            .has_concept(&ConceptPK::Object {
                r#type: "Drug".to_string(),
                name: "TestDrug".to_string(),
            })
            .await
    );

    let valid_with_handles = r#"
            UPSERT {
                CONCEPT ?dry_drug {
                    {type: "Drug", name: "DryDrug"}
                }
                CONCEPT ?dry_symptom {
                    {type: "Symptom", name: "DrySymptom"}
                }
                PROPOSITION ?dry_fact {
                    (?dry_drug, "treats", ?dry_symptom)
                }
            }
            "#;

    nexus
        .execute_kml(parse_kml(valid_with_handles).unwrap(), true)
        .await
        .unwrap();
    assert!(
        !nexus
            .has_concept(&ConceptPK::Object {
                r#type: "Drug".to_string(),
                name: "DryDrug".to_string(),
            })
            .await
    );

    let unknown_predicate = r#"
            UPSERT {
                CONCEPT ?bad_drug {
                    {type: "Drug", name: "BadDrug"}
                    SET PROPOSITIONS {
                        ("not_registered", {type: "Symptom", name: "Headache"})
                    }
                }
            }
            "#;
    let err = nexus
        .execute_kml(parse_kml(unknown_predicate).unwrap(), true)
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::TypeMismatch));

    let err = nexus
        .execute_kml(parse_kml(unknown_predicate).unwrap(), false)
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::TypeMismatch));
    assert!(
        !nexus
            .has_concept(&ConceptPK::Object {
                r#type: "Drug".to_string(),
                name: "BadDrug".to_string(),
            })
            .await
    );
}

#[tokio::test]
async fn test_kml_delete_attributes_and_metadata_for_concepts_and_propositions() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();

    let setup = r#"
        UPSERT {
            CONCEPT ?person_type {
                {type: "$ConceptType", name: "DeletePerson"}
            }
            CONCEPT ?knows_type {
                {type: "$PropositionType", name: "delete_knows"}
            }
            CONCEPT ?alice {
                {type: "DeletePerson", name: "Alice"}
                SET ATTRIBUTES {
                    "role": "researcher",
                    "drop_attr": true
                }
            } WITH METADATA {
                "source": "unit",
                "drop_meta": true
            }
            CONCEPT ?bob {
                {type: "DeletePerson", name: "Bob"}
            }
            PROPOSITION ?link {
                (?alice, "delete_knows", ?bob)
                SET ATTRIBUTES {
                    "since": 2024,
                    "drop_attr": true
                }
            } WITH METADATA {
                "source": "unit",
                "drop_meta": true
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(setup).unwrap(), false)
        .await
        .unwrap();

    let dry_run_metadata = r#"
        DELETE METADATA {"drop_meta"} FROM ?missing
        WHERE { ?person {type: "DeletePerson", name: "Alice"} }
        "#;
    assert_eq!(
        nexus
            .execute_kml(parse_kml(dry_run_metadata).unwrap(), true)
            .await
            .unwrap(),
        json!({"updated_concepts": 0, "updated_propositions": 0})
    );

    let missing_target = r#"
        DELETE ATTRIBUTES {"drop_attr"} FROM ?missing
        WHERE { ?person {type: "DeletePerson", name: "Alice"} }
        "#;
    let err = nexus
        .execute_kml(parse_kml(missing_target).unwrap(), false)
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::ReferenceError));

    let delete_concept_attr = r#"
        DELETE ATTRIBUTES {"drop_attr", "missing"} FROM ?person
        WHERE { ?person {type: "DeletePerson", name: "Alice"} }
        "#;
    assert_eq!(
        nexus
            .execute_kml(parse_kml(delete_concept_attr).unwrap(), false)
            .await
            .unwrap(),
        json!({"updated_concepts": 1, "updated_propositions": 0})
    );
    let alice = nexus
        .get_concept(&ConceptPK::Object {
            r#type: "DeletePerson".to_string(),
            name: "Alice".to_string(),
        })
        .await
        .unwrap();
    assert_eq!(alice.attributes["role"], json!("researcher"));
    assert!(!alice.attributes.contains_key("drop_attr"));

    let delete_concept_metadata = r#"
        DELETE METADATA {"drop_meta"} FROM ?person
        WHERE { ?person {type: "DeletePerson", name: "Alice"} }
        "#;
    assert_eq!(
        nexus
            .execute_kml(parse_kml(delete_concept_metadata).unwrap(), false)
            .await
            .unwrap(),
        json!({"updated_concepts": 1, "updated_propositions": 0})
    );
    let alice = nexus
        .get_concept(&ConceptPK::Object {
            r#type: "DeletePerson".to_string(),
            name: "Alice".to_string(),
        })
        .await
        .unwrap();
    assert_eq!(alice.metadata["source"], json!("unit"));
    assert!(!alice.metadata.contains_key("drop_meta"));

    let delete_link_attr = r#"
        DELETE ATTRIBUTES {"drop_attr"} FROM ?link
        WHERE {
            ?link ({type: "DeletePerson", name: "Alice"}, "delete_knows", {type: "DeletePerson", name: "Bob"})
        }
        "#;
    assert_eq!(
        nexus
            .execute_kml(parse_kml(delete_link_attr).unwrap(), false)
            .await
            .unwrap(),
        json!({"updated_concepts": 0, "updated_propositions": 1})
    );

    let delete_link_metadata = r#"
        DELETE METADATA {"drop_meta"} FROM ?link
        WHERE {
            ?link ({type: "DeletePerson", name: "Alice"}, "delete_knows", {type: "DeletePerson", name: "Bob"})
        }
        "#;
    assert_eq!(
        nexus
            .execute_kml(parse_kml(delete_link_metadata).unwrap(), false)
            .await
            .unwrap(),
        json!({"updated_concepts": 0, "updated_propositions": 1})
    );

    let (result, _) = nexus
            .execute_kql(
                parse_kql(
                    r#"
                FIND(?link)
                WHERE {
                    ?link ({type: "DeletePerson", name: "Alice"}, "delete_knows", {type: "DeletePerson", name: "Bob"})
                }
                "#,
                )
                .unwrap(),
            )
            .await
            .unwrap();
    let links: Vec<PropositionLink> = serde_json::from_value(result).unwrap();
    assert_eq!(links.len(), 1);
    assert_eq!(links[0].attributes["since"], json!(2024));
    assert!(!links[0].attributes.contains_key("drop_attr"));
    assert_eq!(links[0].metadata["source"], json!("unit"));
    assert!(!links[0].metadata.contains_key("drop_meta"));
}

#[tokio::test]
async fn test_kml_upsert_preflight_prevents_partial_writes() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let partial_write_kml = r#"
        UPSERT {
            CONCEPT ?partial {
                {type: "Drug", name: "PartialDrug"}
            }
            PROPOSITION ?bad_fact {
                (?partial, "not_registered", {type: "Symptom", name: "Headache"})
            }
        }
        "#;

    let err = nexus
        .execute_kml(parse_kml(partial_write_kml).unwrap(), false)
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::TypeMismatch));
    assert!(
        !nexus
            .has_concept(&ConceptPK::Object {
                r#type: "Drug".to_string(),
                name: "PartialDrug".to_string(),
            })
            .await
    );
}

#[tokio::test]
async fn test_kml_upsert_preflight_accepts_schema_defined_earlier() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();

    let schema_and_data_kml = r#"
        UPSERT {
            CONCEPT ?source_type {
                {type: "$ConceptType", name: "PreflightSource"}
            }
            CONCEPT ?target_type {
                {type: "$ConceptType", name: "PreflightTarget"}
            }
            CONCEPT ?relation_type {
                {type: "$PropositionType", name: "preflight_link"}
            }
            CONCEPT ?target {
                {type: "PreflightTarget", name: "Target"}
            }
            CONCEPT ?source {
                {type: "PreflightSource", name: "Source"}
                SET PROPOSITIONS {
                    ("preflight_link", ?target)
                }
            }
        }
        "#;

    nexus
        .execute_kml(parse_kml(schema_and_data_kml).unwrap(), false)
        .await
        .unwrap();

    assert!(
        nexus
            .has_concept(&ConceptPK::Object {
                r#type: "PreflightSource".to_string(),
                name: "Source".to_string(),
            })
            .await
    );

    let (result, _) = nexus
        .execute_kql(
            parse_kql(
                r#"
        FIND(?target.name)
        WHERE {
            ?source {type: "PreflightSource", name: "Source"}
            (?source, "preflight_link", ?target)
        }
        "#,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(result, json!(["Target"]));
}

#[tokio::test]
async fn test_kml_core_directives_are_immutable() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    nexus
        .execute_kml(parse_kml(PERSON_SELF_KIP).unwrap(), false)
        .await
        .unwrap();

    let update_core = r#"
        UPSERT {
            CONCEPT ?self_actor {
                {type: "Person", name: "$self"}
                SET ATTRIBUTES {
                    core_directives: []
                }
            }
        }
        "#;
    let err = nexus
        .execute_kml(parse_kml(update_core).unwrap(), false)
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::ImmutableTarget));

    let delete_core = r#"
        DELETE ATTRIBUTES {"core_directives"} FROM ?self_actor
        WHERE { ?self_actor {type: "Person", name: "$self"} }
        "#;
    let err = nexus
        .execute_kml(parse_kml(delete_core).unwrap(), false)
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::ImmutableTarget));
    let err = nexus
        .execute_kml(parse_kml(delete_core).unwrap(), true)
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::ImmutableTarget));

    let update_persona = r#"
        UPSERT {
            CONCEPT ?self_actor {
                {type: "Person", name: "$self"}
                SET ATTRIBUTES {
                    persona: "updated persona"
                }
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(update_persona).unwrap(), false)
        .await
        .unwrap();

    let self_concept = nexus
        .get_concept(&ConceptPK::Object {
            r#type: PERSON_TYPE.to_string(),
            name: META_SELF_NAME.to_string(),
        })
        .await
        .unwrap();
    assert_eq!(self_concept.attributes["persona"], json!("updated persona"));
    assert!(
        self_concept
            .attributes
            .get("core_directives")
            .and_then(Json::as_array)
            .is_some_and(|items| !items.is_empty())
    );
}

#[tokio::test]
async fn test_meta_describe_primer() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let meta_cmd = MetaCommand::Describe(DescribeTarget::Primer);
    let result = nexus.execute_meta(meta_cmd).await;
    assert!(result.is_err());
    assert!(matches!(
        result.as_ref().unwrap_err().code,
        KipErrorCode::NotFound
    ));
    assert!(
        result
            .err()
            .unwrap()
            .to_string()
            .contains(r#"{type: "Person", name: "$self"}"#)
    );

    let kml = PERSON_SELF_KIP.replace(
        "$self_reserved_principal_id",
        "gcxml-rtxjo-ib7ov-5si5r-5jluv-zek7y-hvody-nneuz-hcg5i-6notx-aae",
    );

    let result = nexus
        .execute_kml(parse_kml(&kml).unwrap(), false)
        .await
        .unwrap();
    assert!(result.is_object());

    let (result, _) = nexus
        .execute_meta(parse_meta("DESCRIBE PRIMER").unwrap())
        .await
        .unwrap();
    assert!(result.is_object());

    let primer = result.as_object().unwrap();
    assert!(primer.contains_key("identity"));
    assert!(primer.contains_key("domain_map"));
}

#[tokio::test]
async fn test_meta_describe_domains() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let (result, _) = nexus
        .execute_meta(parse_meta("DESCRIBE DOMAINS").unwrap())
        .await
        .unwrap();
    let domains = result.as_array().unwrap();
    // println!("{:#?}", domains);
    assert_eq!(domains.len(), 3);
    assert_eq!(domains[0]["type"], "Domain");
    assert_eq!(domains[0]["name"], "CoreSchema");
}

#[tokio::test]
async fn test_meta_describe_concept_types() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    nexus
        .execute_kml(
            parse_kml(
                r#"
        UPSERT {
            CONCEPT ?unused_type {
                {type: "$ConceptType", name: "UnusedType"}
            }
        }
        "#,
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap();

    let (result, _) = nexus
        .execute_meta(parse_meta("DESCRIBE CONCEPT TYPES").unwrap())
        .await
        .unwrap();

    let types = result.as_array().unwrap();
    let names: Vec<&str> = types.iter().filter_map(Json::as_str).collect();
    for expected in [
        "$ConceptType",
        "$PropositionType",
        "Domain",
        "Drug",
        "Event",
        "Person",
        "Symptom",
        "UnusedType",
    ] {
        assert!(names.contains(&expected));
    }
    assert!(names.windows(2).all(|pair| pair[0] <= pair[1]));

    let (page1, cursor) = nexus
        .execute_meta(MetaCommand::Describe(DescribeTarget::ConceptTypes {
            limit: Some(3),
            cursor: None,
        }))
        .await
        .unwrap();
    assert!(cursor.is_some());
    let (page2, _) = nexus
        .execute_meta(MetaCommand::Describe(DescribeTarget::ConceptTypes {
            limit: Some(3),
            cursor,
        }))
        .await
        .unwrap();
    for item in page1.as_array().unwrap() {
        assert!(!page2.as_array().unwrap().contains(item));
    }

    let (result, _) = nexus
        .execute_meta(parse_meta("DESCRIBE CONCEPT TYPE \"Drug\"").unwrap())
        .await
        .unwrap();
    assert_eq!(result["type"], "$ConceptType");
    assert_eq!(result["name"], "Drug");

    let res = nexus
        .execute_meta(parse_meta("DESCRIBE CONCEPT TYPE \"drug\"").unwrap())
        .await;
    assert!(res.is_err());
    assert!(matches!(res.unwrap_err().code, KipErrorCode::NotFound));
}

#[tokio::test]
async fn test_meta_describe_proposition_types() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    nexus
        .execute_kml(
            parse_kml(
                r#"
        UPSERT {
            CONCEPT ?unused_predicate {
                {type: "$PropositionType", name: "unused_relation"}
            }
        }
        "#,
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap();

    let (result, _) = nexus
        .execute_meta(parse_meta("DESCRIBE PROPOSITION TYPES").unwrap())
        .await
        .unwrap();

    let types = result.as_array().unwrap();
    let names: Vec<&str> = types.iter().filter_map(Json::as_str).collect();
    for expected in ["belongs_to_domain", "learned", "treats", "unused_relation"] {
        assert!(names.contains(&expected));
    }
    assert!(names.windows(2).all(|pair| pair[0] <= pair[1]));

    let (result, _) = nexus
        .execute_meta(parse_meta("DESCRIBE PROPOSITION TYPE \"belongs_to_domain\"").unwrap())
        .await
        .unwrap();
    assert_eq!(result["type"], "$PropositionType");
    assert_eq!(result["name"], "belongs_to_domain");

    let res = nexus
        .execute_meta(parse_meta("DESCRIBE PROPOSITION TYPE \"treats1\"").unwrap())
        .await;
    assert!(res.is_err());
    assert!(matches!(res.unwrap_err().code, KipErrorCode::NotFound));
}

#[tokio::test]
async fn test_meta_search() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let (result, _) = nexus
        .execute_meta(parse_meta(r#"SEARCH CONCEPT "aspirin""#).unwrap())
        .await
        .unwrap();
    let result = result.as_array().unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0]["name"], "Aspirin");

    let (result, _) = nexus
        .execute_meta(parse_meta(r#"SEARCH CONCEPT "C9H8O4""#).unwrap())
        .await
        .unwrap();
    let result = result.as_array().unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0]["name"], "Aspirin");

    let (result, _) = nexus
        .execute_meta(parse_meta(r#"SEARCH CONCEPT "test_data""#).unwrap())
        .await
        .unwrap();
    let result = result.as_array().unwrap();
    // println!("{:#?}", result);
    assert_eq!(result.len(), 6);

    let (result, _) = nexus
        .execute_meta(parse_meta(r#"SEARCH CONCEPT "test_data" LIMIT 5"#).unwrap())
        .await
        .unwrap();
    let result = result.as_array().unwrap();
    assert_eq!(result.len(), 5);

    let (result, _) = nexus
        .execute_meta(
            parse_meta(r#"SEARCH CONCEPT "test_data" WITH TYPE "$PropositionType""#).unwrap(),
        )
        .await
        .unwrap();
    let result = result.as_array().unwrap();
    assert_eq!(result.len(), 1);

    let (result, _) = nexus
        .execute_meta(parse_meta(r#"SEARCH PROPOSITION "test_data""#).unwrap())
        .await
        .unwrap();
    let result = result.as_array().unwrap();
    assert_eq!(result.len(), 2);

    let (result, _) = nexus
        .execute_meta(parse_meta(r#"SEARCH PROPOSITION "test_data" LIMIT 5"#).unwrap())
        .await
        .unwrap();
    let result = result.as_array().unwrap();
    assert_eq!(result.len(), 2);

    let (result, _) = nexus
        .execute_meta(parse_meta(r#"SEARCH PROPOSITION "test_data" WITH TYPE "treats""#).unwrap())
        .await
        .unwrap();
    let result = result.as_array().unwrap();
    assert_eq!(result.len(), 2);
}

#[tokio::test]
async fn test_error_handling() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();

    // 测试查询不存在的概念
    let result = nexus
        .get_concept(&ConceptPK::Object {
            r#type: "NonExistent".to_string(),
            name: "Test".to_string(),
        })
        .await;
    assert!(result.is_err());

    // 测试无效的KQL
    let invalid_kql = r#"
        FIND(?invalid)
        WHERE {
            ?invalid {invalid_field: "test"}
        }
        "#;

    let parse_result = parse_kql(invalid_kql);
    assert!(parse_result.is_err());
}

#[tokio::test]
async fn test_complex_query_scenario() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // 创建更复杂的测试数据
    let complex_data_kml = r#"
        UPSERT {
            CONCEPT ?drug_class_type {
                {type: "$ConceptType", name: "DrugClass"}
            }
            CONCEPT ?belongs_to_pred {
                {type: "$PropositionType", name: "belongs_to_class"}
            }
            CONCEPT ?nsaid_class {
                {type: "DrugClass", name: "NSAID"}
                SET ATTRIBUTES {
                    "description": "Non-steroidal anti-inflammatory drugs"
                }
            }
            PROPOSITION ?aspirin_nsaid {
                ({type: "Drug", name: "Aspirin"}, "belongs_to_class", {type: "DrugClass", name: "NSAID"})
                SET ATTRIBUTES {
                    "classification_confidence": 0.99
                }
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(complex_data_kml).unwrap(), false)
        .await
        .unwrap();

    // 复杂查询：找到所有NSAID类药物及其治疗的症状
    let complex_kql = r#"
        FIND(?drug.name, ?symptom.name, ?treatment.metadata)
        WHERE {
            ?drug {type: "Drug"}
            ?nsaid_class {type: "DrugClass", name: "NSAID"}
            ?symptom {type: "Symptom"}

            (?drug, "belongs_to_class", ?nsaid_class)
            ?treatment (?drug, "treats", ?symptom)

            FILTER(?drug.attributes.risk_level <= 3)
        }
        ORDER BY ?drug.name ASC
        "#;

    let query = parse_kql(complex_kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    // println!("{:#?}", result);
    let result = result.as_array().unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(result[0], json!(["Aspirin".to_string()]));
    assert_eq!(
        result[1],
        json!(["Headache".to_string(), "Fever".to_string()])
    );
}

#[tokio::test]
async fn test_concurrent_operations() {
    let nexus = Arc::new(setup_test_db(async |_| Ok(())).await.unwrap());
    setup_test_data(&nexus).await.unwrap();

    // 测试并发查询
    let nexus1 = nexus.clone();
    let nexus2 = nexus.clone();

    let task1 = tokio::spawn(async move {
        let kql = r#"
            FIND(?drug.name)
            WHERE {
                ?drug {type: "Drug"}
            }
            "#;
        nexus1.execute_kql(parse_kql(kql).unwrap()).await
    });

    let task2 = tokio::spawn(async move {
        let kql = r#"
            FIND(?symptom.name)
            WHERE {
                ?symptom {type: "Symptom"}
            }
            "#;
        nexus2.execute_kql(parse_kql(kql).unwrap()).await
    });

    let (result1, result2) = tokio::try_join!(task1, task2).unwrap();
    assert!(result1.is_ok());
    assert!(result2.is_ok());
}

#[tokio::test]
async fn test_kql_filter_in() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // IN 匹配 - 名称在列表中
    let kql = r#"
        FIND(?symptom.name)
        WHERE {
            ?symptom {type: "Symptom"}
            FILTER(IN(?symptom.name, ["Headache", "Migraine"]))
        }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!(["Headache"]));

    // IN 匹配 - 数值在列表中
    let kql = r#"
        FIND(?drug.name)
        WHERE {
            ?drug {type: "Drug"}
            FILTER(IN(?drug.attributes.risk_level, [1, 2, 3]))
        }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!(["Aspirin"]));

    // IN 不匹配 - 值不在列表中
    let kql = r#"
        FIND(?drug.name)
        WHERE {
            ?drug {type: "Drug"}
            FILTER(IN(?drug.attributes.risk_level, [5, 6, 7]))
        }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!([]));
}

#[tokio::test]
async fn test_kql_filter_is_null() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // IS_NULL - 字段不存在（视为 null）
    let kql = r#"
        FIND(?drug.name)
        WHERE {
            ?drug {type: "Drug"}
            FILTER(IS_NULL(?drug.attributes.nonexistent_field))
        }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!(["Aspirin"]));

    // IS_NULL - 字段存在（不为 null）
    let kql = r#"
        FIND(?drug.name)
        WHERE {
            ?drug {type: "Drug"}
            FILTER(IS_NULL(?drug.attributes.risk_level))
        }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!([]));
}

#[tokio::test]
async fn test_kql_filter_is_not_null() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // IS_NOT_NULL - 字段存在
    let kql = r#"
        FIND(?drug.name)
        WHERE {
            ?drug {type: "Drug"}
            FILTER(IS_NOT_NULL(?drug.attributes.risk_level))
        }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!(["Aspirin"]));

    // IS_NOT_NULL - 字段不存在
    let kql = r#"
        FIND(?drug.name)
        WHERE {
            ?drug {type: "Drug"}
            FILTER(IS_NOT_NULL(?drug.attributes.nonexistent_field))
        }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!([]));
}

#[tokio::test]
async fn test_kql_filter_new_functions_combined() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // 组合使用: IS_NOT_NULL && IN
    let kql = r#"
        FIND(?symptom.name)
        WHERE {
            ?symptom {type: "Symptom"}
            FILTER(IS_NOT_NULL(?symptom.attributes.severity) && IN(?symptom.name, ["Headache", "Fever"]))
        }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    // Headache has severity, Fever does not
    assert_eq!(result, json!(["Headache"]));

    // 组合使用: IS_NULL || IN
    let kql = r#"
        FIND(?symptom.name)
        WHERE {
            ?symptom {type: "Symptom"}
            FILTER(IS_NULL(?symptom.attributes.severity) || IN(?symptom.name, ["Headache"]))
        }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    // Fever has no severity (IS_NULL true), Headache matches IN
    assert_eq!(result, json!(["Headache", "Fever"]));
}

#[tokio::test]
async fn test_kql_filter_not_and_invalid_function_arguments() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let query = parse_kql(
        r#"
            FIND(?drug.name)
            WHERE {
                ?drug {type: "Drug"}
                FILTER(!(?drug.attributes.risk_level > 2))
            }
            "#,
    )
    .unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!(["Aspirin"]));

    let mut ctx = QueryContext::default();
    let mut bindings_snapshot = FxHashMap::default();
    let mut bindings_cursor = FxHashMap::default();
    let err = nexus
        .evaluate_filter_expression(
            &mut ctx,
            FilterExpression::Function {
                func: FilterFunction::IsNull,
                args: vec![
                    FilterOperand::Literal("a".into()),
                    FilterOperand::Literal("b".into()),
                ],
            },
            &mut bindings_snapshot,
            &mut bindings_cursor,
        )
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::InvalidSyntax));
    assert!(err.message.contains("requires exactly 1 argument"));

    let err = nexus
        .evaluate_filter_expression(
            &mut ctx,
            FilterExpression::Function {
                func: FilterFunction::In,
                args: vec![
                    FilterOperand::Literal("Aspirin".into()),
                    FilterOperand::Literal("Aspirin".into()),
                ],
            },
            &mut bindings_snapshot,
            &mut bindings_cursor,
        )
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::InvalidSyntax));
    assert!(err.message.contains("IN second argument"));

    let err = nexus
        .evaluate_filter_expression(
            &mut ctx,
            FilterExpression::Function {
                func: FilterFunction::Contains,
                args: vec![FilterOperand::Literal("Aspirin".into())],
            },
            &mut bindings_snapshot,
            &mut bindings_cursor,
        )
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::InvalidSyntax));
    assert!(err.message.contains("Filter functions"));
}

#[tokio::test]
async fn test_private_relation_row_helpers_and_predicate_value_loading() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    let relation = QueryRelationBinding {
        proposition_var: Some("link".to_string()),
        subject_var: Some("subject".to_string()),
        predicate_var: Some("pred".to_string()),
        object_var: Some("object".to_string()),
        rows: vec![],
    };
    let row = QueryRelationRow {
        proposition: EntityID::Proposition(7, "knows".to_string()),
        subject: EntityID::Concept(1),
        predicate: "knows".to_string(),
        object: EntityID::Concept(2),
    };

    assert!(CognitiveNexus::relation_covers_var(&relation, "link"));
    assert!(CognitiveNexus::relation_covers_var(&relation, "subject"));
    assert!(CognitiveNexus::relation_covers_var(&relation, "pred"));
    assert!(CognitiveNexus::relation_covers_var(&relation, "object"));
    assert!(!CognitiveNexus::relation_covers_var(&relation, "missing"));
    assert_eq!(
        CognitiveNexus::relation_row_entity(&relation, &row, "link"),
        Some(&row.proposition)
    );
    assert_eq!(
        CognitiveNexus::relation_row_entity(&relation, &row, "subject"),
        Some(&row.subject)
    );
    assert_eq!(
        CognitiveNexus::relation_row_entity(&relation, &row, "object"),
        Some(&row.object)
    );
    assert_eq!(
        CognitiveNexus::relation_row_entity(&relation, &row, "pred"),
        None
    );
    assert_eq!(
        CognitiveNexus::relation_row_predicate(&relation, &row, "pred"),
        Some("knows")
    );
    assert_eq!(
        CognitiveNexus::relation_row_predicate(&relation, &row, "subject"),
        None
    );

    let mut ctx = QueryContext::default();
    ctx.entities
        .insert("subject".to_string(), vec![EntityID::Concept(1)].into());
    ctx.entities
        .insert("object".to_string(), vec![EntityID::Concept(2)].into());
    ctx.predicates
        .insert("pred".to_string(), vec!["knows".to_string()].into());
    assert!(CognitiveNexus::relation_row_matches_context(
        &ctx, &relation, &row
    ));

    ctx.predicates
        .insert("pred".to_string(), vec!["likes".to_string()].into());
    assert!(!CognitiveNexus::relation_row_matches_context(
        &ctx, &relation, &row
    ));
    ctx.predicates
        .insert("pred".to_string(), vec!["knows".to_string()].into());
    ctx.entities
        .insert("object".to_string(), vec![EntityID::Concept(3)].into());
    assert!(!CognitiveNexus::relation_row_matches_context(
        &ctx, &relation, &row
    ));

    let value = nexus
        .load_relation_row_value(
            &ctx.cache,
            &relation,
            &row,
            &DotPathVar {
                var: "pred".to_string(),
                path: vec![],
            },
        )
        .await
        .unwrap();
    assert_eq!(value, json!("knows"));

    let value = nexus
        .load_relation_row_value(
            &ctx.cache,
            &relation,
            &row,
            &DotPathVar {
                var: "pred".to_string(),
                path: vec!["metadata".to_string()],
            },
        )
        .await
        .unwrap();
    assert_eq!(value, Json::Null);

    let err = nexus
        .load_relation_row_value(
            &ctx.cache,
            &relation,
            &row,
            &DotPathVar {
                var: "missing".to_string(),
                path: vec![],
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::ReferenceError));

    let mut vars = FxHashSet::default();
    CognitiveNexus::collect_filter_row_sensitive_vars(
        &FilterExpression::Not(Box::new(FilterExpression::Comparison {
            left: FilterOperand::Variable(DotPathVar {
                var: "link".to_string(),
                path: vec!["metadata".to_string(), "confidence".to_string()],
            }),
            operator: ComparisonOperator::GreaterThan,
            right: FilterOperand::Literal(serde_json::Number::from_f64(0.5).unwrap().into()),
        })),
        &mut vars,
    );
    assert!(vars.contains("link"));
}

#[tokio::test]
async fn test_kql_find_predicate_variable() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let extra_predicate_kml = r#"
        UPSERT {
            CONCEPT ?related_type {
                {type: "$PropositionType", name: "related_to"}
            }
            CONCEPT ?aspirin {
                {type: "Drug", name: "Aspirin"}
                SET PROPOSITIONS {
                    ("related_to", {type: "Symptom", name: "Headache"})
                }
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(extra_predicate_kml).unwrap(), false)
        .await
        .unwrap();

    // Test 1: FIND with predicate variable ?p alongside entity variables
    let kql = r#"
        FIND(?n, ?p, ?o)
        WHERE {
            ?n {name: "Aspirin"}
            (?n, ?p, ?o)
        }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    // ?n should have Aspirin concept
    assert!(!arr[0].as_array().unwrap().is_empty());
    // ?p should have predicate strings (e.g., "treats")
    let predicates = arr[1].as_array().unwrap();
    assert!(!predicates.is_empty());
    assert!(predicates.iter().any(|p| p.as_str() == Some("treats")));
    // ?o should have matched objects (Headache, Fever)
    assert!(!arr[2].as_array().unwrap().is_empty());

    // Test 2: FIND with only predicate variable
    let kql = r#"
        FIND(?p)
        WHERE {
            ?drug {type: "Drug", name: "Aspirin"}
            (?drug, ?p, ?symptom)
        }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    let predicates = result.as_array().unwrap();
    assert!(!predicates.is_empty());
    assert!(predicates.iter().any(|p| p.as_str() == Some("treats")));

    // Test 2b: predicate-only pagination consumes the returned cursor
    let page_kql = r#"
        FIND(?p)
        WHERE {
            ?drug {type: "Drug", name: "Aspirin"}
            (?drug, ?p, ?symptom)
        }
        LIMIT 1
        "#;
    let query = parse_kql(page_kql).unwrap();
    let (page1, cursor) = nexus.execute_kql(query).await.unwrap();
    assert!(cursor.is_some());
    let page1 = page1.as_array().unwrap();
    assert_eq!(page1.len(), 1);

    let mut query = parse_kql(page_kql).unwrap();
    query.cursor = cursor;
    let (page2, _) = nexus.execute_kql(query).await.unwrap();
    let page2 = page2.as_array().unwrap();
    assert_eq!(page2.len(), 1);
    assert_ne!(page1[0], page2[0]);

    // Test 3: FIND with literal predicate (not a variable) should still work
    let kql = r#"
        FIND(?drug.name, ?symptom.name)
        WHERE {
            ?drug {type: "Drug"}
            (?drug, "treats", ?symptom)
        }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!([["Aspirin"], ["Headache", "Fever"]]));

    // Test 4: Unbound variable should still produce an error
    let kql = r#"
        FIND(?unbound)
        WHERE {
            ?drug {type: "Drug"}
        }
        "#;
    let query = parse_kql(kql).unwrap();
    let result = nexus.execute_kql(query).await;
    assert!(result.is_err());
    if let Err(err) = result {
        assert!(matches!(err.code, KipErrorCode::ReferenceError));
        assert!(err.message.contains("Unbound variable"));
    }
}

#[tokio::test]
async fn test_kql_variable_rebind_as_filter() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // Setup: create Person concepts and "working_on" propositions
    let setup_kml = r#"
        UPSERT {
            CONCEPT ?working_on_type {
                {type: "$PropositionType", name: "working_on"}
            }
            CONCEPT ?alice {
                {type: "Person", name: "Alice"}
                SET ATTRIBUTES { "role": "researcher" }
                SET PROPOSITIONS {
                    ("working_on", {type: "Drug", name: "Aspirin"})
                }
            }
            CONCEPT ?bob {
                {type: "Person", name: "Bob"}
                SET ATTRIBUTES { "role": "engineer" }
            }
        }
        WITH METADATA {
            "source": "test"
        }
        "#;
    nexus
        .execute_kml(parse_kml(setup_kml).unwrap(), false)
        .await
        .unwrap();

    // Test 1: Concept clause rebind filters existing variable
    // ?person is first bound by the proposition clause, then filtered by concept clause {type: "Person"}
    let kql = r#"
        FIND(?person.name, ?link)
        WHERE {
            ?drug {type: "Drug", name: "Aspirin"}
            ?link (?person, "working_on", ?drug)
            ?person {type: "Person"}
        }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    // ?person should have Alice (the only Person working_on Aspirin)
    let persons = arr[0].as_array().unwrap();
    assert_eq!(persons.len(), 1);
    assert_eq!(persons[0], "Alice");

    // Test 2: Concept clause rebind with type filter that excludes all
    // ?person bound by proposition, then filtered by {type: "Symptom"} — no match
    let kql = r#"
        FIND(?person.name)
        WHERE {
            ?drug {type: "Drug", name: "Aspirin"}
            ?link (?person, "working_on", ?drug)
            ?person {type: "Symptom"}
        }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    let arr = result.as_array().unwrap();
    assert!(arr.is_empty());

    // Test 3: Concept clause used as initial bind (no prior variable) still works
    let kql = r#"
        FIND(?drug.name)
        WHERE {
            ?drug {type: "Drug"}
        }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0], "Aspirin");

    // Test 4: Proposition clause rebind filters existing variable
    // ?symptom is first bound by concept clause, then filtered by proposition clause
    let kql = r#"
        FIND(?symptom.name)
        WHERE {
            ?symptom {type: "Symptom"}
            ?drug {type: "Drug", name: "Aspirin"}
            (?drug, "treats", ?symptom)
        }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    let arr = result.as_array().unwrap();
    // Both Headache and Fever are Symptom type and treated by Aspirin
    assert_eq!(arr.len(), 2);

    // Test 5: Multiple alternative predicates with variable rebind
    let kql = r#"
        FIND(?person.name)
        WHERE {
            ?drug {type: "Drug", name: "Aspirin"}
            ?link (?person, "working_on" | "interested_in" | "expert_in", ?drug)
            ?person {type: "Person"}
        }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0], "Alice");
}

#[tokio::test]
async fn test_kql_prefers_query_preserves_link_row_alignment() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();

    let setup_kml = r#"
        UPSERT {
            CONCEPT ?person_type {
                {type: "$ConceptType", name: "Person"}
            }
            CONCEPT ?preference_type {
                {type: "$ConceptType", name: "Preference"}
            }
            CONCEPT ?prefers_type {
                {type: "$PropositionType", name: "prefers"}
            }
            CONCEPT ?person {
                {type: "Person", name: "alice-prefers-query"}
            }
            CONCEPT ?tea {
                {type: "Preference", name: "Tea"}
                SET ATTRIBUTES { "evidence_count": 10 }
            }
            CONCEPT ?music {
                {type: "Preference", name: "Music"}
                SET ATTRIBUTES { "evidence_count": 10 }
            }
            CONCEPT ?coffee {
                {type: "Preference", name: "Coffee"}
                SET ATTRIBUTES { "evidence_count": 7 }
            }
            CONCEPT ?old {
                {type: "Preference", name: "Old"}
                SET ATTRIBUTES { "evidence_count": 99 }
            }
            PROPOSITION ?tea_link {
                ({type: "Person", name: "alice-prefers-query"}, "prefers", {type: "Preference", name: "Tea"})
            } WITH METADATA { "confidence": 0.4 }
            PROPOSITION ?music_link {
                ({type: "Person", name: "alice-prefers-query"}, "prefers", {type: "Preference", name: "Music"})
            } WITH METADATA { "confidence": 0.8 }
            PROPOSITION ?coffee_link {
                ({type: "Person", name: "alice-prefers-query"}, "prefers", {type: "Preference", name: "Coffee"})
            } WITH METADATA { "confidence": 0.9 }
            PROPOSITION ?old_link {
                ({type: "Person", name: "alice-prefers-query"}, "prefers", {type: "Preference", name: "Old"})
            } WITH METADATA { "confidence": 1.0, "superseded": true }
        }
        "#;
    nexus
        .execute_kml(parse_kml(setup_kml).unwrap(), false)
        .await
        .unwrap();

    let command = r#"
        FIND(?pref, ?link.metadata) WHERE {
          ?p {type: "Person", name: :person_id}
          ?link (?p, "prefers", ?pref)
          FILTER(IS_NULL(?link.metadata.superseded) || ?link.metadata.superseded != true)
        } ORDER BY ?pref.attributes.evidence_count DESC, ?link.metadata.confidence DESC LIMIT 20
        "#;
    let mut parameters = Map::new();
    parameters.insert(
        "person_id".to_string(),
        Json::String("alice-prefers-query".to_string()),
    );
    let request = Request {
        command: command.to_string(),
        parameters,
        readonly: true,
        ..Default::default()
    };

    let (cmd_type, response) = request.execute(&nexus).await;
    assert_eq!(cmd_type, CommandType::Kql);
    let result = response.into_result().unwrap();
    let columns = result.as_array().unwrap();
    assert_eq!(columns.len(), 2);

    let prefs = columns[0].as_array().unwrap();
    let pref_names: Vec<&str> = prefs
        .iter()
        .map(|pref| pref["name"].as_str().unwrap())
        .collect();
    assert_eq!(pref_names, vec!["Music", "Tea", "Coffee"]);

    let link_metadata = columns[1].as_array().unwrap();
    let confidences: Vec<Json> = link_metadata
        .iter()
        .map(|metadata| metadata["confidence"].clone())
        .collect();
    assert_eq!(confidences, vec![json!(0.8), json!(0.4), json!(0.9)]);
    assert!(
        link_metadata
            .iter()
            .all(|metadata| metadata.get("superseded") != Some(&Json::Bool(true)))
    );
}

#[tokio::test]
async fn test_kql_grouped_find_count() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // Add more drugs with varying symptom relationships
    let more_drugs_kml = r#"
        UPSERT {
            CONCEPT ?ibuprofen {
                {type: "Drug", name: "Ibuprofen"}
                SET ATTRIBUTES {
                    "risk_level": 3
                }
                SET PROPOSITIONS {
                    ("treats", {type: "Symptom", name: "Headache"})
                }
            }
            CONCEPT ?paracetamol {
                {type: "Drug", name: "Paracetamol"}
                SET ATTRIBUTES {
                    "risk_level": 1
                }
                SET PROPOSITIONS {
                    ("treats", {type: "Symptom", name: "Headache"})
                    ("treats", {type: "Symptom", name: "Fever"})
                }
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(more_drugs_kml).unwrap(), false)
        .await
        .unwrap();

    // Test: FIND(?symptom.name, COUNT(?drug)) — group by symptom, count drugs
    // Headache is treated by Aspirin, Ibuprofen, Paracetamol (3)
    // Fever is treated by Aspirin, Paracetamol (2)
    let kql = r#"
        FIND(?symptom.name, COUNT(?drug))
        WHERE {
            ?symptom {type: "Symptom"}
            (?drug, "treats", ?symptom)
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    // Should return row-mode: [["Headache", "Fever"], [3, 2]]
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    let names = arr[0].as_array().unwrap();
    let counts = arr[1].as_array().unwrap();
    assert_eq!(names.len(), counts.len());
    // Verify each symptom has the correct count
    for (i, name) in names.iter().enumerate() {
        match name.as_str().unwrap() {
            "Headache" => assert_eq!(counts[i], json!(3)),
            "Fever" => assert_eq!(counts[i], json!(2)),
            other => panic!("Unexpected symptom: {other}"),
        }
    }
}

#[tokio::test]
async fn test_kql_grouped_find_order_by_count_asc() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let more_drugs_kml = r#"
        UPSERT {
            CONCEPT ?ibuprofen {
                {type: "Drug", name: "Ibuprofen"}
                SET ATTRIBUTES {
                    "risk_level": 3
                }
                SET PROPOSITIONS {
                    ("treats", {type: "Symptom", name: "Headache"})
                }
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(more_drugs_kml).unwrap(), false)
        .await
        .unwrap();

    // Headache: treated by Aspirin + Ibuprofen = 2
    // Fever: treated by Aspirin = 1
    // ORDER BY COUNT(?drug) ASC → Fever first, then Headache
    let kql = r#"
        FIND(?symptom.name, COUNT(?drug))
        WHERE {
            ?symptom {type: "Symptom"}
            (?drug, "treats", ?symptom)
        }
        ORDER BY COUNT(?drug) ASC
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!([["Fever", "Headache"], [1, 2]]));
}

#[tokio::test]
async fn test_kql_grouped_find_order_by_count_desc() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let more_drugs_kml = r#"
        UPSERT {
            CONCEPT ?ibuprofen {
                {type: "Drug", name: "Ibuprofen"}
                SET ATTRIBUTES {
                    "risk_level": 3
                }
                SET PROPOSITIONS {
                    ("treats", {type: "Symptom", name: "Headache"})
                }
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(more_drugs_kml).unwrap(), false)
        .await
        .unwrap();

    // ORDER BY COUNT(?drug) DESC → Headache first (2), then Fever (1)
    let kql = r#"
        FIND(?symptom.name, COUNT(?drug))
        WHERE {
            ?symptom {type: "Symptom"}
            (?drug, "treats", ?symptom)
        }
        ORDER BY COUNT(?drug) DESC
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!([["Headache", "Fever"], [2, 1]]));
}

#[tokio::test]
async fn test_kql_grouped_find_with_limit() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let more_drugs_kml = r#"
        UPSERT {
            CONCEPT ?ibuprofen {
                {type: "Drug", name: "Ibuprofen"}
                SET ATTRIBUTES {
                    "risk_level": 3
                }
                SET PROPOSITIONS {
                    ("treats", {type: "Symptom", name: "Headache"})
                }
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(more_drugs_kml).unwrap(), false)
        .await
        .unwrap();

    // ORDER BY COUNT(?drug) DESC LIMIT 1 → only Headache (has 2 drugs)
    let kql = r#"
        FIND(?symptom.name, COUNT(?drug))
        WHERE {
            ?symptom {type: "Symptom"}
            (?drug, "treats", ?symptom)
        }
        ORDER BY COUNT(?drug) DESC
        LIMIT 1
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, cursor) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!([["Headache"], [2]]));
    assert!(cursor.is_some());

    let cursor = cursor.unwrap();
    let kql = r#"
        FIND(?symptom.name, COUNT(?drug))
        WHERE {
            ?symptom {type: "Symptom"}
            (?drug, "treats", ?symptom)
        }
        ORDER BY COUNT(?drug) DESC
        LIMIT 1
        CURSOR "$cursor"
        "#;

    let query = parse_kql(&kql.replace("$cursor", cursor.as_str())).unwrap();
    let (result, cursor) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!([["Fever"], [1]]));
    assert!(cursor.is_none());

    let kql = r#"
        FIND(?symptom.name, ?all.name, COUNT(?drug), SUM(?all.attributes.risk_level))
        WHERE {
            ?symptom {type: "Symptom"}
            (?drug, "treats", ?symptom)
            ?all {type: "Drug"}
        }
        ORDER BY COUNT(?drug) DESC
        LIMIT 1
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    let columns = result.as_array().unwrap();
    assert_eq!(columns.len(), 4);
    assert_eq!(columns[0], json!(["Headache"]));
    assert_eq!(columns[2], json!([2]));
    assert_eq!(columns[3], json!(5.0));
}

#[tokio::test]
async fn test_kql_grouped_find_with_optional() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // Add a drug without any "treats" propositions
    let lone_drug_kml = r#"
        UPSERT {
            CONCEPT ?vitamin {
                {type: "Drug", name: "VitaminC"}
                SET ATTRIBUTES {
                    "risk_level": 0
                }
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(lone_drug_kml).unwrap(), false)
        .await
        .unwrap();

    // With OPTIONAL, VitaminC should appear with count 0
    // Aspirin → treats [Headache, Fever] = 2
    // VitaminC → treats [] = 0
    let kql = r#"
        FIND(?drug.name, COUNT(?symptom))
        WHERE {
            ?drug {type: "Drug"}
            OPTIONAL {
                (?drug, "treats", ?symptom)
            }
        }
        ORDER BY COUNT(?symptom) ASC
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    let names = arr[0].as_array().unwrap();
    let counts = arr[1].as_array().unwrap();
    // VitaminC should come first (0 symptoms), then Aspirin (2 symptoms)
    assert_eq!(names[0], json!("VitaminC"));
    assert_eq!(counts[0], json!(0));
    assert_eq!(names[1], json!("Aspirin"));
    assert_eq!(counts[1], json!(2));
}

#[tokio::test]
async fn test_kql_count_skip_io_optimization() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // Plain COUNT without GROUP BY should also work correctly
    // and should use skip-IO optimization (count from bindings directly)
    let kql = r#"
        FIND(COUNT(?drug))
        WHERE {
            ?drug {type: "Drug"}
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!(1));

    // Add more drugs
    let drugs_kml = r#"
        UPSERT {
            CONCEPT ?ibuprofen {
                {type: "Drug", name: "Ibuprofen"}
                SET ATTRIBUTES {
                    "risk_level": 3
                }
            }
            CONCEPT ?paracetamol {
                {type: "Drug", name: "Paracetamol"}
                SET ATTRIBUTES {
                    "risk_level": 1
                }
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(drugs_kml).unwrap(), false)
        .await
        .unwrap();

    let kql = r#"
        FIND(COUNT(?drug))
        WHERE {
            ?drug {type: "Drug"}
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!(3));

    // FIND with COUNT and another variable but same var (non-grouped)
    let kql = r#"
        FIND(COUNT(?drug), COUNT(DISTINCT ?drug))
        WHERE {
            ?drug {type: "Drug"}
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!([3, 3]));
}

#[tokio::test]
async fn test_kql_grouped_find_reverse_direction() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // Test grouping in the other direction:
    // FIND(?drug.name, COUNT(?symptom)) where drug is subject
    // Aspirin → treats → [Headache, Fever] (count 2)
    let kql = r#"
        FIND(?drug.name, COUNT(?symptom))
        WHERE {
            ?drug {type: "Drug"}
            (?drug, "treats", ?symptom)
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(result, json!([["Aspirin"], [2]]));
}

#[tokio::test]
async fn test_kml_delete_concept_protected_scope_returns_kip_3004() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();

    // The default bootstrap loads $ConceptType / $PropositionType meta-types
    // and the CoreSchema domain. Bring up $self / $system as well so we can
    // exercise every category of protected node from KIP v1.0-RC6 §4.2.4.
    nexus
        .execute_kml(parse_kml(PERSON_SELF_KIP).unwrap(), false)
        .await
        .unwrap();
    nexus
        .execute_kml(parse_kml(PERSON_SYSTEM_KIP).unwrap(), false)
        .await
        .unwrap();

    let cases = [
        r#"DELETE CONCEPT ?x DETACH WHERE { ?x {type: "$ConceptType", name: "$ConceptType"} }"#,
        r#"DELETE CONCEPT ?x DETACH WHERE { ?x {type: "$ConceptType", name: "$PropositionType"} }"#,
        r#"DELETE CONCEPT ?x DETACH WHERE { ?x {type: "Person", name: "$self"} }"#,
        r#"DELETE CONCEPT ?x DETACH WHERE { ?x {type: "Person", name: "$system"} }"#,
        r#"DELETE CONCEPT ?x DETACH WHERE { ?x {type: "Domain", name: "CoreSchema"} }"#,
    ];
    for kml in cases {
        let stmt = parse_kml(kml).unwrap();
        // dry_run = false: must error before any side effects.
        let err = nexus.execute_kml(stmt.clone(), false).await.unwrap_err();
        assert!(
            matches!(err.code, KipErrorCode::ImmutableTarget),
            "expected KIP_3004 for {kml}, got {:?}",
            err.code
        );
        // dry_run = true: still must error so agents can probe safely.
        let err = nexus.execute_kml(stmt, true).await.unwrap_err();
        assert!(
            matches!(err.code, KipErrorCode::ImmutableTarget),
            "expected KIP_3004 (dry_run) for {kml}, got {:?}",
            err.code
        );
    }

    // Sanity: protected $self is still present after the rejected deletes.
    assert!(
        nexus
            .has_concept(&ConceptPK::Object {
                r#type: PERSON_TYPE.to_string(),
                name: META_SELF_NAME.to_string(),
            })
            .await
    );
}

#[tokio::test]
async fn test_kml_delete_concept_cascade_is_transitive() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // Build a higher-order chain rooted at an ordinary Drug concept:
    //   (Aspirin, "treats", Headache)               — first-order
    //   (TestActor, "stated", <above proposition>)  — higher-order
    // Deleting Aspirin must cascade through both so no dangling reference
    // remains after the DETACH.
    let bootstrap = r#"
        UPSERT {
            CONCEPT ?actor_type {
                {type: "$ConceptType", name: "Actor"}
                SET ATTRIBUTES { description: "Test actor type" }
            }
            CONCEPT ?stated_type {
                {type: "$PropositionType", name: "stated"}
                SET ATTRIBUTES { description: "Higher-order: an actor stated a proposition" }
            }
            CONCEPT ?actor {
                {type: "Actor", name: "TestActor"}
            }
            PROPOSITION ?claim {
                ({type: "Actor", name: "TestActor"},
                 "stated",
                 ({type: "Drug", name: "Aspirin"}, "treats", {type: "Symptom", name: "Headache"}))
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(bootstrap).unwrap(), false)
        .await
        .unwrap();

    let delete = r#"
        DELETE CONCEPT ?d DETACH
        WHERE { ?d {type: "Drug", name: "Aspirin"} }
        "#;
    let res = nexus
        .execute_kml(parse_kml(delete).unwrap(), false)
        .await
        .unwrap();

    // We expect at least 2 propositions cascaded: the first-order "treats"
    // edge and the higher-order "stated" edge that referenced it.
    assert_eq!(res["deleted_concepts"], json!(1));
    let cascaded = res["deleted_propositions"].as_u64().unwrap();
    assert!(
        cascaded >= 2,
        "expected transitive cascade to delete >=2 propositions, got {cascaded}"
    );

    // Confirm Aspirin is gone.
    assert!(
        !nexus
            .has_concept(&ConceptPK::Object {
                r#type: "Drug".to_string(),
                name: "Aspirin".to_string(),
            })
            .await
    );
}

#[tokio::test]
async fn test_kml_delete_propositions_multi_predicate_no_resurrection() {
    // Regression: previously, a single Proposition row carrying multiple
    // predicates could have already-removed predicates "resurrected" when
    // the same row appeared again in the target set under another
    // predicate, because the per-query QueryCache returned the stale
    // pre-update Proposition.
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // Add a second predicate type and a proposition that carries both
    // "treats" and "alleviates" between Aspirin and Headache (so a single
    // Proposition row holds both predicates simultaneously).
    let bootstrap = r#"
        UPSERT {
            CONCEPT ?alleviates_pred {
                {type: "$PropositionType", name: "alleviates"}
            }
            PROPOSITION ?p {
                ({type: "Drug", name: "Aspirin"}, "alleviates", {type: "Symptom", name: "Headache"})
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(bootstrap).unwrap(), false)
        .await
        .unwrap();

    // Sanity: the Aspirin → Headache row now carries both predicates.
    let kql = r#"
        FIND(?link)
        WHERE {
            ?link ({type: "Drug", name: "Aspirin"}, ?p, {type: "Symptom", name: "Headache"})
        }
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    let links = result.as_array().unwrap();
    let predicates: BTreeSet<String> = links
        .iter()
        .map(|v| v["predicate"].as_str().unwrap().to_string())
        .collect();
    assert!(predicates.contains("treats"));
    assert!(predicates.contains("alleviates"));

    // Delete BOTH predicates in a single statement. The target set
    // expands to two EntityID::Proposition entries that share the same
    // underlying _id but differ in predicate.
    let delete = r#"
        DELETE PROPOSITIONS ?link
        WHERE {
            ?link ({type: "Drug", name: "Aspirin"}, ?p, {type: "Symptom", name: "Headache"})
        }
        "#;
    nexus
        .execute_kml(parse_kml(delete).unwrap(), false)
        .await
        .unwrap();

    // After the cache fix, BOTH predicates must be gone. Without the fix,
    // the second iteration would have re-added the predicate removed by
    // the first iteration.
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    let links = result.as_array().unwrap();
    assert!(
        links.is_empty(),
        "expected all Aspirin→Headache predicates to be gone, got {links:?}"
    );
}

#[tokio::test]
async fn test_reserved_metadata_is_engine_maintained() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // KML cannot write reserved `_` metadata (KIP_2002), at any level.
    for kml in [
        r#"UPSERT {
                CONCEPT ?c { {type: "Drug", name: "Aspirin"} }
                WITH METADATA { "_version": 9 }
            }"#,
        r#"UPSERT {
                CONCEPT ?c { {type: "Drug", name: "Aspirin"} } WITH METADATA { "_score": 1.0 }
            }"#,
        r#"UPSERT {
                PROPOSITION ?p {
                    ({type: "Drug", name: "Aspirin"}, "treats", {type: "Symptom", name: "Headache"})
                } WITH METADATA { "_updated_at": "2026-01-01T00:00:00Z" }
            }"#,
    ] {
        let err = nexus
            .execute_kml(parse_kml(kml).unwrap(), false)
            .await
            .unwrap_err();
        assert!(
            matches!(err.code, KipErrorCode::ConstraintViolation),
            "expected KIP_2002 for {kml}, got {err:?}"
        );
    }

    // ... and cannot delete it either.
    let err = nexus
        .execute_kml(
            parse_kml(
                r#"DELETE METADATA {"_version"} FROM ?c
                    WHERE { ?c {type: "Drug", name: "Aspirin"} }"#,
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::ConstraintViolation));

    // KQL reads reserved metadata like ordinary metadata.
    let (result, _) = nexus
        .execute_kql(
            parse_kql(
                r#"FIND(?c.metadata._version)
                    WHERE { ?c {type: "Drug", name: "Aspirin"} }"#,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(result, json!([1]));

    // A mutation advances `_version` (here: 1 → 2).
    nexus
        .execute_kml(
            parse_kml(
                r#"UPSERT {
                        CONCEPT ?c {
                            {type: "Drug", name: "Aspirin"}
                            SET ATTRIBUTES { "risk_level": 3 }
                        }
                    }"#,
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap();
    let (result, _) = nexus
        .execute_kql(
            parse_kql(
                r#"FIND(?c.metadata._version)
                    WHERE { ?c {type: "Drug", name: "Aspirin"} }"#,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(result, json!([2]));
}

#[tokio::test]
async fn test_kml_expect_version_guard() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // Guard matching the current version succeeds and bumps the version.
    nexus
        .execute_kml(
            parse_kml(
                r#"UPSERT {
                        CONCEPT ?c {
                            {type: "Drug", name: "Aspirin"}
                            EXPECT VERSION 1
                            SET ATTRIBUTES { "risk_level": 3 }
                        }
                    }"#,
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap();

    // A stale guard aborts the whole statement atomically (KIP_3005):
    // the first block alone would succeed, but nothing may be written.
    let err = nexus
        .execute_kml(
            parse_kml(
                r#"UPSERT {
                        CONCEPT ?a {
                            {type: "Symptom", name: "Headache"}
                            SET ATTRIBUTES { "severity": "severe" }
                        }
                        CONCEPT ?c {
                            {type: "Drug", name: "Aspirin"}
                            EXPECT VERSION 1
                            SET ATTRIBUTES { "risk_level": 4 }
                        }
                    }"#,
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::VersionConflict));
    let (result, _) = nexus
        .execute_kql(
            parse_kql(
                r#"FIND(?c.attributes.risk_level, ?c.metadata._version, ?h.attributes.severity)
                    WHERE {
                        ?c {type: "Drug", name: "Aspirin"}
                        ?h {type: "Symptom", name: "Headache"}
                    }"#,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    // FIND expressions on the same variable are grouped into rows.
    assert_eq!(result, json!([[[3, 2]], ["moderate"]]));

    // EXPECT VERSION 0 is create-only: it fails on existing elements...
    let err = nexus
        .execute_kml(
            parse_kml(
                r#"UPSERT {
                        CONCEPT ?c {
                            {type: "Drug", name: "Aspirin"}
                            EXPECT VERSION 0
                            SET ATTRIBUTES { "risk_level": 9 }
                        }
                    }"#,
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::VersionConflict));

    // ... and succeeds when the element does not exist yet.
    nexus
        .execute_kml(
            parse_kml(
                r#"UPSERT {
                        CONCEPT ?c {
                            {type: "Drug", name: "Naproxen"}
                            EXPECT VERSION 0
                            SET ATTRIBUTES { "risk_level": 2 }
                        }
                    }"#,
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap();

    // Proposition guards follow the same contract.
    let err = nexus
            .execute_kml(
                parse_kml(
                    r#"UPSERT {
                        PROPOSITION ?p {
                            ({type: "Drug", name: "Aspirin"}, "treats", {type: "Symptom", name: "Headache"})
                            EXPECT VERSION 7
                            SET ATTRIBUTES { "effectiveness": 0.9 }
                        }
                    }"#,
                )
                .unwrap(),
                false,
            )
            .await
            .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::VersionConflict));
    nexus
            .execute_kml(
                parse_kml(
                    r#"UPSERT {
                        PROPOSITION ?p {
                            ({type: "Drug", name: "Aspirin"}, "treats", {type: "Symptom", name: "Headache"})
                            EXPECT VERSION 1
                            SET ATTRIBUTES { "effectiveness": 0.9 }
                        }
                    }"#,
                )
                .unwrap(),
                false,
            )
            .await
            .unwrap();

    // Dry run evaluates the guard without writing.
    let err = nexus
        .execute_kml(
            parse_kml(
                r#"UPSERT {
                        CONCEPT ?c {
                            {type: "Drug", name: "Aspirin"}
                            EXPECT VERSION 1
                            SET ATTRIBUTES { "risk_level": 9 }
                        }
                    }"#,
            )
            .unwrap(),
            true,
        )
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::VersionConflict));
}

#[tokio::test]
async fn test_kml_update_statement() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // Bulk update with the reinforcement idiom: COALESCE initializes the
    // missing counter, ADD increments it; plain JSON values pass through.
    let update = r#"
        UPDATE ?s
        SET ATTRIBUTES {
            observed_count: ADD(COALESCE(?s.attributes.observed_count, 0), 1),
            status: "active"
        }
        SET METADATA { last_review: "2026-06-11" }
        WHERE {
            ?s {type: "Symptom"}
        }
        "#;
    let result = nexus
        .execute_kml(parse_kml(update).unwrap(), false)
        .await
        .unwrap();
    assert_eq!(result, json!({ "updated": 2, "matched": 2 }));

    let (result, _) = nexus
            .execute_kql(
                parse_kql(
                    r#"FIND(?s.name, ?s.attributes.observed_count, ?s.attributes.status, ?s.metadata.last_review, ?s.metadata._version)
                    WHERE { ?s {type: "Symptom", name: "Fever"} }"#,
                )
                .unwrap(),
            )
            .await
            .unwrap();
    // FIND expressions on the same variable are grouped into rows.
    assert_eq!(result, json!([["Fever", 1, "active", "2026-06-11", 2]]));

    // Second run increments the now-existing counter (integer preserved).
    nexus
        .execute_kml(parse_kml(update).unwrap(), false)
        .await
        .unwrap();
    let (result, _) = nexus
        .execute_kql(
            parse_kql(
                r#"FIND(?s.attributes.observed_count)
                    WHERE { ?s {type: "Symptom", name: "Fever"} }"#,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(result, json!([2]));

    // UPDATE never creates: an unmatched pattern updates nothing.
    let result = nexus
        .execute_kml(
            parse_kml(
                r#"UPDATE ?x
                    SET ATTRIBUTES { status: "ghost" }
                    WHERE { ?x {type: "Drug", name: "Nonexistent"} }"#,
            )
            .unwrap(),
            false,
        )
        .await;
    // The concept clause reports the missing identity as KIP_3002,
    // consistent with KQL concept matching.
    assert!(result.is_err());

    // LIMIT caps the blast radius; dry_run reports matched only.
    let result = nexus
        .execute_kml(
            parse_kml(
                r#"UPDATE ?s
                    SET ATTRIBUTES { status: "capped" }
                    WHERE { ?s {type: "Symptom"} }
                    LIMIT 1"#,
            )
            .unwrap(),
            true,
        )
        .await
        .unwrap();
    assert_eq!(result, json!({ "updated": 0, "matched": 1 }));

    // Proposition links update with the decay idiom on metadata.
    let result = nexus
            .execute_kml(
                parse_kml(
                    r#"UPDATE ?link
                    SET METADATA {
                        confidence: CLAMP(MUL(COALESCE(?link.metadata.confidence, 1.0), 0.5), 0.0, 1.0)
                    }
                    WHERE {
                        ?link ({type: "Drug", name: "Aspirin"}, "treats", ?o)
                    }"#,
                )
                .unwrap(),
                false,
            )
            .await
            .unwrap();
    assert_eq!(result, json!({ "updated": 2, "matched": 2 }));
    let (result, _) = nexus
        .execute_kql(
            parse_kql(
                r#"FIND(?link.metadata.confidence)
                    WHERE {
                        ?h {type: "Symptom", name: "Headache"}
                        ?link ({type: "Drug", name: "Aspirin"}, "treats", ?h)
                    }"#,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(result, json!([0.475])); // 0.95 * 0.5

    // Reserved `_` metadata keys are rejected (KIP_2002).
    let err = nexus
        .execute_kml(
            parse_kml(
                r#"UPDATE ?s
                    SET METADATA { "_version": 7 }
                    WHERE { ?s {type: "Symptom"} }"#,
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::ConstraintViolation));

    // Expression paths must address the UPDATE target itself.
    let err = nexus
        .execute_kml(
            parse_kml(
                r#"UPDATE ?s
                    SET ATTRIBUTES { n: ADD(?other.attributes.n, 1) }
                    WHERE { ?s {type: "Symptom"} }"#,
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::InvalidSyntax));

    // Protected schema structures fail the whole statement (KIP_3004).
    let err = nexus
        .execute_kml(
            parse_kml(
                r#"UPDATE ?c
                    SET ATTRIBUTES { status: "hijacked" }
                    WHERE { ?c {type: "$ConceptType", name: "$ConceptType"} }"#,
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::ImmutableTarget));

    // `$self` ordinary attributes may evolve via UPDATE, but its
    // `core_directives` stay immutable.
    nexus
        .execute_kml(
            parse_kml(
                r#"UPSERT {
                        CONCEPT ?self {
                            {type: "Person", name: "$self"}
                            SET ATTRIBUTES { "persona": "nascent" }
                        }
                    }"#,
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap();
    let err = nexus
        .execute_kml(
            parse_kml(
                r#"UPDATE ?p
                    SET ATTRIBUTES { core_directives: [] }
                    WHERE { ?p {type: "Person", name: "$self"} }"#,
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::ImmutableTarget));
    let result = nexus
        .execute_kml(
            parse_kml(
                r#"UPDATE ?p
                    SET ATTRIBUTES { persona: "curious and patient" }
                    WHERE { ?p {type: "Person", name: "$self"} }"#,
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap();
    assert_eq!(result, json!({ "updated": 1, "matched": 1 }));
}

#[tokio::test]
async fn test_kml_merge_statement() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // A duplicate of Aspirin with its own links and attributes:
    // - one link that the canonical node lacks (repointed, id preserved)
    // - one duplicate link (deduplicated; missing keys filled)
    nexus
        .execute_kml(
            parse_kml(
                r#"UPSERT {
                        CONCEPT ?dup {
                            {type: "Drug", name: "ASA"}
                            SET ATTRIBUTES {
                                "aliases": ["acetylsalicylic acid"],
                                "origin": "willow bark",
                                "risk_level": 5
                            }
                            SET PROPOSITIONS {
                                ("treats", {type: "Symptom", name: "Headache"})
                                    WITH METADATA { "source": "dup_only", "note": "from ASA" }
                            }
                        }
                    }"#,
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap();
    // A link only the duplicate has: ASA treats a brand-new symptom.
    nexus
        .execute_kml(
            parse_kml(
                r#"UPSERT {
                        CONCEPT ?pain {
                            {type: "Symptom", name: "Toothache"}
                        }
                        PROPOSITION ?p {
                            ({type: "Drug", name: "ASA"}, "treats", ?pain)
                            SET ATTRIBUTES { "evidence": "weak" }
                        }
                    }"#,
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap();

    let result = nexus
        .execute_kml(
            parse_kml(
                r#"MERGE CONCEPT ?dup INTO ?canonical
                    WHERE {
                        ?dup {type: "Drug", name: "ASA"}
                        ?canonical {type: "Drug", name: "Aspirin"}
                    }"#,
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap();
    assert_eq!(result["merged"], json!(true));
    assert_eq!(result["links_repointed"], json!(1)); // treats Toothache
    assert_eq!(result["links_deduplicated"], json!(1)); // treats Headache
    // origin + aliases (risk_level conflicts: target wins)
    assert_eq!(result["attributes_filled"], json!(2));

    // The source node is gone; retrying reports KIP_3002.
    assert!(
        !nexus
            .has_concept(&ConceptPK::Object {
                r#type: "Drug".to_string(),
                name: "ASA".to_string(),
            })
            .await
    );
    let err = nexus
        .execute_kml(
            parse_kml(
                r#"MERGE CONCEPT ?dup INTO ?canonical
                    WHERE {
                        ?dup {type: "Drug", name: "ASA"}
                        ?canonical {type: "Drug", name: "Aspirin"}
                    }"#,
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::NotFound));

    // Target state: attributes filled (target wins on conflict), aliases
    // unioned with the source name appended, `_merged_from` provenance.
    let aspirin = nexus
        .get_concept(&ConceptPK::Object {
            r#type: "Drug".to_string(),
            name: "Aspirin".to_string(),
        })
        .await
        .unwrap();
    assert_eq!(aspirin.attributes["origin"], json!("willow bark"));
    assert_eq!(aspirin.attributes["risk_level"], json!(2)); // target wins
    assert_eq!(
        aspirin.attributes["aliases"],
        json!(["acetylsalicylic acid", "ASA"])
    );
    assert_eq!(aspirin.metadata["_merged_from"], json!(["Drug:ASA"]));

    // The repointed link kept its attributes and now hangs off Aspirin;
    // the deduplicated link kept the target's keys and filled the
    // source-only ones.
    let (result, _) = nexus
        .execute_kql(
            parse_kql(
                r#"FIND(?link.attributes.evidence)
                    WHERE {
                        ?t {type: "Symptom", name: "Toothache"}
                        ?link ({type: "Drug", name: "Aspirin"}, "treats", ?t)
                    }"#,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(result, json!(["weak"]));
    let (result, _) = nexus
        .execute_kql(
            parse_kql(
                r#"FIND(?link.metadata.source, ?link.metadata.note)
                    WHERE {
                        ?h {type: "Symptom", name: "Headache"}
                        ?link ({type: "Drug", name: "Aspirin"}, "treats", ?h)
                    }"#,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(result, json!([["test_data", "from ASA"]]));

    // Error paths: ambiguous bindings, differing types, protected nodes.
    let err = nexus
        .execute_kml(
            parse_kml(
                r#"MERGE CONCEPT ?dup INTO ?canonical
                    WHERE {
                        ?dup {type: "Symptom"}
                        ?canonical {type: "Drug", name: "Aspirin"}
                    }"#,
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::DuplicateExists));

    let err = nexus
        .execute_kml(
            parse_kml(
                r#"MERGE CONCEPT ?dup INTO ?canonical
                    WHERE {
                        ?dup {type: "Symptom", name: "Fever"}
                        ?canonical {type: "Drug", name: "Aspirin"}
                    }"#,
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::ConstraintViolation));

    // Protected system structures cannot be merged: `CoreSchema` is a
    // core domain created by the Genesis capsule.
    nexus
        .execute_kml(
            parse_kml(
                r#"UPSERT {
                        CONCEPT ?d {
                            {type: "Domain", name: "TestDomain"}
                            SET ATTRIBUTES { "description": "scratch domain" }
                        }
                    }"#,
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap();
    let err = nexus
        .execute_kml(
            parse_kml(
                r#"MERGE CONCEPT ?dup INTO ?canonical
                    WHERE {
                        ?dup {type: "Domain", name: "TestDomain"}
                        ?canonical {type: "Domain", name: "CoreSchema"}
                    }"#,
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::ImmutableTarget));

    // Merging a node into itself is a no-op success.
    let result = nexus
        .execute_kml(
            parse_kml(
                r#"MERGE CONCEPT ?dup INTO ?canonical
                    WHERE {
                        ?dup {type: "Drug", name: "Aspirin"}
                        ?canonical {type: "Drug", name: "Aspirin"}
                    }"#,
            )
            .unwrap(),
            false,
        )
        .await
        .unwrap();
    assert_eq!(result["merged"], json!(true));
    assert_eq!(result["links_repointed"], json!(0));
}

#[tokio::test]
async fn test_meta_export_round_trip() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // Export the Drug subgraph: the concept, its outgoing links, plus the
    // schema nodes so the capsule can bootstrap a fresh nexus (KIP §5.3).
    let export = r#"
        EXPORT ?n
        WHERE {
            UNION { ?n {type: "$ConceptType"} }
            UNION { ?n {type: "$PropositionType", name: "treats"} }
            UNION { ?n {type: "Drug"} }
            UNION { ?n {type: "Symptom"} }
            UNION { ?n (?drug, "treats", ?o) }
        }
        "#;
    let (result, _) = nexus
        .execute_meta(parse_meta(export).unwrap())
        .await
        .unwrap();
    assert_eq!(result["propositions"], json!(2));
    let capsule = result["capsule"].as_str().unwrap();
    // Engine bookkeeping never leaves the source engine.
    assert!(!capsule.contains("_version"));
    assert!(!capsule.contains("_updated_at"));
    // Author metadata survives.
    assert!(capsule.contains("test_data"));

    // The capsule is a valid, idempotent UPSERT script: importing it into
    // a fresh nexus reproduces the knowledge.
    let parsed = parse_kml(capsule).unwrap();
    let second = setup_test_db(async |_| Ok(())).await.unwrap();
    second.execute_kml(parsed.clone(), false).await.unwrap();
    // Idempotent: a second import succeeds and changes nothing visible.
    second.execute_kml(parsed, false).await.unwrap();

    let (result, _) = second
        .execute_kql(
            parse_kql(
                r#"FIND(?drug.attributes.risk_level, ?s.name)
                    WHERE {
                        ?drug {type: "Drug", name: "Aspirin"}
                        ?s {type: "Symptom"}
                        (?drug, "treats", ?s)
                    }"#,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(result, json!([[2], ["Headache", "Fever"]]));
    let (result, _) = second
        .execute_kql(
            parse_kql(
                r#"FIND(?link.metadata.source)
                    WHERE {
                        ?h {type: "Symptom", name: "Headache"}
                        ?link ({type: "Drug", name: "Aspirin"}, "treats", ?h)
                    }"#,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(result, json!(["test_data"]));

    // Out-of-set endpoints are referenced structurally: exporting only
    // the links yields `{type, name}` references that require the
    // endpoints to exist on import.
    let (result, _) = nexus
        .execute_meta(
            parse_meta(r#"EXPORT ?link WHERE { ?link (?s, "treats", ?o) } LIMIT 1"#).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(result["concepts"], json!(0));
    assert_eq!(result["propositions"], json!(1));
    let capsule = result["capsule"].as_str().unwrap();
    assert!(capsule.contains(r#"{type: "Drug", name: "Aspirin"}"#));
    let third = setup_test_db(async |_| Ok(())).await.unwrap();
    let err = third
        .execute_kml(parse_kml(capsule).unwrap(), false)
        .await
        .unwrap_err();
    assert!(matches!(
        err.code,
        KipErrorCode::NotFound | KipErrorCode::TypeMismatch
    ));
}

#[tokio::test]
async fn test_meta_search_modes_threshold_and_score() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // Keyword search returns hits ordered by descending transient _score.
    let (result, _) = nexus
        .execute_meta(parse_meta(r#"SEARCH CONCEPT "Aspirin" LIMIT 5"#).unwrap())
        .await
        .unwrap();
    let hits = result.as_array().unwrap();
    assert!(!hits.is_empty());
    assert_eq!(hits[0]["name"], json!("Aspirin"));
    let scores: Vec<f64> = hits
        .iter()
        .map(|h| h["metadata"]["_score"].as_f64().unwrap())
        .collect();
    assert_eq!(scores[0], 1.0);
    assert!(scores.windows(2).all(|w| w[0] >= w[1]));
    assert!(scores.iter().all(|s| (0.0..=1.0).contains(s)));

    // `_score` is transient: it is not persisted on the element.
    let aspirin = nexus
        .get_concept(&ConceptPK::Object {
            r#type: "Drug".to_string(),
            name: "Aspirin".to_string(),
        })
        .await
        .unwrap();
    assert!(!aspirin.metadata.contains_key("_score"));

    // An engine without semantic capability treats semantic/hybrid as
    // keyword instead of failing.
    for mode in ["semantic", "hybrid", "keyword"] {
        let (result, _) = nexus
            .execute_meta(
                parse_meta(&format!(
                    r#"SEARCH CONCEPT "Aspirin" MODE "{mode}" LIMIT 5"#
                ))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(result[0]["name"], json!("Aspirin"), "mode {mode}");
    }

    // THRESHOLD 1.0 keeps only the best hit(s).
    let (result, _) = nexus
        .execute_meta(parse_meta(r#"SEARCH CONCEPT "Aspirin" THRESHOLD 1.0 LIMIT 10"#).unwrap())
        .await
        .unwrap();
    for hit in result.as_array().unwrap() {
        assert_eq!(hit["metadata"]["_score"], json!(1.0));
    }

    // WITH TYPE constrains the result set.
    let (result, _) = nexus
        .execute_meta(
            parse_meta(r#"SEARCH CONCEPT "Aspirin" WITH TYPE "Symptom" LIMIT 5"#).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(result, json!([]));

    // Proposition search carries _score as well.
    let (result, _) = nexus
        .execute_meta(parse_meta(r#"SEARCH PROPOSITION "treats" LIMIT 10"#).unwrap())
        .await
        .unwrap();
    let hits = result.as_array().unwrap();
    assert!(!hits.is_empty());
    for hit in hits {
        assert_eq!(hit["predicate"], json!("treats"));
        assert!(hit["metadata"]["_score"].as_f64().unwrap() > 0.0);
    }
}
