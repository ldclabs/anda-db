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
    // Avoid asserting the raw doc id: it shifts whenever the bundled
    // bootstrap capsules add or remove concepts.
    let id = result[0]["id"].as_str().unwrap().to_string();
    assert!(id.starts_with("C:"), "unexpected id: {id}");
    result[0]["id"] = json!("C:<dyn>");
    assert_eq!(
        result,
        json!([{
            "_type":"ConceptNode",
            "id":"C:<dyn>",
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
    // Columnar result model (KIP §6.2.2): (Aspirin, Headache) and
    // (Aspirin, Fever) are two solutions, so the columns stay index-aligned.
    assert_eq!(
        result,
        json!([["Aspirin", "Aspirin"], ["Headache", "Fever"]])
    );

    let kql = r#"
        FIND(?drug.name, ?symptom.name)
        WHERE {
            ?drug {type: "Drug"}
            (?drug, "treats", ?symptom) // find symptom by proposition matching
        }
        "#;

    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(
        result,
        json!([["Aspirin", "Aspirin"], ["Headache", "Fever"]])
    );

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
    // Unordered legacy projections iterate bindings in ascending EntityID
    // order (deterministic pagination); Medicine was created before
    // PainReliever in this fixture.
    assert_eq!(
        result,
        json!([["Aspirin"], ["NSAID"], ["Medicine", "PainReliever"]])
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
    // 超出实际路径长度：最后一个模式无解，WHERE 各子句是 AND 关系，
    // 因此整个解集为空 —— 所有投影列都为空。
    assert_eq!(result, json!([[], []]));
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
    // SUM/MIN/MAX keep integer typing for integer inputs; AVG is a float
    // (anda_kip aggregation semantics).
    assert_eq!(result, json!([["Aspirin"], 2, 2.0, 2, 2]));
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
    // Columnar result model (KIP §6.2.2): (Headache, Aspirin) and
    // (Fever, Aspirin) are two solutions, so ?drug.name repeats.
    assert_eq!(
        result,
        json!([["Headache", "Fever"], ["Aspirin", "Aspirin"]])
    );

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
    // OPTIONAL miss keeps the solution and projects null for the unbound
    // variable (KIP §3.4.7.2).
    assert_eq!(result, json!([["Headache", "Fever"], [null, null]]));

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
    // Unordered projections iterate bindings in ascending EntityID order
    // (deterministic pagination); the Symptom fixtures were created before
    // Aspirin. The solution set itself is unchanged.
    assert_eq!(
        result,
        json!([
            "Headache".to_string(),
            "Fever".to_string(),
            "Aspirin".to_string(),
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
    // Columnar result model (KIP §6.2.2): the two solutions
    // (link1, Aspirin, Headache) / (link2, Aspirin, Fever) keep every
    // column index-aligned, so ?drug.name repeats per solution.
    assert_eq!(
        json!(result[1..]),
        json!([
            ["Aspirin".to_string(), "Aspirin".to_string()],
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

    // dry_run validates the statement's logic: an unbound target is a
    // reference error even without executing.
    let dry_run_metadata_missing = r#"
        DELETE METADATA {"drop_meta"} FROM ?missing
        WHERE { ?person {type: "DeletePerson", name: "Alice"} }
        "#;
    let err = nexus
        .execute_kml(parse_kml(dry_run_metadata_missing).unwrap(), true)
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::ReferenceError));

    let dry_run_metadata = r#"
        DELETE METADATA {"drop_meta"} FROM ?person
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

    // `$self` is applied by the application, not the bundled capsules: the
    // PRIMER degrades its identity layer to `null` instead of failing, so
    // agents still get the domain map on a fresh nexus.
    let meta_cmd = MetaCommand::Describe(DescribeTarget::Primer);
    let (result, _) = nexus.execute_meta(meta_cmd).await.unwrap();
    let primer = result.as_object().unwrap();
    assert!(primer["identity"].is_null());
    assert!(primer["domain_map"].is_array());
    // Out-of-band SEARCH capability advertisement (KIP §5.2.1).
    assert_eq!(primer["search_modes"], json!(["keyword"]));

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
    assert!(primer["identity"].is_object());
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
    // Genesis bootstrap (KIP RC10 Appendix 2): CoreSchema plus the three
    // operational domains Unsorted / Archived / System.
    assert_eq!(domains.len(), 4);
    assert_eq!(domains[0]["type"], "Domain");
    assert_eq!(domains[0]["name"], "CoreSchema");
    let names: Vec<&str> = domains.iter().filter_map(|d| d["name"].as_str()).collect();
    for expected in ["CoreSchema", "Unsorted", "Archived", "System"] {
        assert!(names.contains(&expected), "missing domain {expected}");
    }
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
    // Columnar result model (KIP §6.2.2): one column per FIND expression,
    // index-aligned across solutions — (Aspirin, Headache) and
    // (Aspirin, Fever) are two solutions, so ?drug.name repeats.
    assert_eq!(
        result[0],
        json!(["Aspirin".to_string(), "Aspirin".to_string()])
    );
    assert_eq!(
        result[1],
        json!(["Headache".to_string(), "Fever".to_string()])
    );
    assert_eq!(result[2].as_array().unwrap().len(), 2);
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

    let ctx = QueryContext::default();
    let mut regex_cache = FxHashMap::default();
    let assign = kql::FilterAssignment::default();
    let err = nexus
        .eval_filter_assigned(
            &ctx.cache,
            &mut regex_cache,
            &FilterExpression::Function {
                func: FilterFunction::IsNull,
                args: vec![
                    FilterOperand::Literal("a".into()),
                    FilterOperand::Literal("b".into()),
                ],
            },
            &assign,
        )
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::InvalidSyntax));
    assert!(err.message.contains("requires exactly 1 argument"));

    let err = nexus
        .eval_filter_assigned(
            &ctx.cache,
            &mut regex_cache,
            &FilterExpression::Function {
                func: FilterFunction::In,
                args: vec![
                    FilterOperand::Literal("Aspirin".into()),
                    FilterOperand::Literal("Aspirin".into()),
                ],
            },
            &assign,
        )
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::InvalidSyntax));
    assert!(err.message.contains("IN second argument"));

    let err = nexus
        .eval_filter_assigned(
            &ctx.cache,
            &mut regex_cache,
            &FilterExpression::Function {
                func: FilterFunction::Contains,
                args: vec![FilterOperand::Literal("Aspirin".into())],
            },
            &assign,
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
        origin: RelationOrigin::default(),
    };
    let row = QueryRelationRow {
        proposition: Some(EntityID::Proposition(7, "knows".to_string())),
        subject: Some(EntityID::Concept(1)),
        predicate: Some("knows".to_string()),
        object: Some(EntityID::Concept(2)),
    };

    assert!(CognitiveNexus::relation_covers_var(&relation, "link"));
    assert!(CognitiveNexus::relation_covers_var(&relation, "subject"));
    assert!(CognitiveNexus::relation_covers_var(&relation, "pred"));
    assert!(CognitiveNexus::relation_covers_var(&relation, "object"));
    assert!(!CognitiveNexus::relation_covers_var(&relation, "missing"));
    assert_eq!(
        CognitiveNexus::relation_row_entity(&relation, &row, "link"),
        Some(row.proposition.as_ref())
    );
    assert_eq!(
        CognitiveNexus::relation_row_entity(&relation, &row, "subject"),
        Some(row.subject.as_ref())
    );
    assert_eq!(
        CognitiveNexus::relation_row_entity(&relation, &row, "object"),
        Some(row.object.as_ref())
    );
    assert_eq!(
        CognitiveNexus::relation_row_entity(&relation, &row, "pred"),
        None
    );
    assert_eq!(
        CognitiveNexus::relation_row_predicate(&relation, &row, "pred"),
        Some(Some("knows"))
    );
    assert_eq!(
        CognitiveNexus::relation_row_predicate(&relation, &row, "subject"),
        None
    );

    // OPTIONAL-padded rows: covered positions with `None` values project
    // null and are unconstrained during context matching.
    let padded = QueryRelationRow {
        proposition: None,
        subject: Some(EntityID::Concept(1)),
        predicate: None,
        object: None,
    };
    assert_eq!(
        CognitiveNexus::relation_row_entity(&relation, &padded, "object"),
        Some(None)
    );
    assert_eq!(
        CognitiveNexus::relation_row_predicate(&relation, &padded, "pred"),
        Some(None)
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

    // Test 3: FIND with literal predicate (not a variable) should still work.
    // Columnar result model (KIP §6.2.2): two solutions keep the columns
    // index-aligned, so ?drug.name repeats.
    let kql = r#"
        FIND(?drug.name, ?symptom.name)
        WHERE {
            ?drug {type: "Drug"}
            (?drug, "treats", ?symptom)
        }
        "#;
    let query = parse_kql(kql).unwrap();
    let (result, _) = nexus.execute_kql(query).await.unwrap();
    assert_eq!(
        result,
        json!([["Aspirin", "Aspirin"], ["Headache", "Fever"]])
    );

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
    // Integer inputs keep integer typing under SUM (anda_kip semantics).
    assert_eq!(columns[3], json!(5));
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

    // LIMIT caps the blast radius; dry_run reports matched only. `matched`
    // counts the pattern's actual matches before the LIMIT cap so the agent
    // can detect truncation (2 symptoms matched, at most 1 updated).
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
    assert_eq!(result, json!({ "updated": 0, "matched": 2 }));

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
    // Columnar result model (KIP §6.2.2): two solutions, index-aligned.
    assert_eq!(result, json!([[2, 2], ["Headache", "Fever"]]));
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

// --- Regression tests for the 2026-07 KIP RC10 review fixes ---

/// NOT is a pure filter (KIP §3.4.7.1): it must only narrow variables its
/// own pattern references — outer bindings it never mentions survive.
#[tokio::test]
async fn test_kql_not_clause_preserves_unrelated_bindings() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let setup = r#"
        UPSERT {
            CONCEPT ?cat_type { {type: "$ConceptType", name: "Category"} }
            CONCEPT ?btc { {type: "$PropositionType", name: "belongs_to_class"} }
            CONCEPT ?nsaid { {type: "Category", name: "NSAID"} }
            CONCEPT ?aspirin {
                {type: "Drug", name: "Aspirin"}
                SET PROPOSITIONS { ("belongs_to_class", ?nsaid) }
            }
            CONCEPT ?vitamin {
                {type: "Drug", name: "VitaminC"}
                SET PROPOSITIONS {
                    ("treats", {type: "Symptom", name: "Headache"})
                }
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(setup).unwrap(), false)
        .await
        .unwrap();

    // ?headache is bound outside NOT and never mentioned inside it.
    let kql = r#"
        FIND(?drug.name, ?headache.name)
        WHERE {
            ?drug {type: "Drug"}
            ?headache {type: "Symptom", name: "Headache"}
            (?drug, "treats", ?headache)
            NOT {
                (?drug, "belongs_to_class", {type: "Category", name: "NSAID"})
            }
        }
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    assert_eq!(result, json!([["VitaminC"], ["Headache"]]));
}

/// `{0,n}` includes the zero-hop reflexive match (KIP §3.4.2), and explicit
/// quantifiers beyond the engine cap fail with KIP_4002 instead of being
/// silently truncated.
#[tokio::test]
async fn test_kql_multi_hop_zero_hop_and_cap() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let setup = r#"
        UPSERT {
            CONCEPT ?cat_type { {type: "$ConceptType", name: "Category"} }
            CONCEPT ?isa { {type: "$PropositionType", name: "is_subclass_of"} }
            CONCEPT ?a { {type: "Category", name: "CatA"} }
            CONCEPT ?b {
                {type: "Category", name: "CatB"}
                SET PROPOSITIONS { ("is_subclass_of", ?a) }
            }
            CONCEPT ?c {
                {type: "Category", name: "CatC"}
                SET PROPOSITIONS { ("is_subclass_of", ?b) }
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(setup).unwrap(), false)
        .await
        .unwrap();

    let kql = r#"
        FIND(?parent.name)
        WHERE {
            ?concept {type: "Category", name: "CatC"}
            (?concept, "is_subclass_of"{0,5}, ?parent)
        }
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    let names = result.as_array().unwrap();
    for expected in ["CatC", "CatB", "CatA"] {
        assert!(
            names.contains(&json!(expected)),
            "missing {expected}: {names:?}"
        );
    }

    // Explicit bounds beyond the cap are rejected, not silently truncated.
    for quantifier in ["{1,20}", "{12,}"] {
        let kql = format!(
            r#"FIND(?parent.name)
               WHERE {{
                   ?concept {{type: "Category", name: "CatC"}}
                   (?concept, "is_subclass_of"{quantifier}, ?parent)
               }}"#
        );
        let err = nexus
            .execute_kql(parse_kql(&kql).unwrap())
            .await
            .unwrap_err();
        assert!(
            matches!(err.code, KipErrorCode::ResourceExhausted),
            "{quantifier}: {err:?}"
        );
    }
}

/// Predicate variables participate in FILTER (KIP §3.4.2) — the associative
/// recall pattern from the spec.
#[tokio::test]
async fn test_kql_filter_on_predicate_variable() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let setup = r#"
        UPSERT {
            CONCEPT ?cat_type { {type: "$ConceptType", name: "Category"} }
            CONCEPT ?btc { {type: "$PropositionType", name: "belongs_to_class"} }
            CONCEPT ?nsaid { {type: "Category", name: "NSAID"} }
            CONCEPT ?aspirin {
                {type: "Drug", name: "Aspirin"}
                SET PROPOSITIONS { ("belongs_to_class", ?nsaid) }
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(setup).unwrap(), false)
        .await
        .unwrap();

    let kql = r#"
        FIND(?pred, ?neighbor.name)
        WHERE {
            ?a {type: "Drug", name: "Aspirin"}
            ?link (?a, ?pred, ?neighbor)
            FILTER(?pred != "belongs_to_class")
        }
        LIMIT 50
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    let cols = result.as_array().unwrap();
    assert_eq!(cols[0], json!(["treats", "treats"]));
    let neighbors = cols[1].as_array().unwrap();
    assert!(neighbors.contains(&json!("Headache")));
    assert!(neighbors.contains(&json!("Fever")));
}

/// The RC10 sleep-cycle confidence-decay UPDATE: fully unconstrained
/// `(?s, ?p, ?o)` exploration plus predicate-variable FILTER plus update
/// expressions, in one statement.
#[tokio::test]
async fn test_kml_update_decay_with_full_scan_pattern() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let update = r#"
        UPDATE ?link
        SET METADATA {
            confidence: CLAMP(MUL(?link.metadata.confidence, 0.9), 0.0, 1.0),
            decay_applied_at: "2026-07-05T00:00:00Z"
        }
        WHERE {
            ?link (?s, ?p, ?o)
            FILTER(?p != "belongs_to_domain")
            FILTER(?link.metadata.confidence > 0.3 && ?link.metadata.confidence < 1.0)
        }
        LIMIT 500
        "#;
    let result = nexus
        .execute_kml(parse_kml(update).unwrap(), false)
        .await
        .unwrap();
    // Exactly the two `treats` links carry confidence 0.95; the bootstrap
    // `belongs_to_domain` links (confidence 1.0) are spared twice over.
    assert_eq!(result["updated"], json!(2));
    assert_eq!(result["matched"], json!(2));

    let (confidences, _) = nexus
        .execute_kql(
            parse_kql(
                r#"FIND(?link.metadata.confidence)
                   WHERE { ?link (?s, "treats", ?o) }"#,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    for value in confidences.as_array().unwrap() {
        let v = value.as_f64().unwrap();
        assert!((v - 0.855).abs() < 1e-9, "confidence not decayed: {v}");
    }
}

/// MERGE provenance survives chained merges (source `_merged_from` entries
/// carry over, deduplicated) and a replayed merge self-diagnoses via the
/// target's `_merged_from` (KIP §4.4).
#[tokio::test]
async fn test_kml_merge_chained_provenance_and_replay_hint() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let setup = r#"
        UPSERT {
            CONCEPT ?cat_type { {type: "$ConceptType", name: "Category"} }
            CONCEPT ?a { {type: "Category", name: "CatA"} }
            CONCEPT ?b { {type: "Category", name: "CatB"} }
            CONCEPT ?c { {type: "Category", name: "CatC"} }
        }
        "#;
    nexus
        .execute_kml(parse_kml(setup).unwrap(), false)
        .await
        .unwrap();

    let merge_a_into_b = r#"
        MERGE CONCEPT ?dup INTO ?canonical
        WHERE {
            ?dup {type: "Category", name: "CatA"}
            ?canonical {type: "Category", name: "CatB"}
        }
        "#;
    nexus
        .execute_kml(parse_kml(merge_a_into_b).unwrap(), false)
        .await
        .unwrap();

    let merge_b_into_c = r#"
        MERGE CONCEPT ?dup INTO ?canonical
        WHERE {
            ?dup {type: "Category", name: "CatB"}
            ?canonical {type: "Category", name: "CatC"}
        }
        "#;
    nexus
        .execute_kml(parse_kml(merge_b_into_c).unwrap(), false)
        .await
        .unwrap();

    let cat_c = nexus
        .get_concept(&ConceptPK::Object {
            r#type: "Category".to_string(),
            name: "CatC".to_string(),
        })
        .await
        .unwrap();
    // Chained provenance: CatA's trail rode along when CatB merged in.
    assert_eq!(
        cat_c.metadata["_merged_from"],
        json!(["Category:CatA", "Category:CatB"])
    );

    // Replaying the second merge self-diagnoses as "already merged".
    let err = nexus
        .execute_kml(parse_kml(merge_b_into_c).unwrap(), false)
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::NotFound));
    assert!(
        err.message.contains("already"),
        "hint missing from: {}",
        err.message
    );
}

/// DELETE PROPOSITIONS cascades to higher-order propositions referencing the
/// deleted links, leaving no dangling references (same guarantee as
/// DELETE CONCEPT ... DETACH).
#[tokio::test]
async fn test_kml_delete_propositions_cascades_higher_order() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let setup = r#"
        UPSERT {
            CONCEPT ?stated { {type: "$PropositionType", name: "stated"} }
            CONCEPT ?alice { {type: "Person", name: "Alice"} }
            PROPOSITION ?statement {
                (
                    {type: "Person", name: "Alice"},
                    "stated",
                    ({type: "Drug", name: "Aspirin"}, "treats", {type: "Symptom", name: "Headache"})
                )
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(setup).unwrap(), false)
        .await
        .unwrap();

    let delete = r#"
        DELETE PROPOSITIONS ?link
        WHERE {
            ?link (?s, "treats", {type: "Symptom", name: "Headache"})
        }
        "#;
    let result = nexus
        .execute_kml(parse_kml(delete).unwrap(), false)
        .await
        .unwrap();
    // The treats link plus the higher-order stated link.
    assert_eq!(result["deleted_propositions"], json!(2));

    let (stated, _) = nexus
        .execute_kql(parse_kql(r#"FIND(?st) WHERE { ?st (?who, "stated", ?what) }"#).unwrap())
        .await
        .unwrap();
    assert_eq!(stated, json!([]));
}

/// EXPORT paginates with LIMIT + CURSOR (KIP §5.3): each page is an
/// independently valid capsule and the cursor resumes deterministically.
#[tokio::test]
async fn test_meta_export_pagination() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let page1_cmd = r#"EXPORT ?n WHERE { ?n {type: "Symptom"} } LIMIT 1"#;
    let (page1, cursor) = nexus
        .execute_meta(parse_meta(page1_cmd).unwrap())
        .await
        .unwrap();
    assert_eq!(page1["concepts"], json!(1));
    let cursor = cursor.expect("first page must return a cursor");

    let page2_cmd =
        format!(r#"EXPORT ?n WHERE {{ ?n {{type: "Symptom"}} }} LIMIT 1 CURSOR "{cursor}""#);
    let (page2, cursor2) = nexus
        .execute_meta(parse_meta(&page2_cmd).unwrap())
        .await
        .unwrap();
    assert_eq!(page2["concepts"], json!(1));
    assert_ne!(page1["capsule"], page2["capsule"]);
    assert!(cursor2.is_none(), "two symptoms fit in two pages");

    // Both pages import cleanly into a fresh nexus (given the schema).
    let second = setup_test_db(async |_| Ok(())).await.unwrap();
    second
        .execute_kml(
            parse_kml(r#"UPSERT { CONCEPT ?t { {type: "$ConceptType", name: "Symptom"} } }"#)
                .unwrap(),
            false,
        )
        .await
        .unwrap();
    for page in [&page1, &page2] {
        let capsule = page["capsule"].as_str().unwrap();
        second
            .execute_kml(parse_kml(capsule).unwrap(), false)
            .await
            .unwrap();
    }
    let (symptoms, _) = second
        .execute_kql(parse_kql(r#"FIND(?s.name) WHERE { ?s {type: "Symptom"} }"#).unwrap())
        .await
        .unwrap();
    assert_eq!(symptoms.as_array().unwrap().len(), 2);
}

/// ORDER BY sorts null (missing) keys last regardless of direction
/// (KIP §3.5).
#[tokio::test]
async fn test_kql_order_by_nulls_last() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let setup = r#"
        UPSERT {
            CONCEPT ?unscored {
                {type: "Drug", name: "UnscoredDrug"}
            }
            CONCEPT ?strong {
                {type: "Drug", name: "StrongDrug"}
                SET ATTRIBUTES { "risk_level": 5 }
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(setup).unwrap(), false)
        .await
        .unwrap();

    for direction in ["ASC", "DESC"] {
        let kql = format!(
            r#"FIND(?drug.name)
               WHERE {{ ?drug {{type: "Drug"}} }}
               ORDER BY ?drug.attributes.risk_level {direction}"#
        );
        let (result, _) = nexus.execute_kql(parse_kql(&kql).unwrap()).await.unwrap();
        let names = result.as_array().unwrap();
        assert_eq!(
            names.last(),
            Some(&json!("UnscoredDrug")),
            "null must sort last with {direction}: {names:?}"
        );
    }
}

/// Seeds three drugs with distinct risk levels for cross-variable FILTER and
/// cartesian FIND tests.
async fn setup_risk_ladder(nexus: &CognitiveNexus) {
    let kml = r#"
        UPSERT {
            CONCEPT ?high {
                {type: "Drug", name: "HighRisk"}
                SET ATTRIBUTES { "risk_level": 5 }
            }
            CONCEPT ?mid {
                {type: "Drug", name: "MidRisk"}
                SET ATTRIBUTES { "risk_level": 3 }
            }
            CONCEPT ?low {
                {type: "Drug", name: "LowRisk"}
                SET ATTRIBUTES { "risk_level": 1 }
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(kml).unwrap(), false)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_kql_filter_cross_variable_join_pairs() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();
    setup_risk_ladder(&nexus).await;

    // Two disconnected variables compared per solution (KIP §3.4.3): only
    // combinations with d1.risk > d2.risk survive, and FIND must project the
    // exact satisfying pairs, index-aligned (KIP §6.2.2).
    // Risks: Aspirin 2, HighRisk 5, MidRisk 3, LowRisk 1.
    let kql = r#"
        FIND(?d1.name, ?d2.name)
        WHERE {
            ?d1 {type: "Drug"}
            ?d2 {type: "Drug"}
            FILTER(?d1.attributes.risk_level > ?d2.attributes.risk_level)
        }
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    let cols = result.as_array().unwrap();
    assert_eq!(cols.len(), 2);
    let c1 = cols[0].as_array().unwrap();
    let c2 = cols[1].as_array().unwrap();
    assert_eq!(
        c1.len(),
        c2.len(),
        "columns must be index-aligned: {result}"
    );

    let mut pairs: Vec<(String, String)> = c1
        .iter()
        .zip(c2.iter())
        .map(|(a, b)| {
            (
                a.as_str().unwrap().to_string(),
                b.as_str().unwrap().to_string(),
            )
        })
        .collect();
    pairs.sort();
    let mut expected = vec![
        ("Aspirin".to_string(), "LowRisk".to_string()), // 2 > 1
        ("HighRisk".to_string(), "Aspirin".to_string()), // 5 > 2
        ("HighRisk".to_string(), "MidRisk".to_string()), // 5 > 3
        ("HighRisk".to_string(), "LowRisk".to_string()), // 5 > 1
        ("MidRisk".to_string(), "Aspirin".to_string()), // 3 > 2
        ("MidRisk".to_string(), "LowRisk".to_string()), // 3 > 1
    ];
    expected.sort();
    assert_eq!(pairs, expected);
}

#[tokio::test]
async fn test_kql_filter_predicate_variable_narrows_link() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // Add a second predicate so the filter has something to exclude.
    let kml = r#"
        UPSERT {
            CONCEPT ?p { {type: "$PropositionType", name: "alleviates"} }
            CONCEPT ?a {
                {type: "Drug", name: "Aspirin"}
                SET PROPOSITIONS {
                    ("alleviates", {type: "Symptom", name: "Headache"})
                }
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(kml).unwrap(), false)
        .await
        .unwrap();

    // The memory-metabolism idiom: a predicate-variable FILTER must narrow
    // the *link* variable too (the excluded predicate's links disappear from
    // ?link), not just the predicate binding set.
    let kql = r#"
        FIND(?link.predicate)
        WHERE {
            ?link (?s, ?p, ?o)
            FILTER(?p != "treats")
        }
        LIMIT 50
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    let predicates = result.as_array().unwrap();
    assert!(!predicates.is_empty());
    assert!(
        predicates.iter().all(|p| p != "treats"),
        "links with the excluded predicate must be narrowed out: {result}"
    );
    assert!(predicates.contains(&json!("alleviates")));
}

#[tokio::test]
async fn test_kql_find_disconnected_cartesian_alignment() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // Two variables with no connecting proposition: the solution set is the
    // cartesian product, and the columns must stay index-aligned.
    // 2 drugs would require another drug; setup has 1 drug × 2 symptoms.
    let kql = r#"
        FIND(?drug.name, ?symptom.name)
        WHERE {
            ?drug {type: "Drug"}
            ?symptom {type: "Symptom"}
        }
        ORDER BY ?symptom.name ASC
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    let cols = result.as_array().unwrap();
    let c1 = cols[0].as_array().unwrap();
    let c2 = cols[1].as_array().unwrap();
    assert_eq!(c1.len(), 2, "1 drug × 2 symptoms = 2 solutions: {result}");
    assert_eq!(c1.len(), c2.len(), "columns must be index-aligned");
    assert_eq!(c1, &vec![json!("Aspirin"), json!("Aspirin")]);
    assert_eq!(c2, &vec![json!("Fever"), json!("Headache")]);

    // Offset-cursor pagination over the materialized rows.
    let kql = r#"
        FIND(?drug.name, ?symptom.name)
        WHERE {
            ?drug {type: "Drug"}
            ?symptom {type: "Symptom"}
        }
        ORDER BY ?symptom.name ASC
        LIMIT 1
        "#;
    let (page1, cursor) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    assert_eq!(page1.as_array().unwrap()[1], json!(["Fever"]));
    let cursor = cursor.expect("first page must carry next_cursor");

    let kql = format!(
        r#"
        FIND(?drug.name, ?symptom.name)
        WHERE {{
            ?drug {{type: "Drug"}}
            ?symptom {{type: "Symptom"}}
        }}
        ORDER BY ?symptom.name ASC
        LIMIT 1
        CURSOR "{cursor}"
        "#
    );
    let (page2, cursor2) = nexus.execute_kql(parse_kql(&kql).unwrap()).await.unwrap();
    assert_eq!(page2.as_array().unwrap()[1], json!(["Headache"]));
    assert!(cursor2.is_none(), "no further pages expected");
}

#[tokio::test]
async fn test_kql_find_relation_with_loose_variable_alignment() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // A relation-connected pair (drug treats symptom) crossed with a loose
    // variable (?tag): rows = relation rows × tag bindings, all aligned.
    let kml = r#"
        UPSERT {
            CONCEPT ?tag_type { {type: "$ConceptType", name: "Tag"} }
            CONCEPT ?t1 { {type: "Tag", name: "Verified"} }
        }
        "#;
    nexus
        .execute_kml(parse_kml(kml).unwrap(), false)
        .await
        .unwrap();

    let kql = r#"
        FIND(?drug.name, ?symptom.name, ?tag.name)
        WHERE {
            (?drug, "treats", ?symptom)
            ?tag {type: "Tag"}
        }
        ORDER BY ?symptom.name ASC
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    let cols = result.as_array().unwrap();
    assert_eq!(cols.len(), 3);
    let lens: Vec<usize> = cols.iter().map(|c| c.as_array().unwrap().len()).collect();
    assert_eq!(lens, vec![2, 2, 2], "2 treats-rows × 1 tag: {result}");
    assert_eq!(cols[1], json!(["Fever", "Headache"]));
    assert_eq!(cols[2], json!(["Verified", "Verified"]));
}

#[tokio::test]
async fn test_kql_filter_constant_expression() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // A constant-false FILTER discards every solution (and must not hang —
    // the previous consume-based evaluator looped forever on it).
    let kql = r#"
        FIND(?drug.name)
        WHERE {
            ?drug {type: "Drug"}
            FILTER("a" == "b")
        }
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    assert_eq!(result, json!([]));

    // A constant-true FILTER keeps everything.
    let kql = r#"
        FIND(?drug.name)
        WHERE {
            ?drug {type: "Drug"}
            FILTER(1 < 2)
        }
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    assert_eq!(result, json!(["Aspirin"]));
}

#[tokio::test]
async fn test_kql_filter_cross_variable_inside_not() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();
    setup_risk_ladder(&nexus).await;

    // Cross-variable FILTER inside NOT (runs in the lightweight child
    // context): exclude drugs that are riskier than some other drug —
    // only the minimum-risk drug survives.
    let kql = r#"
        FIND(?d1.name)
        WHERE {
            ?d1 {type: "Drug"}
            NOT {
                ?d2 {type: "Drug"}
                FILTER(?d1.attributes.risk_level > ?d2.attributes.risk_level)
            }
        }
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    assert_eq!(
        result,
        json!(["LowRisk"]),
        "only the least risky drug survives"
    );
}

/// Multi-hop relation rows carry no proposition id, so row-based FIND
/// pagination must use offset cursors: an entity-anchored cursor could
/// neither be issued for nor resume after such a row, silently truncating
/// the result at the first page boundary.
#[tokio::test]
async fn test_kql_multi_hop_find_pagination() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let setup = r#"
        UPSERT {
            CONCEPT ?cat_type { {type: "$ConceptType", name: "Category"} }
            CONCEPT ?isa { {type: "$PropositionType", name: "is_subclass_of"} }
            CONCEPT ?a { {type: "Category", name: "CatA"} }
            CONCEPT ?b {
                {type: "Category", name: "CatB"}
                SET PROPOSITIONS { ("is_subclass_of", ?a) }
            }
            CONCEPT ?c {
                {type: "Category", name: "CatC"}
                SET PROPOSITIONS { ("is_subclass_of", ?b) }
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(setup).unwrap(), false)
        .await
        .unwrap();

    // 1..=3 hops over the CatC -> CatB -> CatA chain yields three
    // (?concept, ?parent) solutions: (B,A), (C,B), (C,A).
    let query = |cursor: Option<&str>| {
        let cursor_clause = cursor
            .map(|cursor| format!("CURSOR \"{cursor}\""))
            .unwrap_or_default();
        format!(
            r#"
            FIND(?concept.name, ?parent.name)
            WHERE {{
                ?concept {{type: "Category"}}
                (?concept, "is_subclass_of"{{1,3}}, ?parent)
            }}
            LIMIT 2
            {cursor_clause}
            "#
        )
    };

    let (page1, cursor) = nexus
        .execute_kql(parse_kql(&query(None)).unwrap())
        .await
        .unwrap();
    let cols = page1.as_array().unwrap();
    assert_eq!(cols[0].as_array().unwrap().len(), 2, "page 1: {page1}");
    let cursor = cursor.expect("truncated multi-hop page must carry next_cursor");

    let (page2, cursor2) = nexus
        .execute_kql(parse_kql(&query(Some(&cursor))).unwrap())
        .await
        .unwrap();
    let cols2 = page2.as_array().unwrap();
    assert_eq!(cols2[0].as_array().unwrap().len(), 1, "page 2: {page2}");
    assert!(cursor2.is_none(), "no further pages expected");

    // The two pages together cover exactly the three distinct solutions.
    let mut pairs: Vec<(String, String)> = Vec::new();
    for (concepts, parents) in [(&cols[0], &cols[1]), (&cols2[0], &cols2[1])] {
        let concepts = concepts.as_array().unwrap();
        let parents = parents.as_array().unwrap();
        assert_eq!(concepts.len(), parents.len(), "columns must stay aligned");
        for (concept, parent) in concepts.iter().zip(parents) {
            pairs.push((
                concept.as_str().unwrap().to_string(),
                parent.as_str().unwrap().to_string(),
            ));
        }
    }
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("CatB".to_string(), "CatA".to_string()),
            ("CatC".to_string(), "CatA".to_string()),
            ("CatC".to_string(), "CatB".to_string()),
        ],
        "pages must partition the solution set without loss or duplication"
    );

    // A cursor that is not a plain decimal offset is rejected, not treated
    // as page one again.
    let err = nexus
        .execute_kql(parse_kql(&query(Some("bogus"))).unwrap())
        .await
        .unwrap_err();
    assert!(
        matches!(err.code, KipErrorCode::InvalidSyntax),
        "bogus cursor: {err:?}"
    );
}

#[tokio::test]
async fn test_kql_grouped_find_count_respects_filter_and_not() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // Headache: Aspirin(2), Ibuprofen(3), Paracetamol(1); Fever: Aspirin(2),
    // Paracetamol(1).
    let kml = r#"
        UPSERT {
            CONCEPT ?ibuprofen {
                {type: "Drug", name: "Ibuprofen"}
                SET ATTRIBUTES { "risk_level": 3 }
                SET PROPOSITIONS { ("treats", {type: "Symptom", name: "Headache"}) }
            }
            CONCEPT ?paracetamol {
                {type: "Drug", name: "Paracetamol"}
                SET ATTRIBUTES { "risk_level": 1 }
                SET PROPOSITIONS {
                    ("treats", {type: "Symptom", name: "Headache"})
                    ("treats", {type: "Symptom", name: "Fever"})
                }
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(kml).unwrap(), false)
        .await
        .unwrap();

    // FILTER narrows the member variable after the groups were built: the
    // grouped COUNT must only count surviving members (risk_level >= 2 keeps
    // Aspirin and Ibuprofen).
    let kql = r#"
        FIND(?symptom.name, COUNT(?drug))
        WHERE {
            ?symptom {type: "Symptom"}
            (?drug, "treats", ?symptom)
            FILTER(?drug.attributes.risk_level >= 2)
        }
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    let arr = result.as_array().unwrap();
    let names = arr[0].as_array().unwrap();
    let counts = arr[1].as_array().unwrap();
    assert_eq!(names.len(), counts.len());
    for (i, name) in names.iter().enumerate() {
        match name.as_str().unwrap() {
            "Headache" => assert_eq!(counts[i], json!(2), "filtered grouped count: {result}"),
            "Fever" => assert_eq!(counts[i], json!(1), "filtered grouped count: {result}"),
            other => panic!("Unexpected symptom: {other}"),
        }
    }

    // NOT excludes members the same way: drugs that treat Fever are removed
    // from ?drug entirely, so Headache only counts Ibuprofen.
    let kql = r#"
        FIND(?symptom.name, COUNT(?drug))
        WHERE {
            ?symptom {type: "Symptom"}
            (?drug, "treats", ?symptom)
            NOT { (?drug, "treats", {type: "Symptom", name: "Fever"}) }
        }
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    let arr = result.as_array().unwrap();
    let names = arr[0].as_array().unwrap();
    let counts = arr[1].as_array().unwrap();
    for (i, name) in names.iter().enumerate() {
        match name.as_str().unwrap() {
            "Headache" => assert_eq!(counts[i], json!(1), "NOT-narrowed grouped count: {result}"),
            "Fever" => assert_eq!(counts[i], json!(0), "NOT-narrowed grouped count: {result}"),
            other => panic!("Unexpected symptom: {other}"),
        }
    }
}

/// Seeds two relation predicates over shared endpoints for join/union tests:
/// p1: (A1,B1), (A2,B2); p2: (A1,B2), (A2,B2).
async fn setup_pair_graph(nexus: &CognitiveNexus) {
    let kml = r#"
        UPSERT {
            CONCEPT ?t { {type: "$ConceptType", name: "PairNode"} }
            CONCEPT ?p1 { {type: "$PropositionType", name: "p1"} }
            CONCEPT ?p2 { {type: "$PropositionType", name: "p2"} }
            CONCEPT ?a1 { {type: "PairNode", name: "A1"} }
            CONCEPT ?a2 { {type: "PairNode", name: "A2"} }
            CONCEPT ?b1 { {type: "PairNode", name: "B1"} }
            CONCEPT ?b2 { {type: "PairNode", name: "B2"} }
            PROPOSITION ?l1 { (?a1, "p1", ?b1) }
            PROPOSITION ?l2 { (?a2, "p1", ?b2) }
            PROPOSITION ?l3 { (?a1, "p2", ?b2) }
            PROPOSITION ?l4 { (?a2, "p2", ?b2) }
        }
        "#;
    nexus
        .execute_kml(parse_kml(kml).unwrap(), false)
        .await
        .unwrap();
}

fn collect_pairs(result: &Json) -> Vec<(String, String)> {
    let cols = result.as_array().unwrap();
    assert_eq!(cols.len(), 2, "expected two columns: {result}");
    let c1 = cols[0].as_array().unwrap();
    let c2 = cols[1].as_array().unwrap();
    assert_eq!(
        c1.len(),
        c2.len(),
        "columns must be index-aligned: {result}"
    );
    let mut pairs: Vec<(String, String)> = c1
        .iter()
        .zip(c2.iter())
        .map(|(a, b)| {
            (
                a.as_str().unwrap_or("null").to_string(),
                b.as_str().unwrap_or("null").to_string(),
            )
        })
        .collect();
    pairs.sort();
    pairs
}

#[tokio::test]
async fn test_kql_union_multi_var_row_union() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_pair_graph(&nexus).await;

    // Row-wise union (KIP §3.4.7.3): both branches bind the same (?a, ?b)
    // pair; the result must contain every branch's rows, deduplicated.
    let kql = r#"
        FIND(?a.name, ?b.name)
        WHERE {
            (?a, "p1", ?b)
            UNION { (?a, "p2", ?b) }
        }
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    assert_eq!(
        collect_pairs(&result),
        vec![
            ("A1".to_string(), "B1".to_string()), // p1
            ("A1".to_string(), "B2".to_string()), // p2
            ("A2".to_string(), "B2".to_string()), // p1 and p2, deduplicated
        ],
        "row-wise union must keep both branches' solutions: {result}"
    );
}

/// P1-01: a concept-only UNION branch is still a solution branch.  It must
/// contribute one row with the variables absent from that branch padded with
/// null instead of being collapsed into the global binding columns.
#[tokio::test]
async fn test_kql_union_concept_branch_preserves_solution_row() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_pair_graph(&nexus).await;

    let kql = r#"
        FIND(?a.name, ?b.name)
        WHERE {
            (?a, "p1", ?b)
            UNION { ?a {type: "PairNode", name: "A1"} }
        }
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    assert_eq!(
        result,
        json!([["A1", "A2", "A1"], ["B1", "B2", null]]),
        "concept UNION branch must remain an independent padded row: {result}"
    );
}

/// P1-01: mandatory patterns form one conjunctive main branch; a UNION
/// relation is an independent disjunct and must not be irreversibly appended
/// to one of the mandatory pattern relations.
#[tokio::test]
async fn test_kql_union_survives_empty_multi_pattern_main_branch() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_pair_graph(&nexus).await;

    let kql = r#"
        FIND(?a.name, ?b.name)
        WHERE {
            (?a, "p1", ?b)
            (?b, "p1", ?a)
            UNION { (?a, "p2", ?b) }
        }
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    assert_eq!(
        collect_pairs(&result),
        vec![
            ("A1".to_string(), "B2".to_string()),
            ("A2".to_string(), "B2".to_string()),
        ],
        "UNION rows must survive an empty conjunctive main branch: {result}"
    );
}

/// P1-02: NOT removes matching solution rows from the UNION branch only.
/// Shared bindings and grouped pairs that are still used by the mandatory
/// branch must remain live.
#[tokio::test]
async fn test_kql_not_anti_join_preserves_other_union_branch_group_count() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_pair_graph(&nexus).await;

    let kql = r#"
        FIND(?b.name, COUNT(?a))
        WHERE {
            (?a, "p1", ?b)
            UNION { ?link (?a, "p2", ?b) }
            NOT { ?link (?a, "p2", ?b) }
        }
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    assert_eq!(
        result,
        json!([["B1", "B2"], [1, 1]]),
        "NOT must not erase bindings/groups still used by the p1 branch: {result}"
    );
}

#[tokio::test]
async fn test_kql_sequential_patterns_pair_join() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_pair_graph(&nexus).await;

    // Sequential patterns are conjunctive per solution: (?a, ?b) must
    // satisfy both p1 and p2 as a pair. Endpoint-set approximation would
    // also produce (A1, B2) (A1 ∈ p1 subjects, B2 ∈ p1 objects, (A1,B2) ∈ p2)
    // — a false pair, since (A1,B2) does not satisfy p1.
    let kql = r#"
        FIND(?a.name, ?b.name)
        WHERE {
            (?a, "p1", ?b)
            (?a, "p2", ?b)
        }
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    assert_eq!(
        collect_pairs(&result),
        vec![("A2".to_string(), "B2".to_string())],
        "pair-wise join must not produce endpoint-approximate rows: {result}"
    );
}

#[tokio::test]
async fn test_kql_cross_variable_not_keeps_valid_solutions() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_pair_graph(&nexus).await;

    // Outer solutions via p1: (A1,B1), (A2,B2). NOT { (?a, "p2", ?b) }
    // matches (A1,B2) and (A2,B2): only the (A2,B2) *solution* is excluded.
    // Column-level subtraction would also kill (A1,B1) via B2's column, or
    // A1 via the (A1,B2) cross pair.
    let kql = r#"
        FIND(?a.name, ?b.name)
        WHERE {
            (?a, "p1", ?b)
            NOT { (?a, "p2", ?b) }
        }
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    assert_eq!(
        collect_pairs(&result),
        vec![("A1".to_string(), "B1".to_string())],
        "NOT must exclude solutions, not binding columns: {result}"
    );
}

#[tokio::test]
async fn test_kql_nested_literal_predicate_requires_proposition_endpoint() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_pair_graph(&nexus).await;

    // confirmed_by links: one whose subject is a proposition (the (A1,p1,B1)
    // link) and one whose subject is a plain concept (A2).
    let kml = r#"
        UPSERT {
            CONCEPT ?cb { {type: "$PropositionType", name: "confirmed_by"} }
            CONCEPT ?w1 { {type: "PairNode", name: "WitnessLink"} }
            CONCEPT ?w2 { {type: "PairNode", name: "WitnessConcept"} }
            PROPOSITION ?c1 {
                (({type: "PairNode", name: "A1"}, "p1", {type: "PairNode", name: "B1"}), "confirmed_by", ?w1)
            }
            PROPOSITION ?c2 { ({type: "PairNode", name: "A2"}, "confirmed_by", ?w2) }
        }
        "#;
    nexus
        .execute_kml(parse_kml(kml).unwrap(), false)
        .await
        .unwrap();

    // The nested subject pattern requires the endpoint to be a proposition
    // link: the concept-subject confirmed_by row must not pollute ?x.
    let kql = r#"
        FIND(?x.name)
        WHERE {
            ((?s, ?p, ?o), "confirmed_by", ?x)
        }
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    assert_eq!(
        result,
        json!(["WitnessLink"]),
        "concept-subject rows must be filtered out: {result}"
    );
}

#[tokio::test]
async fn test_kml_set_propositions_self_loop_preflight() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let version_of = async |nexus: &CognitiveNexus| -> u64 {
        let concept = nexus
            .get_concept(&ConceptPK::Object {
                r#type: "Drug".to_string(),
                name: "Aspirin".to_string(),
            })
            .await
            .unwrap();
        system_metadata_version(&concept.metadata)
    };
    let before = version_of(&nexus).await;

    // A SET PROPOSITIONS target equal to the (pre-existing) subject is a
    // self-loop: rejected by the preflight in both dry-run and real mode,
    // with no partial write (the concept block itself must not be applied).
    let kml = r#"
        UPSERT {
            CONCEPT ?x {
                {type: "Drug", name: "Aspirin"}
                SET ATTRIBUTES { "poisoned": true }
                SET PROPOSITIONS { ("treats", {type: "Drug", name: "Aspirin"}) }
            }
        }
        "#;
    for dry_run in [true, false] {
        let err = nexus
            .execute_kml(parse_kml(kml).unwrap(), dry_run)
            .await
            .unwrap_err();
        assert!(
            matches!(err.code, KipErrorCode::InvalidSyntax),
            "dry_run={dry_run}: {err:?}"
        );
        assert!(err.message.contains("self-loop"), "{err:?}");
    }

    let concept = nexus
        .get_concept(&ConceptPK::Object {
            r#type: "Drug".to_string(),
            name: "Aspirin".to_string(),
        })
        .await
        .unwrap();
    assert_eq!(
        system_metadata_version(&concept.metadata),
        before,
        "failed statement must not bump the concept version"
    );
    assert!(
        !concept.attributes.contains_key("poisoned"),
        "failed statement must not leave partial attribute writes"
    );
}

#[tokio::test]
async fn test_kql_union_pagination_no_missing_pages() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();

    // TypeA concepts are created first (smaller ids), TypeB after (larger
    // ids). The main pattern binds TypeB, then UNION appends the smaller
    // TypeA ids at the tail of the binding list.
    let kml = r#"
        UPSERT {
            CONCEPT ?ta { {type: "$ConceptType", name: "TypeA"} }
            CONCEPT ?tb { {type: "$ConceptType", name: "TypeB"} }
            CONCEPT ?a1 { {type: "TypeA", name: "a1"} }
            CONCEPT ?a2 { {type: "TypeA", name: "a2"} }
            CONCEPT ?b1 { {type: "TypeB", name: "b1"} }
            CONCEPT ?b2 { {type: "TypeB", name: "b2"} }
        }
        "#;
    nexus
        .execute_kml(parse_kml(kml).unwrap(), false)
        .await
        .unwrap();

    let query = |cursor: Option<&str>| {
        let cursor_clause = cursor
            .map(|c| format!("CURSOR \"{c}\""))
            .unwrap_or_default();
        format!(
            r#"
            FIND(?c.name)
            WHERE {{
                ?c {{type: "TypeB"}}
                UNION {{ ?c {{type: "TypeA"}} }}
            }}
            LIMIT 3
            {cursor_clause}
            "#
        )
    };

    let mut names: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..4 {
        let (result, next) = nexus
            .execute_kql(parse_kql(&query(cursor.as_deref())).unwrap())
            .await
            .unwrap();
        names.extend(
            result
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap().to_string()),
        );
        cursor = next;
        if cursor.is_none() {
            break;
        }
    }
    names.sort();
    assert_eq!(
        names,
        vec!["a1", "a2", "b1", "b2"],
        "pages must cover every binding exactly once regardless of branch id order"
    );
}

#[tokio::test]
async fn test_kql_union_disjoint_vars_null_padding() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let kml = r#"
        UPSERT {
            CONCEPT ?pt { {type: "$ConceptType", name: "Product"} }
            CONCEPT ?mb { {type: "$PropositionType", name: "manufactured_by"} }
            CONCEPT ?mk { {type: "$ConceptType", name: "Maker"} }
            CONCEPT ?bayer { {type: "Maker", name: "Bayer"} }
            CONCEPT ?prod {
                {type: "Product", name: "AspirinPlus"}
                SET PROPOSITIONS { ("manufactured_by", {type: "Maker", name: "Bayer"}) }
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(kml).unwrap(), false)
        .await
        .unwrap();

    // Spec §3.4.7.3 Execution Flow Example 1: disjoint variables across the
    // main block and the UNION branch produce a row-wise union with `null`
    // padding, not a cross product.
    let kql = r#"
        FIND(?drug.name, ?product.name)
        WHERE {
            ?drug {type: "Drug"}
            (?drug, "treats", {type: "Symptom", name: "Headache"})
            UNION {
                ?product {type: "Product"}
                (?product, "manufactured_by", {type: "Maker", name: "Bayer"})
            }
        }
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    assert_eq!(
        collect_pairs(&result),
        vec![
            ("Aspirin".to_string(), "null".to_string()),
            ("null".to_string(), "AspirinPlus".to_string()),
        ],
        "disjoint UNION branches must union rows with null padding: {result}"
    );
}

#[tokio::test]
async fn test_entity_id_colon_predicate_roundtrip() {
    // Unit-level: `P:<id>:<predicate>` round-trips when the predicate itself
    // contains ':'.
    let id = EntityID::Proposition(9, "a:b".to_string());
    assert_eq!(id.to_string(), "P:9:a:b");
    assert_eq!(EntityID::from_str("P:9:a:b").unwrap(), id);
    assert!(EntityID::from_str("P:9:").is_err());

    // Integration: a predicate named with ':' can be created, and the link
    // id the engine returns can be matched back via `(id: "...")`.
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    let kml = r#"
        UPSERT {
            CONCEPT ?t { {type: "$ConceptType", name: "ColonNode"} }
            CONCEPT ?p { {type: "$PropositionType", name: "rel:of"} }
            CONCEPT ?a { {type: "ColonNode", name: "A"} }
            CONCEPT ?b { {type: "ColonNode", name: "B"} }
            PROPOSITION ?l { (?a, "rel:of", ?b) }
        }
        "#;
    let result = nexus
        .execute_kml(parse_kml(kml).unwrap(), false)
        .await
        .unwrap();
    let link_id = result["upsert_proposition_links"][0].as_str().unwrap();
    assert!(link_id.contains("rel:of"), "{link_id}");

    let kql = format!(
        r#"
        FIND(?link.predicate)
        WHERE {{
            ?link (id: "{link_id}")
        }}
        "#
    );
    let (result, _) = nexus.execute_kql(parse_kql(&kql).unwrap()).await.unwrap();
    assert_eq!(result, json!(["rel:of"]));
}

#[tokio::test]
async fn test_kql_dangling_id_matchers_return_kip_3002() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // Match-only `{id:}` with a dangling concept id: KIP_3002 (spec RC8),
    // not a silent empty result.
    let err = nexus
        .execute_kql(parse_kql(r#"FIND(?c.name) WHERE { ?c {id: "C:999999"} }"#).unwrap())
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::NotFound), "{err:?}");

    // Match-only `(id:)` with a dangling link id: KIP_3002.
    let err = nexus
        .execute_kql(parse_kql(r#"FIND(?l.predicate) WHERE { ?l (id: "P:999999:none") }"#).unwrap())
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::NotFound), "{err:?}");
}

#[tokio::test]
async fn test_kql_grouped_find_order_by_group_var() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // Group rows must actually be sorted by the group variable's field —
    // previously ORDER BY ?symptom.name was silently ignored.
    let kql = r#"
        FIND(?symptom.name, COUNT(?drug))
        WHERE {
            ?symptom {type: "Symptom"}
            (?drug, "treats", ?symptom)
        }
        ORDER BY ?symptom.name DESC
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    let arr = result.as_array().unwrap();
    assert_eq!(
        arr[0],
        json!(["Headache", "Fever"]),
        "groups must sort by ?symptom.name DESC: {result}"
    );

    let kql_asc = r#"
        FIND(?symptom.name, COUNT(?drug))
        WHERE {
            ?symptom {type: "Symptom"}
            (?drug, "treats", ?symptom)
        }
        ORDER BY ?symptom.name ASC
        "#;
    let (result, _) = nexus
        .execute_kql(parse_kql(kql_asc).unwrap())
        .await
        .unwrap();
    let arr = result.as_array().unwrap();
    assert_eq!(arr[0], json!(["Fever", "Headache"]), "{result}");
}

#[tokio::test]
async fn test_kql_invalid_cursor_rejected_on_legacy_path() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // Single-variable concept projection goes through the legacy path; an
    // unparseable cursor must be rejected (KIP_1001), not silently replayed
    // from the start with duplicate pages.
    let err = nexus
        .execute_kql(
            parse_kql(
                r#"
                FIND(?c.name)
                WHERE { ?c {type: "Drug"} }
                LIMIT 1
                CURSOR "!!!not-a-cursor!!!"
                "#,
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(err.code, KipErrorCode::InvalidSyntax), "{err:?}");
}

#[tokio::test]
async fn test_kql_self_loop_pattern_yields_empty() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // `(?x, "treats", ?x)` is an equality constraint on both endpoints; the
    // engine stores no self-loop links, so the solution set is empty —
    // previously ?x silently bound every `treats` object.
    let kql = r#"
        FIND(?x.name)
        WHERE { (?x, "treats", ?x) }
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    assert_eq!(result, json!([]), "{result}");
}

#[tokio::test]
async fn test_kql_unaligned_filter_projection_rejected() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();
    setup_risk_ladder(&nexus).await;

    // A cross-variable FILTER over three entity variables cannot record its
    // satisfying combinations; projecting those variables together must be
    // rejected (KIP_4002) instead of silently re-materializing a misaligned
    // cross product.
    let kql = r#"
        FIND(?d1.name, ?d2.name, ?d3.name)
        WHERE {
            ?d1 {type: "Drug"}
            ?d2 {type: "Drug"}
            ?d3 {type: "Drug"}
            FILTER((?d1.attributes.risk_level > ?d2.attributes.risk_level) && (?d2.attributes.risk_level > ?d3.attributes.risk_level))
        }
        "#;
    let err = nexus
        .execute_kql(parse_kql(kql).unwrap())
        .await
        .unwrap_err();
    assert!(
        matches!(err.code, KipErrorCode::ResourceExhausted),
        "{err:?}"
    );

    // Single-column projection over the same narrowing stays valid
    // (existential semantics).
    let kql_single = r#"
        FIND(?d2.name)
        WHERE {
            ?d1 {type: "Drug"}
            ?d2 {type: "Drug"}
            ?d3 {type: "Drug"}
            FILTER((?d1.attributes.risk_level > ?d2.attributes.risk_level) && (?d2.attributes.risk_level > ?d3.attributes.risk_level))
        }
        "#;
    let (result, _) = nexus
        .execute_kql(parse_kql(kql_single).unwrap())
        .await
        .unwrap();
    let names: Vec<&str> = result
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(!names.is_empty(), "{result}");
    assert!(
        !names.contains(&"HighRisk") || names.contains(&"MidRisk"),
        "{result}"
    );
}

#[tokio::test]
async fn test_meta_search_uppercase_attribute_text() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    // The per-predicate re-check lowercases the source texts; an all-caps
    // attribute value must still match its lowercased BM25 token.
    let kml = r#"
        UPSERT {
            PROPOSITION ?l {
                ({type: "Drug", name: "Aspirin"}, "treats", {type: "Symptom", name: "Headache"})
                SET ATTRIBUTES { "note": "SHOUTED UNIQUEMARKER TEXT" }
            }
        }
        "#;
    nexus
        .execute_kml(parse_kml(kml).unwrap(), false)
        .await
        .unwrap();

    let (result, _) = nexus
        .execute_meta(parse_meta(r#"SEARCH PROPOSITION "uniquemarker""#).unwrap())
        .await
        .unwrap();
    let hits = result.as_array().unwrap();
    assert!(
        hits.iter()
            .any(|hit| hit["attributes"]["note"] == json!("SHOUTED UNIQUEMARKER TEXT")),
        "all-caps attribute text must survive the re-check: {result}"
    );
}

#[tokio::test]
async fn test_get_or_init_concept_sets_system_metadata() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_test_data(&nexus).await.unwrap();

    let created = nexus
        .get_or_init_concept(
            "Drug".to_string(),
            "FreshDrug".to_string(),
            Map::new(),
            Map::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        created.metadata.get(METADATA_VERSION),
        Some(&json!(1)),
        "fresh concepts must carry engine-maintained _version"
    );
    assert!(
        created.metadata.contains_key(METADATA_UPDATED_AT),
        "fresh concepts must carry engine-maintained _updated_at"
    );

    // Idempotent: the second call returns the same row.
    let again = nexus
        .get_or_init_concept(
            "Drug".to_string(),
            "FreshDrug".to_string(),
            Map::new(),
            Map::new(),
        )
        .await
        .unwrap();
    assert_eq!(again._id, created._id);
}

/// #5: the per-solution NOT anti-join and the `ctx.groups` cleanup must
/// agree — a group whose members were only *partially* excluded must keep
/// its surviving members in the grouped COUNT instead of being deleted
/// wholesale (which returned COUNT = 0).
#[tokio::test]
async fn test_kql_grouped_count_not_anti_join_keeps_surviving_members() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();

    // belongs: (N1,D1), (N2,D1), (N1,D2); excl: (N1,D1).
    let kml = r#"
        UPSERT {
            CONCEPT ?node_t { {type: "$ConceptType", name: "GNode"} }
            CONCEPT ?domain_t { {type: "$ConceptType", name: "GDomain"} }
            CONCEPT ?belongs { {type: "$PropositionType", name: "belongs"} }
            CONCEPT ?excl { {type: "$PropositionType", name: "excl"} }
            CONCEPT ?n1 { {type: "GNode", name: "N1"} }
            CONCEPT ?n2 { {type: "GNode", name: "N2"} }
            CONCEPT ?d1 { {type: "GDomain", name: "D1"} }
            CONCEPT ?d2 { {type: "GDomain", name: "D2"} }
            PROPOSITION ?b1 { (?n1, "belongs", ?d1) }
            PROPOSITION ?b2 { (?n2, "belongs", ?d1) }
            PROPOSITION ?b3 { (?n1, "belongs", ?d2) }
            PROPOSITION ?e1 { (?n1, "excl", ?d1) }
        }
        "#;
    nexus
        .execute_kml(parse_kml(kml).unwrap(), false)
        .await
        .unwrap();

    // The NOT anti-join removes only the (N1,D1) solution: D1 keeps N2
    // (COUNT 1, not 0), and N1 stays a member of D2 (COUNT 1).
    let kql = r#"
        FIND(?d.name, COUNT(?n))
        WHERE {
            (?n, "belongs", ?d)
            NOT { (?n, "excl", ?d) }
        }
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    let arr = result.as_array().unwrap();
    let names = arr[0].as_array().unwrap();
    let counts = arr[1].as_array().unwrap();
    assert_eq!(names.len(), counts.len(), "columns aligned: {result}");
    assert_eq!(
        names.len(),
        2,
        "both domains keep surviving members: {result}"
    );
    for (i, name) in names.iter().enumerate() {
        match name.as_str().unwrap() {
            "D1" => assert_eq!(counts[i], json!(1), "D1 keeps N2: {result}"),
            "D2" => assert_eq!(counts[i], json!(1), "D2 keeps N1: {result}"),
            other => panic!("Unexpected domain: {other}"),
        }
    }
}

/// #10: a dangling id inside OPTIONAL degrades to "no optional match"
/// (outer solutions kept, block variables project null) instead of failing
/// the whole query with KIP_3002.
#[tokio::test]
async fn test_kql_dangling_id_in_optional_keeps_outer_solution() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_pair_graph(&nexus).await;

    let kql = r#"
        FIND(?a.name)
        WHERE {
            ?a {type: "PairNode", name: "A1"}
            OPTIONAL { (?a, "p1", ?x) ?x {id: "C:999999"} }
        }
        "#;
    let (result, _) = nexus
        .execute_kql(parse_kql(kql).unwrap())
        .await
        .expect("dangling id inside OPTIONAL must not fail the query");
    assert_eq!(result, json!(["A1"]), "outer solution survives");

    // The OPTIONAL variable stays visible and projects null (§3.4.7.2).
    let kql = r#"
        FIND(?a.name, ?x.name)
        WHERE {
            ?a {type: "PairNode", name: "A1"}
            OPTIONAL { (?a, "p1", ?x) ?x {id: "C:999999"} }
        }
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    assert_eq!(
        collect_pairs(&result),
        vec![("A1".to_string(), "null".to_string())],
        "unmatched OPTIONAL projects null: {result}"
    );
}

/// #10: a dangling id inside NOT means the NOT pattern cannot match — the
/// clause succeeds and excludes nothing (§3.4.7.1) instead of failing the
/// query. A resolvable matcher in the same position still excludes.
#[tokio::test]
async fn test_kql_dangling_id_in_not_makes_clause_succeed() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_pair_graph(&nexus).await;

    let kql = r#"
        FIND(?a.name)
        WHERE {
            ?a {type: "PairNode", name: "A1"}
            NOT { (?a, "p1", ?x) ?x {id: "C:999999"} }
        }
        "#;
    let (result, _) = nexus
        .execute_kql(parse_kql(kql).unwrap())
        .await
        .expect("dangling id inside NOT must not fail the query");
    assert_eq!(
        result,
        json!(["A1"]),
        "NOT with unmatchable pattern keeps the solution"
    );

    // Control: the same NOT with a resolvable target still excludes A1.
    let kql = r#"
        FIND(?a.name)
        WHERE {
            ?a {type: "PairNode", name: "A1"}
            NOT { (?a, "p1", ?x) ?x {type: "PairNode", name: "B1"} }
        }
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    assert_eq!(result, json!([]), "matching NOT still excludes: {result}");
}

/// #10: a dangling id inside a UNION branch makes that branch contribute
/// an empty set (§3.4.7.3) instead of failing the query. The main pattern
/// keeps strict KIP_3002 semantics (covered by
/// `test_kql_dangling_id_matchers_return_kip_3002`).
#[tokio::test]
async fn test_kql_dangling_id_in_union_branch_contributes_empty() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_pair_graph(&nexus).await;

    let kql = r#"
        FIND(?a.name)
        WHERE {
            ?a {type: "PairNode", name: "A1"}
            UNION { ?a {id: "C:999999"} }
        }
        "#;
    let (result, _) = nexus
        .execute_kql(parse_kql(kql).unwrap())
        .await
        .expect("dangling id inside UNION must not fail the query");
    assert_eq!(
        result,
        json!(["A1"]),
        "main branch survives, union adds nothing"
    );

    // A dangling proposition link id in a UNION branch behaves the same.
    let kql = r#"
        FIND(?a.name)
        WHERE {
            ?a {type: "PairNode", name: "A1"}
            UNION { ?link (id: "P:999999:p1") }
        }
        "#;
    let (result, _) = nexus
        .execute_kql(parse_kql(kql).unwrap())
        .await
        .expect("dangling link id inside UNION must not fail the query");
    assert_eq!(result, json!(["A1"]), "{result}");
}

/// #11: the row-wise UNION merge must survive a semantically redundant
/// sibling pattern that binds one extra variable (`?link`): the branch's
/// solutions were dropped by the conjunctive FIND equi-join because the
/// branch rows were only merged into the exact-signature relation.
#[tokio::test]
async fn test_kql_union_row_union_with_hetero_signature_sibling() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_pair_graph(&nexus).await;

    let kql = r#"
        FIND(?a.name, ?b.name)
        WHERE {
            (?a, "p1", ?b)
            ?link (?a, "p1", ?b)
            UNION { (?a, "p2", ?b) }
        }
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    assert_eq!(
        collect_pairs(&result),
        vec![
            ("A1".to_string(), "B1".to_string()), // p1
            ("A1".to_string(), "B2".to_string()), // p2 (UNION branch)
            ("A2".to_string(), "B2".to_string()), // p1 and p2, deduplicated
        ],
        "a redundant ?link pattern must not drop UNION branch solutions: {result}"
    );
}

/// #16: a multi-clause NOT block only excludes solutions its *whole*
/// pattern matches: `NOT { (?a,"blocked",?b) ?b {type:"Bot"} }` must not
/// exclude (a, b) pairs where b is blocked but not a Bot.
#[tokio::test]
async fn test_kql_not_block_narrows_excluded_tuples_by_later_clauses() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();

    let kml = r#"
        UPSERT {
            CONCEPT ?user_t { {type: "$ConceptType", name: "GUser"} }
            CONCEPT ?bot_t { {type: "$ConceptType", name: "GBot"} }
            CONCEPT ?human_t { {type: "$ConceptType", name: "GHuman"} }
            CONCEPT ?linked { {type: "$PropositionType", name: "linked"} }
            CONCEPT ?blocked { {type: "$PropositionType", name: "blocked"} }
            CONCEPT ?a1 { {type: "GUser", name: "UA1"} }
            CONCEPT ?a2 { {type: "GUser", name: "UA2"} }
            CONCEPT ?b1 { {type: "GBot", name: "TB1"} }
            CONCEPT ?b2 { {type: "GHuman", name: "TB2"} }
            PROPOSITION ?l1 { (?a1, "linked", ?b1) }
            PROPOSITION ?l2 { (?a2, "linked", ?b2) }
            PROPOSITION ?k1 { (?a1, "blocked", ?b1) }
            PROPOSITION ?k2 { (?a2, "blocked", ?b2) }
        }
        "#;
    nexus
        .execute_kml(parse_kml(kml).unwrap(), false)
        .await
        .unwrap();

    // UA1→TB1 is blocked AND TB1 is a bot: excluded. UA2→TB2 is blocked
    // but TB2 is human: it must survive.
    let kql = r#"
        FIND(?a.name, ?b.name)
        WHERE {
            (?a, "linked", ?b)
            NOT {
                (?a, "blocked", ?b)
                ?b {type: "GBot"}
            }
        }
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    assert_eq!(
        collect_pairs(&result),
        vec![("UA2".to_string(), "TB2".to_string())],
        "NOT must only exclude tuples matching the whole block: {result}"
    );
}

/// #32: solution dedup keys must not be `|`-joined strings — predicates may
/// contain `|`, so ("|q", null) and (null, "q|") collided into one key and
/// a distinct solution row was silently dropped.
#[tokio::test]
async fn test_kql_dedup_key_not_ambiguous_for_pipe_predicates() {
    let nexus = setup_test_db(async |_| Ok(())).await.unwrap();
    setup_pair_graph(&nexus).await;

    let kml = r#"
        UPSERT {
            CONCEPT ?pt1 { {type: "$PropositionType", name: "|q"} }
            CONCEPT ?pt2 { {type: "$PropositionType", name: "q|"} }
            CONCEPT ?x1 { {type: "PairNode", name: "X1"} }
            CONCEPT ?y1 { {type: "PairNode", name: "Y1"} }
            CONCEPT ?x2 { {type: "PairNode", name: "X2"} }
            CONCEPT ?y2 { {type: "PairNode", name: "Y2"} }
            PROPOSITION ?e1 { (?x1, "|q", ?y1) }
            PROPOSITION ?e2 { (?x2, "q|", ?y2) }
        }
        "#;
    nexus
        .execute_kml(parse_kml(kml).unwrap(), false)
        .await
        .unwrap();

    // Two disjoint UNION partitions, each contributing one solution:
    // ("|q", null) from the main block and (null, "q|") from the branch.
    // The old string keys were both "||q|", collapsing them to one row.
    let kql = r#"
        FIND(?p1, ?p2)
        WHERE {
            (?s1, ?p1, {type: "PairNode", name: "Y1"})
            UNION { (?s2, ?p2, {type: "PairNode", name: "Y2"}) }
        }
        "#;
    let (result, _) = nexus.execute_kql(parse_kql(kql).unwrap()).await.unwrap();
    assert_eq!(
        collect_pairs(&result),
        vec![
            ("null".to_string(), "q|".to_string()),
            ("|q".to_string(), "null".to_string()),
        ],
        "both pipe-predicate solutions must survive dedup: {result}"
    );
}
