use anda_cognitive_nexus::{CognitiveNexus, ConceptPK, db_to_kip_error};
use anda_db::{
    database::{AndaDB, DBConfig},
    storage::StorageConfig,
};
use anda_kip::{
    KipError, META_SELF_NAME, PERSON_SELF_KIP, PERSON_SYSTEM_KIP, PERSON_TYPE, parse_kml, parse_kql,
};
use anda_object_store::MetaStoreBuilder;
use object_store::local::LocalFileSystem;
use std::sync::Arc;

// mkdir -p ./debug/metastore
// cargo run --example kip_demo
#[tokio::main]
async fn main() -> Result<(), KipError> {
    // init structured logger
    structured_logger::init();

    // let object_store = InMemory::new();
    let object_store = MetaStoreBuilder::new(
        LocalFileSystem::new_with_prefix("./debug/metastore")
            .map_err(|err| KipError::internal_error(err.to_string()))?,
        10000,
    )
    .build();

    let db_config = DBConfig {
        name: "anda_kip_demo".to_string(),
        description: "Anda Cognitive Nexus demo".to_string(),
        storage: StorageConfig {
            compress_level: 0, // no compression
            ..Default::default()
        },
        lock: None, // no lock for demo
    };

    // connect to the database (create if it doesn't exist)
    let db = AndaDB::connect(Arc::new(object_store), db_config)
        .await
        .map_err(db_to_kip_error)?;
    log::info!(
        action = "connect",
        database = db.name();
        "connected to database"
    );

    let nexus = CognitiveNexus::connect(Arc::new(db), async |nexus| {
        if !nexus
            .has_concept(&ConceptPK::Object {
                r#type: PERSON_TYPE.to_string(),
                name: META_SELF_NAME.to_string(),
            })
            .await
        {
            let kml = &[
                &PERSON_SELF_KIP.replace(
                    "$self_reserved_principal_id",
                    "gcxml-rtxjo-ib7ov-5si5r-5jluv-zek7y-hvody-nneuz-hcg5i-6notx-aae",
                ),
                PERSON_SYSTEM_KIP,
            ]
            .join("\n");

            let result = nexus.execute_kml(parse_kml(&kml)?, false).await?;
            log::info!(result:serde = result; "Init $self and $system");
        }

        Ok(())
    })
    .await?;

    log::info!(
        action = "connect",
        database = nexus.name();
        "connected to Anda Cognitive Nexus"
    );

    // Demonstrate KML: Create knowledge capsules
    demo_kml_operations(&nexus).await?;

    // Demonstrate KQL: Query knowledge
    demo_kql_queries(&nexus).await?;

    nexus.close().await?;

    Ok(())
}

/// Demonstrate KML operations: Create and update knowledge
async fn demo_kml_operations(nexus: &CognitiveNexus) -> Result<(), KipError> {
    log::info!("=== KML Demo: Knowledge Manipulation ===");

    // Create basic concept types and medical knowledge capsule
    let medical_knowledge_kml = r#"
    UPSERT {
        // Define concept types
        CONCEPT ?drug_type {
            {type: "$ConceptType", name: "Drug"}
            SET ATTRIBUTES {
                description: "Pharmaceutical drug concept type"
            }
        }

        CONCEPT ?symptom_type {
            {type: "$ConceptType", name: "Symptom"}
            SET ATTRIBUTES {
                description: "Medical symptom concept type"
            }
        }

        // Define relation types
        CONCEPT ?treats_relation {
            {type: "$PropositionType", name: "treats"}
            SET ATTRIBUTES {
                description: "Drug treats symptom relationship"
            }
        }

        CONCEPT ?has_side_effect_relation {
            {type: "$PropositionType", name: "has_side_effect"}
            SET ATTRIBUTES {
                description: "Drug has side effect relationship"
            }
        }

        // Create symptom concepts
        CONCEPT ?headache {
            {type: "Symptom", name: "Headache"}
            SET ATTRIBUTES {
                severity_scale: "1-10",
                description: "Pain in the head or neck area"
            }
        }

        CONCEPT ?fever {
            {type: "Symptom", name: "Fever"}
            SET ATTRIBUTES {
                normal_temp: "98.6°F (37°C)",
                description: "Elevated body temperature"
            }
        }

        CONCEPT ?stomach_irritation {
            {type: "Symptom", name: "Stomach Irritation"}
            SET ATTRIBUTES {
                severity: "mild to moderate",
                description: "Gastrointestinal discomfort"
            }
        }

        // Create specific drug concepts
        CONCEPT ?aspirin {
            {type: "Drug", name: "Aspirin"}
            SET ATTRIBUTES {
                molecular_formula: "C9H8O4",
                risk_level: 1,
                description: "Common pain reliever and anti-inflammatory drug"
            }
            SET PROPOSITIONS {
                ("treats", ?headache)
                ("treats", ?fever)
                ("has_side_effect", ?stomach_irritation)
            }
        }
    }
    WITH METADATA {
        source: "KIP Demo Medical Knowledge",
        author: "Demo System",
        confidence: 0.95,
        created_at: "2025-07-01T00:00:00Z"
    }
    "#;

    let result = nexus
        .execute_kml(parse_kml(medical_knowledge_kml)?, false)
        .await?;
    log::info!(result:serde = result; "Created medical knowledge capsule");

    // Create a new hypothetical drug
    let new_drug_kml = r#"
    UPSERT {
        CONCEPT ?brain_fog {
            {type: "Symptom", name: "Brain Fog"}
            SET ATTRIBUTES {
                description: "Mental fatigue and lack of clarity",
                cognitive_impact: "high"
            }
        }

        CONCEPT ?neural_bloom {
            {type: "Symptom", name: "Neural Bloom"}
            SET ATTRIBUTES {
                description: "A rare side effect characterized by temporary burst of creative thoughts",
                frequency: "rare",
                severity: "mild"
            }
        }

        CONCEPT ?cognizine {
            {type: "Drug", name: "Cognizine"}
            SET ATTRIBUTES {
                molecular_formula: "C12H15N5O3",
                risk_level: 2,
                description: "A novel nootropic drug designed to enhance cognitive functions",
                status: "experimental"
            }
            SET PROPOSITIONS {
                ("treats", {type: "Symptom", name: "Brain Fog"})
                ("has_side_effect", ?neural_bloom)
            }
        }
    }
    WITH METADATA {
        source: "Experimental Drug Research",
        confidence: 0.75,
        status: "under_review"
    }
    "#;

    let result = nexus.execute_kml(parse_kml(new_drug_kml)?, false).await?;
    log::info!("Created experimental drug knowledge");
    println!("{result:#?}");

    Ok(())
}

/// Demonstrate KQL queries: Retrieve and reason about knowledge
async fn demo_kql_queries(nexus: &CognitiveNexus) -> Result<(), KipError> {
    log::info!("=== KQL Demo: Knowledge Queries ===");

    // Query 1: Find all drugs and their properties
    let query1 = r#"
    FIND(?drug.name, ?drug.attributes.molecular_formula, ?drug.attributes.risk_level)
    WHERE {
        ?drug {type: "Drug"}
    }
    ORDER BY ?drug.attributes.risk_level ASC
    "#;

    let (result, _) = nexus.execute_kql(parse_kql(query1)?).await?;
    log::info!("Query 1: All drugs with properties");
    println!("{result:#?}");

    // Query 2: Find drugs that treat headache
    let query2 = r#"
    FIND(?drug.name, ?drug.attributes.description)
    WHERE {
        ?drug {type: "Drug"}
        (?drug, "treats", {type: "Symptom", name: "Headache"})
    }
    "#;

    let (result, _) = nexus.execute_kql(parse_kql(query2)?).await?;
    log::info!("Query 2: Drugs that treat headache");
    println!("{result:#?}");

    // Query 3: Find all drug side effects
    let query3 = r#"
    FIND(?drug.name, ?side_effect.name, ?side_effect.attributes.description)
    WHERE {
        ?drug {type: "Drug"}
        (?drug, "has_side_effect", ?side_effect)
    }
    "#;

    let (result, _) = nexus.execute_kql(parse_kql(query3)?).await?;
    log::info!("Query 3: Drugs and their side effects");
    println!("{result:#?}");

    // Query 4: Complex query - Find low-risk drugs that treat symptoms, excluding those with stomach side effects
    let query4 = r#"
    FIND(?drug.name, ?symptom, ?drug.attributes.risk_level)
    WHERE {
        ?drug {type: "Drug"}
        (?drug, "treats", ?symptom)

        FILTER(?drug.attributes.risk_level < 3)

        NOT {
            (?drug, "has_side_effect", {name: "Stomach Irritation"})
        }
    }
    ORDER BY ?drug.attributes.risk_level ASC
    "#;

    let (result, _) = nexus.execute_kql(parse_kql(query4)?).await?;
    log::info!("Query 4: Low-risk drugs without stomach side effects");
    println!("{result:#?}");

    // Query 5: Use OPTIONAL to query drugs and their possible side effects
    let query5 = r#"
    FIND(?drug.name, ?side_effect.name)
    WHERE {
        ?drug {type: "Drug"}

        OPTIONAL {
            (?drug, "has_side_effect", ?side_effect)
        }
    }
    "#;

    let (result, _) = nexus.execute_kql(parse_kql(query5)?).await?;
    log::info!("Query 5: All drugs with optional side effects");
    println!("{result:#?}");

    // Query 6: Aggregation query - Count the number of symptoms treated by each drug
    let query6 = r#"
    FIND(?drug.name, COUNT(?symptom))
    WHERE {
        ?drug {type: "Drug"}
        (?drug, "treats", ?symptom)
    }
    "#;

    let (result, _) = nexus.execute_kql(parse_kql(query6)?).await?;
    log::info!("Query 6: Count of symptoms treated by each drug");
    println!("{result:#?}");

    Ok(())
}
