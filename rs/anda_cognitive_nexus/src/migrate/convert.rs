//! Phase 3: staged 1.x rows become 2.0 elements.
//!
//! Everything here goes through the ordinary engine: KML statements, parsed and
//! executed exactly as a client's would be. A migration that wrote rows
//! directly would be the one writer allowed to produce elements the engine
//! would have refused, and the first sign of it would be a query that cannot
//! explain its own answer.
//!
//! ## The shape change
//!
//! A 1.x Proposition row is a *multi-predicate edge*: one subject, one object,
//! a set of predicates, and per-predicate properties. A 2.0 Proposition is one
//! tuple. So one row fans out:
//!
//! ```text
//! {subject: "C:1", object: "C:2", predicates: ["knows", "trusts"]}
//!
//!   → Proposition (C:1, "knows",  C:2) + Assertion
//!   → Proposition (C:1, "trusts", C:2) + Assertion
//! ```
//!
//! Each gets its own Assertion because each had its own 1.x properties —
//! including its own `confidence`, which is the whole reason the fan-out
//! cannot be collapsed.
//!
//! ## Values travel as parameters
//!
//! Legacy attribute keys and values are arbitrary: a 1.x deployment could put
//! anything in that map. They are bound as request parameters rather than
//! rendered into the command text, because a parameter is a complete value
//! position (§74, §88.2) — no amount of punctuation inside a legacy string can
//! turn into syntax that way.

use anda_kip::{Executor, Json, KipError, KipErrorCode, Map, Operation, Request, TopLevelStatus};
use serde_json::json;
use std::collections::BTreeMap;

use super::MIGRATION_KEY_PREFIX;
use super::package::{Vocabulary, legacy_package_ref};
use super::stage::{self, LegacyKind, LegacyRow};
use crate::CognitiveNexus;
use crate::nexus::DEFAULT_SPACE;

/// The Concept type the generated package carries for the migration actor.
pub const MIGRATION_ACTOR_TYPE: &str = "MigrationActor";
/// Its stable client key.
const MIGRATION_ACTOR_KEY: &str = "kip:migrate:v1:actor";

/// How many elements one MUTATE carries.
///
/// A transaction per element would be correct and slow; one transaction for
/// everything would be fast and would make a single bad legacy row undo the
/// whole migration. Batching keeps a failure's blast radius readable in the log
/// while a resumed run still skips what landed.
const BATCH: usize = 64;

fn internal(message: impl std::fmt::Display) -> KipError {
    KipError::new(KipErrorCode::InternalError, format!("migration: {message}"))
}

/// Runs one KML/KQL command through the engine, with parameters bound.
async fn run(
    nexus: &CognitiveNexus,
    command: &str,
    parameters: Map<String, Json>,
) -> Result<Json, KipError> {
    let request = Request {
        operations: vec![Operation::new(command)],
        parameters: (!parameters.is_empty()).then_some(parameters),
        ..Default::default()
    };
    let parsed = request.operations[0].parse()?;
    let response = nexus
        .execute(parsed, &request, &request.operations[0])
        .await;
    if response.status != TopLevelStatus::Succeeded {
        let detail = response
            .error
            .map(|error| format!("{}: {}", error.code, error.message))
            .unwrap_or_else(|| "no error reported".to_string());
        return Err(internal(format!("{detail}\nwhile running: {command}")));
    }
    Ok(response.first_result().cloned().unwrap_or(Json::Null))
}

/// The handles a MUTATE receipt reports, as handle name → element id.
async fn mutate_handles(
    nexus: &CognitiveNexus,
    command: &str,
    parameters: Map<String, Json>,
) -> Result<BTreeMap<String, String>, KipError> {
    let result = run(nexus, command, parameters).await?;
    let handles = result
        .get("handles")
        .and_then(Json::as_object)
        .ok_or_else(|| internal("a MUTATE receipt carried no handles"))?;
    Ok(handles
        .iter()
        .filter_map(|(name, id)| id.as_str().map(|id| (name.clone(), id.to_string())))
        .collect())
}

/// Loads everything staged, if anything is outstanding.
pub(crate) async fn load(nexus: &CognitiveNexus) -> Result<(), KipError> {
    let Some(staging) = stage::open(&nexus.store.db).await? else {
        return Ok(());
    };
    if stage::is_complete(&staging).await? {
        return Ok(());
    }

    let concepts = stage::rows(&staging, LegacyKind::Concept).await?;
    let propositions = stage::rows(&staging, LegacyKind::Proposition).await?;
    if concepts.is_empty() && propositions.is_empty() {
        stage::mark_complete(&staging, json!({"concepts": 0, "propositions": 0})).await?;
        return Ok(());
    }

    log::warn!(
        action = "migrate::load",
        concepts = concepts.len(),
        propositions = propositions.len();
        "loading {} staged KIP 1.x concept(s) and {} proposition row(s) into KIP 2.0",
        concepts.len(),
        propositions.len(),
    );

    let mut vocabulary = Vocabulary::scan(&concepts, &propositions);
    // The actor every migrated Assertion is attributed to needs a type, and no
    // type in the cognitive-memory profile means "the engine that imported
    // this". Generating one keeps the attribution honest without bending
    // `Person`, which the profile is explicit is never a Principal (§88.1).
    vocabulary
        .concept_types
        .insert(MIGRATION_ACTOR_TYPE.to_string());
    activate_legacy_package(nexus, &vocabulary).await?;

    let actor = ensure_actor(nexus).await?;
    let concept_ids = load_concepts(nexus, &concepts, &vocabulary).await?;
    let speakers = unambiguous_speakers(&concepts, &concept_ids);
    let claims = load_propositions(
        nexus,
        &propositions,
        &vocabulary,
        &concept_ids,
        &actor,
        &speakers,
    )
    .await?;

    stage::mark_complete(
        &staging,
        json!({
            "concepts": concept_ids.len(),
            "proposition_rows": propositions.len(),
            "assertions": claims,
            "package": legacy_package_ref(),
            "actor": actor,
        }),
    )
    .await?;
    log::warn!(
        action = "migrate::load",
        concepts = concept_ids.len(),
        assertions = claims;
        "KIP 1.x migration complete: {} concept(s), {claims} assertion(s). \
         The 1.x rows are kept in {}.",
        concept_ids.len(),
        stage::LEGACY_STAGING,
    );
    Ok(())
}

/// Installs the generated package and adds it to the Space's active lock.
///
/// Added to the lock rather than replacing it: the bundled cognitive-memory
/// profile is usually already active, and activating only the legacy package
/// would deactivate the vocabulary everything written since is resolved
/// against.
async fn activate_legacy_package(
    nexus: &CognitiveNexus,
    vocabulary: &Vocabulary,
) -> Result<(), KipError> {
    if vocabulary.is_empty() {
        return Ok(());
    }
    let artifact = vocabulary.artifact()?;
    let package = crate::schema::SchemaPackage::parse(&artifact.to_string())?;
    let package_ref = nexus.install_package(&package, "kip-1.x-migration").await?;

    let mut lock = nexus
        .store
        .schema_environment(DEFAULT_SPACE)
        .await?
        .lock
        .clone();
    lock.packages.insert(
        package_ref.package_id.clone(),
        package_ref.version.to_string(),
    );
    lock.states
        .insert(package_ref.package_id, crate::schema::PackageState::Active);
    nexus.ensure_schema(DEFAULT_SPACE, lock).await?;
    Ok(())
}

/// Finds or creates the Concept migrated Assertions are attributed to.
async fn ensure_actor(nexus: &CognitiveNexus) -> Result<String, KipError> {
    if let Some(id) = find_by_client_key(nexus, MIGRATION_ACTOR_KEY).await? {
        return Ok(id);
    }
    let mut parameters = Map::new();
    parameters.insert("k".to_string(), json!(MIGRATION_ACTOR_KEY));
    let handles = mutate_handles(
        nexus,
        &format!(
            r#"CREATE CONCEPT ?actor {{
                 TYPE "{MIGRATION_ACTOR_TYPE}"
                 NAME "KIP 1.x migration"
                 CLIENT KEY :k
                 SET ATTRIBUTES {{
                   "description": "The engine, standing as the recorded source of every claim carried in from the KIP 1.x database. Not a person, and not a Principal: it names where these Assertions came from, which is the one thing the old rows actually established."
                 }}
               }}"#
        ),
        parameters,
    )
    .await?;
    handles
        .get("actor")
        .cloned()
        .ok_or_else(|| internal("the migration actor was not created"))
}

/// The element a client key already resolves to, if any.
async fn find_by_client_key(
    nexus: &CognitiveNexus,
    client_key: &str,
) -> Result<Option<String>, KipError> {
    let mut parameters = Map::new();
    parameters.insert("k".to_string(), json!(client_key));
    let found = run(
        nexus,
        "FIND(?c.id) WHERE { ?c CONCEPT {client_key: :k} }",
        parameters,
    )
    .await?;
    Ok(found
        .as_array()
        .and_then(|rows| rows.first())
        .and_then(Json::as_str)
        .map(str::to_string))
}

fn concept_key(legacy_id: u64) -> String {
    format!("{MIGRATION_KEY_PREFIX}C:{legacy_id}")
}

fn proposition_key(legacy_id: u64, predicate: &str) -> String {
    format!("{MIGRATION_KEY_PREFIX}P:{legacy_id}:{predicate}")
}

/// The 1.x `author` strings that name exactly one migrated Concept.
///
/// §12 is blunt that a legacy `author` is ambiguous: it may be a semantic
/// speaker, the application that wrote the row, or a bookkeeping actor. Mapping
/// it is allowed only when justified, and never by inventing an ActorBinding.
///
/// The one case that clears that bar is an `author` matching exactly one
/// Concept by name — then the old system did record who said it, and dropping
/// that would lose attribution the data actually had. A name shared by two
/// Concepts identifies neither, so it is left alone rather than resolved by
/// picking one.
fn unambiguous_speakers(
    concepts: &[LegacyRow],
    ids: &BTreeMap<u64, String>,
) -> BTreeMap<String, String> {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for row in concepts {
        if let Some(name) = row.doc.get("name").and_then(Json::as_str) {
            *counts.entry(name).or_default() += 1;
        }
    }
    let mut speakers = BTreeMap::new();
    for row in concepts {
        let Some(name) = row.doc.get("name").and_then(Json::as_str) else {
            continue;
        };
        if counts.get(name) != Some(&1) {
            continue;
        }
        if let Some(id) = ids.get(&row.legacy_id) {
            speakers.insert(name.to_string(), id.clone());
        }
    }
    speakers
}

/// Creates every staged Concept that is not already there.
async fn load_concepts(
    nexus: &CognitiveNexus,
    rows: &[LegacyRow],
    vocabulary: &Vocabulary,
) -> Result<BTreeMap<u64, String>, KipError> {
    let mut ids = BTreeMap::new();
    let mut pending: Vec<&LegacyRow> = Vec::new();
    for row in rows {
        match find_by_client_key(nexus, &concept_key(row.legacy_id)).await? {
            Some(id) => {
                ids.insert(row.legacy_id, id);
            }
            None => pending.push(row),
        }
    }

    for chunk in pending.chunks(BATCH) {
        let mut clauses = Vec::new();
        let mut parameters = Map::new();
        for (index, row) in chunk.iter().enumerate() {
            let handle = format!("c{index}");
            let type_name = row
                .doc
                .get("type")
                .and_then(Json::as_str)
                .filter(|name| !name.is_empty())
                .ok_or_else(|| {
                    internal(format!(
                        "1.x Concept {} has no type; it cannot be given a schema_ref",
                        row.legacy_id
                    ))
                })?;
            let symbol = vocabulary.concept_ref(type_name).ok_or_else(|| {
                internal(format!(
                    "1.x type {type_name:?} is missing from the generated package"
                ))
            })?;
            let name = row.doc.get("name").and_then(Json::as_str).unwrap_or("");

            parameters.insert(format!("t{index}"), json!(symbol));
            parameters.insert(format!("n{index}"), json!(name));
            parameters.insert(format!("k{index}"), json!(concept_key(row.legacy_id)));
            let attributes = render_assignments(
                &legacy_attributes(row),
                &format!("a{index}"),
                &mut parameters,
            );

            clauses.push(format!(
                "CREATE CONCEPT ?{handle} {{ TYPE :t{index} NAME :n{index} \
                 CLIENT KEY :k{index} SET ATTRIBUTES {attributes} }}"
            ));
        }
        let handles = mutate_handles(
            nexus,
            &format!("MUTATE {{\n{}\n}}", clauses.join("\n")),
            parameters,
        )
        .await?;
        for (index, row) in chunk.iter().enumerate() {
            let handle = format!("c{index}");
            let id = handles
                .get(&handle)
                .ok_or_else(|| internal(format!("handle {handle} missing from the receipt")))?;
            ids.insert(row.legacy_id, id.clone());
        }
    }
    Ok(ids)
}

/// The attributes a migrated Concept carries.
///
/// 1.x `attributes` stay attributes: they were representation-local state with
/// no epistemic lifecycle, which is exactly what §10 says to keep. 1.x
/// `metadata` moves under a `legacy` key rather than being spread into native
/// fields, because its members meant different things in different deployments
/// — `access_level` annotated where 2.0's classification enforces, and
/// `confidence` may have been truth, staleness or importance (§13, §21).
/// Preserved and labelled is recoverable; guessed is not.
fn legacy_attributes(row: &LegacyRow) -> Json {
    let mut attributes = row
        .doc
        .get("attributes")
        .and_then(Json::as_object)
        .cloned()
        .unwrap_or_default();
    let mut legacy = Map::new();
    legacy.insert("id".to_string(), json!(row.legacy_id));
    if let Some(metadata) = row.doc.get("metadata").and_then(Json::as_object) {
        legacy.insert("metadata".to_string(), Json::Object(metadata.clone()));
    }
    attributes.insert("legacy".to_string(), Json::Object(legacy));
    Json::Object(attributes)
}

/// Renders an assignment object, with every value bound as a parameter.
///
/// `SET ATTRIBUTES` takes a literal object (`assignment_object` in the
/// grammar), so the braces and keys have to be text. Only the keys are — each
/// value goes in as `:param`, which is a complete value position, so nothing
/// inside a legacy value can be read as syntax. Keys are quoted rather than
/// bare because `field_name` admits a string literal and a 1.x attribute key
/// was whatever the writer put in the map.
fn render_assignments(object: &Json, prefix: &str, parameters: &mut Map<String, Json>) -> String {
    let Some(members) = object.as_object() else {
        return "{}".to_string();
    };
    let mut rendered = Vec::with_capacity(members.len());
    for (index, (key, value)) in members.iter().enumerate() {
        let name = format!("{prefix}_{index}");
        parameters.insert(name.clone(), value.clone());
        rendered.push(format!("{}: :{name}", anda_kip::quote_str(key)));
    }
    format!("{{ {} }}", rendered.join(", "))
}

/// Resolves a 1.x endpoint string to a migrated element id.
fn endpoint(
    reference: &str,
    concepts: &BTreeMap<u64, String>,
    propositions: &BTreeMap<String, String>,
) -> Option<String> {
    // 1.x wrote endpoints as `C:{id}` or `P:{id}:{predicate}` (Display for
    // EntityID). The second form is a higher-order reference: it names one
    // predicate of a multi-predicate row, which is precisely the tuple that
    // row's fan-out produced.
    if let Some(rest) = reference.strip_prefix("C:") {
        return rest.parse().ok().and_then(|id| concepts.get(&id).cloned());
    }
    if let Some(rest) = reference.strip_prefix("P:") {
        let (id, predicate) = rest.split_once(':')?;
        let id: u64 = id.parse().ok()?;
        return propositions.get(&proposition_key(id, predicate)).cloned();
    }
    None
}

/// Creates the Propositions and their imported Assertions.
///
/// Repeated in passes because a 1.x Proposition could point at another one, and
/// the target has to exist first. Each pass resolves whatever became
/// resolvable; the loop stops when a pass adds nothing, and anything still
/// unresolved is reported rather than dropped.
async fn load_propositions(
    nexus: &CognitiveNexus,
    rows: &[LegacyRow],
    vocabulary: &Vocabulary,
    concepts: &BTreeMap<u64, String>,
    actor: &str,
    speakers: &BTreeMap<String, String>,
) -> Result<usize, KipError> {
    let mut created: BTreeMap<String, String> = BTreeMap::new();
    let mut assertions = 0usize;
    let mut outstanding: Vec<(u64, String, Json)> = Vec::new();

    for row in rows {
        let Some(predicates) = row.doc.get("predicates").and_then(Json::as_array) else {
            continue;
        };
        for predicate in predicates.iter().filter_map(Json::as_str) {
            let properties = row
                .doc
                .get("properties")
                .and_then(|p| p.get(predicate))
                .cloned()
                .unwrap_or(Json::Null);
            outstanding.push((row.legacy_id, predicate.to_string(), properties));
        }
    }

    // Endpoints are per row, so keep them addressable while passes run.
    let endpoints: BTreeMap<u64, (String, String)> = rows
        .iter()
        .filter_map(|row| {
            let subject = row.doc.get("subject").and_then(Json::as_str)?;
            let object = row.doc.get("object").and_then(Json::as_str)?;
            Some((row.legacy_id, (subject.to_string(), object.to_string())))
        })
        .collect();

    while !outstanding.is_empty() {
        let mut deferred = Vec::new();
        let mut ready = Vec::new();
        for (legacy_id, predicate, properties) in outstanding {
            let Some((subject, object)) = endpoints.get(&legacy_id) else {
                continue;
            };
            match (
                endpoint(subject, concepts, &created),
                endpoint(object, concepts, &created),
            ) {
                (Some(subject), Some(object)) => {
                    ready.push((legacy_id, predicate, properties, subject, object))
                }
                _ => deferred.push((legacy_id, predicate, properties)),
            }
        }

        if ready.is_empty() {
            let names: Vec<String> = deferred
                .iter()
                .map(|(id, predicate, _)| format!("P:{id}:{predicate}"))
                .collect();
            return Err(internal(format!(
                "these 1.x Propositions reference endpoints that do not resolve, so migrating \
                 them would invent a graph the old one did not have: {}",
                names.join(", ")
            )));
        }

        for chunk in ready.chunks(BATCH) {
            assertions +=
                write_batch(nexus, chunk, vocabulary, actor, speakers, &mut created).await?;
        }
        outstanding = deferred;
    }
    Ok(assertions)
}

type Ready = (u64, String, Json, String, String);

async fn write_batch(
    nexus: &CognitiveNexus,
    chunk: &[Ready],
    vocabulary: &Vocabulary,
    actor: &str,
    speakers: &BTreeMap<String, String>,
    created: &mut BTreeMap<String, String>,
) -> Result<usize, KipError> {
    let mut clauses = Vec::new();
    let mut parameters = Map::new();
    let mut assertions = 0usize;

    parameters.insert("actor".to_string(), json!({"id": actor}));

    for (index, (legacy_id, predicate, properties, subject, object)) in chunk.iter().enumerate() {
        let symbol = vocabulary.predicate_ref(predicate).ok_or_else(|| {
            internal(format!(
                "1.x predicate {predicate:?} is missing from the generated package"
            ))
        })?;
        parameters.insert(format!("s{index}"), json!({"id": subject}));
        parameters.insert(format!("o{index}"), json!({"id": object}));
        parameters.insert(format!("p{index}"), json!(symbol));
        clauses.push(format!(
            "ENSURE PROPOSITION ?p{index} (:s{index}, :p{index}, :o{index})"
        ));

        // A 1.x fact-like Proposition becomes a truth-neutral 2.0 Proposition
        // *plus* a positive Assertion (§11): without one, nothing would be
        // believed after migration, because silence in 2.0 is `insufficient`
        // rather than assent.
        let confidence = properties
            .get("metadata")
            .and_then(|m| m.get("confidence"))
            .and_then(Json::as_f64)
            .filter(|value| (0.0..=1.0).contains(value));
        parameters.insert(
            format!("ak{index}"),
            json!(proposition_key(*legacy_id, predicate)),
        );
        // A legacy `author` that names exactly one migrated Concept is a
        // speaker the old system really did record; anything else stays the
        // migration actor, and the string stays an attribute (§12).
        let speaker = properties
            .get("metadata")
            .and_then(|m| m.get("author"))
            .and_then(Json::as_str)
            .and_then(|author| speakers.get(author))
            .cloned();
        let by = match speaker {
            Some(id) => {
                parameters.insert(format!("by{index}"), json!({"id": id}));
                format!(":by{index}")
            }
            None => ":actor".to_string(),
        };
        let confidence_clause = match confidence {
            Some(value) => {
                parameters.insert(format!("cf{index}"), json!(value));
                format!(", confidence: :cf{index}")
            }
            None => String::new(),
        };
        clauses.push(format!(
            "CREATE ASSERTION ?a{index} {{ CLIENT KEY :ak{index} SET FIELDS {{ \
             proposition: ?p{index}, asserted_by: {by}, stance: \"support\", \
             mode: \"imported\"{confidence_clause} }} }}"
        ));
        assertions += 1;
    }

    let handles = mutate_handles(
        nexus,
        &format!("MUTATE {{\n{}\n}}", clauses.join("\n")),
        parameters,
    )
    .await?;
    for (index, (legacy_id, predicate, _, _, _)) in chunk.iter().enumerate() {
        if let Some(id) = handles.get(&format!("p{index}")) {
            created.insert(proposition_key(*legacy_id, predicate), id.clone());
        }
    }
    Ok(assertions)
}
