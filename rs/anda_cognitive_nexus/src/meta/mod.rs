//! # Executing META
//!
//! META is how an Agent finds its footing before it reads or writes anything:
//! what this Nexus is, which Space it is in, what the schema says, what the
//! engine can actually do.
//!
//! ## The five-layer discipline
//!
//! The syntax card states it, and the module layout follows it, because
//! collapsing any two of these is how a caller ends up believing something the
//! engine never said:
//!
//! ```text
//! DESCRIBE / SEARCH   find        — what is here
//! VERIFY              integrity   — is this artifact what it claims to be
//! VALIDATE            legality    — would this be accepted
//! PREVIEW             effect      — what would it do
//! Receipt             fact        — what actually committed
//! ```
//!
//! A `VALIDATE` that passed is not a promise that a write will commit, and a
//! `PREVIEW` that computed an effect is not a commit. Only a receipt says
//! something happened.
//!
//! ## Capabilities are data
//!
//! [`DESCRIBE CAPABILITIES`](capabilities) reports what this engine supports
//! *and* what it does not, as a structured list. An Agent that has to discover
//! a gap by triggering an error has already wasted a turn, and one that never
//! discovers it will read an absent feature as an absent fact.

pub mod describe;
pub mod history;
pub mod inspect;

use anda_kip::{
    Json, KipError, MetaCommand, Operation, Request, Response, ResponseContext, ResultContext,
};

use crate::store::Store;

/// Runs one META command.
pub async fn execute(
    store: &Store,
    space: &str,
    command: &MetaCommand,
    request: &Request,
    operation: &Operation,
) -> Response {
    let mut cx = match crate::kql::Context::open(
        store,
        space,
        request.parameters.as_ref(),
        operation.parameters.as_ref(),
    )
    .await
    {
        Ok(cx) => cx,
        Err(err) => return Response::from(err),
    };
    let environment_version = cx.env.version;

    match run(&mut cx, command).await {
        Ok(Answer {
            result,
            next_cursor,
        }) => Response {
            context: Some(ResponseContext {
                space_id: Some(space.to_string()),
                schema_environment_version: Some(environment_version),
                compatibility_profile_used: None,
                extensions: None,
            }),
            next_cursor: next_cursor.clone(),
            results: vec![anda_kip::OperationResult {
                context: Some(ResultContext {
                    space_id: Some(space.to_string()),
                    schema_environment_version: Some(environment_version),
                    ..Default::default()
                }),
                next_cursor,
                ..anda_kip::OperationResult::ok(result)
            }],
            ..Default::default()
        },
        Err(err) => Response::from(err),
    }
}

/// One META answer, with its page cursor when it pages.
pub struct Answer {
    /// The answer body.
    pub result: Json,
    /// The cursor for the next page, when more remain.
    pub next_cursor: Option<String>,
}

impl Answer {
    /// An answer that does not page.
    pub fn whole(result: Json) -> Self {
        Self {
            result,
            next_cursor: None,
        }
    }

    /// Adds one field to an object answer.
    pub fn with_detail(mut self, key: &str, value: Json) -> Self {
        if let Some(object) = self.result.as_object_mut() {
            object.insert(key.to_string(), value);
        }
        self
    }
}

async fn run(cx: &mut crate::kql::Context<'_>, command: &MetaCommand) -> Result<Answer, KipError> {
    match command {
        MetaCommand::Describe(target) => describe::run(cx, target).await,
        MetaCommand::List(list) => describe::list(cx, list).await,
        MetaCommand::Search(search) => inspect::search(cx, search).await,
        MetaCommand::Validate(validate) => inspect::validate(cx, validate),
        MetaCommand::Preview(preview) => inspect::preview(cx, preview).await,
        MetaCommand::Verify { target, value } => inspect::verify(cx, *target, value),
        MetaCommand::History(history) => history::history(cx, history).await,
        MetaCommand::Changes(changes) => history::changes(cx, changes).await,
        MetaCommand::Snapshot { as_of } => history::snapshot(cx, as_of.as_ref()).await,
        MetaCommand::ExportCapsule(command) => inspect::export_capsule(cx, command).await,
    }
}

/// What this engine can and cannot do, as data.
///
/// The `unsupported` list is not an apology: an Agent that can read it will
/// not spend a turn discovering a gap, and — more importantly — will not read
/// a missing feature as a missing fact.
pub fn capabilities() -> Json {
    serde_json::json!({
        "kip": anda_kip::KIP_VERSION,
        "languages": ["KQL", "KML", "META"],
        "supported": {
            "kml": [
                "CREATE CONCEPT", "UPSERT CONCEPT", "ENSURE PROPOSITION",
                "CREATE EVIDENCE", "CREATE ASSERTION", "CREATE ACTIVITY",
                "ASSERT (desugared)", "RETRACT ASSERTION", "SUPERSEDE ASSERTION",
                "CORRECT EVIDENCE", "TRANSITION ACTIVITY", "SET RETENTION",
                "ARCHIVE", "TOMBSTONE"
            ],
            "kql": [
                "CONCEPT", "PROPOSITION", "ASSERTION", "EVIDENCE", "ACTIVITY",
                "STRUCTURAL", "BELIEF", "BELIEF SLOT", "FILTER", "NOT",
                "OPTIONAL", "UNION", "ORDER BY", "LIMIT", "CURSOR", "FOR TIME",
                "WITH EPISTEMIC", "aggregates"
            ],
            "meta": [
                "DESCRIBE", "LIST", "SEARCH", "VALIDATE", "PREVIEW KML",
                "PREVIEW IMPORT CAPSULE", "HISTORY", "CHANGES", "SNAPSHOT",
                "EXPORT CAPSULE", "VERIFY CAPSULE"
            ],
            "execution_modes": ["independent", "sequence"],
            "search_modes": ["keyword"],
            "transactions": {
                "atomic_visibility": "in_process",
                "idempotency": true,
                "preconditions": ["EXPECT VERSION", "EXPECT STATE"],
                "dry_run": true
            },
            "projection": {
                "policies": ["kip:policy:baseline", "kip:policy:forecast"],
                "explanation": true,
                "conflict_set_expansion": true,
                "corroboration_grouping": true
            }
        },
        "unsupported": [
            {
                "capability": "atomic_batch",
                "detail": "execution.mode \"atomic\" over several operations",
                "reason": "one transaction, one snapshot and all-or-none commit across \
                           operations are not implemented; a batch runs operation by operation"
            },
            {
                "capability": "historical_read",
                "detail": "AS OF SEQ / TX / TIME",
                "reason": "no historical snapshots are retained, so a past coordinate cannot be \
                           reconstructed"
            },
            {
                "capability": "transitive_traversal",
                "detail": "hop quantifiers such as \"knows\"{1,3}",
                "reason": "not implemented"
            },
            {
                "capability": "semantic_search",
                "detail": "SEARCH ... MODE \"semantic\" | \"hybrid\"",
                "reason": "no embedding model is configured; keyword search is the only mode"
            },
            {
                "capability": "trust_model",
                "detail": "source trust and evidence-quality evaluation in the projection",
                "reason": "not implemented; every eligible corroboration group counts equally, \
                           and every projection says so"
            },
            {
                "capability": "governance",
                "detail": "DESCRIBE ACCESS / TRUST, classification enforcement, PURGE",
                "reason": "no Governance plane; there is no authorization to report"
            },
            {
                "capability": "capsule_import",
                "detail": "importing a Capsule's records into a Space",
                "reason": "export, verification and import preview work; the semantic merge — \
                           rewriting every reference onto destination ids, recording import \
                           provenance and staying idempotent across restarts — is a write path \
                           that must not be half-built"
            },
            {
                "capability": "capsule_signatures",
                "detail": "signing an exported Capsule and verifying a signed one",
                "reason": "no signing keys; an exported Capsule is unsigned, and its stated \
                           source is a claim a destination cannot check"
            },
            {
                "capability": "kml_selection_blocks",
                "detail": "UPDATE / PURGE / MERGE CONCEPT, and clause forms with a WHERE block",
                "reason": "the mutation path does not run the KQL solver yet"
            }
        ]
    })
}

/// The protocol this engine speaks.
pub fn protocol() -> Json {
    serde_json::json!({
        "kip": anda_kip::KIP_VERSION,
        "implementation": {
            "name": env!("CARGO_PKG_NAME"),
            "version": env!("CARGO_PKG_VERSION"),
        },
        // The syntax card is what an Agent needs in context to write a
        // well-formed command; shipping it here saves a round trip.
        "syntax": anda_kip::KIP_SYNTAX,
    })
}
