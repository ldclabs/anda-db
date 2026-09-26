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
pub mod host;
pub mod inspect;

pub use host::HostCapabilities;

use anda_kip::{
    Json, KipError, Map, MetaCommand, Operation, Request, Response, ResponseContext, ResultContext,
    Warning,
};

use crate::governance::{AuthContext, EffectiveAuthority};
use crate::store::Store;

/// Runs one META command.
pub async fn execute(
    store: &Store,
    space: &str,
    command: &MetaCommand,
    request: &Request,
    operation: &Operation,
    authority: &EffectiveAuthority,
    auth: &AuthContext,
) -> Response {
    let mut cx = match crate::kql::Context::open(
        store,
        space,
        request.parameters.as_ref(),
        operation.parameters.as_ref(),
        authority,
        auth,
    )
    .await
    {
        Ok(cx) => cx,
        Err(err) => return Response::from(err),
    };
    cx.traversal = crate::store::history::traversal_of(
        command,
        request.parameters.as_ref(),
        operation.parameters.as_ref(),
    );
    if let Err(error) = bind_meta_read(&mut cx, command, request).await {
        return Response::from(error);
    }
    let environment_version = cx.env.version;

    match run(&mut cx, command).await {
        Ok(Answer {
            result,
            next_cursor,
            warnings,
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
                warnings,
                ..anda_kip::OperationResult::ok(result)
            }],
            ..Default::default()
        },
        Err(err) => Response::from(err),
    }
}

/// META histories preserve their first page's coordinate. Catalogs and the
/// live search index expire instead of silently continuing over changed state.
async fn bind_meta_read(
    cx: &mut crate::kql::Context<'_>,
    command: &MetaCommand,
    request: &Request,
) -> Result<(), KipError> {
    use crate::store::history::CursorFamily;
    use anda_kip::HistoryCommand;
    match command {
        MetaCommand::History(history) => {
            let scalar = match history {
                HistoryCommand::Element { cursor, .. } | HistoryCommand::Space { cursor, .. } => {
                    cursor.as_ref()
                }
            };
            let cursor = scalar
                .map(|s| read_cursor(cx, s, CursorFamily::History))
                .transpose()?;
            cx.bind_read(None, request, cursor).await
        }
        MetaCommand::List(list) => {
            if let Some(scalar) = &list.cursor {
                let cursor = read_cursor(cx, scalar, CursorFamily::List)?;
                if cursor.snapshot_seq != cx.pinned_seq {
                    return Err(KipError::cursor_expired(
                        "list",
                        "catalog changed; start a new traversal",
                    ));
                }
            }
            if request
                .read
                .as_ref()
                .is_some_and(|r| r.snapshot_token.is_some())
            {
                return Err(KipError::unsupported_capability(
                    "LIST does not support snapshot_token reads",
                ));
            }
            Ok(())
        }
        _ if request
            .read
            .as_ref()
            .is_some_and(|r| r.snapshot_token.is_some()) =>
        {
            Err(KipError::unsupported_capability(
                "this META command does not support snapshot_token reads",
            ))
        }
        _ => Ok(()),
    }
}

/// Reads a `CURSOR` slot as the opaque token this engine issues.
///
/// §88.4: a cursor is opaque or authenticated, never a number a caller can
/// invent. §102.28 adds that one family's cursor must not continue another's,
/// which is why the family is checked rather than merely encoded.
pub(crate) fn read_cursor(
    cx: &crate::kql::Context<'_>,
    scalar: &anda_kip::Scalar,
    family: crate::store::history::CursorFamily,
) -> Result<crate::store::history::PageCursor, KipError> {
    let token = describe::scalar_str(cx, scalar, "CURSOR")?;
    let cursor =
        crate::store::history::PageCursor::from_token(&token, &cx.space, family, &cx.traversal)?;
    cursor.require_issued(cx.store, &token, &cx.auth.principal_id)?;
    Ok(cursor)
}

/// Issues the cursor for the next page, when one remains.
pub(crate) fn next_cursor(
    cx: &crate::kql::Context<'_>,
    family: crate::store::history::CursorFamily,
    consumed: usize,
    total: usize,
) -> Option<String> {
    (consumed < total).then(|| {
        crate::store::history::PageCursor {
            family,
            snapshot_seq: cx.pinned_seq,
            offset: consumed,
            traversal: cx.traversal.clone(),
        }
        .issue(cx.store, &cx.space, &cx.auth.principal_id, None)
    })
}

/// One META answer, with its page cursor when it pages.
pub struct Answer {
    /// The answer body.
    pub result: Json,
    /// The cursor for the next page, when more remain.
    pub next_cursor: Option<String>,
    /// Non-fatal caveats about the answer, carried on the operation result.
    pub warnings: Vec<Warning>,
}

impl Answer {
    /// An answer that does not page.
    pub fn whole(result: Json) -> Self {
        Self {
            result,
            next_cursor: None,
            warnings: Vec::new(),
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
        MetaCommand::Verify { target, value } => inspect::verify(cx, *target, value).await,
        MetaCommand::History(history) => history::history(cx, history).await,
        MetaCommand::Changes(changes) => history::changes(cx, changes).await,
        MetaCommand::ExportCapsule(command) => inspect::export_capsule(cx, command).await,
    }
}

/// What this engine can and cannot do, as data (§67).
///
/// Three layers, because §67 asks for three and they answer different
/// questions. **`supported`** is what this build implements. **`available`**
/// is what *this* Principal may actually request — a caller told only the
/// first will try things it will be refused for, and one told only the second
/// reads an authorization gap as a missing feature. **`limits`** is the
/// ceilings that apply to it either way.
///
/// The `unsupported` list is not an apology: an Agent that can read it will
/// not spend a turn discovering a gap, and — more importantly — will not read
/// a missing feature as a missing fact.
///
/// Built through [`anda_kip::Capabilities`] so that the profile list §89
/// requires an implementation to declare is spelled the way §89 spells it,
/// rather than being invented per engine.
pub fn capabilities(
    host: &HostCapabilities,
    authority: Option<&EffectiveAuthority>,
    auth: &AuthContext,
) -> Json {
    let capabilities = anda_kip::Capabilities {
        profiles: CONFORMANCE_PROFILES.to_vec(),
        supported: as_map(serde_json::json!({
            // §67.4: the registry names a `requires` block may ask about,
            // each with its value. Reported beside the engine's own names —
            // which stay, because clients read them — rather than instead
            // of them.
            "registry": registry_json(host),
            "kml": [
                "CREATE CONCEPT", "UPSERT CONCEPT", "ENSURE PROPOSITION",
                "CREATE EVIDENCE", "CREATE ASSERTION", "CREATE ACTIVITY",
                "ASSERT (desugared)", "UPDATE", "TRANSITION", "SET RETENTION",
                "PURGE", "PURGE PAYLOAD", "MERGE CONCEPT", "WHERE selection blocks",
                "LIMIT", "EXPECT VERSION [OF plane]"
            ],
            // §52.5: one lifecycle statement, and what it moves.
            "transition": {
                "states": anda_kip::transition_state::ALL,
                "by": anda_kip::transition_state::WITH_BY,
                "finalizing": anda_kip::transition_state::ACTIVITY,
                "same_state": "no_effect",
                "illegal_move": "InvalidLifecycleTransition with details {from, to}",
                "terminal_activity": "ActivityTerminal"
            },
            // §6.3, §35.1: the version planes a guard may name, and the rule
            // each counter advances by.
            "version_planes": {
                "planes": ["attributes", "structural", "retention", "facets.<Symbol>"],
                "advances": "a plane counter moves once per committed transaction that \
                             changed that plane; _system.version moves on every change",
                "creation": "a new element starts every plane it carries content in at 1 \
                             and every other plane at 0",
                "guard": "EXPECT VERSION n OF <plane> compares the plane counter; the bare \
                          guard compares _system.version, and only the bare 0 is create-only"
            },
            // §11: identity consolidation is non-destructive, and the three
            // rules that make it so are stated because a caller who assumed
            // any of them backwards would read a forwarded write as a lost one.
            "merge": {
                "source": "stays addressable, in state `merged`, forwarding via merged_into",
                "history": "a Proposition written before the merge keeps referring to what it \
                            referred to (§11.2); raw history is not rewritten",
                "new_writes": "canonicalized to the surviving identity (§11.3) — tuple endpoints, \
                               asserted_by, and structural references on create and on \
                               SET STRUCTURAL",
                "cycles": "a merge whose target already resolves back to the source is refused \
                           (§11.1), so following merged_into to its fixpoint always terminates"
            },
            "selection": {
                // §52.7: a bounded sweep may be assumed repeatable only where
                // the runtime documents an order. This one does.
                "limit_order": "ascending element id",
                // A selection block reads the state the transaction started
                // from, so a sweep cannot act on what the same MUTATE created.
                "reads": "transaction snapshot"
            },
            "kql": [
                "CONCEPT", "PROPOSITION", "ASSERTION", "EVIDENCE", "ACTIVITY",
                "STRUCTURAL", "BELIEF", "BELIEF SLOT", "FILTER", "NOT",
                "OPTIONAL", "UNION", "ORDER BY", "LIMIT", "CURSOR", "FOR TIME",
                "WITH EPISTEMIC", "aggregates", "predicate alternation",
                "hop quantifiers", "AS OF SEQ", "canonical matching through merged_into",
                "?p.canonical_subject / ?p.canonical_object"
            ],
            "epistemic": {
                // §49's settings, honored rather than parsed and dropped.
                // Every member `WITH EPISTEMIC` accepts, and what each does —
                // an unlisted one is refused (SchemaFieldNotFound), so the list
                // is the contract rather than a sample.
                "settings": [
                    "policy", "accept", "material", "modes",
                    "include_hypothetical", "include_predicted", "explanation",
                    "purpose", "risk", "include_historical"
                ],
                "settings_note": "`purpose` and `risk` are the caller's own non-authoritative \
                                  context and do not move a verdict; `include_historical` is \
                                  accepted and refused, because admitting retracted and \
                                  superseded Assertions would let a withdrawn claim decide a \
                                  current belief",
                "explanation_levels": ["none", "summary", "ledger"],
                // §25.1 and §92: both conflict shapes, not just the strong one.
                "conflicts": ["functional", "exclusive values"]
            },
            "meta": [
                "DESCRIBE", "LIST", "LIST DEPENDENTS", "SEARCH", "VALIDATE",
                "PREVIEW KML", "PREVIEW IMPORT CAPSULE", "HISTORY", "CHANGES",
                "DESCRIBE SNAPSHOT [AS OF SEQ | AT TIME]", "EXPORT CAPSULE",
                "VERIFY CAPSULE", "DESCRIBE CAPSULE"
            ],
            // §63.5: what this engine actually traverses, stated because the
            // Structural-Field extension is optional and an Agent that assumed
            // it would read a missing route as an absent dependent.
            "dependents": {
                "traverses": "Activity inputs -> Activity -> Activity outputs",
                "structural_lineage": false,
                "default_depth": 1,
                "max_depth": describe::MAX_DEPENDENTS_DEPTH,
                "row": ["id", "kind", "distance", "via.activity"],
                "truncated": "a coded `truncated` warning on the result when an element the \
                              caller may not discover cut the traversal (§63.5)",
                "note": "a transformation that recorded no Activity provenance is not \
                         discoverable here"
            },
            // §36.1, §68.1: HISTORY and CHANGES are the same unit — one
            // committed transition — asked for over different ranges, so they
            // answer in one shape. Stated because a consumer that assumed a
            // flat change list would lose the atomicity §36.2 guarantees and
            // the deduplication key §36.3 needs.
            "change_stream": {
                "grain": "one Change Envelope per committed transition",
                "envelope": [
                    "space_id", "space_seq", "tx_id", "committed_at",
                    "transaction_class", "snapshot_seq", "status",
                    "schema_environment_version", "changes"
                ],
                "change": [
                    "op", "kind", "id", "schema_ref", "old_version", "new_version",
                    "state", "refs", "touched", "planes"
                ],
                "ops": ["create", "update", "lifecycle", "retention", "merge", "purge",
                        "payload_purge"],
                "touched": "changed paths, names only: attributes.<name>, fields.<name>, \
                            facets.<Symbol>, structural.<field>, retention, governance.<member>",
                "planes": "the element's complete plane counters after the commit, on every \
                           entry that moved a plane",
                "deduplicate_by": "space_id + space_seq + tx_id",
                "shared_by": ["HISTORY ELEMENT", "HISTORY SPACE", "CHANGES"],
                // The cursor is the coordinate the page consumed, issued
                // whenever it consumed one — not only when the stream was
                // truncated, and never taken from the rows that survived the
                // visibility filter.
                "cursor": "the last space_seq consumed, opaque to nobody"
            },
            "paging": {
                "continuations": "principal-bound server mapping; expires after reconnect or eviction",
                "retained_continuations": 1024,
                // §44.8 and §88.4: a cursor is opaque, carries the coordinate the
                // traversal began at, and belongs to the family that issued it.
                "cursor": "opaque token, snapshot-pinned, per operation family",
                "families": ["kql", "search", "list", "history", "changes"],
                "refusals": "CursorInvalid {family, reason: malformed} for a token this engine \
                             did not issue for this Space and family; a bound of the wrong type \
                             is TypeMismatch"
            },
            "structural": {
                // §8.2 and §17: the pattern reads both planes. A Profile field
                // is addressed by its resolved symbol, a Core one by its plain
                // name, and `?edge.field` says which answered — so a Profile
                // that declares a field named `evidence` adds edges rather
                // than changing what an Assertion cites.
                "planes": {
                    "profile": "addressed by resolved symbol; ordered where declared",
                    "core": "Assertion.evidence and .context, Evidence.source and \
                             .generated_by, Activity.inputs, .outputs and \
                             .associated_actors — addressed by plain name, and reporting \
                             no `index`, because their order is storage order rather \
                             than a declared position"
                },
                // §17.4: an ordered field keeps one dense zero-based order per
                // source element, and exposes each reference's position.
                "ordered_fields": true,
                "edge_binding": "?edge STRUCTURAL (...) binds virtual edge state \
                                 carrying source, field, target and index",
                "single_cardinality": "SET STRUCTURAL replaces rather than appends"
            },
            "envelope": {
                // What the runtime honors from the request envelope, stated
                // because ignoring one of these changes what the caller gets.
                "preconditions": ["space_seq", "schema_environment_version"],
                "requires": "capability names are checked against this list before \
                             the command runs",
                "ingest": "Evidence minted from the transport envelope inside the \
                           command's own transaction (§71.1)",
                "client_key": "a CREATE under a client_key already used resolves to \
                               that element instead of creating a second (§52.1)"
            },
            "retention": {
                // §19.2: this is storage lifecycle, never world validity.
                "hook": ["retention_class", "expires_at", "legal_hold"],
                "expiry": "enforced by an explicit sweep the host runs, not by a \
                           background timer: forgetting happens when a Principal \
                           asks for it and is accountable for it",
                "actions": ["archive", "tombstone"],
                // §19.1 and §60.3. Stated because the replacement semantics and the gate
                // are one contract: a caller who read only the first would
                // expect an omitted `legal_hold` to leave the hold alone.
                "set": "SET RETENTION replaces the whole block rather than \
                        patching it, so an omitted member is cleared",
                "legal_hold": "gated in both directions — placing a hold needs \
                               `legal_hold`, and so does any SET RETENTION over \
                               an element that currently holds one, because \
                               replacement would otherwise lift it silently"
            },
            "capsule": {
                // The import itself is a host operation: KML has no import
                // clause and META is read-only, so a command cannot decide
                // that this Space accepts another Brain's cognition.
                "import_modes": ["preview", "merge", "isolate"],
                "identity_resolution": ["prior import", "canonical_id", "proposition tuple"]
            },
            "execution_modes": ["independent", "sequence"],
            // §33.2, §75: where a Receipt sits and what it carries.
            "receipts": {
                "location": "results[].receipt for every state-changing operation; the \
                             top-level receipt is reserved for atomic execution",
                "no_effect": "carries no space_seq",
                "receipt_digest": "sha3-256 over RFC 8785 canonical JSON of the Receipt \
                                   without receipt_digest, proofs and extensions",
                "origin": ["principal_id", "actor_binding_id", "delegation_digest"],
                "on_error_default": "stop; the operations not started are reported skipped"
            },
            "historical_read": {
                // Every commit appends the row it wrote, so a past coordinate
                // is reconstructed rather than approximated.
                "retention": "unbounded: every element version is kept",
                "coordinates": ["SEQ"],
                "resolution": "a transaction id resolves through DESCRIBE TRANSACTION, an \
                               instant through DESCRIBE SNAPSHOT AT TIME",
                "snapshot_token": true,
                // The indexes describe the present, so a historical pattern
                // reconstructs its candidates from the version log.
                "cost": "a historical read scans the version log for its Space"
            },
            "search_modes": ["keyword"],
            "lifecycle": {
                "states": ["active", "archived", "quarantined", "tombstoned", "merged", "purged"],
                // Quarantine is not retraction and not archival: it says this
                // Brain does not currently allow ordinary use, which is a
                // statement about the Brain rather than about the source.
                "quarantine": "excluded from ordinary recall, readable by a reviewer",
                // §14.3: expiry is neither retraction nor supersession. Nobody
                // withdrew these; their own stated windows ran out.
                "assertion_expiry": "an explicit pass marks Assertions whose \
                                     valid_time closed, and a projection at a \
                                     coordinate the window covered still admits them",
                "purge": {
                    "reference_policies": [
                        "deny_if_referenced", "tombstone_reference", "authorized_cascade"
                    ],
                    "default_reference_policy": "deny_if_referenced",
                    "leaves": "an identity stub carrying a content digest",
                    "destroys": "every recorded version of the element"
                },
                // §60.6: the data-minimization instrument. Byte destruction
                // that keeps the evidence event, which is a different promise
                // from element purge and worth stating as one.
                "payload_purge": {
                    "targets": "Evidence only",
                    "destroys": "inline payload and content_ref bytes, in the current row \
                                 and in every recorded version",
                    "keeps": "identity, evidence_class, content_digest, media_type, \
                              observed_at, source, generated_by, citations",
                    "reports": "payload.mode becomes \"purged\"",
                    "repeat": "purging an already-purged payload is a no_effect"
                }
            },
            "transactions": {
                "atomic_visibility": "in_process",
                // Recorded, not replayed. A key is stored on the committed
                // transaction and `DESCRIBE TRANSACTION BY IDEMPOTENCY KEY`
                // will find it. §26 and §33: a timeout is not an abort, so a
                // resend under a key this Space already committed hands back
                // that transaction's receipt instead of writing again. This is
                // the block a retry policy reads before deciding a resend is
                // free.
                "idempotency": {
                    "mode": "replayed",
                    "scope": "per Space; an operation's own key wins over the request's, \
                              so a batch sharing one key does not have its second write \
                              replay the first",
                    "answer": "the recorded receipt — same tx_id, space_seq, committed_at \
                               and handles — plus a warning saying it is a replay, because \
                               the caller resent precisely to find out whether the first \
                               attempt landed",
                    "warnings": "the original run's own warnings are not persisted and are \
                                 not reconstructed; inventing them would be worse than \
                                 saying nothing",
                    "dry_run": "never replays and is never replayed: a preview establishes \
                                no durable commit (§69.3), and answering one from an \
                                earlier real commit would report a write as a preview of \
                                itself",
                    "authorization": "the command's own permissions, checked as they would \
                                      be for the write; an outstanding approval obligation \
                                      does not block a replay, because the approval \
                                      authorized work that already happened"
                },
                "preconditions": ["EXPECT VERSION", "EXPECT VERSION OF <plane>"],
                "dry_run": true
            },
            // §21.10, §27.2: the projection capability, folded in here from
            // the statement that used to answer it on its own.
            "projection": {
                "policies": ["kip:policy:baseline", "kip:policy:forecast"],
                "statuses": ["accepted", "rejected", "contested", "uncertain", "insufficient"],
                "leading": ["support", "opposition", "none"],
                "score_semantics": "normalized_support_not_probability",
                "baseline": "structural: every eligible corroboration group counts equally",
                "weighted": true,
                "explanation": true,
                "conflict_set_expansion": true,
                "corroboration_grouping": true,
                "implemented_stages": [
                    // Not a stage the projection performs so much as one it
                    // inherits: every Assertion it reads comes through the
                    // same authorization gate every other read does, so a
                    // claim the caller may not see contributes nothing.
                    "governance_visibility",
                    "semantic_grounding", "conflict_set_expansion", "lifecycle_eligibility",
                    "temporal_eligibility", "mode_eligibility", "corroboration_grouping",
                    "aggregation", "classification", "explanation"
                ],
                "missing_stages": ["evidence_quality"]
            },
            "governance": {
                // What is enforced, stated as what it is rather than as a
                // bare "true": a client that knows the granularity knows
                // which questions this endpoint can actually answer.
                "default_deny": true,
                "deny_overrides": true,
                "principals": true,
                "groups": true,
                "grants": true,
                "delegation": "attenuating, non-transitive by default",
                "actor_bindings": true,
                "policies": "versioned, append-only",
                "approvals": "multi-party, separation of duties",
                "audit": "append-preserving: control-plane mutations and decisions",
                "historical": "policy-as-of, and which Grants were in force at an instant",
                "approvals_bind": "one operation, and are spent by using it",
                "receipt_provenance": "high-impact commits carry the identity and policy \
                                       version that authorized them",
                "enforcement": "every KQL, KML and META command is authorized before it \
                                runs, and every element a read or a write touches is \
                                authorized again",
                "element_scope": {
                    // What per-element authorization actually does here.
                    "visibility": "an element the caller may not read is outside the query \
                                   universe: not matched, not counted, not ranked, not paged",
                    "field_mask": "a Grant's `fields` narrows the view before FILTER and \
                                   ORDER BY read it, so a mask cannot be probed by membership",
                    "raw_origin": "`_system.origin` needs read_raw_origin; without it the \
                                   member says it was withheld rather than disappearing",
                    "counts": "withheld, with a reason, for a Principal whose read authority \
                               is narrower than the Space",
                    "writes": "every mutation target is authorized individually, and a sweep \
                               that reaches one it may not touch fails rather than doing less",
                    "protected_fields": "`governance` is refused by the protocol's parser, \
                                         `authority_class` and `classification` as \
                                         ProtectedGovernanceField; `retention` needs \
                                         manage_retention wherever it is written, and its \
                                         legal_hold manage_legal_hold",
                    "record_outcome": "outcome-class Evidence and outcome_observation \
                                       Activities need record_outcome, by KML and by ingest",
                    "attribution": "which epistemic permission a new Assertion needs is \
                                    decided by the writer's ActorBinding, not by the command",
                    "retraction": "recorded only by the Principal that wrote the Assertion or \
                                   one representing its actor; moderation uses ARCHIVE"
                },
                "permission_registry": "DESCRIBE ACCESS"
            }
        })),
        available: available(authority, auth),
        limits: as_map(serde_json::json!({
            // A read is bounded by elements examined rather than by a clock:
            // a timeout makes the same query succeed or fail depending on
            // machine load, which is not a property a caller can plan around.
            "kql_elements_examined": crate::kql::MAX_CANDIDATES,
            "kql_intermediate_work": crate::kql::MAX_SOLUTIONS,
            "search_results_per_page": 100,
            "meta_page_default": 100,
        })),
        extensions: as_map(serde_json::json!({
            "kip": anda_kip::KIP_VERSION,
            "languages": ["KQL", "KML", "META"],
            "unsupported": unsupported_prose(),
        })),
    };
    serde_json::to_value(capabilities).unwrap_or(Json::Null)
}

/// The `unsupported` prose (§67.1): each gap this engine declares, with what it
/// covers and why it is missing. Kept as data in `unsupported.json` so the two
/// engines' lists can be diffed side by side and this file is not a page of
/// string literals; the `capability` names are still held to
/// `UNSUPPORTED_NAMES` by the tests below.
fn unsupported_prose() -> Json {
    static PROSE: std::sync::LazyLock<Json> = std::sync::LazyLock::new(|| {
        serde_json::from_str(include_str!("unsupported.json"))
            .expect("unsupported.json is the well-formed prose this file ships")
    });
    PROSE.clone()
}

/// The §89 conformance levels this engine claims.
///
/// A claim, not a wish: §89 makes a level rest on the engine suite run against
/// the engine itself, and `tests/conformance.rs` runs KIP's suite here — world
/// time, `functional_by`, `kip:memory-default` and the Search Pattern
/// included, and the optional draft vocabulary (`DEFINE`). Several operations
/// in one transaction is the `atomic_batch` capability, not a level
/// requirement.
///
/// `KIP-CognitiveMemory` is not claimed: it adds recording repair (§57.8),
/// which this engine lacks and lists in `unsupported`.
pub const CONFORMANCE_PROFILES: &[anda_kip::ConformanceProfile] =
    &[anda_kip::ConformanceProfile::Core];

/// What the calling Principal may request, in at least some scope (§67.2).
///
/// Not a Grant dump (§67.2) and not a promise: "available" means the command
/// family will not be refused at the Space gate, never that every element
/// inside it is readable. Without an authority resolved — the capability
/// answer is reachable unauthenticated, which is how a caller learns *how* to
/// authenticate (§67.2) — the list is omitted rather than guessed.
fn available(authority: Option<&EffectiveAuthority>, auth: &AuthContext) -> Map<String, Json> {
    let Some(authority) = authority else {
        return Map::new();
    };
    let resource = crate::governance::ResourceContext::default();
    let mut granted = Vec::new();
    for permission in crate::governance::Permission::ALL {
        if authority
            .authorize(*permission, &resource, auth)
            .is_permitted()
        {
            granted.push(Json::String(permission.as_str().to_string()));
        }
    }
    as_map(serde_json::json!({
        "principal_id": auth.principal_id,
        "permissions": granted,
        "note": "a permitted command family is not a promise about every element in it; \
                 per-element authorization still applies",
    }))
}

fn as_map(value: Json) -> Map<String, Json> {
    match value {
        Json::Object(map) => map,
        _ => Map::new(),
    }
}

/// Whether this engine implements one named capability, for `requires` (§67).
///
/// `None` means the name is not one this engine knows — neither the §67.4
/// registry nor a name this engine reports. A fail-fast check that passes
/// because nobody recognized the requirement is worse than no check at all,
/// because the caller believes it ran.
pub fn capability_state(host: &HostCapabilities, name: &str) -> Option<bool> {
    if let Some(state) = host.state(name) {
        return Some(state);
    }
    if let Some((_, supported, _)) = REGISTRY.iter().find(|(entry, _, _)| *entry == name) {
        return Some(*supported);
    }
    if UNSUPPORTED_NAMES.contains(&name) {
        return Some(false);
    }
    SUPPORTED_NAMES.contains(&name).then_some(true)
}

/// The §67.4 capability registry, as this engine answers it.
///
/// Name, whether it is supported, and the value `DESCRIBE CAPABILITIES`
/// reports — the boolean itself unless the registry gives the entry a richer
/// value. A runtime MUST NOT rename these.
const REGISTRY: &[(&str, bool, Option<&str>)] = &[
    // §32.2: mutations serialize behind one write lock that readers share.
    ("serializable_isolation", true, None),
    ("atomic_batch", false, None),
    // §34.5: every journalled transaction is kept, so a key never expires.
    (
        "idempotency_retention",
        true,
        Some(r#"{"unbounded": true}"#),
    ),
    ("historical_reads", true, None),
    ("historical_search", false, None),
    ("semantic_search", false, None),
    ("hybrid_search", false, None),
    // §66.5: the keyword index is written by the committing transaction.
    (
        "search_index_freshness",
        true,
        Some(r#"{"mode": "synchronous"}"#),
    ),
    // §21.10: the structural baseline; no trust-weighted policy.
    ("weighted_projection", true, None),
    ("signed_receipts", false, None),
    ("streaming", false, None),
    ("artifacts", true, None),
    ("change_stream", true, None),
    ("filtered_delivery", true, None),
    ("watch_evaluation", true, None),
    // §66.8: `Session::record_exposures` / `read_exposures`, outside the
    // cognitive store and erased with the element.
    ("exposure_log", true, None),
    ("draft_vocabulary", true, None),
    ("identity_repair", true, None),
    // §57.8: the protected repair, `_system.recording_validity` and the
    // projection exclusion (`Session::repair_recording`).
    ("recording_repair", true, None),
    ("derive_permission", true, None),
    ("record_outcome_permission", true, None),
    ("capsule_export", true, None),
    ("capsule_import", true, None),
    ("capsule_signatures", false, None),
    ("kip1_migration", true, None),
    ("memory_interface", false, None),
    ("durable_brain_runtime", false, None),
    ("receiver_fencing", false, None),
    ("prospective_trials", false, None),
];

/// The registry as `DESCRIBE CAPABILITIES` reports it (§67.4), with the
/// host's answers for the names it owns ([`host::HOST_NAMES`]).
fn registry_json(host: &HostCapabilities) -> Json {
    let mut out = Map::new();
    for (name, supported, value) in REGISTRY {
        let value = match (host.registry_value(name), value) {
            (Some(hosted), _) => hosted,
            (None, Some(text)) => serde_json::from_str(text).unwrap_or(Json::Bool(*supported)),
            (None, None) => Json::Bool(*supported),
        };
        out.insert((*name).to_string(), value);
    }
    Json::Object(out)
}

/// The capability names `requires` may ask about and get `true` for.
///
/// Spelled out rather than derived from the `supported` map, because the map
/// is organized for a reader and this list is a contract: a name here is one a
/// caller may build a fail-fast check on.
const SUPPORTED_NAMES: &[&str] = &[
    "trust_model",
    "trust_governance",
    "artifact_store",
    "kql",
    "kml",
    "meta",
    "governance",
    "projection",
    "historical_read",
    "keyword_search",
    "capsule_export",
    "capsule_import",
    "client_key_retry",
    "set_retention",
    "structural_core_fields",
    "hop_quantifiers",
    "idempotent_replay",
    "ingest",
    "preconditions",
    "dry_run",
    "snapshot_token",
    "ordered_structural",
    "structural_edge_binding",
    "exclusive_conflict",
    "space_self_identity",
    "discover_read_separation",
    "retention_expiry",
    "opaque_cursors",
    "payload_purge",
    "list_dependents",
    // §67.4: registry entries of earlier drafts that are now level
    // requirements, still answered as engine-local names.
    "belief_slot",
    "ingestion_context",
    "dependency_validity",
    // §52.5, §35.1, §68, §33.2, §12.3, §20.14: the vocabulary the 2026-09-02
    // consolidation added. Every one of them is built here; they are named so
    // a `requires` block gets `true` rather than the `unrecognized` §67.4
    // makes a failure.
    "transition",
    "version_planes",
    "snapshot_at_time",
    "per_operation_receipts",
    "canonical_matching",
    "symbol_lineage",
    // §44.6: implicit grouping over the non-aggregated projected expressions,
    // and ORDER BY over an aggregate.
    "grouped_aggregation",
    // §76: `anda_kip::execute_readonly` is the read-only path in front of this
    // engine — it refuses a mutation on parsed semantics, so no envelope field
    // can talk a write past it.
    "readonly_endpoint",
];

/// The capability names this engine reports as *not* implemented.
///
/// Kept beside [`SUPPORTED_NAMES`] so the two cannot drift into claiming and
/// disclaiming the same thing; the unit test below checks they do not overlap.
const UNSUPPORTED_NAMES: &[&str] = &[
    "unregistered_permissions",
    "capsule_digest_profiles",
    "historical_search",
    "semantic_search",
    "search_over_assertions_and_activities",
    "capsule_restore_mode",
    "capsule_signatures",
    "retention_policy",
    "deadlines",
    "nested_proposition_endpoint",
    "materialized_projection",
];

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

#[cfg(test)]
mod tests {
    use super::*;

    /// The two capability lists are one contract read from two directions.
    ///
    /// A name in both would let `requires` answer `true` and `false` for the
    /// same question depending on which list was consulted first — and a
    /// fail-fast check that can go either way is worse than none.
    #[test]
    fn no_capability_is_both_claimed_and_disclaimed() {
        for name in SUPPORTED_NAMES {
            assert!(
                !UNSUPPORTED_NAMES.contains(name),
                "{name} is in both capability lists"
            );
            assert_eq!(
                capability_state(&HostCapabilities::default(), name),
                Some(true)
            );
        }
        for name in UNSUPPORTED_NAMES {
            assert_eq!(
                capability_state(&HostCapabilities::default(), name),
                Some(false)
            );
        }
        // An unknown name is not "supported by omission" (§67): a `requires`
        // check that passed because nobody recognized it is the failure mode
        // this exists to prevent.
        assert_eq!(
            capability_state(&HostCapabilities::default(), "read_everything"),
            None
        );
    }

    /// The §67.4 registry and the engine's own names agree about every gap
    /// both of them name, and every registry entry is answerable.
    #[test]
    fn the_registry_agrees_with_the_local_names() {
        for (name, supported, _) in REGISTRY {
            assert_eq!(
                capability_state(&HostCapabilities::default(), name),
                Some(*supported),
                "{name}"
            );
            if UNSUPPORTED_NAMES.contains(name) {
                assert!(
                    !supported,
                    "{name} is disclaimed locally and claimed by the registry"
                );
            }
            if SUPPORTED_NAMES.contains(name) {
                assert!(
                    supported,
                    "{name} is claimed locally and disclaimed by the registry"
                );
            }
        }
        let declared = capabilities(&HostCapabilities::default(), None, &AuthContext::system());
        for (name, _, _) in REGISTRY {
            assert!(
                declared["supported"]["registry"].get(name).is_some(),
                "{name} is not reported"
            );
        }
        assert_eq!(
            declared["supported"]["registry"]["weighted_projection"],
            true
        );
        assert_eq!(
            capability_state(&HostCapabilities::default(), "belief_slot"),
            Some(true)
        );
        assert!(declared["supported"]["projection"]["missing_stages"].is_array());
    }

    /// The prose gap list and the `requires` registry name the same gaps.
    ///
    /// `DESCRIBE CAPABILITIES` answers two audiences from one set of facts: an
    /// Agent reading `unsupported` for a reason, and a `requires` check asking
    /// a yes/no question (§67). A gap documented in the first and missing from
    /// the second reports itself as *unrecognized* rather than as absent — and
    /// §67's whole point is that an unrecognized requirement must not pass.
    #[test]
    fn every_documented_gap_is_a_capability_requires_can_ask_about() {
        let declared = capabilities(&HostCapabilities::default(), None, &AuthContext::system());
        let listed = declared["unsupported"]
            .as_array()
            .expect("an unsupported list");
        assert!(!listed.is_empty());
        for entry in listed {
            let name = entry["capability"].as_str().expect("a capability name");
            assert_eq!(
                capability_state(&HostCapabilities::default(), name),
                Some(false),
                "{name} is documented as a gap but `requires` does not know it"
            );
        }
        // And the other direction. A disclaimed name with no entry answers
        // `requires` correctly and tells the Agent reading `unsupported`
        // nothing about why — which is the half of §67 that is for a reader.
        let documented: Vec<&str> = listed
            .iter()
            .filter_map(|entry| entry["capability"].as_str())
            .collect();
        for name in UNSUPPORTED_NAMES {
            assert!(
                documented.contains(name),
                "{name} is disclaimed with no `unsupported` entry saying why"
            );
        }
    }

    /// The two engines answer for the same vocabulary (§67.4).
    ///
    /// `rs/anda_kip/capabilities.json` is the shared list, and this is the
    /// half of it this engine owns: every name there is answered, and every
    /// name answered is there. Six names once lived in `ts/kip-do` alone, for
    /// capabilities this engine had also built, so a client that fail-fast
    /// checked them here was refused a capability it was standing on.
    #[test]
    fn the_shared_capability_vocabulary_is_answered_in_full() {
        let mut answered: Vec<&str> = SUPPORTED_NAMES
            .iter()
            .chain(UNSUPPORTED_NAMES.iter())
            .copied()
            .collect();
        answered.sort_unstable();
        let shared: Vec<&str> = anda_kip::capability_engine_names()
            .iter()
            .map(String::as_str)
            .collect();
        assert_eq!(answered, shared);

        // The §67.4 registry is the Specification's, not this engine's.
        let registry: Vec<&str> = REGISTRY.iter().map(|(name, _, _)| *name).collect();
        let spec: Vec<&str> = anda_kip::capability_registry_names()
            .iter()
            .map(String::as_str)
            .collect();
        assert_eq!(registry, spec);
    }

    /// §89's levels are the only names a claim may use, and every claimed
    /// level is one this engine is held to.
    #[test]
    fn the_declared_levels_are_the_ones_the_spec_names() {
        let declared = capabilities(&HostCapabilities::default(), None, &AuthContext::system());
        let profiles: Vec<&str> = declared["profiles"]
            .as_array()
            .map(|list| list.iter().filter_map(Json::as_str).collect())
            .unwrap_or_default();
        let claimed: Vec<&str> = CONFORMANCE_PROFILES.iter().map(|p| p.as_str()).collect();
        assert_eq!(profiles, claimed);
        for name in profiles {
            assert!(
                anda_kip::ConformanceProfile::from_wire(name).is_some(),
                "{name} is not a level §89 names"
            );
        }
    }
}
