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
    Json, KipError, Map, MetaCommand, Operation, Request, Response, ResponseContext, ResultContext,
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
    crate::store::history::PageCursor::from_token(&token, &cx.space, family)
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
        }
        .to_token(&cx.space)
    })
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
pub fn capabilities(authority: Option<&EffectiveAuthority>, auth: &AuthContext) -> Json {
    let capabilities = anda_kip::Capabilities {
        profiles: CONFORMANCE_PROFILES.to_vec(),
        supported: as_map(serde_json::json!({
            "kml": [
                "CREATE CONCEPT", "UPSERT CONCEPT", "ENSURE PROPOSITION",
                "CREATE EVIDENCE", "CREATE ASSERTION", "CREATE ACTIVITY",
                "ASSERT (desugared)", "UPDATE", "RETRACT ASSERTION",
                "SUPERSEDE ASSERTION", "CORRECT EVIDENCE", "TRANSITION ACTIVITY",
                "SET RETENTION", "ARCHIVE", "TOMBSTONE", "MERGE CONCEPT",
                "WHERE selection blocks", "LIMIT"
            ],
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
                "hop quantifiers", "AS OF SEQ | TX | TIME"
            ],
            "epistemic": {
                // §49's settings, honored rather than parsed and dropped.
                "settings": [
                    "policy", "accept", "material", "modes",
                    "include_hypothetical", "include_predicted", "explanation"
                ],
                "explanation_levels": ["none", "summary", "ledger"],
                // §25.1 and §92: both conflict shapes, not just the strong one.
                "conflicts": ["functional", "exclusive values"]
            },
            "meta": [
                "DESCRIBE", "LIST", "SEARCH", "VALIDATE", "PREVIEW KML",
                "PREVIEW IMPORT CAPSULE", "HISTORY", "CHANGES", "SNAPSHOT",
                "EXPORT CAPSULE", "VERIFY CAPSULE", "DESCRIBE CAPSULE"
            ],
            "paging": {
                // §44.8 and §88.4: a cursor is opaque, carries the coordinate the
                // traversal began at, and belongs to the family that issued it.
                "cursor": "opaque token, snapshot-pinned, per operation family",
                "families": ["find", "search", "list", "history"]
            },
            "structural": {
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
                "actions": ["archive", "tombstone"]
            },
            "capsule": {
                // The import itself is a host operation: KML has no import
                // clause and META is read-only, so a command cannot decide
                // that this Space accepts another Brain's cognition.
                "import_modes": ["preview", "merge", "isolate"],
                "identity_resolution": ["prior import", "canonical_id", "proposition tuple"]
            },
            "execution_modes": ["independent", "sequence"],
            "historical_read": {
                // Every commit appends the row it wrote, so a past coordinate
                // is reconstructed rather than approximated.
                "retention": "unbounded: every element version is kept",
                "coordinates": ["SEQ", "TX", "TIME"],
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
                }
            },
            "transactions": {
                "atomic_visibility": "in_process",
                // Recorded, not replayed. A key is stored on the committed
                // transaction and `DESCRIBE TRANSACTION BY IDEMPOTENCY KEY`
                // will find it, which is what lets a client that lost a
                // response discover the outcome — but the write path does not
                // check the key first, so re-sending re-executes. Stated as
                // what it is: reporting `true` here is what a retry policy
                // reads before deciding a resend is free.
                "idempotency": "recorded_not_replayed",
                "preconditions": ["EXPECT VERSION", "EXPECT STATE"],
                "dry_run": true
            },
            "projection": {
                "policies": ["kip:policy:baseline", "kip:policy:forecast"],
                "explanation": true,
                "conflict_set_expansion": true,
                "corroboration_grouping": true
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
                    "protected_fields": "`governance` is refused by the protocol's parser; \
                                         `retention` needs manage_retention wherever it is \
                                         written",
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
            "search_results_per_page": 100,
            "meta_page_default": 100,
        })),
        extensions: as_map(serde_json::json!({
                "kip": anda_kip::KIP_VERSION,
                "languages": ["KQL", "KML", "META"],
                "unsupported": [
            {
                "capability": "atomic_batch",
                "detail": "execution.mode \"atomic\" over several operations",
                "reason": "one transaction, one snapshot and all-or-none commit across \
                           operations are not implemented; a batch runs operation by operation"
            },
            {
                "capability": "idempotent_replay",
                "detail": "execution.idempotency_key returning the original outcome on a resend",
                "reason": "the key is recorded on the committed transaction and is findable with \
                           DESCRIBE TRANSACTION BY IDEMPOTENCY KEY, but the write path does not \
                           look it up before executing: a resend commits a second time rather \
                           than replaying the first, and a resend under a key that named a \
                           different request is not detected either. A client that lost a \
                           response must look the transaction up before retrying — which is what \
                           the outcome_lookup_required retry class is telling it to do. \
                           ts/kip-do has the same gap"
            },
            {
                "capability": "grouped_aggregation",
                "detail": "FIND(?c.name, COUNT(?x)) and ORDER BY COUNT(?x)",
                "reason": "a plain variable projected beside an aggregate, or an aggregate used \
                           as a sort key, needs grouping. Answering either without it returns one \
                           global row where the caller asked for one per group, or sorts by the \
                           bare variable instead of the aggregate"
            },
            {
                "capability": "capsule_digest_profiles",
                    "detail": "verifying a Capsule digested under an algorithm other than sha3-256",
                    "reason": "this engine digests a Capsule as sha3-256 over RFC 8785 canonical \
                               JSON. An artifact under another profile — ts/kip-do writes sha256 — \
                               is refused as an unsupported profile rather than reported as a digest \
                               mismatch, because the second is an accusation of tampering and the \
                               first is the truth"
                },
                {
                    "capability": "structural_core_fields",
                "detail": "STRUCTURAL over an Assertion's evidence, an Activity's inputs/outputs, \
                           an Evidence record's source",
                "reason": "the pattern walks Profile structural fields only, so it cannot ask \
                           which Assertions cite a given Evidence. The derived index holds the \
                           answer; the pattern is what does not ask it. ts/kip-do has the same gap"
            },
            {
                "capability": "ungated_permissions",
                "detail": "derive, moderate_assertion, share, bind_canonical_identity, and the \
                           control-plane management names: manage_membership, manage_grants, \
                           manage_delegation, delegate, manage_actor_binding, manage_trust, \
                           manage_schema, approve_high_risk",
                "reason": "these are registered names that no gate currently asks for, so a Grant \
                           listing one confers nothing — named here rather than discovered during \
                           an incident. Two causes. The control-plane names are host APIs by \
                           design: no KML clause reaches the plane, which is what keeps a prompt \
                           injection off it, and the consequence is that managing the plane \
                           cannot be delegated through KIP. The rest name operations this engine \
                           does not distinguish yet — setting canonical_id needs only `update`, \
                           and a moderator uses ARCHIVE or TOMBSTONE. ts/kip-do has the same gap, \
                           so closing it is a change both engines make together"
            },
            {
                "capability": "historical_search",
                "detail": "SEARCH ... AS OF SEQ",
                "reason": "the search index reflects the present only; a historical SEARCH would \
                           report today's matches as if they were then's. FIND ... AS OF reads \
                           the past exactly"
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
                "capability": "trust_governance",
                "detail": "DESCRIBE TRUST",
                "reason": "the trust policy binding is Governance state, but this engine \
                           evaluates no source trust, so there is no trust judgement to report"
            },
            {
                "capability": "capsule_restore_mode",
                "detail": "the \"restore\" import mode (§39.4)",
                "reason": "restore is same-Brain recovery, whose whole point is mapping a source \
                           autobiographical self onto the destination's. That mapping is the one \
                           thing a Capsule import must never do by resemblance, and verifying it \
                           needs identity continuity this engine does not model"
            },
            {
                "capability": "capsule_signatures",
                "detail": "signing an exported Capsule and verifying a signed one",
                "reason": "no signing keys; an exported Capsule is unsigned, and its stated \
                           source is a claim a destination cannot check"
            },
            {
                "capability": "retention_policy",
                "detail": "Space-level retention defaults by kind, type or classification (§162)",
                "reason": "retention is set per element and enforced per element; a Space cannot \
                           yet declare that raw Experiences expire in 90 days and audit records \
                           in 7 years"
            }
        ],
            })),
    };
    serde_json::to_value(capabilities).unwrap_or(Json::Null)
}

/// The §89 profiles this engine claims.
///
/// A claim, not a wish: each of these is exercised by the shared conformance
/// fixtures both engines run. `KIP-High-Assurance` is absent because this
/// engine signs nothing (§101), and `KIP-1-Migration` is present because it
/// does migrate a 1.x database (§103).
pub const CONFORMANCE_PROFILES: &[anda_kip::ConformanceProfile] = &[
    anda_kip::ConformanceProfile::Core,
    anda_kip::ConformanceProfile::Schema,
    anda_kip::ConformanceProfile::Epistemic,
    anda_kip::ConformanceProfile::Governance,
    anda_kip::ConformanceProfile::Transactions,
    anda_kip::ConformanceProfile::Capsule,
    anda_kip::ConformanceProfile::Kql,
    anda_kip::ConformanceProfile::Kml,
    anda_kip::ConformanceProfile::Meta,
    anda_kip::ConformanceProfile::Runtime,
    anda_kip::ConformanceProfile::Historical,
    anda_kip::ConformanceProfile::Migration1x,
];

/// What the calling Principal may request, in at least some scope (§67.2).
///
/// Not a Grant dump (§67.2) and not a promise: "available" means the command
/// family will not be refused at the Space gate, never that every element
/// inside it is readable. Without an authority resolved — the capability
/// answer is reachable unauthenticated, which is how a caller learns *how* to
/// authenticate (§266) — the list is omitted rather than guessed.
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
/// `None` means the name is not one this engine knows. A fail-fast check that
/// passes because nobody recognized the requirement is worse than no check at
/// all, because the caller believes it ran.
pub fn capability_state(name: &str) -> Option<bool> {
    if UNSUPPORTED_NAMES.contains(&name) {
        return Some(false);
    }
    SUPPORTED_NAMES.contains(&name).then_some(true)
}

/// The capability names `requires` may ask about and get `true` for.
///
/// Spelled out rather than derived from the `supported` map, because the map
/// is organized for a reader and this list is a contract: a name here is one a
/// caller may build a fail-fast check on.
const SUPPORTED_NAMES: &[&str] = &[
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
];

/// The capability names this engine reports as *not* implemented.
///
/// Kept beside [`SUPPORTED_NAMES`] so the two cannot drift into claiming and
/// disclaiming the same thing; the unit test below checks they do not overlap.
const UNSUPPORTED_NAMES: &[&str] = &[
    "atomic_batch",
    "idempotent_replay",
    "grouped_aggregation",
    "structural_core_fields",
    "ungated_permissions",
    "capsule_digest_profiles",
    "historical_search",
    "semantic_search",
    "trust_model",
    "trust_governance",
    "capsule_restore_mode",
    "capsule_signatures",
    "retention_policy",
    "deadlines",
    "artifact_store",
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
            assert_eq!(capability_state(name), Some(true));
        }
        for name in UNSUPPORTED_NAMES {
            assert_eq!(capability_state(name), Some(false));
        }
        // An unknown name is not "supported by omission" (§67): a `requires`
        // check that passed because nobody recognized it is the failure mode
        // this exists to prevent.
        assert_eq!(capability_state("read_everything"), None);
    }

    /// §89 makes declaring the profiles a MUST, and the names are §89's.
    #[test]
    fn the_declared_profiles_are_the_ones_the_spec_names() {
        let declared = capabilities(None, &AuthContext::system());
        let profiles = declared["profiles"].as_array().expect("a profile list");
        assert!(!profiles.is_empty());
        for profile in profiles {
            let name = profile.as_str().expect("a profile name");
            assert!(
                anda_kip::ConformanceProfile::ALL
                    .iter()
                    .any(|known| known.name() == name),
                "{name} is not a profile §89 names"
            );
        }
        // Claimed only where it is true: this engine signs nothing (§101).
        assert!(
            !profiles
                .iter()
                .any(|p| p.as_str() == Some("KIP-High-Assurance"))
        );
    }
}
