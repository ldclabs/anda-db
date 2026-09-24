//! # Changing an element's own Governance members
//!
//! An element's `governance` block — its classification, its influence-authority
//! ceiling, its policy reference — is not an author-writable field. `anda_kip`'s
//! parser refuses it in every assignment, on the text path and on the pre-parsed
//! AST path alike, so no KML statement can reach it (§50, §51). This module is
//! the other half: the authorized ways it *does* change.
//!
//! ```text
//! classify              raise a label            update
//!                       lower one                declassify
//! elevate_authority     raise a ceiling          elevate_authority + approvals
//!                       lower one                elevate_authority
//! quarantine / release  hold out of ordinary use quarantine
//! ```
//!
//! ## Why raising is ordinary and lowering is not
//!
//! Classification and authority move in opposite directions, and each has one
//! privileged direction: the one that *reveals* or *empowers*. An agent that
//! notices it has written something sensitive should be able to say so without a
//! Governance ticket, and an agent that has decided a Skill is dangerous should
//! be able to demote it immediately (§31.5). Making the cautious direction
//! privileged would make caution rare.
//!
//! ## Why non-amplification is checked at elevation, not at derivation
//!
//! Everything is created `descriptive`, the bottom of the ladder, so a derived
//! artifact cannot exceed its inputs by default — the rule holds without doing
//! anything. It becomes load-bearing only when somebody asks to *raise* one, and
//! that is where the lineage recorded at commit is read: a summary of a
//! descriptive Skill cannot become behavioral, however locally it was written
//! (§31.5).
//!
//! ## Why these commit as transactions
//!
//! Each writes a new element version and takes a Space sequence, exactly as a
//! cognitive write does. That is what keeps §48.5 answerable — *what
//! classification did this element have then* — and what puts the change in the
//! authorized change stream (§36.1). Each is recorded in the Governance audit as
//! well, because the two logs answer different questions: the version log says
//! what the element looked like, the audit says who decided that and why.

use anda_kip::{Json, KipError};

use super::approval::Approved;
use super::auth::AuthContext;
use super::decision::{EffectiveAuthority, ResourceContext};
use super::{Permission, authority, classification};
use crate::id::ElementId;
use crate::store::rows::state;
use crate::store::{Element, Store};

/// The `governance` member holding an element's influence-authority ceiling
/// (§31.3): read as `?x.governance.authority_class`, `descriptive` when absent.
pub const AUTHORITY_KEY: &str = "authority_class";
/// The `governance` member recording what a derived element was derived from.
pub const LINEAGE_KEY: &str = "authority_lineage";
/// The `governance` member recording why an element is held out of use.
pub const QUARANTINE_KEY: &str = "quarantine_reason";

/// The influence-authority ceiling an element carries (§31.3).
pub fn ceiling_of(element: &Element) -> &str {
    let stated = element
        .governance()
        .get(AUTHORITY_KEY)
        .and_then(Json::as_str)
        .unwrap_or_default();
    if stated.is_empty() {
        authority::DEFAULT
    } else {
        stated
    }
}

/// The elements a derived artifact inherits its ceiling from (§31.5).
pub fn lineage_of(element: &Element) -> Vec<String> {
    element
        .governance()
        .get(LINEAGE_KEY)
        .and_then(Json::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Sets one element's classification label (§93, §100).
///
/// Returns the label that was there before, so a caller can report the
/// transition rather than only the destination.
pub async fn classify(
    store: &Store,
    space_id: &str,
    id: ElementId,
    label: &str,
    authority: &EffectiveAuthority,
    auth: &AuthContext,
) -> Result<String, KipError> {
    let element = readable(store, space_id, id, authority, auth).await?;
    let resource = ResourceContext::of_element(&element);

    let current = element.classification().to_string();
    let effective = if current.is_empty() {
        authority.default_classification()
    } else {
        current.as_str()
    };
    let lowering = classification::rank(label) < classification::rank(effective);
    let permission = if lowering {
        Permission::Declassify
    } else {
        Permission::Update
    };
    let approved = decide(store, space_id, &resource, permission, authority, auth).await?;

    let op = if lowering { "declassify" } else { "classify" };
    let patch = |governance: &Json| set_member(governance, "classification", Json::from(label));
    apply(
        &Governed {
            store,
            space_id,
            auth,
        },
        element,
        Change {
            op,
            audit_op: op,
            new_state: None,
        },
        patch,
        |version| serde_json::json!({"from": current, "to": label, "version": version}),
        approved,
    )
    .await?;
    Ok(current)
}

/// Raises or lowers how strongly one element may influence action (§31.5).
///
/// Raising is checked against the element's authority lineage: a derived
/// artifact cannot be elevated past the lowest ceiling it was derived from, so
/// no chain of summarizing turns a descriptive note into an executable one.
///
/// Returns the ceiling the element carried before.
pub async fn elevate_authority(
    store: &Store,
    space_id: &str,
    id: ElementId,
    class: &str,
    authority_state: &EffectiveAuthority,
    auth: &AuthContext,
) -> Result<String, KipError> {
    if authority::rank(class) == 0 && class != authority::DESCRIPTIVE && !class.is_empty() {
        return Err(KipError::constraint_violation(format!(
            "{class:?} is not an influence-authority class this engine implements"
        )));
    }
    let element = readable(store, space_id, id, authority_state, auth).await?;
    let resource = ResourceContext::of_element(&element);
    // §31.5: elevation is exactly the operation a policy asks for independent
    // approval on, and §28.5 requires that one approval of two is not partial
    // activation. That is decided here rather than by the caller.
    let approved = decide(
        store,
        space_id,
        &resource,
        Permission::ElevateAuthority,
        authority_state,
        auth,
    )
    .await?;

    let current = ceiling_of(&element).to_string();
    let raising = authority::rank(class) > authority::rank(&current);
    if raising {
        let granted_ceiling = super::decision::authority_ceiling(&approved.decision().constraints);
        if authority::rank(class) > authority::rank(granted_ceiling) {
            return Err(KipError::not_authorized(format!(
                "{class:?} exceeds this Principal's influence-authority ceiling {granted_ceiling:?}"
            )));
        }
        let bound = inherited_ceiling(store, &element).await?;
        if authority::rank(class) > authority::rank(&bound) {
            return Err(KipError::not_authorized(format!(
                "{id} was derived from material capped at {bound:?}, so it cannot be raised to \
                 {class:?}. Transformation does not raise authority — elevate what it was \
                 derived from, or record an independent artifact"
            )));
        }
    }

    let op = if raising { "elevate" } else { "downgrade" };
    let patch = |governance: &Json| set_member(governance, AUTHORITY_KEY, Json::from(class));
    apply(
        &Governed {
            store,
            space_id,
            auth,
        },
        element,
        Change {
            op,
            audit_op: if raising {
                "elevate_authority"
            } else {
                "downgrade_authority"
            },
            new_state: None,
        },
        patch,
        // §31.5: an elevation record names the artifact, both ceilings, who
        // decided, and when. The transaction and the audit entry supply the
        // rest between them.
        |version| serde_json::json!({"from": current, "to": class, "version": version}),
        approved,
    )
    .await?;
    Ok(current)
}

/// Holds an element out of ordinary use, pending review (§39.2).
///
/// Not a retraction and not an archive: it says *local Governance does not
/// currently allow ordinary use of this*, which is a statement about this Brain
/// and not about the source (§39.2).
pub async fn quarantine(
    store: &Store,
    space_id: &str,
    id: ElementId,
    reason: &str,
    authority: &EffectiveAuthority,
    auth: &AuthContext,
) -> Result<(), KipError> {
    let element = readable(store, space_id, id, authority, auth).await?;
    let resource = ResourceContext::of_element(&element);
    let approved = decide(
        store,
        space_id,
        &resource,
        Permission::Quarantine,
        authority,
        auth,
    )
    .await?;
    // §31.6: quarantine holds an *active* element out of ordinary use and
    // leaves its lifecycle status alone. Holding an archived element and then
    // releasing it would return it as active, rewriting a status quarantine
    // is not allowed to touch.
    if element.state() != state::ACTIVE {
        return Err(KipError::invalid_lifecycle_transition_from(
            element.state(),
            state::QUARANTINED,
            format!(
                "{id} is {:?}; quarantine holds an active element out of ordinary use and \
                 leaves its lifecycle status alone (§31.6), so there is nothing here to hold",
                element.state()
            ),
        ));
    }
    let reason = reason.to_string();
    let patch =
        |governance: &Json| set_member(governance, QUARANTINE_KEY, Json::from(reason.as_str()));
    apply(
        &Governed {
            store,
            space_id,
            auth,
        },
        element,
        Change {
            op: "quarantine",
            audit_op: "quarantine",
            new_state: Some(state::QUARANTINED),
        },
        patch,
        |version| serde_json::json!({"reason": reason, "version": version}),
        approved,
    )
    .await?;
    Ok(())
}

/// Returns a quarantined element to ordinary use.
pub async fn release(
    store: &Store,
    space_id: &str,
    id: ElementId,
    authority: &EffectiveAuthority,
    auth: &AuthContext,
) -> Result<(), KipError> {
    let element = readable(store, space_id, id, authority, auth).await?;
    if element.state() != state::QUARANTINED {
        return Err(KipError::invalid_lifecycle_transition(format!(
            "{id} is {:?}, not quarantined; releasing it would silently revive an element that \
             was archived or tombstoned for a different reason",
            element.state()
        )));
    }
    let resource = ResourceContext::of_element(&element);
    let approved = decide(
        store,
        space_id,
        &resource,
        Permission::Quarantine,
        authority,
        auth,
    )
    .await?;
    let patch = |governance: &Json| set_member(governance, QUARANTINE_KEY, Json::Null);
    apply(
        &Governed {
            store,
            space_id,
            auth,
        },
        element,
        Change {
            op: "release",
            // `release_quarantine`, matching the reference engine: `HISTORY
            // ELEMENT` returns this verb, so a name only one engine uses is a
            // wire divergence.
            audit_op: "release_quarantine",
            new_state: Some(state::ACTIVE),
        },
        patch,
        |version| serde_json::json!({"version": version}),
        approved,
    )
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Shared plumbing
// ---------------------------------------------------------------------------

/// Decides one governed element operation, taking custody of any approval.
async fn decide(
    store: &Store,
    space_id: &str,
    resource: &ResourceContext,
    permission: Permission,
    authority: &EffectiveAuthority,
    auth: &AuthContext,
) -> Result<Approved, KipError> {
    super::approval::require(
        store,
        space_id,
        resource,
        authority.authorize(permission, resource, auth),
        auth,
    )
    .await
}

/// Writes the patched element, audits it, then spends the approval.
///
/// In that order, and in one place, because the order is the rule: an approval
/// buys a completed operation, not an attempt at one. Every governed element
/// operation ends here so that a fifth one cannot end differently.
/// Where a governed element operation runs, and as whom.
struct Governed<'a> {
    store: &'a Store,
    space_id: &'a str,
    auth: &'a AuthContext,
}

/// What a governed element operation records: the verb the transaction
/// carries, the verb the audit trail carries, and the state it moves to.
struct Change<'a> {
    op: &'static str,
    audit_op: &'static str,
    new_state: Option<&'a str>,
}

async fn apply<F, R>(
    gov: &Governed<'_>,
    element: Element,
    change: Change<'_>,
    patch: F,
    record: R,
    approved: Approved,
) -> Result<u64, KipError>
where
    F: Fn(&Json) -> Json,
    R: FnOnce(u64) -> Json,
{
    let Governed { space_id, auth, .. } = *gov;
    let id = element.id();
    let version = element
        .version()
        .checked_add(1)
        .filter(|v| *v <= anda_kip::MAX_SAFE_INTEGER)
        .ok_or_else(|| KipError::constraint_violation("element version exhausted"))?;
    let mut record = record(version);
    if let Some(object) = record.as_object_mut() {
        object.insert("element".into(), Json::String(id.to_string()));
    }
    let audit = super::rows::GovernanceAuditRow {
        entry_class: "mutation".into(),
        space_id: space_id.into(),
        resource: id.to_string(),
        principal_id: auth.principal_id.clone(),
        operation: change.audit_op.into(),
        decision: change.audit_op.into(),
        record,
        ..Default::default()
    };
    let version = commit(
        gov,
        element,
        change.op,
        change.new_state,
        patch,
        CommitAudit {
            row: audit,
            approvals: approved.into_ids(),
        },
    )
    .await?;
    Ok(version)
}

/// Loads an element the caller is entitled to see.
///
/// Reading it is the floor for every operation here: a caller who may not see
/// an element must not be able to learn what it is classified as by trying to
/// change it.
async fn readable(
    store: &Store,
    space_id: &str,
    id: ElementId,
    authority: &EffectiveAuthority,
    auth: &AuthContext,
) -> Result<Element, KipError> {
    let element = store.get_element(id).await?;
    if element.space() != space_id {
        return Err(KipError::not_found_or_not_visible(format!(
            "{id} does not live in {space_id}"
        )));
    }
    authority
        .authorize(
            Permission::Read,
            &ResourceContext::of_element(&element),
            auth,
        )
        .into_result()?;
    Ok(element)
}

/// The lowest ceiling among the material this element was derived from.
///
/// An element with no recorded lineage is not derived from anything this engine
/// knows about, so nothing bounds it beyond policy. An input that has since been
/// erased bounds it at the bottom: authority that cannot be verified is
/// authority that is not held.
async fn inherited_ceiling(store: &Store, element: &Element) -> Result<String, KipError> {
    let lineage = lineage_of(element);
    if lineage.is_empty() {
        return Ok(authority::EXECUTABLE.to_string());
    }
    let mut bound = authority::EXECUTABLE.to_string();
    for reference in lineage {
        let Ok(id) = reference.parse::<ElementId>() else {
            continue;
        };
        let input = match store.get_element(id).await {
            Ok(input) => input,
            Err(_) => return Ok(authority::DESCRIPTIVE.to_string()),
        };
        bound = authority::meet(&bound, ceiling_of(&input)).to_string();
    }
    Ok(bound)
}

/// Writes a Governance patch onto an element as its own transaction.
struct CommitAudit {
    row: super::rows::GovernanceAuditRow,
    approvals: Vec<u64>,
}

async fn commit<F>(
    gov: &Governed<'_>,
    element: Element,
    op: &'static str,
    new_state: Option<&str>,
    patch: F,
    audit: CommitAudit,
) -> Result<u64, KipError>
where
    F: Fn(&Json) -> Json,
{
    let Governed {
        store,
        space_id,
        auth,
    } = *gov;
    let cx = store
        .begin_transaction(space_id, engine_origin(auth))
        .await?;
    macro_rules! write {
        ($row:expr) => {
            put(store, &cx, *$row, op, new_state, patch, audit).await
        };
    }
    match element {
        Element::Concept(row) => write!(row),
        Element::Proposition(row) => write!(row),
        Element::Assertion(row) => write!(row),
        Element::Evidence(row) => write!(row),
        Element::Activity(row) => write!(row),
    }
}

/// Writes one row's new Governance block and records the version.
///
/// The version log entry is appended in the same commit as the row, for the
/// same reason every cognitive write does it: a history written afterwards can
/// be missing exactly the change a crash interrupted, and a history with a hole
/// answers `AS OF` wrongly instead of refusing.
async fn put<R, F>(
    store: &Store,
    cx: &crate::store::write::WriteContext,
    mut row: R,
    op: &'static str,
    new_state: Option<&str>,
    patch: F,
    audit: CommitAudit,
) -> Result<u64, KipError>
where
    R: crate::store::write::Row,
    F: Fn(&Json) -> Json,
{
    let CommitAudit {
        row: mut audit,
        approvals,
    } = audit;
    // Read before the patch, not after it: an entry whose `before` was taken
    // from the already-patched row can only ever report that nothing moved,
    // and §36.1's `touched` and `state {from, to}` are the two things a
    // follower reads to decide whether to re-read the element at all.
    let before_version = *row.envelope_mut().version;
    let before_state = row.envelope_mut().state.clone();
    let before_governance = row.envelope_mut().governance.clone();
    {
        let envelope = row.envelope_mut();
        let updated = patch(envelope.governance);
        *envelope.governance = updated;
        if let Some(state) = new_state {
            *envelope.state = state.to_string();
        }
    }
    cx.stamp_update(&mut row);
    let version = *row.envelope_mut().version;
    let id = ElementId::new(R::KIND, row.id());
    let schema_environment_version = store.get_space(&cx.space).await?.schema_environment_version;
    // The same entry shape a cognitive commit journals (§36.1): a Governance
    // decision that moved the state is a `lifecycle` entry, one that relabelled
    // the element is an `update` naming the Governance member it touched.
    let after_state = row.envelope_mut().state.clone();
    let after_governance = row.envelope_mut().governance.clone();
    let mut touched: Vec<String> = Vec::new();
    let empty = serde_json::Map::new();
    let before_members = before_governance.as_object().unwrap_or(&empty);
    let after_members = after_governance.as_object().unwrap_or(&empty);
    for member in before_members.keys().chain(after_members.keys()) {
        if before_members.get(member) != after_members.get(member) {
            touched.push(format!("governance.{member}"));
        }
    }
    touched.sort();
    touched.dedup();
    let moved = before_state != after_state || op == "expire";
    let entry = anda_kip::ChangeEntry {
        op: if moved {
            anda_kip::ChangeOp::Lifecycle
        } else {
            anda_kip::ChangeOp::Update
        },
        kind: id.kind,
        id: id.to_string(),
        schema_ref: None,
        old_version: Some(before_version),
        new_version: version,
        state: moved.then(|| anda_kip::ChangeState {
            from: lifecycle_word(&before_state, op, true),
            to: lifecycle_word(&after_state, op, false),
        }),
        refs: None,
        touched,
        planes: None,
        extensions: None,
    };
    let encoded =
        serde_json::to_value(&row).map_err(|e| KipError::internal_error(e.to_string()))?;
    let element = match R::KIND {
        anda_kip::ElementKind::Concept => Element::Concept(
            serde_json::from_value(encoded).map_err(|e| KipError::internal_error(e.to_string()))?,
        ),
        anda_kip::ElementKind::Proposition => Element::Proposition(
            serde_json::from_value(encoded).map_err(|e| KipError::internal_error(e.to_string()))?,
        ),
        anda_kip::ElementKind::Assertion => Element::Assertion(
            serde_json::from_value(encoded).map_err(|e| KipError::internal_error(e.to_string()))?,
        ),
        anda_kip::ElementKind::Evidence => Element::Evidence(
            serde_json::from_value(encoded).map_err(|e| KipError::internal_error(e.to_string()))?,
        ),
        anda_kip::ElementKind::Activity => Element::Activity(
            serde_json::from_value(encoded).map_err(|e| KipError::internal_error(e.to_string()))?,
        ),
    };
    audit.at = cx.at.clone();
    store
        .commit_plan(crate::store::control::CommitPlan {
            cx: cx.clone(),
            journal: crate::store::space::JournalEntry {
                status: "committed".into(),
                transaction_class: "governance".into(),
                schema_environment_version,
                result: serde_json::json!({"element":id.to_string(),"op":op}),
                changes: vec![crate::tx::entry_json(&entry)],
                ..Default::default()
            },
            writes: vec![(element, op.into())],
            controls: vec![],
            control_replacements: vec![],
            space: None,
            purge_versions: vec![],
            scrub_versions: vec![],
            audits: vec![audit],
            approvals,
        })
        .await?;
    Ok(version)
}

/// The lifecycle word a Governance move records (§36.1).
///
/// An Assertion expiry keeps the engine state `active` and moves the record's
/// own status, so it is spelled from that status rather than from `state`.
fn lifecycle_word(state: &str, op: &str, before: bool) -> String {
    if op == "expire" {
        return if before { "active" } else { "expired" }.to_string();
    }
    if state.is_empty() {
        crate::store::rows::state::ACTIVE.to_string()
    } else {
        state.to_string()
    }
}

/// Merges one member into an element's Governance block.
///
/// A merge rather than a replacement, and `null` removes rather than stores:
/// classification, authority ceiling and policy reference are separate
/// decisions under separate permissions, and changing one must not silently
/// drop another.
fn set_member(governance: &Json, key: &str, value: Json) -> Json {
    let mut object = governance
        .as_object()
        .cloned()
        .unwrap_or_else(serde_json::Map::new);
    if value.is_null() {
        object.remove(key);
    } else {
        object.insert(key.to_string(), value);
    }
    Json::Object(object)
}

/// The engine origin a Governance write stamps.
fn engine_origin(auth: &AuthContext) -> Json {
    serde_json::json!({
        "principal_id": auth.principal_id,
        "channel": "governance",
    })
}

/// Archives one element whose retention has lapsed (§19.1).
///
/// The lapse is the *reason*, and it is recorded on the element rather than
/// only in the audit: an element that left ordinary recall on a schedule and
/// one a moderator archived are different facts, and a reader that cannot tell
/// them apart will read a retention sweep as a judgement about the content.
pub async fn archive_expired(
    store: &Store,
    space_id: &str,
    id: ElementId,
    authority: &EffectiveAuthority,
    auth: &AuthContext,
) -> Result<(), KipError> {
    expire(
        store,
        space_id,
        id,
        state::ARCHIVED,
        Permission::Archive,
        authority,
        auth,
    )
    .await
}

/// Tombstones one element whose retention has lapsed (§19.1).
pub async fn tombstone_expired(
    store: &Store,
    space_id: &str,
    id: ElementId,
    authority: &EffectiveAuthority,
    auth: &AuthContext,
) -> Result<(), KipError> {
    expire(
        store,
        space_id,
        id,
        state::TOMBSTONED,
        Permission::Tombstone,
        authority,
        auth,
    )
    .await
}

/// The shared body of the retention sweep's two actions.
async fn expire(
    store: &Store,
    space_id: &str,
    id: ElementId,
    new_state: &'static str,
    permission: Permission,
    authority: &EffectiveAuthority,
    auth: &AuthContext,
) -> Result<(), KipError> {
    let element = readable(store, space_id, id, authority, auth).await?;
    if element.state() != state::ACTIVE {
        // Already out of ordinary recall. Re-stating it would bump a version
        // and write a change record for a transition that did not happen.
        return Ok(());
    }
    let resource = ResourceContext::of_element(&element);
    // Expiry is not an exemption: reaching an element still costs what
    // reaching it always costs.
    let approved = decide(store, space_id, &resource, permission, authority, auth).await?;
    let patch =
        |governance: &Json| set_member(governance, RETENTION_LAPSED_KEY, Json::from("expired"));
    apply(
        &Governed {
            store,
            space_id,
            auth,
        },
        element,
        Change {
            op: "retention_expiry",
            audit_op: "retention_expiry",
            new_state: Some(new_state),
        },
        patch,
        |version| serde_json::json!({"state": new_state, "version": version}),
        approved,
    )
    .await?;
    Ok(())
}

/// The Governance member that records why an element left ordinary recall.
///
/// Its value is the reason rather than a bare `true`, matching how
/// [`QUARANTINE_KEY`] records its own — and matching `ts/kip-do`, which writes
/// the same member: a reader following `HISTORY ELEMENT` across either engine
/// sees *what happened*, not that something did.
const RETENTION_LAPSED_KEY: &str = "retention_lapsed";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn setting_one_member_keeps_the_others() {
        let before = serde_json::json!({"policy_ref": "policy-7"});
        let after = set_member(
            &before,
            "classification",
            Json::from(classification::SECRET),
        );
        assert_eq!(after["policy_ref"], "policy-7");
        assert_eq!(after["classification"], classification::SECRET);
    }

    #[test]
    fn a_null_value_removes_the_member() {
        let before = serde_json::json!({"quarantine_reason": "under review"});
        let after = set_member(&before, QUARANTINE_KEY, Json::Null);
        assert!(after.get(QUARANTINE_KEY).is_none());
    }

    #[test]
    fn a_block_that_was_absent_becomes_one() {
        let after = set_member(&Json::Null, "classification", Json::from("public"));
        assert_eq!(after["classification"], "public");
    }
}
