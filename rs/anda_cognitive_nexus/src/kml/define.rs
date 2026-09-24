//! # `DEFINE` — the Space's draft vocabulary (Spec §20.16)
//!
//! A standalone operation, never a clause of a mutation plan: it adds one
//! Predicate or Concept Type to `kip://local/draft@0.0.0` and commits as its
//! own governance transaction, so the symbol resolves for the next
//! operation. The write lane around it is the ordinary one — idempotent
//! replay, preconditions, the `propose_schema` gate and its approvals — and
//! only the commit differs.

use anda_kip::{
    DefineCommand, Json, KipError, Map, Operation, Receipt, ReceiptStatus, Request, Response,
    SymbolRef,
};

use super::value::Bindings;
use crate::governance::AuthContext;
use crate::store::Store;
use crate::store::space::JournalEntry;

/// Runs one `DEFINE`.
pub(crate) async fn execute(
    store: &Store,
    space_id: &str,
    define: &DefineCommand,
    statement: &anda_kip::KmlStatement,
    request: &Request,
    operation: &Operation,
    auth: &AuthContext,
) -> Response {
    match run(store, space_id, define, statement, request, operation, auth).await {
        Ok(response) => response,
        Err(err) => Response::from(err),
    }
}

async fn run(
    store: &Store,
    space_id: &str,
    define: &DefineCommand,
    statement: &anda_kip::KmlStatement,
    request: &Request,
    operation: &Operation,
    auth: &AuthContext,
) -> Result<Response, KipError> {
    // Parameters bind before any check (§20.16): a parameter can carry a
    // closed world as easily as a literal can.
    let handles = Default::default();
    let bindings = Bindings {
        request: request.parameters.as_ref(),
        operation: operation.parameters.as_ref(),
        handles: &handles,
        env: None,
    };
    let name = match &define.name {
        SymbolRef::Name(name) => name.clone(),
        SymbolRef::Param(param) => match bindings.param(param)? {
            Json::String(name) => name,
            other => {
                return Err(KipError::type_mismatch(format!(
                    "DEFINE names its symbol with a string, got {other}"
                )));
            }
        },
    };
    let mut definition = Map::new();
    for (member, value) in &define.definition {
        definition.insert(member.clone(), bindings.bound(value, None)?);
    }

    let key = super::idempotency_key(request, operation);
    let entry = JournalEntry {
        request_digest: if key.is_empty() {
            String::new()
        } else {
            super::request_digest(statement, request, operation)
        },
        idempotency_key: super::scoped_idempotency_key(auth, &key),
        origin: serde_json::to_value(anda_kip::ReceiptOrigin {
            principal_id: auth.principal_id.clone(),
            actor_binding_id: None,
            delegation_digest: crate::tx::delegation_digest(&auth.delegation_chain),
        })
        .unwrap_or(Json::Null),
        ..Default::default()
    };
    let dry_run = request.is_dry_run();
    let (result, row) = store
        .define_draft_symbol(
            space_id,
            define.kind,
            &name,
            Json::Object(definition),
            entry,
            dry_run,
        )
        .await?;
    Ok(match row {
        Some(row) => super::journal_response(&row),
        None => {
            // A dry run commits nothing, so it names no transaction (§69.3).
            let version = store.get_space(space_id).await?.schema_environment_version;
            let receipt = crate::tx::seal_receipt(Receipt {
                status: ReceiptStatus::NoEffect,
                tx_id: None,
                space_id: Some(space_id.to_string()),
                snapshot_seq: None,
                space_seq: None,
                committed_at: None,
                transaction_class: Some("governance".to_string()),
                request_digest: None,
                semantic_plan_digest: None,
                result_digest: None,
                schema_environment_version: Some(version),
                change_summary: None,
                proofs: Vec::new(),
                receipt_digest: None,
                origin: None,
                extensions: None,
            });
            super::operation_response(result, space_id, version, receipt)
        }
    })
}
