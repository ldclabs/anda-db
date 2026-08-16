//! # Choosing what a mutation acts on
//!
//! A mutation either names its target — `ARCHIVE :old` — or binds it with a
//! selection block: `ARCHIVE ?c WHERE { ?c CONCEPT {type: "Note"} } LIMIT 100`.
//! This module is the second case, and it is where KML borrows the KQL solver.
//!
//! ## The snapshot the block reads
//!
//! A selection block reads the state the **transaction started from**, not the
//! rows this transaction has staged. Two reasons, and the second is the load
//! bearing one:
//!
//! - `anda_db` has no transactional read path, so the solver reads committed
//!   rows by construction;
//! - clause order carries no mutation semantics (§24). A block that could see
//!   its own transaction's writes would make `ARCHIVE ?c WHERE {...}` mean
//!   something different depending on where it sat in the `MUTATE` — which is
//!   exactly the dependency §24 exists to deny.
//!
//! Elements this transaction minted are `pending` and no pattern matches them,
//! so the rule holds by construction rather than by a filter someone must
//! remember to write.
//!
//! ## The order a `LIMIT` cuts
//!
//! §52.7 says a bounded sweep must not be assumed deterministic *unless the
//! runtime documents an order*. This one does: ascending element id, which is
//! creation order within a kind. A sweep that runs twice therefore takes the
//! same elements both times instead of an arbitrary slice that changes with an
//! index's internal ordering.

use anda_kip::{ElementRef, KipError, Scalar, WhereClause};

use super::value::Bindings;
use crate::id::ElementId;
use crate::kql;
use crate::store::Store;
use crate::tx::Transaction;

/// The elements one clause will act on, ascending by id.
pub struct Targets {
    /// The elements themselves. Empty is an ordinary outcome: a sweep whose
    /// block matched nothing changes nothing.
    pub ids: Vec<ElementId>,
}

/// Resolves the targets of a mutation clause.
///
/// `WHERE` present:
/// - a `?var` target is bound by the block, and every element it takes is a
///   target (bounded by `LIMIT`);
/// - a directly named target (`:id`, `"id"`) keeps its identity and the block
///   is a **guard**: it must have at least one solution, or the clause does
///   nothing. A guard that fails is not an error — that is what makes
///   `ARCHIVE :x WHERE {...}` a conditional sweep rather than a precondition.
///
/// `WHERE` absent: the statement names its target, and `LIMIT` has nothing to
/// bound.
pub async fn targets(
    store: &Store,
    tx: &Transaction,
    what: &str,
    target: &ElementRef,
    where_clauses: Option<&Vec<WhereClause>>,
    limit: Option<&Scalar>,
    b: &Bindings<'_>,
) -> Result<Targets, KipError> {
    let Some(clauses) = where_clauses else {
        return Ok(Targets {
            ids: vec![b.element_ref(target)?],
        });
    };

    let mut cx = kql::Context::open(
        store,
        &tx.cx.space,
        b.request,
        b.operation,
        &tx.authority,
        &tx.auth,
    )
    .await?;
    let solutions = cx.solve(clauses).await?;
    let limit = limit
        .map(|scalar| b.scalar_u64(scalar, "LIMIT"))
        .transpose()?
        .map(|limit| limit as usize);

    let variable = match target {
        // A handle this transaction already bound is a different element from
        // whatever the block selects, and silently preferring one of them would
        // make the statement mean something the author cannot see. Two names,
        // two meanings, one spelling: refuse.
        ElementRef::Handle(name) if tx.handles().contains_key(name.as_str()) => {
            return Err(KipError::reference_error(format!(
                "?{name} is bound both by a clause in this transaction and by the {what} \
                 selection block; give the selection variable a different name"
            )));
        }
        ElementRef::Handle(name) => Some(name.as_str()),
        _ => None,
    };

    let ids = match variable {
        Some(name) => {
            if !solutions.binds(name) {
                return Err(KipError::reference_error(format!(
                    "the {what} selection block does not bind ?{name}"
                )));
            }
            let mut ids = solutions.elements_of(name);
            // §52.7: a documented order, so a bounded sweep is repeatable.
            ids.sort_unstable();
            ids.dedup();
            if let Some(limit) = limit {
                ids.truncate(limit);
            }
            ids
        }
        None => {
            // A guard: the statement already names its target.
            if solutions.is_empty() {
                Vec::new()
            } else {
                vec![b.element_ref(target)?]
            }
        }
    };

    Ok(Targets { ids })
}
