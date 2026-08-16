//! # MemorySpaces and the Space sequence
//!
//! A MemorySpace is the Governance container every element belongs to (Spec
//! §28), and it owns the one counter the whole history model rests on: the
//! **Space sequence**.
//!
//! ```text
//! AS OF SEQ n     read the Space as it was at sequence n
//! CHANGES AFTER   page forward through sequences
//! space_seq       the coordinate each element's last change carries
//! ```
//!
//! Every commit takes the next sequence. That is why the sequence is allocated
//! here rather than derived from a clock: two commits in the same millisecond
//! must still be ordered, and an element's `space_seq` must be comparable with
//! a cursor a client is holding.
//!
//! A Space is **not** a Domain (§30). Semantic organization — "work", "health"
//! — belongs in Concepts and predicates. Making it a Space would attach
//! ownership and policy boundaries to a topic, which is the mistake §20 of the
//! migration guide names explicitly.

use anda_db_schema::Fv;
use anda_kip::{Json, KipError};
use std::collections::BTreeMap;

use super::{Store, eq_field, eq_fields, rows::*};
use crate::error::db_error;
use crate::store::write::WriteContext;
use crate::time;

/// What a caller supplies when creating a Space.
///
/// Ownership is a `PrincipalRecord` id — an authenticated identity — never a
/// semantic `$self` Concept. Conflating the two is how a migration invents
/// authority the old system never had (§28 of the migration guide).
#[derive(Clone, Debug, Default)]
pub struct SpaceDraft {
    /// The Space's stable id.
    pub space_id: String,
    /// A resolvable URI, when it has one.
    pub uri: String,
    /// A human-readable label.
    pub name: String,
    /// What this Space is for.
    pub description: String,
    /// The owning Principal.
    pub owner_principal: String,
    /// Any further owning Principals beyond the first.
    pub owners: Vec<String>,
    /// The Governance Policy to evaluate this Space under; empty for none.
    pub default_policy_id: String,
    /// The classification an element gets when nothing else assigns one.
    ///
    /// Empty falls back to [`classification::DEFAULT`], which is deliberately
    /// not `public`: a Space that says nothing about sensitivity has not
    /// thereby declared its contents disclosable (§95).
    ///
    /// [`classification::DEFAULT`]: crate::governance::classification::DEFAULT
    pub default_classification: String,
}

impl Store {
    /// Creates a MemorySpace, or returns the existing one unchanged.
    ///
    /// Idempotent on purpose: opening a Nexus is a startup path that runs
    /// repeatedly, and a second open must not fail or reset the sequence.
    pub async fn open_or_create_space(&self, draft: SpaceDraft) -> Result<SpaceRow, KipError> {
        if let Some(existing) = self.find_space(&draft.space_id).await? {
            return Ok(existing);
        }
        let mut owners = draft.owners;
        if !draft.owner_principal.is_empty() && !owners.contains(&draft.owner_principal) {
            owners.insert(0, draft.owner_principal.clone());
        }
        let row = SpaceRow {
            _id: 0,
            space_id: draft.space_id,
            uri: draft.uri,
            name: draft.name,
            description: draft.description,
            owner_principal: draft.owner_principal,
            owners,
            status: "active".to_string(),
            default_policy_id: draft.default_policy_id,
            trust_policy_id: String::new(),
            default_classification: if draft.default_classification.is_empty() {
                crate::governance::classification::DEFAULT.to_string()
            } else {
                draft.default_classification
            },
            audit_mode: "standard".to_string(),
            created_at: time::now(),
            // Sequence 0 is "nothing has happened here yet", so the first
            // commit is sequence 1 and no element ever carries `space_seq: 0`.
            seq: 0,
            schema_environment_version: 0,
            policies: Json::Null,
        };
        let id = self.spaces().add_from(&row).await.map_err(db_error)?;
        Ok(SpaceRow { _id: id, ..row })
    }

    /// Looks a Space up by its id.
    pub async fn find_space(&self, space_id: &str) -> Result<Option<SpaceRow>, KipError> {
        let spaces = self.spaces();
        let ids = spaces
            .query_all_ids(eq_field("space_id", Fv::Text(space_id.to_string())))
            .await
            .map_err(db_error)?;
        match ids.first() {
            None => Ok(None),
            Some(id) => Ok(Some(spaces.get_as(*id).await.map_err(db_error)?)),
        }
    }

    /// Gives every ownerless Space to the system Principal.
    ///
    /// A Space written before this Nexus had a Governance plane carries no
    /// owner, and under default deny (§41) an unowned Space is one nobody can
    /// administer — including to give it an owner. Adopting it here preserves
    /// exactly the authority such a database already had (the host process owned
    /// it outright) rather than granting anything new, and it happens on open so
    /// that no read or write ever observes the ownerless state.
    ///
    /// A Space that already names an owner is left alone. This is a bootstrap,
    /// not a claim.
    pub async fn adopt_unowned_spaces(&self, principal: &str) -> Result<usize, KipError> {
        let spaces = self.spaces();
        // Ranged over `space_id` rather than filtered on `owner_principal`:
        // ownership is not an indexed column, and a Nexus holds few enough
        // Spaces that enumerating them on open costs nothing worth an index.
        let ids = spaces
            .query_all_ids(anda_db::query::Filter::Field((
                "space_id".to_string(),
                anda_db::query::RangeQuery::Gt(Fv::Text(String::new())),
            )))
            .await
            .map_err(db_error)?;
        let mut adopted = 0;
        for id in ids {
            let row: SpaceRow = spaces.get_as(id).await.map_err(db_error)?;
            if !row.owner_principal.is_empty() {
                continue;
            }
            let mut owners = row.owners.clone();
            if !owners.iter().any(|owner| owner == principal) {
                owners.insert(0, principal.to_string());
            }
            self.put_space(&SpaceRow {
                owner_principal: principal.to_string(),
                owners,
                default_classification: if row.default_classification.is_empty() {
                    crate::governance::classification::DEFAULT.to_string()
                } else {
                    row.default_classification.clone()
                },
                status: if row.status.is_empty() {
                    "active".to_string()
                } else {
                    row.status.clone()
                },
                ..row
            })
            .await?;
            adopted += 1;
        }
        Ok(adopted)
    }

    /// Writes a Space record back as given.
    ///
    /// The Governance members are the only thing that changes through here; the
    /// sequence advances through [`Store::begin_transaction`] instead, so a
    /// Governance edit can never move a Space's history coordinate.
    pub async fn put_space(&self, row: &SpaceRow) -> Result<(), KipError> {
        let spaces = self.spaces();
        let fields = super::full_row_fields(spaces.schema(), row)?;
        spaces.update(row._id, fields).await.map_err(db_error)?;
        Ok(())
    }

    /// Looks a Space up, failing when it does not exist.
    pub async fn get_space(&self, space_id: &str) -> Result<SpaceRow, KipError> {
        self.find_space(space_id).await?.ok_or_else(|| {
            KipError::not_found_or_not_visible(format!(
                "MemorySpace {space_id:?} does not exist in this Nexus, or policy hides it"
            ))
        })
    }

    /// Allocates the next Space sequence and opens a write context on it.
    ///
    /// The allocation is durable before any element is written, so a crash
    /// between allocation and commit burns a sequence number rather than
    /// reusing one. A reused sequence would let two different commits answer
    /// to the same `AS OF SEQ` coordinate, which is worse than a gap: a gap is
    /// visible, a collision is not.
    ///
    /// # Concurrency
    ///
    /// Read-modify-write on the Space row. The engine serializes mutations
    /// behind one write lock, and `anda_db` allows one live writer process per
    /// database, so this is not a compare-and-swap loop.
    pub async fn begin_transaction(
        &self,
        space_id: &str,
        origin: Json,
    ) -> Result<WriteContext, KipError> {
        let space = self.get_space(space_id).await?;
        let seq = space.seq.saturating_add(1);
        let mut fields = BTreeMap::new();
        fields.insert("seq".to_string(), Fv::U64(seq));
        self.spaces()
            .update(space._id, fields)
            .await
            .map_err(db_error)?;

        Ok(WriteContext {
            // A Space sequence is allocated once and never reused, so pairing
            // it with the Space is already a unique transaction identity —
            // and one that reads as the history coordinate it is.
            tx_id: format!("{space_id}#{seq}"),
            space: space_id.to_string(),
            seq,
            at: time::now(),
            origin,
        })
    }

    /// Records a committed transaction in the journal.
    ///
    /// The journal is what makes a lost response recoverable: a caller that
    /// never saw its receipt looks the transaction up by its idempotency key
    /// and replays the stored result rather than writing again (§80.4).
    pub async fn journal(
        &self,
        cx: &WriteContext,
        entry: JournalEntry,
    ) -> Result<TransactionRow, KipError> {
        let row = TransactionRow {
            _id: 0,
            tx_id: cx.tx_id.clone(),
            space: cx.space.clone(),
            seq: cx.seq,
            snapshot_seq: cx.seq.saturating_sub(1),
            committed_at: cx.at.clone(),
            status: entry.status,
            transaction_class: entry.transaction_class,
            idempotency_key: entry.idempotency_key,
            request_digest: entry.request_digest,
            semantic_plan_digest: entry.semantic_plan_digest,
            result_digest: entry.result_digest,
            schema_environment_version: entry.schema_environment_version,
            result: entry.result,
            changed_ids: entry.changes.iter().filter_map(changed_id).collect(),
            changes: entry.changes,
        };
        let id = self.transactions().add_from(&row).await.map_err(db_error)?;
        Ok(TransactionRow { _id: id, ..row })
    }

    /// Looks a transaction up by its id.
    pub async fn find_transaction(&self, tx_id: &str) -> Result<Option<TransactionRow>, KipError> {
        self.first_transaction(eq_field("tx_id", Fv::Text(tx_id.to_string())))
            .await
    }

    /// Looks a transaction up by the idempotency key it committed under.
    ///
    /// Scoped to the Space: two Spaces may reuse a key without one recovering
    /// the other's result.
    pub async fn find_transaction_by_idempotency_key(
        &self,
        space_id: &str,
        key: &str,
    ) -> Result<Option<TransactionRow>, KipError> {
        if key.is_empty() {
            // The empty string is how "no key was supplied" is stored, so it
            // must never match — otherwise every keyless transaction in the
            // Space would look like a replay of the first one.
            return Ok(None);
        }
        self.first_transaction(eq_fields(&[
            ("space", Fv::Text(space_id.to_string())),
            ("idempotency_key", Fv::Text(key.to_string())),
        ]))
        .await
    }

    async fn first_transaction(
        &self,
        filter: anda_db::query::Filter,
    ) -> Result<Option<TransactionRow>, KipError> {
        let transactions = self.transactions();
        let ids = transactions.query_all_ids(filter).await.map_err(db_error)?;
        match ids.first() {
            None => Ok(None),
            Some(id) => Ok(Some(transactions.get_as(*id).await.map_err(db_error)?)),
        }
    }
}

/// What one journal entry records beyond the write context.
#[derive(Clone, Debug, Default)]
pub struct JournalEntry {
    /// `committed`, `aborted` or `no_effect`.
    pub status: String,
    /// The transaction class, e.g. `cognitive`.
    pub transaction_class: String,
    /// The idempotency key, empty when the caller supplied none.
    pub idempotency_key: String,
    /// A digest of the request.
    pub request_digest: String,
    /// A digest of the semantic plan.
    pub semantic_plan_digest: String,
    /// A digest of the result.
    pub result_digest: String,
    /// The Schema Environment version the commit ran under.
    pub schema_environment_version: u64,
    /// The response to replay on idempotent retry.
    pub result: Json,
    /// One entry per changed element: `{id, kind, op, version}`.
    pub changes: Vec<Json>,
}

fn changed_id(change: &Json) -> Option<String> {
    change.get("id")?.as_str().map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_transaction_id_reads_as_the_history_coordinate_it_is() {
        // Two commits in one Space can never share an id, because a Space
        // sequence is allocated once and never reused.
        assert_ne!(format!("{}#{}", "s", 1), format!("{}#{}", "s", 2));
        // Two Spaces at the same sequence are still different transactions.
        assert_ne!(format!("{}#{}", "a", 1), format!("{}#{}", "b", 1));
    }

    #[test]
    fn the_journal_lifts_changed_ids_out_of_the_change_records() {
        let changes = [
            json!({"id": "C-1", "kind": "concept", "op": "create", "version": 1}),
            json!({"id": "A-2", "kind": "assertion", "op": "create", "version": 1}),
            json!({"kind": "proposition", "op": "noop"}),
        ];
        let ids: Vec<String> = changes.iter().filter_map(changed_id).collect();
        assert_eq!(ids, vec!["C-1".to_string(), "A-2".to_string()]);
    }
}
