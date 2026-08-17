//! # Persisting the control plane
//!
//! Eight collections, opened beside the cognitive ones and reached only from
//! here. Nothing in [`kml`](crate::kml) can name them, which is the storage-level
//! half of "ordinary KML MUST NOT mutate protected Governance state" (§2) — the
//! other half is that every entry point in this module is a host API.
//!
//! ## Revoke, never delete
//!
//! Every mutating method here either appends or flips a status. Deleting a
//! Grant would make the audit record that cites it dangle, and §36 requires
//! precisely the opposite: revocation stops future authority without rewriting
//! the record that a past operation was authorized.
//!
//! ## Every mutation is mirrored into the audit
//!
//! [`GovernanceStore::record_mutation`] runs on the way out of each write, with
//! the complete new record. That is what makes §176's questions answerable —
//! *who had access at time T, which policy version authorized this* — without a
//! second temporal index to keep in step with the first.

use anda_db::{
    collection::{Collection, CollectionConfig},
    database::AndaDB,
    error::DBError,
    query::Filter,
};
use anda_db_schema::Fv;
use anda_kip::{Json, KipError};
use serde::Serialize;
use std::sync::Arc;

use super::rows::*;
use crate::error::{db_error, reopen_error, schema_error};
use crate::store::rows::SpaceRow;
use crate::store::{Slot, eq_field, eq_fields, full_row_fields};
use crate::time;

/// The Principal registry collection name.
pub const PRINCIPALS: &str = "gov_principals";
/// The Principal group collection name.
pub const PRINCIPAL_GROUPS: &str = "gov_principal_groups";
/// The ActorBinding collection name.
pub const ACTOR_BINDINGS: &str = "gov_actor_bindings";
/// The Grant collection name.
pub const GRANTS: &str = "gov_grants";
/// The Delegation collection name.
pub const DELEGATIONS: &str = "gov_delegations";
/// The Governance Policy version collection name.
pub const POLICIES: &str = "gov_policies";
/// The Approval collection name.
pub const APPROVALS: &str = "gov_approvals";
/// The Governance audit collection name.
pub const AUDIT: &str = "gov_audit";

/// The scope value that means "every Space".
pub const ANY_SPACE: &str = "*";

/// The id an ActorBinding row answers to.
pub fn binding_id(row_id: u64) -> String {
    format!("kip:binding:{row_id}")
}

/// The id a Grant row answers to.
pub fn grant_id(row_id: u64) -> String {
    format!("kip:grant:{row_id}")
}

/// The id a Delegation row answers to.
pub fn delegation_id(row_id: u64) -> String {
    format!("kip:delegation:{row_id}")
}

/// The id an Approval row answers to.
pub fn approval_id(row_id: u64) -> String {
    format!("kip:approval:{row_id}")
}

/// The id a Governance audit entry answers to.
pub fn audit_id(row_id: u64) -> String {
    format!("kip:audit:{row_id}")
}

/// Reads the row number back out of a minted Governance id.
pub fn row_id_of(id: &str) -> Option<u64> {
    id.rsplit_once(':')?.1.parse().ok()
}

// ---------------------------------------------------------------------------
// Collections
// ---------------------------------------------------------------------------

/// The Governance Control Plane's persistent home.
#[derive(Clone, Debug)]
pub struct GovernanceStore {
    db: Arc<AndaDB>,
    principals: Slot,
    groups: Slot,
    bindings: Slot,
    grants: Slot,
    delegations: Slot,
    policies: Slot,
    approvals: Slot,
    audit: Slot,
}

async fn init_principals(c: &mut Collection) -> Result<(), DBError> {
    c.create_btree_index_nx(&["principal_id"]).await?;
    c.create_btree_index_nx(&["status"]).await?;
    Ok(())
}

async fn init_groups(c: &mut Collection) -> Result<(), DBError> {
    c.create_btree_index_nx(&["group_id"]).await?;
    // Indexed elementwise, so "which groups is this Principal in" — the first
    // question every authorization asks — is one lookup rather than a scan.
    c.create_btree_index_nx(&["members"]).await?;
    c.create_btree_index_nx(&["status"]).await?;
    Ok(())
}

async fn init_bindings(c: &mut Collection) -> Result<(), DBError> {
    c.create_btree_index_nx(&["principal_id"]).await?;
    c.create_btree_index_nx(&["actor_key"]).await?;
    c.create_btree_index_nx(&["scope"]).await?;
    c.create_btree_index_nx(&["status"]).await?;
    Ok(())
}

async fn init_grants(c: &mut Collection) -> Result<(), DBError> {
    c.create_btree_index_nx(&["space_id"]).await?;
    c.create_btree_index_nx(&["grantee_principal"]).await?;
    c.create_btree_index_nx(&["grantee_group"]).await?;
    c.create_btree_index_nx(&["status"]).await?;
    Ok(())
}

async fn init_delegations(c: &mut Collection) -> Result<(), DBError> {
    c.create_btree_index_nx(&["space_id"]).await?;
    c.create_btree_index_nx(&["delegate_principal"]).await?;
    c.create_btree_index_nx(&["delegator_principal"]).await?;
    c.create_btree_index_nx(&["parent_delegation"]).await?;
    c.create_btree_index_nx(&["status"]).await?;
    Ok(())
}

async fn init_policies(c: &mut Collection) -> Result<(), DBError> {
    c.create_btree_index_nx(&["policy_ref"]).await?;
    c.create_btree_index_nx(&["policy_id"]).await?;
    c.create_btree_index_nx(&["space_id"]).await?;
    // `AS OF` over policy versions ranges on this (§177).
    c.create_btree_index_nx(&["created_at"]).await?;
    Ok(())
}

async fn init_approvals(c: &mut Collection) -> Result<(), DBError> {
    c.create_btree_index_nx(&["space_id"]).await?;
    c.create_btree_index_nx(&["subject_digest"]).await?;
    c.create_btree_index_nx(&["status"]).await?;
    c.create_btree_index_nx(&["approver_ids"]).await?;
    Ok(())
}

async fn init_audit(c: &mut Collection) -> Result<(), DBError> {
    c.create_btree_index_nx(&["space_id"]).await?;
    c.create_btree_index_nx(&["principal_id"]).await?;
    c.create_btree_index_nx(&["operation"]).await?;
    c.create_btree_index_nx(&["entry_class"]).await?;
    c.create_btree_index_nx(&["at"]).await?;
    c.create_btree_index_nx(&["tx_id"]).await?;
    Ok(())
}

fn config(name: &str, description: &str) -> CollectionConfig {
    CollectionConfig {
        name: name.to_string(),
        description: description.to_string(),
    }
}

impl GovernanceStore {
    /// Opens — creating if absent — every Governance collection.
    pub async fn open(db: Arc<AndaDB>) -> Result<Self, KipError> {
        macro_rules! open {
            ($row:ty, $name:ident, $init:ident, $description:literal) => {
                db.open_or_create_collection(
                    <$row>::schema().map_err(schema_error)?,
                    config($name, $description),
                    $init,
                )
                .await
                .map_err(db_error)?
            };
        }

        let principals = open!(
            PrincipalRow,
            PRINCIPALS,
            init_principals,
            "Principals — authenticated runtime identities"
        );
        let groups = open!(
            PrincipalGroupRow,
            PRINCIPAL_GROUPS,
            init_groups,
            "Principal groups — named sets that carry authority"
        );
        let bindings = open!(
            ActorBindingRow,
            ACTOR_BINDINGS,
            init_bindings,
            "ActorBindings — Principal to semantic actor, under Governance authority"
        );
        let grants = open!(
            GrantRow,
            GRANTS,
            init_grants,
            "Grants — authority conferred over one MemorySpace"
        );
        let delegations = open!(
            DelegationRow,
            DELEGATIONS,
            init_delegations,
            "Delegations — one Principal conferring part of its own authority"
        );
        let policies = open!(
            GovernancePolicyRow,
            POLICIES,
            init_policies,
            "Governance Policy versions — append-only"
        );
        let approvals = open!(
            ApprovalRow,
            APPROVALS,
            init_approvals,
            "Approvals — the control state a high-risk operation waits on"
        );
        let audit = open!(
            GovernanceAuditRow,
            AUDIT,
            init_audit,
            "The Governance audit log — append-preserving"
        );

        Ok(Self {
            db,
            principals: Slot::new(principals),
            groups: Slot::new(groups),
            bindings: Slot::new(bindings),
            grants: Slot::new(grants),
            delegations: Slot::new(delegations),
            policies: Slot::new(policies),
            approvals: Slot::new(approvals),
            audit: Slot::new(audit),
        })
    }

    fn all(&self) -> [Arc<Collection>; 8] {
        [
            self.principals.get(),
            self.groups.get(),
            self.bindings.get(),
            self.grants.get(),
            self.delegations.get(),
            self.policies.get(),
            self.approvals.get(),
            self.audit.get(),
        ]
    }

    /// Whether any handle has been poisoned and needs reopening.
    pub fn has_poisoned_handle(&self) -> bool {
        self.all().iter().any(|c| c.is_poisoned())
    }

    /// Reloads every handle from storage.
    pub async fn reopen(&self) -> Result<(), KipError> {
        self.principals
            .set(self.reload(PRINCIPALS, init_principals).await?);
        self.groups
            .set(self.reload(PRINCIPAL_GROUPS, init_groups).await?);
        self.bindings
            .set(self.reload(ACTOR_BINDINGS, init_bindings).await?);
        self.grants.set(self.reload(GRANTS, init_grants).await?);
        self.delegations
            .set(self.reload(DELEGATIONS, init_delegations).await?);
        self.policies
            .set(self.reload(POLICIES, init_policies).await?);
        self.approvals
            .set(self.reload(APPROVALS, init_approvals).await?);
        self.audit.set(self.reload(AUDIT, init_audit).await?);
        Ok(())
    }

    async fn reload<F>(&self, name: &str, init: F) -> Result<Arc<Collection>, KipError>
    where
        F: AsyncFnOnce(&mut Collection) -> Result<(), DBError>,
    {
        self.db
            .open_collection(name.to_string(), init)
            .await
            .map_err(reopen_error)
    }

    /// Flushes every Governance collection.
    pub async fn flush(&self, now_ms: u64) -> Result<(), KipError> {
        for collection in self.all() {
            collection.flush(now_ms).await.map_err(db_error)?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Principals
    // -----------------------------------------------------------------------

    /// Creates a Principal, or returns the existing one unchanged.
    ///
    /// Idempotent because bootstrap runs on every open: the system Principal
    /// must survive a restart without being recreated, and recreating it would
    /// reset a deployment's own edits to it.
    pub async fn ensure_principal(&self, draft: PrincipalDraft) -> Result<PrincipalRow, KipError> {
        if let Some(existing) = self.find_principal(&draft.principal_id).await? {
            return Ok(existing);
        }
        let now = time::now();
        let row = PrincipalRow {
            _id: 0,
            principal_id: draft.principal_id,
            principal_class: draft.principal_class,
            status: status::ACTIVE.to_string(),
            display_name: draft.display_name,
            auth_provider: draft.auth_provider,
            auth_subject: draft.auth_subject,
            created_at: now.clone(),
            updated_at: now,
            revoked_at: String::new(),
            version: 1,
        };
        let id = self
            .principals
            .get()
            .add_from(&row)
            .await
            .map_err(db_error)?;
        let row = PrincipalRow { _id: id, ..row };
        self.record_mutation(MutationEntry {
            operation: "create_principal",
            at: row.created_at.clone(),
            resource: row.principal_id.clone(),
            record: json_of(&row)?,
            ..Default::default()
        })
        .await?;
        Ok(row)
    }

    /// Looks a Principal up by id.
    pub async fn find_principal(&self, id: &str) -> Result<Option<PrincipalRow>, KipError> {
        self.first(
            &self.principals.get(),
            eq_field("principal_id", Fv::Text(id.to_string())),
        )
        .await
    }

    /// Every audit entry about one resource, under any of the named verbs.
    ///
    /// Ranged on `operation`, which is indexed, and narrowed to the resource in
    /// memory, which is not. Narrowing to the resource is not an optimization:
    /// "this record has no history" has to mean *this* one, or a Space that
    /// happens to share the log with another would be reported as having never
    /// existed.
    async fn history_of(
        &self,
        resource: &str,
        operations: &[&str],
    ) -> Result<Vec<GovernanceAuditRow>, KipError> {
        let mut out: Vec<GovernanceAuditRow> = Vec::new();
        for operation in operations {
            let rows: Vec<GovernanceAuditRow> = self
                .all_rows(
                    &self.audit.get(),
                    eq_field("operation", Fv::Text((*operation).to_string())),
                )
                .await?;
            out.extend(rows.into_iter().filter(|row| row.resource == resource));
        }
        Ok(out)
    }

    /// The record a resource carried at an instant, and how it got that way.
    ///
    /// The selection rule — newest entry at or before the coordinate, ties
    /// broken by row id — is what makes "who was suspended then" deterministic,
    /// so it is written once here rather than per record type. Each caller
    /// supplies only what its own three outcomes mean.
    async fn record_at<T: serde::de::DeserializeOwned>(
        &self,
        resource: &str,
        operations: &[&str],
        at: &str,
        noun: &str,
    ) -> Result<Historical<T>, KipError> {
        let rows = self.history_of(resource, operations).await?;
        let has_history = !rows.is_empty();
        let latest = rows
            .into_iter()
            .filter(|row| row.at.as_str() <= at)
            .max_by(|a, b| (a.at.as_str(), a._id).cmp(&(b.at.as_str(), b._id)));
        match latest {
            Some(row) => serde_json::from_value(row.record)
                .map(Historical::At)
                .map_err(|err| KipError::internal_error(format!("historical {noun}: {err}"))),
            None if has_history => Ok(Historical::NotYet),
            None => Ok(Historical::NoHistory),
        }
    }

    /// The Principal record that was current at an instant.
    pub async fn principal_at(
        &self,
        principal_id: &str,
        at: &str,
    ) -> Result<Option<PrincipalRow>, KipError> {
        match self
            .record_at(
                principal_id,
                &["create_principal", "set_principal_status"],
                at,
                "Principal",
            )
            .await?
        {
            Historical::At(row) => Ok(Some(row)),
            Historical::NotYet => Ok(None),
            Historical::NoHistory => self.find_principal(principal_id).await,
        }
    }

    /// The MemorySpace governance record that was current at an instant.
    pub async fn space_at(&self, current: &SpaceRow, at: &str) -> Result<SpaceRow, KipError> {
        match self
            .record_at(
                &current.space_id,
                &["create_space", "put_space"],
                at,
                "MemorySpace",
            )
            .await?
        {
            Historical::At(row) => Ok(row),
            Historical::NotYet => Err(KipError::not_found_or_not_visible(format!(
                "MemorySpace {:?} did not exist at {at}",
                current.space_id
            ))),
            Historical::NoHistory => Ok(current.clone()),
        }
    }

    /// Moves a Principal to a new lifecycle status (§9).
    ///
    /// Never a delete: a historical write by a later-revoked Principal stays
    /// attributable to it, and the origin stamp on that element is not rewritten.
    pub async fn set_principal_status(
        &self,
        id: &str,
        new_status: &str,
        actor: &str,
    ) -> Result<PrincipalRow, KipError> {
        let mut row = self
            .find_principal(id)
            .await?
            .ok_or_else(|| KipError::not_found_or_not_visible(format!("no Principal {id:?}")))?;
        row.status = new_status.to_string();
        row.updated_at = time::now();
        row.revoked_at = if new_status == status::REVOKED {
            row.updated_at.clone()
        } else {
            String::new()
        };
        row.version = row.version.saturating_add(1);
        self.put(&self.principals.get(), row._id, &row).await?;
        self.record_mutation(MutationEntry {
            operation: "set_principal_status",
            at: row.updated_at.clone(),
            resource: row.principal_id.clone(),
            principal_id: actor.to_string(),
            record: json_of(&row)?,
            ..Default::default()
        })
        .await?;
        Ok(row)
    }

    // -----------------------------------------------------------------------
    // Groups
    // -----------------------------------------------------------------------

    /// Creates or replaces a Principal group's membership.
    pub async fn put_group(
        &self,
        draft: GroupDraft,
        actor: &str,
    ) -> Result<PrincipalGroupRow, KipError> {
        let now = time::now();
        let existing = self.find_group(&draft.group_id).await?;
        let row = match existing {
            Some(previous) => PrincipalGroupRow {
                name: draft.name,
                description: draft.description,
                members: draft.members,
                updated_at: now,
                version: previous.version.saturating_add(1),
                ..previous
            },
            None => PrincipalGroupRow {
                _id: 0,
                group_id: draft.group_id,
                name: draft.name,
                description: draft.description,
                members: draft.members,
                status: status::ACTIVE.to_string(),
                created_at: now.clone(),
                updated_at: now,
                version: 1,
            },
        };
        let row = if row._id == 0 {
            let id = self.groups.get().add_from(&row).await.map_err(db_error)?;
            PrincipalGroupRow { _id: id, ..row }
        } else {
            self.put(&self.groups.get(), row._id, &row).await?;
            row
        };
        self.record_mutation(MutationEntry {
            operation: "put_group",
            at: row.updated_at.clone(),
            resource: row.group_id.clone(),
            principal_id: actor.to_string(),
            record: json_of(&row)?,
            ..Default::default()
        })
        .await?;
        Ok(row)
    }

    /// Looks a group up by id.
    pub async fn find_group(&self, id: &str) -> Result<Option<PrincipalGroupRow>, KipError> {
        self.first(
            &self.groups.get(),
            eq_field("group_id", Fv::Text(id.to_string())),
        )
        .await
    }

    /// The active groups a Principal belongs to.
    pub async fn groups_of(&self, principal_id: &str) -> Result<Vec<String>, KipError> {
        let rows: Vec<PrincipalGroupRow> = self
            .all_rows(
                &self.groups.get(),
                eq_fields(&[
                    ("members", Fv::Text(principal_id.to_string())),
                    ("status", Fv::Text(status::ACTIVE.to_string())),
                ]),
            )
            .await?;
        Ok(rows.into_iter().map(|row| row.group_id).collect())
    }

    // -----------------------------------------------------------------------
    // ActorBindings
    // -----------------------------------------------------------------------

    /// Binds a Principal to a semantic actor.
    pub async fn create_binding(
        &self,
        draft: ActorBindingDraft,
        actor: &str,
    ) -> Result<ActorBindingRow, KipError> {
        let now = time::now();
        let row = ActorBindingRow {
            _id: 0,
            principal_id: draft.principal_id,
            actor_key: actor_key(&draft.actor_key),
            actor_ref: draft.actor_key,
            binding_class: draft.binding_class,
            assurance: draft.assurance,
            scope: draft.scope,
            status: status::ACTIVE.to_string(),
            created_at: now.clone(),
            updated_at: now,
            revoked_at: String::new(),
            version: 1,
        };
        let id = self.bindings.get().add_from(&row).await.map_err(db_error)?;
        let row = ActorBindingRow { _id: id, ..row };
        self.record_mutation(MutationEntry {
            operation: "create_actor_binding",
            resource: binding_id(id),
            principal_id: actor.to_string(),
            record: json_of(&row)?,
            ..Default::default()
        })
        .await?;
        Ok(row)
    }

    /// Revokes an ActorBinding.
    pub async fn revoke_binding(&self, id: u64, actor: &str) -> Result<(), KipError> {
        let mut row: ActorBindingRow = self
            .bindings
            .get()
            .get_as(id)
            .await
            .map_err(|_| KipError::not_found_or_not_visible("no such ActorBinding"))?;
        row.status = status::REVOKED.to_string();
        row.updated_at = time::now();
        row.revoked_at = row.updated_at.clone();
        row.version = row.version.saturating_add(1);
        self.put(&self.bindings.get(), id, &row).await?;
        self.record_mutation(MutationEntry {
            operation: "revoke_actor_binding",
            resource: binding_id(id),
            principal_id: actor.to_string(),
            record: json_of(&row)?,
            ..Default::default()
        })
        .await
        .map(|_| ())
    }

    /// The active bindings a Principal holds in a Space.
    ///
    /// Includes bindings scoped to every Space: representation that a
    /// deployment declared globally still applies here, but a Space-scoped
    /// binding never leaks into another Space.
    pub async fn bindings_of(
        &self,
        principal_id: &str,
        space_id: &str,
    ) -> Result<Vec<ActorBindingRow>, KipError> {
        let rows: Vec<ActorBindingRow> = self
            .all_rows(
                &self.bindings.get(),
                eq_fields(&[
                    ("principal_id", Fv::Text(principal_id.to_string())),
                    ("status", Fv::Text(status::ACTIVE.to_string())),
                ]),
            )
            .await?;
        Ok(rows
            .into_iter()
            .filter(|row| row.scope == space_id || row.scope == ANY_SPACE)
            .collect())
    }

    // -----------------------------------------------------------------------
    // Grants
    // -----------------------------------------------------------------------

    /// Creates a Grant.
    pub async fn create_grant(&self, draft: GrantDraft, actor: &str) -> Result<GrantRow, KipError> {
        let now = time::now();
        let row = GrantRow {
            _id: 0,
            space_id: draft.space_id,
            grantee_principal: draft.grantee_principal,
            grantee_group: draft.grantee_group,
            actions: draft.actions,
            scope: json_of(&draft.scope)?,
            conditions: json_of(&draft.conditions)?,
            constraints: json_of(&draft.constraints)?,
            delegation_allowed: draft.delegation_allowed,
            status: status::ACTIVE.to_string(),
            granted_by: actor.to_string(),
            created_at: now.clone(),
            updated_at: now,
            revoked_at: String::new(),
            version: 1,
        };
        let id = self.grants.get().add_from(&row).await.map_err(db_error)?;
        let row = GrantRow { _id: id, ..row };
        self.record_mutation(MutationEntry {
            operation: "create_grant",
            at: row.created_at.clone(),
            space_id: row.space_id.clone(),
            resource: grant_id(id),
            principal_id: actor.to_string(),
            record: json_of(&row)?,
        })
        .await?;
        Ok(row)
    }

    /// Revokes a Grant. Future operations lose it; past ones keep their audit.
    pub async fn revoke_grant(&self, id: u64, actor: &str) -> Result<(), KipError> {
        let mut row: GrantRow = self
            .grants
            .get()
            .get_as(id)
            .await
            .map_err(|_| KipError::not_found_or_not_visible("no such Grant"))?;
        row.status = status::REVOKED.to_string();
        row.updated_at = time::now();
        row.revoked_at = row.updated_at.clone();
        row.version = row.version.saturating_add(1);
        self.put(&self.grants.get(), id, &row).await?;
        self.record_mutation(MutationEntry {
            operation: "revoke_grant",
            at: row.updated_at.clone(),
            space_id: row.space_id.clone(),
            resource: grant_id(id),
            principal_id: actor.to_string(),
            record: json_of(&row)?,
        })
        .await
        .map(|_| ())
    }

    /// Every active Grant that could apply to a Principal in a Space.
    ///
    /// Direct Grants and group Grants together, because a decision is about the
    /// Principal's whole standing and evaluating them separately would let a
    /// group deny fail to see a direct allow.
    pub async fn grants_for(
        &self,
        space_id: &str,
        principal_id: &str,
        groups: &[String],
    ) -> Result<Vec<GrantRow>, KipError> {
        let collection = self.grants.get();
        let mut rows: Vec<GrantRow> = self
            .all_rows(
                &collection,
                eq_fields(&[
                    ("space_id", Fv::Text(space_id.to_string())),
                    ("grantee_principal", Fv::Text(principal_id.to_string())),
                    ("status", Fv::Text(status::ACTIVE.to_string())),
                ]),
            )
            .await?;
        for group in groups {
            let group_rows: Vec<GrantRow> = self
                .all_rows(
                    &collection,
                    eq_fields(&[
                        ("space_id", Fv::Text(space_id.to_string())),
                        ("grantee_group", Fv::Text(group.clone())),
                        ("status", Fv::Text(status::ACTIVE.to_string())),
                    ]),
                )
                .await?;
            rows.extend(group_rows);
        }
        Ok(rows)
    }

    /// The Grants that were in force at a past instant (§177).
    ///
    /// Reads the same rows as the live lookup and judges them by their own
    /// timestamps instead of by their current status — which is exactly what
    /// "revoke, never delete" was for. An auditor asking *who could read this
    /// in January* gets January's answer, and gets it without that being a
    /// claim about today (§179).
    pub async fn grants_at(
        &self,
        space_id: &str,
        principal_id: &str,
        groups: &[String],
        at: &str,
    ) -> Result<Vec<GrantRow>, KipError> {
        let collection = self.grants.get();
        let mut rows: Vec<GrantRow> = self
            .all_rows(
                &collection,
                eq_fields(&[
                    ("space_id", Fv::Text(space_id.to_string())),
                    ("grantee_principal", Fv::Text(principal_id.to_string())),
                ]),
            )
            .await?;
        for group in groups {
            let group_rows: Vec<GrantRow> = self
                .all_rows(
                    &collection,
                    eq_fields(&[
                        ("space_id", Fv::Text(space_id.to_string())),
                        ("grantee_group", Fv::Text(group.clone())),
                    ]),
                )
                .await?;
            rows.extend(group_rows);
        }
        rows.retain(|row| in_force_at(&row.created_at, &row.revoked_at, at));
        Ok(rows)
    }

    /// The Delegations that were in force at a past instant.
    pub async fn delegations_at(
        &self,
        space_id: &str,
        principal_id: &str,
        at: &str,
    ) -> Result<Vec<DelegationRow>, KipError> {
        let mut rows: Vec<DelegationRow> = self
            .all_rows(
                &self.delegations.get(),
                eq_fields(&[
                    ("space_id", Fv::Text(space_id.to_string())),
                    ("delegate_principal", Fv::Text(principal_id.to_string())),
                ]),
            )
            .await?;
        rows.retain(|row| in_force_at(&row.created_at, &row.revoked_at, at));
        Ok(rows)
    }

    /// The ActorBindings that were in force at a past instant.
    pub async fn bindings_at(
        &self,
        principal_id: &str,
        space_id: &str,
        at: &str,
    ) -> Result<Vec<ActorBindingRow>, KipError> {
        let mut rows: Vec<ActorBindingRow> = self
            .all_rows(
                &self.bindings.get(),
                eq_field("principal_id", Fv::Text(principal_id.to_string())),
            )
            .await?;
        rows.retain(|row| {
            (row.scope == space_id || row.scope == ANY_SPACE)
                && in_force_at(&row.created_at, &row.revoked_at, at)
        });
        Ok(rows)
    }

    /// Which groups a Principal belonged to at a past instant.
    ///
    /// Replayed from the audit rather than read off the group rows, because a
    /// group's membership is stored as one current list: the row says who is in
    /// it now and the audit says who was in it then. §177 needs the second, and
    /// the audit carrying whole records rather than diffs is what makes the
    /// replay a lookup instead of a reconstruction.
    pub async fn groups_of_at(
        &self,
        principal_id: &str,
        at: &str,
    ) -> Result<Vec<String>, KipError> {
        let entries: Vec<GovernanceAuditRow> = self
            .all_rows(
                &self.audit.get(),
                eq_field("operation", Fv::Text("put_group".to_string())),
            )
            .await?;
        let mut latest: std::collections::BTreeMap<String, (String, bool)> = Default::default();
        for entry in entries {
            if entry.at.as_str() > at {
                continue;
            }
            let Some(group) = entry.record.get("group_id").and_then(Json::as_str) else {
                continue;
            };
            let member = entry
                .record
                .get("members")
                .and_then(Json::as_array)
                .is_some_and(|members| {
                    members
                        .iter()
                        .any(|value| value.as_str() == Some(principal_id))
                });
            let slot = latest
                .entry(group.to_string())
                .or_insert_with(|| (String::new(), false));
            if entry.at >= slot.0 {
                *slot = (entry.at.clone(), member);
            }
        }
        Ok(latest
            .into_iter()
            .filter_map(|(group, (_, member))| member.then_some(group))
            .collect())
    }

    // -----------------------------------------------------------------------
    // Delegations
    // -----------------------------------------------------------------------

    /// Creates a Delegation.
    pub async fn create_delegation(
        &self,
        draft: DelegationDraft,
        actor: &str,
    ) -> Result<DelegationRow, KipError> {
        let now = time::now();
        let row = DelegationRow {
            _id: 0,
            space_id: draft.space_id,
            delegator_principal: draft.delegator_principal,
            delegate_principal: draft.delegate_principal,
            actions: draft.actions,
            scope: json_of(&draft.scope)?,
            conditions: json_of(&draft.conditions)?,
            constraints: json_of(&draft.constraints)?,
            parent_delegation: draft.parent_delegation,
            may_redelegate: draft.may_redelegate,
            status: status::ACTIVE.to_string(),
            created_at: now.clone(),
            updated_at: now,
            revoked_at: String::new(),
            version: 1,
        };
        let id = self
            .delegations
            .get()
            .add_from(&row)
            .await
            .map_err(db_error)?;
        let row = DelegationRow { _id: id, ..row };
        self.record_mutation(MutationEntry {
            operation: "create_delegation",
            at: row.created_at.clone(),
            space_id: row.space_id.clone(),
            resource: delegation_id(id),
            principal_id: actor.to_string(),
            record: json_of(&row)?,
        })
        .await?;
        Ok(row)
    }

    /// Revokes a Delegation.
    pub async fn revoke_delegation(&self, id: u64, actor: &str) -> Result<(), KipError> {
        let mut row: DelegationRow = self
            .delegations
            .get()
            .get_as(id)
            .await
            .map_err(|_| KipError::not_found_or_not_visible("no such Delegation"))?;
        row.status = status::REVOKED.to_string();
        row.updated_at = time::now();
        row.revoked_at = row.updated_at.clone();
        row.version = row.version.saturating_add(1);
        self.put(&self.delegations.get(), id, &row).await?;
        self.record_mutation(MutationEntry {
            operation: "revoke_delegation",
            at: row.updated_at.clone(),
            space_id: row.space_id.clone(),
            resource: delegation_id(id),
            principal_id: actor.to_string(),
            record: json_of(&row)?,
        })
        .await
        .map(|_| ())
    }

    /// Looks a Delegation up by row id.
    pub async fn delegation(&self, id: u64) -> Result<Option<DelegationRow>, KipError> {
        Ok(self.delegations.get().get_as(id).await.ok())
    }

    /// Looks a Grant up by row id.
    pub async fn grant(&self, id: u64) -> Result<Option<GrantRow>, KipError> {
        Ok(self.grants.get().get_as(id).await.ok())
    }

    /// The active Delegations naming a Principal as delegate in a Space.
    pub async fn delegations_to(
        &self,
        space_id: &str,
        principal_id: &str,
    ) -> Result<Vec<DelegationRow>, KipError> {
        self.all_rows(
            &self.delegations.get(),
            eq_fields(&[
                ("space_id", Fv::Text(space_id.to_string())),
                ("delegate_principal", Fv::Text(principal_id.to_string())),
                ("status", Fv::Text(status::ACTIVE.to_string())),
            ]),
        )
        .await
    }

    // -----------------------------------------------------------------------
    // Policies
    // -----------------------------------------------------------------------

    /// Publishes the next version of a Policy (§46).
    ///
    /// Always a new row. A policy update that edited the previous version in
    /// place would retroactively change what every audit record citing it means.
    pub async fn publish_policy(
        &self,
        draft: PolicyDraft,
        actor: &str,
    ) -> Result<GovernancePolicyRow, KipError> {
        let version = self
            .active_policy(&draft.policy_id)
            .await?
            .map(|row| row.version.saturating_add(1))
            .unwrap_or(1);
        let statements = draft
            .statements
            .iter()
            .map(json_of)
            .collect::<Result<Vec<_>, _>>()?;
        let row = GovernancePolicyRow {
            _id: 0,
            policy_ref: format!("{}@{version}", draft.policy_id),
            policy_id: draft.policy_id,
            version,
            space_id: draft.space_id,
            description: draft.description,
            statements,
            created_at: time::now(),
            created_by: actor.to_string(),
        };
        let id = self.policies.get().add_from(&row).await.map_err(db_error)?;
        let row = GovernancePolicyRow { _id: id, ..row };
        self.record_mutation(MutationEntry {
            operation: "publish_policy",
            at: row.created_at.clone(),
            space_id: row.space_id.clone(),
            resource: row.policy_ref.clone(),
            principal_id: actor.to_string(),
            record: json_of(&row)?,
        })
        .await?;
        Ok(row)
    }

    /// The greatest version of a Policy.
    pub async fn active_policy(
        &self,
        policy_id: &str,
    ) -> Result<Option<GovernancePolicyRow>, KipError> {
        let mut rows: Vec<GovernancePolicyRow> = self
            .all_rows(
                &self.policies.get(),
                eq_field("policy_id", Fv::Text(policy_id.to_string())),
            )
            .await?;
        rows.sort_by_key(|row| row.version);
        Ok(rows.pop())
    }

    /// The version of a Policy that was in force at an instant (§177).
    pub async fn policy_at(
        &self,
        policy_id: &str,
        at: &str,
    ) -> Result<Option<GovernancePolicyRow>, KipError> {
        let mut rows: Vec<GovernancePolicyRow> = self
            .all_rows(
                &self.policies.get(),
                eq_field("policy_id", Fv::Text(policy_id.to_string())),
            )
            .await?;
        rows.retain(|row| row.created_at.as_str() <= at);
        rows.sort_by_key(|row| row.version);
        Ok(rows.pop())
    }

    /// Every version of a Policy, oldest first.
    pub async fn policy_versions(
        &self,
        policy_id: &str,
    ) -> Result<Vec<GovernancePolicyRow>, KipError> {
        let mut rows: Vec<GovernancePolicyRow> = self
            .all_rows(
                &self.policies.get(),
                eq_field("policy_id", Fv::Text(policy_id.to_string())),
            )
            .await?;
        rows.sort_by_key(|row| row.version);
        Ok(rows)
    }

    // -----------------------------------------------------------------------
    // Approvals
    // -----------------------------------------------------------------------

    /// Opens an approval request for one concrete operation.
    pub async fn request_approval(
        &self,
        draft: ApprovalDraft,
        actor: &str,
    ) -> Result<ApprovalRow, KipError> {
        let now = time::now();
        let row = ApprovalRow {
            _id: 0,
            space_id: draft.space_id,
            operation: draft.operation,
            resource: draft.resource,
            subject_digest: draft.subject_digest,
            required: draft.required.max(1),
            approvals: Vec::new(),
            approver_ids: Vec::new(),
            allow_self_approval: draft.allow_self_approval,
            status: "pending".to_string(),
            requested_by: actor.to_string(),
            created_at: now.clone(),
            updated_at: now,
            expires_at: draft.expires_at,
            version: 1,
        };
        let id = self
            .approvals
            .get()
            .add_from(&row)
            .await
            .map_err(db_error)?;
        let row = ApprovalRow { _id: id, ..row };
        self.record_mutation(MutationEntry {
            operation: "request_approval",
            at: row.created_at.clone(),
            space_id: row.space_id.clone(),
            resource: approval_id(id),
            principal_id: actor.to_string(),
            record: json_of(&row)?,
        })
        .await?;
        Ok(row)
    }

    /// Adds one Principal's approval.
    ///
    /// Refuses a second approval from the same Principal, and — unless the
    /// request opted out — refuses the requester's own (§170). Both are the same
    /// rule: *independent* approvals, or the count means nothing.
    pub async fn approve(
        &self,
        id: u64,
        approver: &str,
        note: &str,
    ) -> Result<ApprovalRow, KipError> {
        let mut row: ApprovalRow = self
            .approvals
            .get()
            .get_as(id)
            .await
            .map_err(|_| KipError::not_found_or_not_visible("no such Approval"))?;
        if row.status != "pending" {
            return Err(KipError::requires_approval(format!(
                "approval {} is {}, not pending",
                approval_id(id),
                row.status
            )));
        }
        if row.approver_ids.iter().any(|p| p == approver) {
            return Err(KipError::not_authorized(
                "one Principal counts once: a second approval from the same identity would make \
                 a two-of-N requirement satisfiable by one actor",
            ));
        }
        if !row.allow_self_approval && row.requested_by == approver {
            return Err(KipError::not_authorized(
                "separation of duties: the Principal that requested this operation may not also \
                 approve it",
            ));
        }
        row.approvals.push(serde_json::json!({
            "principal_id": approver,
            "at": time::now(),
            "note": note,
        }));
        row.approver_ids.push(approver.to_string());
        if row.approver_ids.len() as u64 >= row.required {
            row.status = "granted".to_string();
        }
        row.updated_at = time::now();
        row.version = row.version.saturating_add(1);
        self.put(&self.approvals.get(), id, &row).await?;
        self.record_mutation(MutationEntry {
            operation: "approve",
            at: row.updated_at.clone(),
            space_id: row.space_id.clone(),
            resource: approval_id(id),
            principal_id: approver.to_string(),
            record: json_of(&row)?,
        })
        .await?;
        Ok(row)
    }

    /// Marks an approval as spent.
    ///
    /// An approval authorizes one operation, not a standing licence: the same
    /// two signatures must not be usable twice. Re-running the operation needs
    /// a new approval, which is the whole point of requiring one.
    pub async fn consume_approval(&self, id: u64) -> Result<(), KipError> {
        let Some(mut row) = self.find_approval(id).await? else {
            return Ok(());
        };
        row.status = "consumed".to_string();
        row.updated_at = time::now();
        row.version = row.version.saturating_add(1);
        self.put(&self.approvals.get(), id, &row).await?;
        self.record_mutation(MutationEntry {
            operation: "consume_approval",
            at: row.updated_at.clone(),
            space_id: row.space_id.clone(),
            resource: approval_id(id),
            principal_id: String::new(),
            record: json_of(&row)?,
        })
        .await
        .map(|_| ())
    }

    /// The granted, unexpired approvals bound to one operation subject.
    pub async fn granted_approvals(
        &self,
        space_id: &str,
        subject_digest: &str,
    ) -> Result<Vec<ApprovalRow>, KipError> {
        let rows: Vec<ApprovalRow> = self
            .all_rows(
                &self.approvals.get(),
                eq_fields(&[
                    ("space_id", Fv::Text(space_id.to_string())),
                    ("subject_digest", Fv::Text(subject_digest.to_string())),
                ]),
            )
            .await?;
        let now = time::now();
        Ok(rows
            .into_iter()
            .filter(|row| {
                row.status == "granted" && (row.expires_at.is_empty() || row.expires_at > now)
            })
            .collect())
    }

    /// Looks an Approval up by row id.
    pub async fn find_approval(&self, id: u64) -> Result<Option<ApprovalRow>, KipError> {
        Ok(self.approvals.get().get_as(id).await.ok())
    }

    // -----------------------------------------------------------------------
    // Audit
    // -----------------------------------------------------------------------

    /// Appends one control-plane mutation to the audit log.
    pub async fn record_mutation(&self, entry: MutationEntry) -> Result<u64, KipError> {
        let row = GovernanceAuditRow {
            _id: 0,
            entry_class: "mutation".to_string(),
            at: if entry.at.is_empty() {
                time::now()
            } else {
                entry.at
            },
            space_id: if entry.space_id.is_empty() {
                ANY_SPACE.to_string()
            } else {
                entry.space_id
            },
            principal_id: entry.principal_id,
            operation: entry.operation.to_string(),
            resource: entry.resource,
            decision: entry.operation.to_string(),
            record: entry.record,
            ..Default::default()
        };
        self.audit.get().add_from(&row).await.map_err(db_error)
    }

    /// Appends one authorization decision to the audit log.
    pub async fn record_decision(&self, row: GovernanceAuditRow) -> Result<u64, KipError> {
        self.audit
            .get()
            .add_from(&GovernanceAuditRow {
                _id: 0,
                entry_class: "decision".to_string(),
                ..row
            })
            .await
            .map_err(db_error)
    }

    /// Reads audit entries for a Space, newest first.
    pub async fn read_audit(
        &self,
        space_id: &str,
        limit: usize,
    ) -> Result<Vec<GovernanceAuditRow>, KipError> {
        let mut rows: Vec<GovernanceAuditRow> = self
            .all_rows(
                &self.audit.get(),
                eq_field("space_id", Fv::Text(space_id.to_string())),
            )
            .await?;
        rows.sort_by_key(|row| std::cmp::Reverse(row._id));
        rows.truncate(limit);
        Ok(rows)
    }

    // -----------------------------------------------------------------------
    // Row plumbing
    // -----------------------------------------------------------------------

    async fn first<T>(&self, collection: &Collection, filter: Filter) -> Result<Option<T>, KipError>
    where
        T: serde::de::DeserializeOwned,
    {
        let ids = collection.query_all_ids(filter).await.map_err(db_error)?;
        match ids.first() {
            None => Ok(None),
            Some(id) => Ok(Some(collection.get_as(*id).await.map_err(db_error)?)),
        }
    }

    async fn all_rows<T>(&self, collection: &Collection, filter: Filter) -> Result<Vec<T>, KipError>
    where
        T: serde::de::DeserializeOwned,
    {
        let ids = collection.query_all_ids(filter).await.map_err(db_error)?;
        let mut rows = Vec::with_capacity(ids.len());
        for id in ids {
            rows.push(collection.get_as(id).await.map_err(db_error)?);
        }
        Ok(rows)
    }

    async fn put<T: Serialize>(
        &self,
        collection: &Collection,
        id: u64,
        row: &T,
    ) -> Result<(), KipError> {
        let fields = full_row_fields(collection.schema(), row)?;
        collection.update(id, fields).await.map_err(db_error)?;
        Ok(())
    }
}

/// One control-plane mutation, as the audit log records it.
/// What a point-in-time lookup found.
///
/// `NotYet` and `NoHistory` are kept apart because they are different answers:
/// the first means the resource did not exist at that instant, the second that
/// nothing about it was ever recorded — a Nexus predating the audit trail,
/// where the live row is the best truth available.
enum Historical<T> {
    /// The record that was current at the coordinate.
    At(T),
    /// The resource has a history, and it starts after the coordinate.
    NotYet,
    /// Nothing was ever recorded about this resource.
    NoHistory,
}

#[derive(Clone, Debug, Default)]
pub struct MutationEntry {
    /// The Governance verb, e.g. `create_grant`.
    pub operation: &'static str,
    /// The instant the new record became current; empty uses the audit instant.
    pub at: String,
    /// The Space it concerned; empty becomes `*`.
    pub space_id: String,
    /// What it acted on.
    pub resource: String,
    /// The acting Principal.
    pub principal_id: String,
    /// The complete new record.
    pub record: Json,
}

/// What a caller supplies to create a Principal.
#[derive(Clone, Debug, Default)]
pub struct PrincipalDraft {
    /// The stable Principal id.
    pub principal_id: String,
    /// One of [`principal_class`].
    pub principal_class: String,
    /// A human-readable label.
    pub display_name: String,
    /// Which deployment subsystem authenticated it.
    pub auth_provider: String,
    /// That subsystem's opaque subject reference.
    pub auth_subject: String,
}

/// What a caller supplies to create or replace a group.
#[derive(Clone, Debug, Default)]
pub struct GroupDraft {
    /// The stable group id.
    pub group_id: String,
    /// A human-readable label.
    pub name: String,
    /// What the group is for.
    pub description: String,
    /// The member Principal ids.
    pub members: Vec<String>,
}

/// What a caller supplies to bind a Principal to a semantic actor.
#[derive(Clone, Debug, Default)]
pub struct ActorBindingDraft {
    /// The Principal side.
    pub principal_id: String,
    /// The semantic actor.
    pub actor_key: String,
    /// One of [`binding_class`].
    pub binding_class: String,
    /// One of [`assurance`].
    pub assurance: String,
    /// The Space it applies in, or `*`.
    pub scope: String,
}

/// What a caller supplies to create a Grant.
#[derive(Clone, Debug, Default)]
pub struct GrantDraft {
    /// The Space the Grant confers authority over.
    pub space_id: String,
    /// The grantee Principal, or empty for a group Grant.
    pub grantee_principal: String,
    /// The grantee group, or empty for a Principal Grant.
    pub grantee_group: String,
    /// The permission names.
    pub actions: Vec<String>,
    /// What it is bounded to.
    pub scope: AuthorityScope,
    /// What must hold at decision time.
    pub conditions: AuthorityConditions,
    /// What every allowed operation carries.
    pub constraints: AuthorityConstraints,
    /// Whether the grantee may delegate any of it.
    pub delegation_allowed: bool,
}

/// What a caller supplies to create a Delegation.
#[derive(Clone, Debug, Default)]
pub struct DelegationDraft {
    /// The Space the delegation acts in.
    pub space_id: String,
    /// The Principal conferring the authority.
    pub delegator_principal: String,
    /// The Principal receiving it.
    pub delegate_principal: String,
    /// The permission names.
    pub actions: Vec<String>,
    /// What it is bounded to.
    pub scope: AuthorityScope,
    /// What must hold at decision time.
    pub conditions: AuthorityConditions,
    /// What every allowed operation carries.
    pub constraints: AuthorityConstraints,
    /// The delegation this descends from, when it is a re-delegation.
    pub parent_delegation: String,
    /// Whether the delegate may re-delegate.
    pub may_redelegate: bool,
}

/// What a caller supplies to publish a Policy version.
#[derive(Clone, Debug, Default)]
pub struct PolicyDraft {
    /// The stable policy id.
    pub policy_id: String,
    /// The Space it governs, or `*`.
    pub space_id: String,
    /// What it is for.
    pub description: String,
    /// The statements, in reading order.
    pub statements: Vec<PolicyStatement>,
}

/// What a caller supplies to open an approval request.
#[derive(Clone, Debug, Default)]
pub struct ApprovalDraft {
    /// The Space the operation runs in.
    pub space_id: String,
    /// The permission being approved.
    pub operation: String,
    /// The resource it targets.
    pub resource: String,
    /// A digest binding the approval to one concrete request.
    pub subject_digest: String,
    /// How many independent approvals are required.
    pub required: u64,
    /// Whether the requester may also approve.
    pub allow_self_approval: bool,
    /// When the approval stops being usable; empty for no expiry.
    pub expires_at: String,
}

/// Whether a record with these timestamps was in force at an instant.
///
/// A record created after the coordinate did not exist then, and one revoked at
/// or before it was already gone. Both bounds are checked against the record's
/// own timestamps rather than against its current status, which is the whole
/// reason revocation is a status change rather than a delete.
fn in_force_at(created_at: &str, revoked_at: &str, at: &str) -> bool {
    created_at <= at && (revoked_at.is_empty() || revoked_at > at)
}

/// Normalizes an actor reference into the endpoint key it is compared against.
///
/// A local element id becomes the local endpoint key; anything else is treated
/// as a canonical identity. The alternative — storing what the caller typed —
/// makes a binding that looks right and matches nothing, which is the worst
/// possible failure for a record whose whole job is to be found.
fn actor_key(reference: &str) -> String {
    match reference.parse::<crate::id::ElementId>() {
        Ok(id) => crate::term::Endpoint::Local(id).key(),
        Err(_) => crate::term::Endpoint::Canonical(reference.to_string()).key(),
    }
}

/// Serializes any value into the `Json` a row column holds.
fn json_of<T: Serialize>(value: &T) -> Result<Json, KipError> {
    serde_json::to_value(value).map_err(|err| {
        KipError::internal_error(format!("a Governance record failed to serialize: {err}"))
    })
}
