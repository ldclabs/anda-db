//! # Deciding whether an operation may happen
//!
//! The resolution order is §42's, and the order is the security property:
//!
//! ```text
//! protocol invariant
//!     ↓
//! matching explicit deny
//!     ↓
//! matching allow: owner, Grant, Delegation, or Policy statement
//!     ↓
//! default deny
//! ```
//!
//! Nothing about cognitive content appears anywhere in it. A Concept's
//! attributes, an Assertion's confidence and a Proposition's subject are not
//! inputs to authorization, which is the whole of "cognitive content may
//! describe authority but only Governance can grant it" (§48).
//!
//! ## Resolved once per request, not once per element
//!
//! [`EffectiveAuthority::resolve`] does every read the control plane needs —
//! Principal, groups, Grants, Delegations, Policy — and then
//! [`EffectiveAuthority::authorize`] is pure. A read that filters ten thousand
//! candidate elements therefore costs one control-plane load, not ten thousand.
//!
//! It also means authority is re-resolved on every request, which is what makes
//! revocation take effect for a session that started before it (§188, §245).
//!
//! ## Why one allow is chosen rather than all of them merged
//!
//! Several authorities may permit the same operation. Each is independently
//! sufficient, so intersecting their constraints would let an unrelated narrow
//! Grant shrink what a broad one already allows. The least restrictive matching
//! allow is chosen, and its constraints are the decision's.
//!
//! Obligations go the other way and accumulate across every matching Policy
//! statement, because an obligation is what the deployment requires of the
//! operation rather than a limit on one authority (§184).

use anda_kip::KipError;
use std::collections::BTreeSet;

use super::rows::*;
use super::store::{delegation_id, grant_id};
use super::{Decision, Permission, authority, classification};
use crate::governance::auth::AuthContext;
use crate::store::Store;
use crate::store::rows::SpaceRow;
use crate::time;

/// How deep a Delegation chain may be walked.
///
/// A bound rather than a cycle check, because both failure modes end the same
/// way: authority that cannot be resolved is authority that is not held.
const MAX_DELEGATION_DEPTH: usize = 8;

/// What an operation is being performed on (§90).
///
/// Every field is "unset means unconstrained", so a Space-wide operation with
/// no particular target — running a query at all, publishing a policy — is
/// expressed by leaving them empty rather than by a second code path.
#[derive(Clone, Debug, Default)]
pub struct ResourceContext {
    /// The Core element kind, e.g. `concept`.
    pub kind: String,
    /// The exact Schema symbol reference.
    pub schema_ref: String,
    /// The element's classification label.
    pub classification: String,
    /// The element id.
    pub element_id: String,
}

impl ResourceContext {
    /// A resource of one Core kind.
    pub fn kind(kind: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            ..Default::default()
        }
    }

    /// Names the element.
    pub fn with_element(mut self, id: impl Into<String>) -> Self {
        self.element_id = id.into();
        self
    }

    /// Names the classification the element carries.
    pub fn with_classification(mut self, label: impl Into<String>) -> Self {
        self.classification = label.into();
        self
    }

    /// Names the Schema symbol the element is typed by.
    pub fn with_schema_ref(mut self, schema_ref: impl Into<String>) -> Self {
        self.schema_ref = schema_ref.into();
        self
    }

    /// The resource one Cognitive Element presents to an authorization.
    ///
    /// Note what is *not* here: the element's name, attributes, confidence or
    /// subject. Authorization reads the element's kind, type and classification
    /// and nothing a cognitive writer controls the meaning of — which is the
    /// storage-level shape of "cognitive content cannot grant authority" (§48).
    pub fn of_element(element: &crate::store::Element) -> Self {
        Self {
            kind: element.kind().to_string(),
            schema_ref: element.schema_ref().to_string(),
            classification: element.classification().to_string(),
            element_id: element.id().to_string(),
        }
    }

    /// Whether this names nothing in particular — the Space as a whole.
    ///
    /// The two authorization layers ask different questions, and this is what
    /// tells them apart. A command gate asks *may this Principal do this here
    /// at all*, so a Grant narrowed to one classification still lets the query
    /// run; the narrowing is then applied element by element, where there is
    /// an element to apply it to. Judging the command against a null resource
    /// would deny every scoped Grant its own commands.
    pub fn is_space_scope(&self) -> bool {
        self.kind.is_empty()
            && self.schema_ref.is_empty()
            && self.classification.is_empty()
            && self.element_id.is_empty()
    }

    /// A one-line description, for audit and for denial messages.
    fn label(&self) -> String {
        if !self.element_id.is_empty() {
            self.element_id.clone()
        } else if !self.kind.is_empty() {
            self.kind.clone()
        } else {
            "the Space".to_string()
        }
    }
}

/// One authorization decision (§39).
#[derive(Clone, Debug)]
pub struct Authorization {
    /// What it evaluated to.
    pub decision: Decision,
    /// The permission that was asked for.
    pub permission: Permission,
    /// What the operation may do, if it may happen.
    pub constraints: AuthorityConstraints,
    /// What the runtime must also do.
    pub obligations: PolicyObligations,
    /// The Policy that decided it, when one did.
    pub policy_id: String,
    /// That Policy's version.
    pub policy_version: u64,
    /// The Grants and Delegations that matched.
    pub authorities_used: Vec<String>,
    /// Whether the authority that permitted this reaches everything.
    ///
    /// What a Space-wide answer — a count, a total — may be built from. A
    /// permitted decision under a narrowed authority is still permitted; it
    /// just cannot be the basis for a number that speaks for the whole Space
    /// (§106).
    pub unrestricted: bool,
    /// Why, in one line. Safe to return to the caller (§267).
    pub reason: String,
}

impl Authorization {
    /// Whether the operation may proceed.
    pub fn is_permitted(&self) -> bool {
        self.decision.is_permitted()
    }

    /// Turns a refusal into the error a caller sees.
    ///
    /// The message names the permission and nothing else. It does not say
    /// whether the target exists, which policy statement matched, or who else
    /// holds the permission — a denial that explained itself fully would be a
    /// disclosure channel for the state it was protecting (§107, §267).
    pub fn into_result(self) -> Result<Self, KipError> {
        match self.decision {
            Decision::Allow | Decision::AllowWithConstraints => Ok(self),
            Decision::RequireApproval => Err(KipError::requires_approval(format!(
                "{} requires an independent approval that has not been recorded",
                self.permission
            ))),
            Decision::Deny => Err(KipError::not_authorized(format!(
                "this operation requires the {} permission",
                self.permission
            ))),
        }
    }
}

/// One authority that could permit an operation.
#[derive(Clone, Debug)]
struct Candidate {
    id: String,
    actions: Vec<String>,
    scope: AuthorityScope,
    conditions: AuthorityConditions,
    constraints: AuthorityConstraints,
}

impl Candidate {
    /// How much this authority restricts, for choosing between two allows.
    ///
    /// Only a tie-breaker: any candidate that reaches this point independently
    /// permits the operation, so the score decides which one's constraints the
    /// decision carries, never whether it is permitted.
    /// Whether this authority narrows nothing at all.
    fn is_unrestricted(&self) -> bool {
        self.scope == AuthorityScope::default()
            && self.constraints.fields.is_empty()
            && self.constraints.max_classification.is_empty()
            && self.constraints.max_results.is_none()
    }

    fn restrictiveness(&self) -> usize {
        self.scope.kinds.len()
            + self.scope.schema_refs.len()
            + self.scope.classifications.len()
            + self.scope.elements.len()
            + self.constraints.fields.len()
            + usize::from(!self.constraints.export)
            + usize::from(self.constraints.max_results.is_some())
            + usize::from(!self.constraints.max_classification.is_empty())
    }
}

/// Everything the control plane says about one Principal in one Space.
#[derive(Clone, Debug)]
pub struct EffectiveAuthority {
    /// The Space this authority is in.
    pub space: SpaceRow,
    /// The acting Principal's record.
    pub principal: PrincipalRow,
    /// The groups it belongs to.
    pub groups: Vec<String>,
    /// Whether it owns the Space.
    pub is_owner: bool,
    /// The Policy version in force, when the Space binds one.
    pub policy: Option<GovernancePolicyRow>,
    /// The Space's ActorBindings for this Principal.
    pub bindings: Vec<ActorBindingRow>,
    candidates: Vec<Candidate>,
}

impl EffectiveAuthority {
    /// Reads the control plane for one Principal in one Space.
    ///
    /// Fails when the asserted Principal has no record. A host that names an
    /// identity the control plane has never heard of has a configuration bug,
    /// and resolving it to "some caller with no Grants" would hide that bug
    /// behind a denial that looks like policy.
    pub async fn resolve(
        store: &Store,
        space_id: &str,
        auth: &AuthContext,
    ) -> Result<Self, KipError> {
        Self::resolve_at_depth(
            store,
            space_id,
            &auth.principal_id,
            &auth.delegation_chain,
            0,
        )
        .await
    }

    async fn resolve_at_depth(
        store: &Store,
        space_id: &str,
        principal_id: &str,
        delegation_chain: &[String],
        depth: usize,
    ) -> Result<Self, KipError> {
        let governance = &store.governance;
        let space = store.get_space(space_id).await?;
        let principal = governance
            .find_principal(principal_id)
            .await?
            .ok_or_else(|| {
                KipError::unauthenticated(format!(
                    "no Principal {principal_id:?} is registered in this Nexus"
                ))
            })?;

        // A suspended or revoked Principal keeps its record and loses its
        // authority. Returning an empty candidate set rather than an error
        // means the refusal reads as "not permitted", which is what it is.
        let live = principal.status == status::ACTIVE;
        let groups = if live {
            governance.groups_of(principal_id).await?
        } else {
            Vec::new()
        };
        let is_owner = live
            && (space.owner_principal == principal_id
                || space.owners.iter().any(|owner| owner == principal_id));

        let mut candidates = Vec::new();
        if live {
            if delegation_chain.is_empty() {
                for grant in governance
                    .grants_for(space_id, principal_id, &groups)
                    .await?
                {
                    candidates.push(candidate_of_grant(&grant)?);
                }
                for delegation in governance.delegations_to(space_id, principal_id).await? {
                    if let Some(candidate) =
                        resolve_delegation(store, space_id, &delegation, depth).await?
                    {
                        candidates.push(candidate);
                    }
                }
            } else {
                candidates.extend(
                    resolve_named_chain(store, space_id, principal_id, delegation_chain, depth)
                        .await?,
                );
            }
        }

        let policy = if space.default_policy_id.is_empty() {
            None
        } else {
            governance.active_policy(&space.default_policy_id).await?
        };
        let bindings = if live {
            governance.bindings_of(principal_id, space_id).await?
        } else {
            Vec::new()
        };

        Ok(Self {
            space,
            principal,
            groups,
            is_owner,
            policy,
            bindings,
            candidates,
        })
    }

    /// Decides whether `permission` may be exercised on `resource`.
    ///
    /// Pure: everything it reads was loaded by [`Self::resolve`].
    pub fn authorize(
        &self,
        permission: Permission,
        resource: &ResourceContext,
        auth: &AuthContext,
    ) -> Authorization {
        let now = time::now();
        let (policy_id, policy_version) = match &self.policy {
            Some(policy) => (policy.policy_id.clone(), policy.version),
            None => (String::new(), 0),
        };
        let deny = |reason: &str| Authorization {
            decision: Decision::Deny,
            permission,
            constraints: AuthorityConstraints::default(),
            obligations: self.baseline_obligations(permission),
            policy_id: policy_id.clone(),
            policy_version,
            authorities_used: Vec::new(),
            unrestricted: false,
            reason: reason.to_string(),
        };

        if self.principal.status != status::ACTIVE {
            return deny("the acting Principal is not active");
        }
        if self.space.status == "suspended" {
            return deny("the MemorySpace is suspended");
        }

        // The classification a resource carries when it names none is the
        // Space default, never `public` (§95). Only for a resource that names
        // something: a Space-scope check has no element to classify, and
        // giving it the default would make every classification-narrowed Grant
        // fail its own commands.
        let resource = if resource.is_space_scope() || !resource.classification.is_empty() {
            resource.clone()
        } else {
            ResourceContext {
                classification: self.default_classification().to_string(),
                ..resource.clone()
            }
        };

        let statements = self.statements();

        // §42: an explicit deny wins over every allow, including the owner's.
        // The owner is not locked out by it — a host holds the control plane
        // directly and can publish a new policy version — but nothing that
        // arrives through a request can talk past a deny.
        for statement in &statements {
            if statement.effect == "deny"
                && self.statement_matches(statement, permission, &resource, auth, &now)
            {
                return deny("an explicit policy statement denies this operation");
            }
        }

        let mut allows: Vec<Candidate> = Vec::new();
        if self.is_owner {
            allows.push(Candidate {
                id: format!("owner:{}", self.principal.principal_id),
                actions: Vec::new(),
                scope: AuthorityScope::default(),
                conditions: AuthorityConditions::default(),
                constraints: AuthorityConstraints {
                    export: true,
                    ..Default::default()
                },
            });
        }
        for candidate in &self.candidates {
            if candidate_matches(candidate, permission, &resource, auth, &now) {
                allows.push(candidate.clone());
            }
        }
        let mut obligations = self.baseline_obligations(permission);
        for statement in &statements {
            if statement.effect != "allow"
                || !self.statement_matches(statement, permission, &resource, auth, &now)
            {
                continue;
            }
            obligations = obligations.merge(&statement.obligations);
            allows.push(Candidate {
                id: format!("policy:{policy_id}@{policy_version}"),
                actions: statement.actions.clone(),
                scope: statement.resource.clone(),
                conditions: statement.conditions.clone(),
                constraints: statement.constraints.clone(),
            });
        }

        let Some(chosen) = allows
            .iter()
            .min_by_key(|candidate| candidate.restrictiveness())
            .cloned()
        else {
            return deny(&format!(
                "nothing grants {permission} over {}",
                resource.label()
            ));
        };

        // §40: an unmet approval blocks. It is not a soft allow, and the
        // operation does not run while it is outstanding.
        if obligations.approvals_required > 0 {
            let reason = format!(
                "{permission} needs {} independent approval(s)",
                obligations.approvals_required
            );
            return Authorization {
                decision: Decision::RequireApproval,
                permission,
                constraints: chosen.constraints,
                obligations,
                policy_id,
                policy_version,
                authorities_used: vec![chosen.id],
                unrestricted: false,
                reason,
            };
        }

        let constrained = chosen.constraints != AuthorityConstraints::default();
        let unrestricted = chosen.is_unrestricted();
        Authorization {
            decision: if constrained {
                Decision::AllowWithConstraints
            } else {
                Decision::Allow
            },
            permission,
            constraints: chosen.constraints,
            obligations,
            policy_id,
            policy_version,
            unrestricted,
            authorities_used: vec![chosen.id],
            reason: format!("{permission} is granted over {}", resource.label()),
        }
    }

    /// Whether this caller may read one element, and under what narrowing.
    ///
    /// `None` means the element is outside this Principal's query universe
    /// (§104): it does not appear in results, is not counted, does not affect
    /// ranking, and asking for it by id answers the same as asking for one that
    /// was never written. That last part is deliberate — a distinguishable
    /// "exists but hidden" is the existence leak §103 is about.
    pub fn may_read(
        &self,
        element: &crate::store::Element,
        auth: &AuthContext,
    ) -> Option<AuthorityConstraints> {
        let resource = ResourceContext::of_element(element);
        let decision = self.authorize(Permission::Read, &resource, auth);
        decision.is_permitted().then_some(decision.constraints)
    }

    /// Whether this caller's authority reaches every element in the Space.
    ///
    /// A Space-wide count is only honest when it is: a caller whose Grant is
    /// narrowed to one classification must not be told how many elements exist
    /// outside it (§106). Answered from the authority rather than by scanning,
    /// because the point is to avoid producing the number at all.
    pub fn reads_whole_space(&self, auth: &AuthContext) -> bool {
        if self.is_owner {
            return true;
        }
        let decision = self.authorize(Permission::Read, &ResourceContext::default(), auth);
        decision.is_permitted() && decision.unrestricted
    }

    /// Whether this Principal may speak as a semantic actor here (§14, §66).
    pub fn is_bound_to_actor(&self, actor_key: &str) -> bool {
        self.bindings
            .iter()
            .any(|binding| binding.actor_key == actor_key)
    }

    /// How well a claim attributed to this actor is attributable (§16).
    pub fn attribution_assurance(&self, actor_key: &str) -> &'static str {
        match self
            .bindings
            .iter()
            .find(|binding| binding.actor_key == actor_key)
        {
            Some(binding) if binding.assurance == assurance::VERIFIED => assurance::VERIFIED,
            Some(binding) if binding.assurance == assurance::STRONGLY_INFERRED => {
                assurance::STRONGLY_INFERRED
            }
            _ => assurance::UNVERIFIED,
        }
    }

    /// The Space's default classification.
    pub fn default_classification(&self) -> &str {
        if self.space.default_classification.is_empty() {
            classification::DEFAULT
        } else {
            &self.space.default_classification
        }
    }

    /// The permission names this Principal holds somewhere in this Space.
    ///
    /// For `DESCRIBE ACCESS`. Deliberately coarse: it answers "could this ever
    /// be allowed" rather than "is this allowed on that element", because the
    /// second question's answer depends on an element whose existence the
    /// caller may not be entitled to learn.
    pub fn permission_names(&self, auth: &AuthContext) -> Vec<String> {
        let mut held: BTreeSet<String> = BTreeSet::new();
        let resource = ResourceContext::default();
        for permission in Permission::ALL {
            if self.authorize(*permission, &resource, auth).is_permitted() {
                held.insert(permission.as_str().to_string());
            }
        }
        held.into_iter().collect()
    }

    fn statements(&self) -> Vec<PolicyStatement> {
        self.policy
            .as_ref()
            .map(|policy| {
                policy
                    .statements
                    .iter()
                    .filter_map(|value| serde_json::from_value(value.clone()).ok())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The obligations that hold before any policy is consulted.
    ///
    /// §172 lists operations whose absence from an audit log is itself the
    /// incident. A deployment may audit more than this; it cannot audit less.
    fn baseline_obligations(&self, permission: Permission) -> PolicyObligations {
        PolicyObligations {
            audit: permission.is_always_audited() || self.space.audit_mode == "verbose",
            ..Default::default()
        }
    }

    fn statement_matches(
        &self,
        statement: &PolicyStatement,
        permission: Permission,
        resource: &ResourceContext,
        auth: &AuthContext,
        now: &str,
    ) -> bool {
        let principal = &self.principal.principal_id;
        if !statement.principals.is_empty() && !statement.principals.contains(principal) {
            return false;
        }
        if !statement.groups.is_empty()
            && !statement
                .groups
                .iter()
                .any(|group| self.groups.contains(group))
        {
            return false;
        }
        if !statement.actions.is_empty()
            && !statement
                .actions
                .iter()
                .any(|action| action == permission.as_str())
        {
            return false;
        }
        (resource.is_space_scope() || scope_matches(&statement.resource, resource))
            && conditions_hold(&statement.conditions, auth, now)
    }
}

fn candidate_matches(
    candidate: &Candidate,
    permission: Permission,
    resource: &ResourceContext,
    auth: &AuthContext,
    now: &str,
) -> bool {
    candidate
        .actions
        .iter()
        .any(|action| action == permission.as_str())
        && (resource.is_space_scope()
            || (scope_matches(&candidate.scope, resource)
                && reaches_classification(&candidate.constraints, resource)))
        && conditions_hold(&candidate.conditions, auth, now)
}

/// Whether an authority's bounds cover this resource.
fn scope_matches(scope: &AuthorityScope, resource: &ResourceContext) -> bool {
    covers(&scope.kinds, &resource.kind)
        && covers(&scope.schema_refs, &resource.schema_ref)
        && covers(&scope.classifications, &resource.classification)
        && covers(&scope.elements, &resource.element_id)
}

/// Whether an authority's classification ceiling reaches this resource.
fn reaches_classification(constraints: &AuthorityConstraints, resource: &ResourceContext) -> bool {
    constraints.max_classification.is_empty()
        || classification::rank(&resource.classification)
            <= classification::rank(&constraints.max_classification)
}

/// Whether the runtime context satisfies an authority's conditions.
fn conditions_hold(conditions: &AuthorityConditions, auth: &AuthContext, now: &str) -> bool {
    if !conditions.valid_from.is_empty() && now < conditions.valid_from.as_str() {
        return false;
    }
    if !conditions.valid_until.is_empty() && now >= conditions.valid_until.as_str() {
        return false;
    }
    if auth_strength::rank(&auth.auth_strength) < auth_strength::rank(&conditions.min_auth_strength)
    {
        return false;
    }
    if purpose_assurance::rank(&auth.purpose_assurance)
        < purpose_assurance::rank(&conditions.min_purpose_assurance)
    {
        return false;
    }
    if !conditions.purpose.is_empty() && !conditions.purpose.contains(&auth.purpose) {
        return false;
    }
    true
}

/// Whether a bound list covers a value, where empty means "every value".
///
/// An empty *value* against a bounded list does not match: a query with no
/// particular element in view must not be judged against a Grant that was
/// narrowed to one element.
fn covers(bound: &[String], value: &str) -> bool {
    bound.is_empty() || (!value.is_empty() && bound.iter().any(|allowed| allowed == value))
}

fn candidate_of_grant(grant: &GrantRow) -> Result<Candidate, KipError> {
    Ok(Candidate {
        id: grant_id(grant._id),
        actions: grant.actions.clone(),
        scope: parse_or_default(&grant.scope),
        conditions: parse_or_default(&grant.conditions),
        constraints: parse_or_default(&grant.constraints),
    })
}

/// Resolves one Delegation against its delegator's *current* authority.
///
/// This is why Delegation is not stored as a kind of Grant. A Grant is checked
/// against its own record; a Delegation is checked against a record plus a live
/// question — does the delegator still hold this? — and the answer can change
/// without the Delegation's own row changing at all (§35).
async fn resolve_delegation(
    store: &Store,
    space_id: &str,
    delegation: &DelegationRow,
    depth: usize,
) -> Result<Option<Candidate>, KipError> {
    if depth >= MAX_DELEGATION_DEPTH {
        return Ok(None);
    }
    let parent = Box::pin(EffectiveAuthority::resolve_at_depth(
        store,
        space_id,
        &delegation.delegator_principal,
        &[],
        depth + 1,
    ))
    .await?;

    let scope: AuthorityScope = parse_or_default(&delegation.scope);
    let conditions: AuthorityConditions = parse_or_default(&delegation.conditions);
    let constraints: AuthorityConstraints = parse_or_default(&delegation.constraints);

    // §31: the delegated actions are what the delegator can actually confer
    // right now, not what the record says it once could.
    let actions: Vec<String> = delegation
        .actions
        .iter()
        .filter(|action| {
            Permission::parse(action).is_ok_and(|permission| {
                parent.candidates.iter().any(|candidate| {
                    candidate
                        .actions
                        .iter()
                        .any(|held| held == permission.as_str())
                        && candidate.scope.contains(&scope)
                        && candidate.conditions.contains(&conditions)
                }) || parent.is_owner
            })
        })
        .cloned()
        .collect();
    if actions.is_empty() {
        return Ok(None);
    }
    Ok(Some(Candidate {
        id: delegation_id(delegation._id),
        actions,
        scope,
        conditions,
        constraints,
    }))
}

/// Resolves a Delegation chain the caller named explicitly.
///
/// Each link must name the previous as its parent and the last must name the
/// caller as its delegate. A chain that does not link is not a narrower
/// authority — it is two unrelated Delegations presented as one, which is how
/// §238's amplification would be spelled if the linkage went unchecked.
async fn resolve_named_chain(
    store: &Store,
    space_id: &str,
    principal_id: &str,
    chain: &[String],
    depth: usize,
) -> Result<Vec<Candidate>, KipError> {
    let mut previous: Option<DelegationRow> = None;
    let mut last: Option<DelegationRow> = None;
    for id in chain {
        let row_id = super::store::row_id_of(id).ok_or_else(|| {
            KipError::not_authorized(format!("{id:?} is not a Delegation identifier"))
        })?;
        let row: DelegationRow = store
            .governance
            .delegation(row_id)
            .await?
            .ok_or_else(|| KipError::not_authorized(format!("no Delegation {id:?}")))?;
        if row.status != status::ACTIVE || row.space_id != space_id {
            return Err(KipError::not_authorized(format!(
                "Delegation {id:?} is not in force in this MemorySpace"
            )));
        }
        if let Some(parent) = &previous {
            if row.parent_delegation != delegation_id(parent._id) {
                return Err(KipError::not_authorized(format!(
                    "Delegation {id:?} does not descend from the one before it"
                )));
            }
            if !parent.may_redelegate {
                return Err(KipError::not_authorized(format!(
                    "Delegation {} does not permit re-delegation",
                    delegation_id(parent._id)
                )));
            }
        }
        previous = Some(row.clone());
        last = Some(row);
    }
    let Some(last) = last else {
        return Ok(Vec::new());
    };
    if last.delegate_principal != principal_id {
        return Err(KipError::not_authorized(
            "the named Delegation chain does not end at the acting Principal",
        ));
    }
    Ok(resolve_delegation(store, space_id, &last, depth)
        .await?
        .into_iter()
        .collect())
}

fn parse_or_default<T: Default + serde::de::DeserializeOwned>(value: &anda_kip::Json) -> T {
    serde_json::from_value(value.clone()).unwrap_or_default()
}

/// The influence-authority ceiling this decision imposes.
pub fn authority_ceiling(constraints: &AuthorityConstraints) -> &str {
    if constraints.max_influence_authority.is_empty() {
        authority::EXECUTABLE
    } else {
        &constraints.max_influence_authority
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conditions(min_strength: &str) -> AuthorityConditions {
        AuthorityConditions {
            min_auth_strength: min_strength.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn an_expired_authority_stops_applying_without_being_revoked() {
        let expired = AuthorityConditions {
            valid_until: "2020-01-01T00:00:00.000Z".into(),
            ..Default::default()
        };
        let auth = AuthContext::principal("p");
        assert!(!conditions_hold(&expired, &auth, &time::now()));
    }

    #[test]
    fn a_weakly_authenticated_caller_does_not_reach_a_strong_bar() {
        let auth = AuthContext::principal("p");
        assert!(conditions_hold(
            &conditions(auth_strength::STANDARD),
            &auth,
            &time::now()
        ));
        assert!(!conditions_hold(
            &conditions(auth_strength::STRONG),
            &auth,
            &time::now()
        ));
    }

    #[test]
    fn a_declared_purpose_does_not_satisfy_an_assured_one() {
        // §12, and the reason high-risk Grants must not depend on purpose alone.
        let declared =
            AuthContext::principal("p").with_purpose("maintenance", purpose_assurance::DECLARED);
        let session = AuthContext::principal("p")
            .with_purpose("maintenance", purpose_assurance::SESSION_BOUND);
        let needs_assurance = AuthorityConditions {
            purpose: vec!["maintenance".into()],
            min_purpose_assurance: purpose_assurance::SESSION_BOUND.into(),
            ..Default::default()
        };
        assert!(!conditions_hold(&needs_assurance, &declared, &time::now()));
        assert!(conditions_hold(&needs_assurance, &session, &time::now()));
    }

    #[test]
    fn a_narrow_authority_does_not_reach_a_resource_outside_it() {
        let scope = AuthorityScope {
            kinds: vec!["evidence".into()],
            ..Default::default()
        };
        assert!(scope_matches(&scope, &ResourceContext::kind("evidence")));
        assert!(!scope_matches(&scope, &ResourceContext::kind("concept")));
        // A resource with no kind in view is not judged against a kind bound.
        assert!(!scope_matches(&scope, &ResourceContext::default()));
    }

    #[test]
    fn a_classification_ceiling_stops_an_authority_short() {
        let constraints = AuthorityConstraints {
            max_classification: classification::INTERNAL.into(),
            ..Default::default()
        };
        assert!(reaches_classification(
            &constraints,
            &ResourceContext::kind("concept").with_classification(classification::PUBLIC)
        ));
        assert!(!reaches_classification(
            &constraints,
            &ResourceContext::kind("concept").with_classification(classification::SECRET)
        ));
    }

    #[test]
    fn the_least_restrictive_matching_allow_is_the_one_that_counts() {
        // Two independent authorities both permit the operation; the narrow one
        // must not shrink what the broad one already allows.
        let broad = Candidate {
            id: "broad".into(),
            actions: vec!["read".into()],
            scope: AuthorityScope::default(),
            conditions: AuthorityConditions::default(),
            constraints: AuthorityConstraints {
                export: true,
                ..Default::default()
            },
        };
        let narrow = Candidate {
            id: "narrow".into(),
            actions: vec!["read".into()],
            scope: AuthorityScope {
                kinds: vec!["concept".into()],
                ..Default::default()
            },
            conditions: AuthorityConditions::default(),
            constraints: AuthorityConstraints {
                fields: vec!["name".into()],
                max_results: Some(5),
                ..Default::default()
            },
        };
        assert!(narrow.restrictiveness() > broad.restrictiveness());
    }
}
