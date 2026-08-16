//! # The authentication context, and what it is not
//!
//! KIP standardizes no authentication protocol. The host authenticates — API
//! key, OAuth, passkey, canister caller, whatever it has — and hands the engine
//! the result. This module is the shape of that result (§10).
//!
//! ## Why none of it comes from the request body
//!
//! [`anda_kip::RequestContext`] carries `purpose`, `risk`, `locale` and
//! `client`, and its own documentation calls them non-authoritative. They are
//! written by the caller, which means an Agent under prompt injection can write
//! anything there. So an [`AuthContext`] is constructed by the host from what it
//! *observed* about the connection, never deserialized from the envelope.
//!
//! The one place the two meet is purpose, and they meet asymmetrically: a
//! declared purpose can *narrow* what a session may do and can never widen it
//! (§12). Writing `purpose: "emergency"` gets a caller nothing. Break-glass is
//! an explicit capability on the session, not a string in a request (§171).
//!
//! ## Sessions do not outlive revocation
//!
//! An `AuthContext` is identity, not authority. Authority is resolved from the
//! control plane on every request, so a long-lived agent session that was
//! granted export in January and had it revoked in February gets a denial in
//! March — the session did not cache what it was allowed to do (§188, §245).

use crate::governance::rows::{auth_strength, purpose_assurance};

/// What the runtime knows about the caller of one request (§10).
///
/// Construct it from authenticated transport state. The `Default` is the
/// anonymous context: no principal, no strength, no purpose — which under
/// default deny is a caller that can do nothing until a Space's policy says
/// otherwise (§217).
#[derive(Clone, Debug)]
pub struct AuthContext {
    /// The authenticated Principal id.
    pub principal_id: String,
    /// The host's session identifier, for correlating audit entries.
    pub session_id: String,
    /// How strongly the caller was authenticated, from [`auth_strength`].
    pub auth_strength: String,
    /// How, in the deployment's own vocabulary. Recorded, never interpreted.
    pub auth_method: String,
    /// The Delegations this request runs under, delegator-first.
    ///
    /// Empty is the ordinary case and means "everything conferred on me".
    /// Naming a chain *narrows*: the request then runs on those Delegations
    /// alone, which is how a sub-agent asks to act with less than it holds.
    pub delegation_chain: Vec<String>,
    /// What the caller is doing, from the deployment's purpose vocabulary.
    pub purpose: String,
    /// How much that purpose can be relied on, from [`purpose_assurance`].
    pub purpose_assurance: String,
    /// The deployment's risk label for this request.
    pub risk: String,
    /// The transport or client the request arrived on.
    pub client: String,
    /// Whether this session carries emergency access (§171).
    ///
    /// A capability the host grants deliberately, never a purpose string a
    /// caller writes. It does not bypass anything on its own; a policy is what
    /// decides what break-glass unlocks.
    pub break_glass: bool,
}

impl Default for AuthContext {
    fn default() -> Self {
        Self::anonymous()
    }
}

impl AuthContext {
    /// The engine's own identity, for host-initiated work (§212).
    pub fn system() -> Self {
        Self {
            principal_id: super::SYSTEM_PRINCIPAL.to_string(),
            session_id: String::new(),
            auth_strength: auth_strength::STRONG.to_string(),
            auth_method: "engine".to_string(),
            delegation_chain: Vec::new(),
            purpose: "system_maintenance".to_string(),
            purpose_assurance: purpose_assurance::SYSTEM_BOUND.to_string(),
            risk: "low".to_string(),
            client: "engine".to_string(),
            break_glass: false,
        }
    }

    /// An unauthenticated caller.
    pub fn anonymous() -> Self {
        Self {
            principal_id: super::ANONYMOUS_PRINCIPAL.to_string(),
            session_id: String::new(),
            auth_strength: auth_strength::NONE.to_string(),
            auth_method: String::new(),
            delegation_chain: Vec::new(),
            purpose: String::new(),
            purpose_assurance: purpose_assurance::DECLARED.to_string(),
            risk: String::new(),
            client: String::new(),
            break_glass: false,
        }
    }

    /// An authenticated Principal at ordinary strength.
    pub fn principal(principal_id: impl Into<String>) -> Self {
        Self {
            principal_id: principal_id.into(),
            auth_strength: auth_strength::STANDARD.to_string(),
            ..Self::anonymous()
        }
    }

    /// Sets how strongly the caller was authenticated.
    pub fn with_auth_strength(mut self, strength: impl Into<String>) -> Self {
        self.auth_strength = strength.into();
        self
    }

    /// Binds a purpose to the session, at an assurance the host vouches for.
    ///
    /// This is the only way a purpose gets past [`purpose_assurance::DECLARED`]:
    /// the host is asserting it, not the caller (§12).
    pub fn with_purpose(
        mut self,
        purpose: impl Into<String>,
        assurance: impl Into<String>,
    ) -> Self {
        self.purpose = purpose.into();
        self.purpose_assurance = assurance.into();
        self
    }

    /// Runs this request on a named Delegation chain, delegator-first.
    pub fn with_delegation_chain(mut self, chain: Vec<String>) -> Self {
        self.delegation_chain = chain;
        self
    }

    /// Records the session id, for audit correlation.
    pub fn with_session(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = session_id.into();
        self
    }

    /// Records the client or transport.
    pub fn with_client(mut self, client: impl Into<String>) -> Self {
        self.client = client.into();
        self
    }

    /// Grants this session emergency access (§171).
    pub fn with_break_glass(mut self) -> Self {
        self.break_glass = true;
        self
    }

    /// Whether a caller was authenticated at all.
    pub fn is_authenticated(&self) -> bool {
        !self.principal_id.is_empty() && self.principal_id != super::ANONYMOUS_PRINCIPAL
    }

    /// The purpose this request should be evaluated under, given what the
    /// caller declared in the envelope.
    ///
    /// A session-bound purpose wins outright: the host already decided what
    /// this session is for, and letting a request body replace it would make
    /// purpose limitation advisory. A declared purpose is used only to fill a
    /// gap, and stays at `declared` assurance when it does — which is exactly
    /// enough to satisfy a Grant that asks for a purpose and never enough to
    /// satisfy one that asks for an assured purpose.
    pub fn effective_purpose(&self, declared: Option<&str>) -> (String, String) {
        if !self.purpose.is_empty() {
            return (self.purpose.clone(), self.purpose_assurance.clone());
        }
        match declared {
            Some(purpose) if !purpose.trim().is_empty() => {
                (purpose.to_string(), purpose_assurance::DECLARED.to_string())
            }
            _ => (String::new(), purpose_assurance::DECLARED.to_string()),
        }
    }

    /// Builds the context from the engine's own identity plus what the request
    /// envelope said about itself.
    ///
    /// Only the fields that cannot confer authority are taken from the
    /// envelope: the client label, which is a log line, and the purpose, under
    /// the rule above.
    pub fn merged_with_request(&self, request: &anda_kip::Request) -> Self {
        let declared = request
            .context
            .as_ref()
            .and_then(|context| context.purpose.as_deref());
        let (purpose, assurance) = self.effective_purpose(declared);
        let client = match (
            &self.client,
            request.context.as_ref().and_then(|c| c.client.as_deref()),
        ) {
            (host, _) if !host.is_empty() => host.clone(),
            (_, Some(client)) => client.to_string(),
            _ => String::new(),
        };
        Self {
            purpose,
            purpose_assurance: assurance,
            client,
            ..self.clone()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_declared_purpose_cannot_replace_a_session_bound_one() {
        // §12: purpose is context, not proof. A caller writing
        // purpose: "emergency" must not get emergency authority.
        let session = AuthContext::principal("kip:principal:agent")
            .with_purpose("answer_user", purpose_assurance::SESSION_BOUND);
        let (purpose, assurance) = session.effective_purpose(Some("emergency"));
        assert_eq!(purpose, "answer_user");
        assert_eq!(assurance, purpose_assurance::SESSION_BOUND);
    }

    #[test]
    fn a_declared_purpose_fills_a_gap_at_declared_assurance() {
        let session = AuthContext::principal("kip:principal:agent");
        let (purpose, assurance) = session.effective_purpose(Some("research"));
        assert_eq!(purpose, "research");
        assert_eq!(assurance, purpose_assurance::DECLARED);
        assert_eq!(
            purpose_assurance::rank(&assurance),
            0,
            "which satisfies no assurance requirement above the floor"
        );
    }

    #[test]
    fn the_anonymous_context_is_named_rather_than_absent() {
        // "No Principal" and "the anonymous Principal" must not be the same
        // value, or a dropped identity looks like a policy choice.
        let anonymous = AuthContext::anonymous();
        assert!(!anonymous.is_authenticated());
        assert_eq!(anonymous.principal_id, super::super::ANONYMOUS_PRINCIPAL);
        assert!(!anonymous.principal_id.is_empty());
    }

    #[test]
    fn break_glass_is_a_capability_not_a_string() {
        let ordinary = AuthContext::principal("p").with_purpose("emergency", "declared");
        assert!(!ordinary.break_glass);
        assert!(AuthContext::principal("p").with_break_glass().break_glass);
    }
}
