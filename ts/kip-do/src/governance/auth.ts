/**
 * # The authentication context, and what it is not
 *
 * KIP standardizes no authentication protocol. The host authenticates — API
 * key, OAuth, passkey, a Worker's own mTLS, whatever it has — and hands the
 * engine the result. This module is the shape of that result (§10).
 *
 * ## Why none of it comes from the request body
 *
 * A KIP request envelope carries `purpose`, `risk`, `locale` and `client`, and
 * the spec calls them non-authoritative. They are written by the caller, which
 * means an Agent under prompt injection can write anything there. So an
 * {@link AuthContext} is built by the host from what it *observed* about the
 * connection, never deserialized from the envelope.
 *
 * The one place the two meet is purpose, and they meet asymmetrically: a
 * declared purpose can *narrow* what a session may do and can never widen it
 * (§12). Writing `purpose: "emergency"` gets a caller nothing. Break-glass is an
 * explicit capability on the session, not a string in a request (§171).
 *
 * ## Sessions do not outlive revocation
 *
 * An `AuthContext` is identity, not authority. Authority is resolved from the
 * control plane on every request, so a long-lived agent session that was granted
 * export in January and had it revoked in February gets a denial in March — the
 * session did not cache what it was allowed to do (§188, §245).
 *
 * @see rs/anda_cognitive_nexus/src/governance/auth.rs
 */

import {
  ANONYMOUS_PRINCIPAL,
  SYSTEM_PRINCIPAL,
  authStrength,
  purposeAssurance,
} from './lattice.js'

/**
 * What the runtime knows about the caller of one request (§10).
 *
 * Build it from authenticated transport state. {@link anonymousAuth} is the
 * unauthenticated case: no principal, no strength, no purpose — which under
 * default deny is a caller that can do nothing until a Space's policy says
 * otherwise (§217).
 */
export interface AuthContext {
  /** The authenticated Principal id. */
  principal_id: string
  /** The host's session identifier, for correlating audit entries. */
  session_id: string
  /** How strongly the caller was authenticated. */
  auth_strength: string
  /** How, in the deployment's own vocabulary. Recorded, never interpreted. */
  auth_method: string
  /**
   * The Delegations this request runs under, delegator-first.
   *
   * Empty is the ordinary case and means "everything conferred on me". Naming a
   * chain *narrows*: the request then runs on those Delegations alone, which is
   * how a sub-agent asks to act with less than it holds.
   */
  delegation_chain: string[]
  /** What the caller is doing, from the deployment's purpose vocabulary. */
  purpose: string
  /** How much that purpose can be relied on. */
  purpose_assurance: string
  /** The deployment's risk label for this request. */
  risk: string
  /** The transport or client the request arrived on. */
  client: string
  /**
   * Whether this session carries emergency access (§171).
   *
   * A capability the host grants deliberately, never a purpose string a caller
   * writes. It does not bypass anything on its own; a policy is what decides
   * what break-glass unlocks.
   */
  break_glass: boolean
}

/** An unauthenticated caller. */
export function anonymousAuth(): AuthContext {
  return {
    principal_id: ANONYMOUS_PRINCIPAL,
    session_id: '',
    auth_strength: authStrength.NONE,
    auth_method: '',
    delegation_chain: [],
    purpose: '',
    purpose_assurance: purposeAssurance.DECLARED,
    risk: '',
    client: '',
    break_glass: false,
  }
}

/** The engine's own identity, for host-initiated work (§212). */
export function systemAuth(): AuthContext {
  return {
    ...anonymousAuth(),
    principal_id: SYSTEM_PRINCIPAL,
    auth_strength: authStrength.STRONG,
    auth_method: 'engine',
    purpose: 'system_maintenance',
    purpose_assurance: purposeAssurance.SYSTEM_BOUND,
    risk: 'low',
    client: 'engine',
  }
}

/**
 * An authenticated Principal at ordinary strength.
 *
 * `overrides` is how a host supplies what it observed — the session id, a
 * stronger factor, a session-bound purpose. It cannot supply a principal that
 * the control plane does not know: {@link EffectiveAuthority.resolve} fails on
 * one, because a host naming an identity that was never registered has a
 * configuration bug, and resolving it to "some caller with no Grants" would hide
 * that bug behind a denial that looks like policy.
 */
export function principalAuth(
  principalId: string,
  overrides: Partial<AuthContext> = {},
): AuthContext {
  return {
    ...anonymousAuth(),
    principal_id: principalId,
    auth_strength: authStrength.STANDARD,
    ...overrides,
  }
}

/** Whether a caller was authenticated at all. */
export function isAuthenticated(auth: AuthContext): boolean {
  return auth.principal_id !== '' && auth.principal_id !== ANONYMOUS_PRINCIPAL
}

/**
 * The purpose this request should be evaluated under, given what the caller
 * declared in the envelope.
 *
 * A session-bound purpose wins outright: the host already decided what this
 * session is for, and letting a request body replace it would make purpose
 * limitation advisory. A declared purpose is used only to fill a gap, and stays
 * at `declared` assurance when it does — which is exactly enough to satisfy a
 * Grant that asks for a purpose and never enough to satisfy one that asks for an
 * assured purpose.
 */
export function effectivePurpose(
  auth: AuthContext,
  declared: string | undefined,
): { purpose: string; assurance: string } {
  if (auth.purpose !== '') {
    return { purpose: auth.purpose, assurance: auth.purpose_assurance }
  }
  if (declared !== undefined && declared.trim() !== '') {
    return { purpose: declared, assurance: purposeAssurance.DECLARED }
  }
  return { purpose: '', assurance: purposeAssurance.DECLARED }
}

/** The non-authoritative members a request envelope may carry (§85). */
export interface RequestContext {
  purpose?: string
  client?: string
  risk?: string
  locale?: string
}

/**
 * Builds the context from the host's identity plus what the envelope said about
 * itself.
 *
 * Only the fields that cannot confer authority are taken from the envelope: the
 * client label, which is a log line, and the purpose, under the rule above.
 * Identity, strength and delegation come from the host and are not overridable
 * here — there is deliberately no branch that reads them from `context`.
 */
export function mergeRequestContext(
  auth: AuthContext,
  context: RequestContext | undefined,
): AuthContext {
  const { purpose, assurance } = effectivePurpose(auth, context?.purpose)
  return {
    ...auth,
    purpose,
    purpose_assurance: assurance,
    client: auth.client !== '' ? auth.client : (context?.client ?? ''),
  }
}
