/**
 * # The Governance Control Plane
 *
 * ```text
 * Cognitive content may describe authority.
 * Only this plane can grant it.
 * ```
 *
 * That sentence is the whole module. A Space can hold a Proposition saying
 * *Alice is an administrator*, an Assertion supporting it with high confidence,
 * and Evidence for both — and Alice still administers nothing, because
 * administering is a {@link GrantRow} and a Proposition is a claim. Without that
 * separation, any path that can write memory is a path to privilege escalation,
 * and every Agent memory system has such a path by construction: it is the
 * entire point of the system.
 *
 * ## Three questions that are not one question
 *
 * ```text
 * Epistemic     should I believe this?
 * Governance    am I allowed to touch it?
 * Operational   how strongly may it influence what I do?
 * ```
 *
 * The projection answers the first, this plane the second, and
 * {@link authority} the third. An external researcher's vulnerability report
 * can be highly believable, unreadable to the current caller, and forbidden to
 * act on — all at once.
 *
 * ## Why the vocabularies live in their own file
 *
 * The Rust engine keeps these in `governance/mod.rs`, which is also its barrel.
 * Here that would make `index.ts` and `rows.ts` import each other: the row
 * helpers rank classifications and authority classes, and the barrel re-exports
 * the rows. ESM tolerates the cycle only because every use is inside a function
 * body — which is exactly the kind of thing that stops being true during a
 * later edit. So the vocabularies have no imports at all, and nothing can make
 * them acquire one by accident.
 *
 * @see rs/anda_cognitive_nexus/src/governance/mod.rs
 */

/**
 * The Principal the engine itself acts as (§212).
 *
 * It exists so that engine-performed maintenance is attributable to something
 * rather than to nobody. `$self` / `$system` semantic identity is a different
 * thing and confers none of this.
 */
export const SYSTEM_PRINCIPAL = 'kip:principal:system'

/**
 * The Principal an unauthenticated caller runs as, where a Space admits one
 * (§217).
 *
 * Named rather than absent: "no Principal" and "the anonymous Principal" must
 * not be the same value, or a bug that dropped the identity would look like a
 * deliberate policy choice.
 */
export const ANONYMOUS_PRINCIPAL = 'kip:principal:anonymous'

/** The scope value that means "every Space". */
export const ANY_SPACE = '*'

/**
 * Sensitivity labels and their order (§93–§95).
 *
 * Conventional names, not universal truth: a policy defines what they mean.
 * What KIP fixes is the two rules a deployment cannot vary — the order must be
 * deterministic so derived content can join classifications, and **a missing
 * classification must never read as public** (§95).
 */
export const classification = {
  /** Freely disclosable. */
  PUBLIC: 'public',
  /** Disclosable inside the owning organization. */
  INTERNAL: 'internal',
  /** Disclosable to the subject and those explicitly granted. */
  PRIVATE: 'private',
  /** Requires handling care beyond ordinary private data. */
  SENSITIVE: 'sensitive',
  /** The most restricted baseline label. */
  SECRET: 'secret',

  /**
   * The label a Space falls back to when it declares none.
   *
   * `internal`, not `public`: §95 forbids treating an absent classification as
   * freely disclosable, and a default that did would make every element written
   * before a Space configured itself world-readable.
   */
  DEFAULT: 'internal',

  /**
   * Where a label sits in the lattice.
   *
   * An unrecognized label ranks **above** every known one. That is the opposite
   * of {@link authStrength.rank}, and deliberately so: an unknown
   * authentication strength must not satisfy a bar, and an unknown sensitivity
   * must not fall below one. Both choices resolve the same way — toward
   * refusing.
   */
  rank(label: string): number {
    switch (label) {
      case 'public':
        return 0
      case 'internal':
      case '':
        return 1
      case 'private':
        return 2
      case 'sensitive':
        return 3
      case 'secret':
        return 4
      default:
        return Number.MAX_SAFE_INTEGER
    }
  },

  /**
   * The join of two classifications: the more restrictive of the two.
   *
   * This is what derived content inherits (§98). A summary of secret Evidence
   * is secret until somebody with `declassify` says otherwise — summarizing is
   * not a declassification mechanism (§242).
   */
  join(a: string, b: string): string {
    return classification.rank(a) >= classification.rank(b) ? a : b
  },
} as const

/**
 * How strongly a memory may influence action (§117–§122).
 *
 * This is an authority ceiling, not a truth score. A memory can be certainly
 * true and still be `descriptive`: believing something and being permitted to
 * act on it are different questions, and an imported Skill that arrives
 * claiming otherwise is claiming, not granting.
 *
 * And the top of this ladder is still not permission to do anything: an
 * `executable` Skill may be *supplied* to an action runtime, which must
 * independently authorize the actual tool call (§122). Memory authority never
 * becomes tool authority.
 */
export const authority = {
  /** May be read, quoted and reasoned over — but is not a recommendation. */
  DESCRIPTIVE: 'descriptive',
  /** May be treated as a recommendation or a candidate plan. */
  ADVISORY: 'advisory',
  /** May influence strategy and automatic choice inside existing bounds. */
  BEHAVIORAL: 'behavioral',
  /** May be supplied to an execution runtime as a procedure. */
  EXECUTABLE: 'executable',

  /** What memory gets when nothing says otherwise, imports included (§125). */
  DEFAULT: 'descriptive',

  /**
   * Where a class sits in the ladder.
   *
   * An unrecognized class is the **lowest** rung: something that arrives naming
   * an authority class this engine does not implement must not thereby outrank
   * `executable`.
   */
  rank(cls: string): number {
    switch (cls) {
      case 'advisory':
        return 1
      case 'behavioral':
        return 2
      case 'executable':
        return 3
      default:
        return 0
    }
  },

  /**
   * The lower of two authority classes.
   *
   * Derivation uses this, which is the whole of the non-amplification rule
   * (§127): a summary of an advisory Skill is at most advisory, and no chain of
   * reformatting turns a descriptive note into an executable one.
   */
  meet(a: string, b: string): string {
    return authority.rank(a) <= authority.rank(b) ? a : b
  },
} as const

/**
 * The deployment-defined authentication strength ladder (§11).
 *
 * Ordered, because a policy says "at least"; the vocabulary is otherwise not
 * KIP's business — KIP consumes authenticated identity, it does not perform
 * authentication.
 */
export const authStrength = {
  /** No authentication was performed. */
  NONE: 'none',
  /** An ordinary authenticated session. */
  STANDARD: 'standard',
  /** Multi-factor, hardware-backed, or otherwise elevated. */
  STRONG: 'strong',

  /**
   * The rung a strength name sits on, for "at least" comparisons.
   *
   * An unrecognized name is the *lowest* rung rather than the highest: a
   * deployment that invents a strength must not have it silently satisfy every
   * `min_auth_strength` in the Space.
   */
  rank(name: string): number {
    switch (name) {
      case 'strong':
        return 2
      case 'standard':
        return 1
      default:
        return 0
    }
  },
} as const

/**
 * How much a Principal's declared purpose can be relied on (§12).
 *
 * A purpose is context, never proof. `declared` is what a caller wrote in the
 * request envelope, and a high-risk Grant must not depend on it alone.
 */
export const purposeAssurance = {
  /** Self-declared in the request. Trusted for nothing on its own. */
  DECLARED: 'declared',
  /** Fixed for the session by the host at authentication time. */
  SESSION_BOUND: 'session_bound',
  /** Set by the runtime itself, not by any caller. */
  SYSTEM_BOUND: 'system_bound',
  /** Carried by a satisfied Approval. */
  APPROVED: 'approved',

  /** The rung a purpose assurance sits on, for "at least" comparisons. */
  rank(name: string): number {
    switch (name) {
      case 'approved':
        return 3
      case 'system_bound':
        return 2
      case 'session_bound':
        return 1
      default:
        return 0
    }
  },
} as const

/** What kind of runtime identity a Principal is (§7.1). */
export const principalClass = {
  /** A human being, authenticated by the deployment. */
  HUMAN: 'human',
  /** An autonomous agent acting under its own or a delegated identity. */
  AGENT: 'agent',
  /** A machine identity for service-to-service calls (§218). */
  SERVICE: 'service',
  /** The engine's own identity for maintenance it performs itself (§212). */
  SYSTEM: 'system',
  /** An unauthenticated caller, where a Space's policy admits one (§217). */
  ANONYMOUS: 'anonymous',
} as const

/**
 * The lifecycle every Governance record shares (§9).
 *
 * `revoked` is terminal and deliberately not a deletion: an operation that ran
 * while the record was active stays attributable to it.
 */
export const govStatus = {
  /** In force. */
  ACTIVE: 'active',
  /** Temporarily ineffective; may return to `active`. */
  SUSPENDED: 'suspended',
  /** Permanently ineffective for future operations. */
  REVOKED: 'revoked',
} as const

/** How a Principal is connected to a semantic actor (§14.2). */
export const bindingClass = {
  /** The Principal *is* this actor. */
  SELF: 'self',
  /** A machine identity standing for a service. */
  SERVICE_IDENTITY: 'service_identity',
  /** The Principal may speak on the actor's behalf. */
  REPRESENTS: 'represents',
  /** An agent acting for an organization. */
  ORGANIZATION_AGENT: 'organization_agent',
  /** A maintenance identity, which represents no one. */
  MAINTENANCE_IDENTITY: 'maintenance_identity',
} as const

/**
 * How well the binding between Principal and actor is established (§16).
 *
 * This is what the Epistemic Model reads as attribution assurance. It is not
 * confidence and not trust: it says how sure the *engine* is about who spoke,
 * not how much anyone should believe what was said.
 */
export const assurance = {
  /** Established by the deployment's identity system. */
  VERIFIED: 'verified',
  /** Inferred from strong but indirect signals. */
  STRONGLY_INFERRED: 'strongly_inferred',
  /** Recorded, but nothing checked it. */
  UNVERIFIED: 'unverified',
} as const

/** What an authorization evaluated to (§40). */
export type Decision =
  /** Permitted, with no narrowing beyond the engine's own limits. */
  | 'allow'
  /** Permitted, but the result is narrowed by the decision's constraints. */
  | 'allow_with_constraints'
  /** Refused. */
  | 'deny'
  /**
   * Blocked until an independent approval exists. **Not** an implicit allow
   * (§40) — the operation does not run.
   */
  | 'require_approval'

/** Whether a decision lets the operation proceed. */
export function isPermitted(decision: Decision): boolean {
  return decision === 'allow' || decision === 'allow_with_constraints'
}
