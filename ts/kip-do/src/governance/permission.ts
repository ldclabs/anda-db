/**
 * # The permission registry
 *
 * One name per distinction the protocol requires an implementation to keep
 * (Spec §29, Governance §52–§89, §249). The names may be refined; the
 * distinctions may not, and every one of them exists because collapsing it
 * silently widens authority:
 *
 * ```text
 * read      ≠ export          reading is not taking a copy away (§78)
 * read      ≠ discover        knowing a thing exists is its own disclosure
 * update    ≠ declassify      a writer must not relabel sensitivity
 * assert    ≠ assert_as_actor recording a claim is not impersonation (§17)
 * maintain  ≠ manage_policy   a maintenance agent must not widen itself
 * tombstone ≠ purge           logical removal is not erasure
 * ```
 *
 * A Grant lists permissions by these names. An unrecognized name is rejected at
 * Grant creation rather than ignored at decision time: a typo that silently
 * confers nothing is a Grant that looks like authority and is not, and the
 * holder discovers it during an incident.
 *
 * ## Why the wire name is the type
 *
 * The Rust engine has a `Permission` enum and a `parse` that maps names onto
 * it. Here the string literal union *is* the enum: a Grant's `actions` column
 * holds wire names, and a separate internal representation would mean every
 * comparison went through a conversion that could be forgotten in one branch.
 * {@link parsePermission} still exists, because narrowing an arbitrary string
 * to the union is exactly where an unknown name must be refused.
 *
 * @see rs/anda_cognitive_nexus/src/governance/permission.rs
 */

import { errors } from '../errors.js'

/**
 * The permission families (Spec §29, Governance §52).
 *
 * Families are for explanation — `DESCRIBE ACCESS` groups by them, and a denial
 * can name one without naming a policy. Authorization never resolves on a
 * family; it always resolves on one {@link Permission}.
 */
export type Family =
  /** Learning that something exists, and reading it. */
  | 'discovery'
  /** Creating and changing cognitive content. */
  | 'cognitive_mutation'
  /** Committing to, withdrawing or moderating epistemic claims. */
  | 'epistemic_mutation'
  /** Binding Principals to semantic actors, and merging identities. */
  | 'identity'
  /** Consolidation, archival and other custodial work. */
  | 'maintenance'
  /** Moving cognition across a Space boundary. */
  | 'sharing'
  /** Retention, legal holds, erasure and declassification. */
  | 'lifecycle'
  /** Changing the control plane itself. */
  | 'governance'
  /** Raising or lowering how strongly memory may influence action. */
  | 'authority'
  /** Reading what the control plane recorded. */
  | 'audit'

/** One entry of the registry. */
interface PermissionSpec {
  family: Family
  /** What the permission allows, in one line. */
  description: string
}

/**
 * Every protected operation, in registry order.
 *
 * Declared as one object so that the name, the family and the description
 * cannot drift apart: the Rust engine keeps them together with a macro for the
 * same reason.
 */
export const PERMISSIONS = {
  // Discovery / Read (§53–§58)
  discover: {
    family: 'discovery',
    description: 'learn that an element or match exists',
  },
  read: {
    family: 'discovery',
    description: 'read the permitted content fields of a known element',
  },
  search: {
    family: 'discovery',
    description: 'retrieve associatively over the authorized search universe',
  },
  project: {
    family: 'discovery',
    description: 'run an Epistemic Projection under a permitted policy',
  },
  read_raw_origin: {
    family: 'discovery',
    description:
      'read engine origin: which Principal and channel wrote an element',
  },
  read_history: {
    family: 'discovery',
    description: 'read past element versions and change streams',
  },

  // Cognitive mutation (§59–§62)
  create: {
    family: 'cognitive_mutation',
    description: 'create Concepts, Propositions, Evidence and Activities',
  },
  update: {
    family: 'cognitive_mutation',
    description: 'change mutable, non-protected fields of an existing element',
  },
  derive: {
    family: 'cognitive_mutation',
    description:
      'create derived output from content already read, under propagation rules',
  },

  // Epistemic mutation (§63–§70)
  assert: {
    family: 'epistemic_mutation',
    description: "record one's own epistemic commitment",
  },
  record_attributed_assertion: {
    family: 'epistemic_mutation',
    description:
      'record that another actor stated or believed something, with provenance',
  },
  assert_as_actor: {
    family: 'epistemic_mutation',
    description: "exercise a bound actor's representation authority",
  },
  retract_own: {
    family: 'epistemic_mutation',
    description: 'retract an Assertion one is authorized to represent',
  },
  supersede_own: {
    family: 'epistemic_mutation',
    description: 'supersede an Assertion one is authorized to represent',
  },
  moderate_assertion: {
    family: 'epistemic_mutation',
    description:
      "administratively exclude a third party's Assertion without claiming they retracted it",
  },

  // Identity (§71–§74)
  manage_actor_binding: {
    family: 'identity',
    description:
      'create, change or revoke the binding between a Principal and a semantic actor',
  },
  bind_canonical_identity: {
    family: 'identity',
    description: 'attach a canonical identity to a Concept',
  },
  merge_identity: {
    family: 'identity',
    description: 'consolidate two Concepts into one identity',
  },

  // Maintenance (§75)
  maintain: {
    family: 'maintenance',
    description: 'perform custodial consolidation and repair',
  },
  archive: {
    family: 'maintenance',
    description: 'remove an element from ordinary recall, keeping it readable',
  },
  quarantine: {
    family: 'maintenance',
    description:
      'place an element in a state ordinary recall excludes, without claiming retraction',
  },
  tombstone: {
    family: 'maintenance',
    description: 'logically delete an element, keeping its identity and references',
  },

  // Sharing (§76–§79)
  import: {
    family: 'sharing',
    description: "accept another Brain's cognition into this Space",
  },
  export: {
    family: 'sharing',
    description: 'take cognition out of the Space',
  },
  share: {
    family: 'sharing',
    description: 'expose a controlled view of this Space to another',
  },

  // Lifecycle (§80–§82, §88, §100)
  manage_retention: {
    family: 'lifecycle',
    description: 'set or change how long an element is retained',
  },
  legal_hold: {
    family: 'lifecycle',
    description: 'place or lift a hold that blocks erasure',
  },
  purge: {
    family: 'lifecycle',
    description: 'physically erase an element and its retained history',
  },
  declassify: {
    family: 'lifecycle',
    description: "lower an element's classification",
  },

  // Governance (§83–§86)
  manage_membership: {
    family: 'governance',
    description: 'change who belongs to a Principal group',
  },
  manage_grants: {
    family: 'governance',
    description: 'create or revoke Grants in this Space',
  },
  manage_delegation: {
    family: 'governance',
    description: 'create or revoke Delegations in this Space',
  },
  delegate: {
    family: 'governance',
    description: "confer part of one's own authority on another Principal",
  },
  manage_policy: {
    family: 'governance',
    description: "publish a new version of the Space's Governance Policy",
  },
  manage_trust: {
    family: 'governance',
    description: 'bind or version the trust policy the projection reads',
  },
  manage_schema: {
    family: 'governance',
    description: 'install a Schema Package or activate a Schema Lock',
  },

  // Authority (§87, §129)
  elevate_authority: {
    family: 'authority',
    description: 'raise how strongly a memory may influence action',
  },
  approve_high_risk: {
    family: 'authority',
    description:
      'supply one of the independent approvals a high-risk operation needs',
  },

  // Audit (§89)
  read_audit: {
    family: 'audit',
    description: 'read the Governance audit log',
  },
  read_governance_history: {
    family: 'audit',
    description:
      'read past Governance state: who had access, under which policy version',
  },
} as const satisfies Record<string, PermissionSpec>

/** One protected operation, named as a Grant spells it. */
export type Permission = keyof typeof PERMISSIONS

/** Every permission this engine knows, in registry order. */
export const ALL_PERMISSIONS = Object.keys(PERMISSIONS) as Permission[]

/** Which family a permission belongs to. */
export function familyOf(permission: Permission): Family {
  return PERMISSIONS[permission].family
}

/** What the permission allows, in one line. */
export function describePermission(permission: Permission): string {
  return PERMISSIONS[permission].description
}

/**
 * Narrows a wire name to a permission.
 *
 * Unknown names fail rather than being dropped: a Grant naming a permission
 * this engine does not implement confers nothing, and the holder must learn
 * that when the Grant is written — not during an incident.
 */
export function parsePermission(name: string): Permission {
  if (!Object.hasOwn(PERMISSIONS, name)) {
    throw errors.notAuthorized(
      `${JSON.stringify(name)} is not a permission this engine implements; ` +
        `DESCRIBE ACCESS lists the registry`,
    )
  }
  return name as Permission
}

/** Whether a wire name is a permission, without throwing. */
export function isPermission(name: string): name is Permission {
  return Object.hasOwn(PERMISSIONS, name)
}

/**
 * Whether this permission is high-impact enough that a Space's audit obligation
 * applies to it even when no policy statement says so (§172).
 *
 * The list is the one §172 enumerates: changing the control plane, moving
 * cognition across the Space boundary, erasing, and raising authority. A
 * deployment may audit more; it may not audit less, because these are the
 * operations whose absence from a log is itself the incident.
 */
export function isAlwaysAudited(permission: Permission): boolean {
  const family = familyOf(permission)
  if (family === 'governance' || family === 'authority' || family === 'identity') {
    return true
  }
  return (
    permission === 'import' ||
    permission === 'export' ||
    permission === 'share' ||
    permission === 'purge' ||
    permission === 'legal_hold'
  )
}
