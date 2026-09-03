/**
 * # What each command asks for
 *
 * One table, from parsed command to the permissions it needs. It lives here
 * rather than being scattered through the executors so that adding a clause
 * cannot quietly add an ungoverned write path.
 *
 * ## Read this table as the security surface
 *
 * Three groupings are deliberate and easy to get wrong in the other direction:
 *
 * - **`EXPORT CAPSULE` asks for `export`, not `read`.** A caller who may read
 *   every element in a Space still may not package them and take them away
 *   (§78, and the `read ≠ export` equation in §102).
 * - **A historical read asks for `read_history` on top of `read`.** What the
 *   Brain contained in January is a different disclosure from what it contains
 *   now — it can include elements since archived, and origins since revoked.
 * - **`DESCRIBE PROTOCOL` and friends ask for nothing.** They describe the
 *   engine, not the Space. Gating them would mean an unauthorized caller could
 *   not discover *how to authenticate*.
 *
 * ## The one that a refactor will get wrong
 *
 * A clause this table does not name gets {@link FALLBACK}, which is every
 * Governance permission the mutation families have. That is deliberately
 * unusable: a new clause arriving without an entry should fail closed and be
 * noticed, not inherit `create` because that is the common case. TypeScript
 * cannot make the match exhaustive here — the AST is a union of single-key
 * objects from an external package — so the fallback does the job the compiler
 * does in the Rust engine.
 *
 * @see rs/anda_cognitive_nexus/src/governance/gate.rs
 */

import type {
  Assignments,
  DescribeTarget,
  KmlStatement,
  KqlQuery,
  MetaCommand,
  MutationClause,
  Transition,
  WhereClause,
} from '../kip/ast.js'
import {
  TRANSITION_ACTIVITY,
  TRANSITION_STATES,
} from '../kip/semantics.js'
import type { Permission } from './permission.js'

/**
 * What an unrecognized clause asks for.
 *
 * Not "nothing" and not "create": a clause nobody has classified must be
 * unreachable for an ordinary caller, and naming a Governance permission no
 * cognitive Grant confers is how that is spelled.
 */
const FALLBACK: Permission[] = ['manage_policy']

/**
 * What a KQL query needs.
 *
 * `boundToSnapshot` is not inferable from the AST and has to be passed: a read
 * pinned by a `read.snapshot_token` is bound to a past coordinate exactly as one
 * naming `AS OF` is, and it discloses the same thing. Reading the permission off
 * the command alone would let the envelope buy a historical read for the price
 * of an ordinary one.
 */
export function kqlPermissions(
  query: KqlQuery,
  boundToSnapshot = false,
): Permission[] {
  const needed: Permission[] = ['read']
  if (query.as_of !== null || boundToSnapshot) needed.push('read_history')
  if (query.where_clauses.some(projectsBelief)) needed.push('project')
  return needed
}

function projectsBelief(clause: WhereClause): boolean {
  if ('Belief' in clause || 'BeliefSlot' in clause) return true
  if ('Not' in clause) return clause.Not.some(projectsBelief)
  if ('Optional' in clause) return clause.Optional.some(projectsBelief)
  if ('Union' in clause) return clause.Union.some(projectsBelief)
  return false
}

/** What a META command needs. */
export function metaPermissions(command: MetaCommand): Permission[] {
  if ('Describe' in command) return describePermissions(command.Describe)
  if ('List' in command) return ['discover']
  if ('Search' in command) return ['search']
  // Legality, not disclosure: `VALIDATE` answers whether a command would be
  // accepted by the schema, which is what `DESCRIBE TYPE` already tells a caller
  // who may discover the Space at all.
  if ('Validate' in command) return ['discover']
  // A preview computes an effect over real state, so it discloses what a read
  // would. It is not a write and does not ask for one.
  if ('Preview' in command) return ['read']
  // Verification runs entirely on the artifact the caller supplied.
  if ('Verify' in command) return []
  if ('History' in command || 'Changes' in command) return ['read', 'read_history']
  if ('ExportCapsule' in command) return ['export']
  return FALLBACK
}

function describePermissions(target: DescribeTarget): Permission[] {
  // About the engine, not about the Space. §68 folded the old EXECUTION
  // CONTEXT answer into DESCRIBE PRIMER and the PROJECTION CAPABILITY answer
  // into DESCRIBE CAPABILITIES, so those two names no longer exist to gate.
  if (target === 'Protocol' || target === 'Capabilities') return []
  if (typeof target === 'string') return ['discover']
  if ('Error' in target || 'Compatibility' in target || 'EpistemicPolicy' in target) {
    return []
  }
  if ('Access' in target) return []
  if ('Trust' in target) return ['read']
  if ('Transaction' in target || 'TransactionByIdempotencyKey' in target) {
    return ['read_history']
  }
  // §68: the head coordinate is what DESCRIBE SPACE already reports; naming a
  // past one — by sequence or by instant — is a historical read.
  if ('Snapshot' in target) {
    return target.Snapshot.as_of === null && target.Snapshot.at_time === null
      ? ['discover']
      : ['read_history']
  }
  if ('SchemaEnvironment' in target && target.SchemaEnvironment.as_of !== null) {
    return ['discover', 'read_history']
  }
  return ['discover']
}

/** What a KML statement needs: the union over its clauses. */
export function kmlPermissions(statement: KmlStatement): Permission[] {
  const needed: Permission[] = []
  for (const clause of statement.clauses) {
    for (const permission of clausePermissions(clause)) {
      if (!needed.includes(permission)) needed.push(permission)
    }
  }
  return needed
}

/** What one clause needs. */
export function clausePermissions(clause: MutationClause): Permission[] {
  if ('CreateConcept' in clause || 'EnsureProposition' in clause) return ['create']
  // §29.8: `outcome` Evidence and the `outcome_observation` Activity that
  // links it to the decision it grades need `record_outcome` on top of
  // `create`. Only a written literal can be read here; a `:parameter` class
  // is bound at execution time, and the executor asks again per element.
  if ('CreateEvidence' in clause) {
    return literalField(clause.CreateEvidence.set_fields, 'evidence_class') ===
      'outcome'
      ? ['create', 'record_outcome']
      : ['create']
  }
  if ('CreateActivity' in clause) {
    return literalField(clause.CreateActivity.set_fields, 'activity_class') ===
      'outcome_observation'
      ? ['create', 'record_outcome']
      : ['create']
  }
  // An upsert either creates or changes, and the caller cannot know which in
  // advance — so it asks for both rather than for whichever turned out to happen.
  if ('UpsertConcept' in clause) return ['create', 'update']
  // The Assertion permission family is refined per Assertion in the write path:
  // recording another actor's claim and speaking as that actor are different
  // permissions, and which one applies depends on `asserted_by` (§17, §18).
  // `assert` is the floor.
  if ('CreateAssertion' in clause) return ['assert']
  if ('Update' in clause) return ['update']
  if ('Transition' in clause) return transitionPermissions(clause.Transition)
  if ('SetRetention' in clause) return ['manage_retention']
  // Payload purge asks for the same authority element purge asks for (§60.6);
  // a policy that wants the two scoped apart does it through the element-scoped
  // approval, not through a second permission name.
  if ('Purge' in clause || 'PurgePayload' in clause) return ['purge']
  if ('MergeConcept' in clause) return ['merge_identity', 'maintain']
  return FALLBACK
}

/**
 * What one lifecycle move asks for (§52.5), by the state it names.
 *
 * ```text
 * retracted                          retract_own        the source withdraws (§57.3)
 * superseded                         supersede_own      revision lineage (§57.4)
 * corrected                          create + maintain  a new record, linked (§57.2)
 * running | completed | failed | …   update             Activity status (§16)
 * archived                           archive            out of recall (§60.1)
 * tombstoned                         tombstone          logical deletion (§60.2)
 * ```
 *
 * A state written as a `:parameter` cannot be classified here: it is bound
 * from the envelope at execution time, and this gate runs before any binding
 * exists. So the gate asks for nothing extra for it, and the executor
 * authorizes each selected element with the precise permission once the state
 * is bound (`kml/clauses.ts`). That is not a way around the gate — every
 * element still pays the permission the literal form would have paid — it is
 * the same check, made at the first moment it can be made.
 */
export function transitionPermissions(transition: Transition): Permission[] {
  const state = literalState(transition)
  if (state === null) return []
  return permissionsForState(state)
}

/** The permission list one literal state maps to; see {@link transitionPermissions}. */
export function permissionsForState(state: string): Permission[] {
  switch (state) {
    case 'retracted':
      return ['retract_own']
    case 'superseded':
      return ['supersede_own']
    // Correcting Evidence is a maintenance act on an immutable record: it
    // writes a new record and links it, never edits the old one.
    case 'corrected':
      return ['create', 'maintain']
    case 'archived':
      return ['archive']
    case 'tombstoned':
      return ['tombstone']
    default:
      return (TRANSITION_ACTIVITY as readonly string[]).includes(state)
        ? ['update']
        : FALLBACK
  }
}

/**
 * The permission each *target element* of a `TRANSITION` is authorized with.
 *
 * One of {@link permissionsForState}, and the one that is about the element the
 * statement moves. `corrected` is the only state whose list has two entries:
 * `maintain` is the act performed on the target, and `create` is the Space-scope
 * cost of writing the replacing record — so `maintain` is what a Grant scoped to
 * the corrected Evidence has to carry, and the rest is asked for once.
 */
export function permissionForTargetState(state: string): Permission {
  if (state === 'corrected') return 'maintain'
  return permissionsForState(state)[0] ?? FALLBACK[0]!
}

/** The state a TRANSITION names, when it names one as a literal. */
function literalState(transition: Transition): string | null {
  const to = transition.to
  if (!('Literal' in to)) return null
  const value = to.Literal
  if (typeof value !== 'object' || value === null || !('String' in value)) {
    return null
  }
  // The parser already refused a word outside the registry; the check here
  // is what keeps a future parser that did not from reaching FALLBACK's
  // deliberately unusable answer by a different route.
  return (TRANSITION_STATES as readonly string[]).includes(value.String)
    ? value.String
    : null
}

/** The written string a `SET FIELDS` assigns to one field, if a literal. */
function literalField(fields: Assignments | null, name: string): string | null {
  if (fields === null) return null
  for (const [field, value] of fields) {
    if (field !== name) continue
    if (!('Value' in value)) return null
    const inner = value.Value
    return typeof inner === 'object' && inner !== null && 'String' in inner
      ? inner.String
      : null
  }
  return null
}
