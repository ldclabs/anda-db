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
 *   (§78, and the `read ≠ export` equation in §271).
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
  DescribeTarget,
  KmlStatement,
  KqlQuery,
  MetaCommand,
  MutationClause,
  WhereClause,
} from '../kip/ast.js'
import type { Permission } from './permission.js'

/**
 * What an unrecognized clause asks for.
 *
 * Not "nothing" and not "create": a clause nobody has classified must be
 * unreachable for an ordinary caller, and naming a Governance permission no
 * cognitive Grant confers is how that is spelled.
 */
const FALLBACK: Permission[] = ['manage_policy']

/** What a KQL query needs. */
export function kqlPermissions(query: KqlQuery): Permission[] {
  const needed: Permission[] = ['read']
  if (query.as_of !== null) needed.push('read_history')
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
  if ('Snapshot' in command) return ['read_history']
  if ('ExportCapsule' in command) return ['export']
  return FALLBACK
}

function describePermissions(target: DescribeTarget): Permission[] {
  // About the engine, not about the Space.
  if (
    target === 'Protocol' ||
    target === 'Capabilities' ||
    target === 'ProjectionCapability'
  ) {
    return []
  }
  // About the caller itself. §266: an Agent must be able to learn what it may do
  // without first being permitted to do it.
  if (target === 'ExecutionContext') return []
  if (typeof target === 'string') return ['discover']
  if ('Error' in target || 'Compatibility' in target || 'EpistemicPolicy' in target) {
    return []
  }
  if ('Access' in target) return []
  if ('Trust' in target) return ['read']
  if (
    'Transaction' in target ||
    'TransactionByIdempotencyKey' in target ||
    'Snapshot' in target
  ) {
    return ['read_history']
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
  if (
    'CreateConcept' in clause ||
    'EnsureProposition' in clause ||
    'CreateEvidence' in clause ||
    'CreateActivity' in clause
  ) {
    return ['create']
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
  if ('RetractAssertion' in clause) return ['retract_own']
  if ('SupersedeAssertion' in clause) return ['supersede_own']
  // Correcting Evidence is a maintenance act on an immutable record: it writes a
  // new record and links it, never edits the old one.
  if ('CorrectEvidence' in clause) return ['create', 'maintain']
  if ('TransitionActivity' in clause) return ['update']
  if ('SetRetention' in clause) return ['manage_retention']
  if ('Archive' in clause) return ['archive']
  if ('Tombstone' in clause) return ['tombstone']
  if ('Purge' in clause) return ['purge']
  if ('MergeConcept' in clause) return ['merge_identity', 'maintain']
  return FALLBACK
}
