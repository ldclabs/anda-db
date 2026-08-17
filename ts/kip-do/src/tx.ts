/**
 * # Transactions
 *
 * A KML statement is one atomic cognitive transition (Spec §2), and three of
 * its properties are the reason this module exists rather than each clause
 * writing directly.
 *
 * **A mutation block is declarative, not sequential** (§21–§24). Forward
 * references are legal, and they have to be: `Evidence.generated_by → Activity`
 * and `Activity.outputs → Evidence` is a legitimate structural cycle, so a
 * define-before-use ordering would make atomic provenance formation impossible.
 * Planning therefore happens in two phases — declare every handle, then
 * interpret every clause with all handles known.
 *
 * **An element's version increments once per transaction** (§44), no matter how
 * many clauses touched it. A transaction is one externally visible state
 * transition, and `EXPECT VERSION`, audit and the change stream all read that
 * counter. So versions are assigned here, at commit, not by each write.
 *
 * **A no-effect final state changes nothing.** Writing the same value back
 * would burn a version and emit a change record for a transition that did not
 * happen.
 *
 * ## What this engine gives you that the Rust one does not
 *
 * `ctx.storage.transactionSync` is a real transaction: a clause that throws
 * rolls the whole statement back, shells included. The Rust engine has no
 * write-ahead log and recovers by sweeping `pending` elements on open. Shells
 * still exist here, because a row id is only assigned by inserting and a
 * forward reference needs the id first — but they are no longer the recovery
 * mechanism, and `sweepPending` is kept as a cheap invariant check rather than
 * as the thing that makes a crash survivable.
 *
 * A Durable Object is also single-threaded, so there is no concurrent reader to
 * observe a half-applied transaction and no lock to hold against one.
 */

import { errors } from './errors.js'
import {
  formatElementId,
  type ElementId,
  type ElementKind,
} from './id.js'
import type { Json, JsonMap } from './json.js'
import type { SchemaEnvironment } from './schema/index.js'
import {
  State,
  type ChangeEntry,
  type ChangeOp,
  type Element,
  type Store,
} from './store/index.js'
import { nowTime } from './time.js'

/** One element this transaction will write. */
interface Staged {
  element: Element
  isNew: boolean
  /** Whether the final state differs from what was there before. */
  changed: boolean
  /** What the change record calls this. */
  op: ChangeOp
  /** The version the element had when this transaction first loaded it. */
  baseVersion: number
}

/** What a committed (or previewed) transaction produced. */
export interface Outcome {
  status: 'committed' | 'no_effect'
  tx_id: string
  space_id: string
  space_seq: number | null
  snapshot_seq: number
  committed_at: string | null
  schema_environment_version: number
  /** Every handle this mutation bound, mapped to the element it named. */
  handles: Record<string, string>
  /** One record per changed element. */
  changes: ChangeEntry[]
  warnings: string[]
}

/** The engine truth stamped on everything one transaction writes. */
export interface WriteContext {
  tx_id: string
  space: string
  at: string
  /**
   * `_system.origin` — what the runtime observed, never a claim (§26).
   *
   * The Principal comes from the authenticated session and the channel from the
   * transport, never from the command's content: content that could set this
   * would be laundering provenance.
   */
  origin: JsonMap
}

/** The engine state one KML statement runs against. */
export class Transaction {
  readonly store: Store
  readonly cx: WriteContext
  /**
   * The Schema Environment this transaction is bound to.
   *
   * Captured once at the start: a transaction evaluates against one consistent
   * environment snapshot (§240.45), so an activation racing alongside cannot
   * change what half of it means.
   */
  readonly env: SchemaEnvironment
  /** Whether this run may become durable. */
  readonly dryRun: boolean
  /** The Space sequence the transaction started from. */
  readonly snapshotSeq: number

  private readonly handleMap = new Map<string, ElementId>()
  private readonly staged = new Map<string, Staged>()
  private readonly shells: ElementId[] = []
  private readonly warnings: string[] = []

  constructor(
    store: Store,
    space: string,
    env: SchemaEnvironment,
    origin: JsonMap,
    dryRun: boolean,
  ) {
    this.store = store
    this.env = env
    this.dryRun = dryRun
    this.snapshotSeq = store.currentSeq(space)
    this.cx = {
      // Provisional until commit: a dry run never advances the Space clock, so
      // the id it reports must not claim a coordinate it did not take.
      tx_id: `tx-${space}-${this.snapshotSeq + 1}-${nowTime()}`,
      space,
      at: nowTime(),
      origin,
    }
  }

  /** Records a non-fatal caveat. */
  warn(message: string): void {
    this.warnings.push(message)
  }

  /** The handles bound so far, in wire form. */
  handles(): Record<string, string> {
    return Object.fromEntries(
      [...this.handleMap].map(([name, id]) => [name, formatElementId(id)]),
    )
  }

  /** The element a handle names, or `null` when it names none. */
  handle(name: string): ElementId | null {
    return this.handleMap.get(name) ?? null
  }

  /**
   * Declares a handle and mints the element it will name.
   *
   * The id has to exist before any clause is interpreted, because a clause may
   * reference a handle a later clause declares.
   *
   * A handle may be declared exactly once (§25): two clauses binding `?x` leave
   * every reference to it ambiguous, and picking either one would be a guess.
   */
  declare(name: string, kind: ElementKind): ElementId {
    this.requireFreeHandle(name)
    const id = this.mint(kind)
    this.handleMap.set(name, id)
    return id
  }

  /** Mints an element with no handle — an anonymous `ENSURE PROPOSITION`. */
  mint(kind: ElementKind): ElementId {
    const id = this.store.reserve(kind, this.cx.space)
    this.shells.push(id)
    return id
  }

  /**
   * Binds a handle to an element that already has an id.
   *
   * Used by the clauses whose target cannot be minted up front — `UPSERT` may
   * resolve to an existing Concept and `ENSURE` to an existing tuple — so their
   * handles are bound during interpretation rather than declaration.
   */
  bindExisting(name: string, id: ElementId): void {
    this.requireFreeHandle(name)
    this.handleMap.set(name, id)
  }

  private requireFreeHandle(name: string): void {
    if (this.handleMap.has(name)) {
      throw errors.duplicateLocalHandle(
        `?${name} is declared more than once in this mutation block`,
      )
    }
  }

  /** Stages a newly created element's final row. */
  stageNew(id: ElementId, element: Element, op: ChangeOp = 'create'): void {
    this.staged.set(formatElementId(id), {
      element,
      isNew: true,
      changed: true,
      op,
      baseVersion: 0,
    })
  }

  /**
   * Loads an existing element for modification, or returns the staged copy.
   *
   * Read-your-writes inside the transaction (§27): a clause that reads an
   * element another clause already changed sees the change, because both are
   * the same staged row.
   */
  load(id: ElementId): Element {
    const key = formatElementId(id)
    const found = this.staged.get(key)
    if (found !== undefined) return found.element

    const element = this.store.load(id)
    if (element === null) {
      throw errors.notFoundOrNotVisible(`no element ${key}`)
    }
    if (element.row.space !== this.cx.space) {
      throw errors.notFoundOrNotVisible(`${key} lives in another MemorySpace`)
    }
    this.staged.set(key, {
      element,
      isNew: false,
      changed: false,
      op: 'update',
      baseVersion: element.row.version,
    })
    return element
  }

  /**
   * The Concept type of a staged element, when this transaction staged one.
   *
   * Endpoint validation has to see the transaction's own writes: a Proposition
   * whose subject was created by an earlier clause of the same block would
   * otherwise look untyped.
   */
  stagedConceptType(id: ElementId): string | null {
    const staged = this.staged.get(formatElementId(id))
    if (staged?.element.kind !== 'Concept') return null
    const ref = staged.element.row.schema_ref
    return ref === '' ? null : ref
  }

  /**
   * Marks a staged element as actually changed.
   *
   * Separate from {@link load} because loading is not modifying: a clause that
   * reads an element and decides to do nothing must not burn a version.
   */
  markChanged(id: ElementId, op: ChangeOp): void {
    const staged = this.staged.get(formatElementId(id))
    if (staged === undefined) return
    staged.changed = true
    if (!staged.isNew) staged.op = op
  }

  /**
   * Checks an `EXPECT VERSION` guard against the pre-transaction version.
   *
   * The comparison is against the version the element had when the transaction
   * started, not a value this transaction produced: a guard is a statement
   * about what the caller believed, and the caller could not have seen a
   * version that does not exist yet.
   */
  expectVersion(id: ElementId, expected: number): void {
    this.load(id)
    const staged = this.staged.get(formatElementId(id))
    const actual = staged?.isNew === true ? 0 : (staged?.baseVersion ?? 0)
    if (actual !== expected) {
      throw errors.versionConflict(
        `${formatElementId(id)} is at version ${actual}, not the expected ` +
          `${expected}; re-read it and decide again`,
      )
    }
  }

  /** Checks an `EXPECT STATE` guard against the engine state. */
  expectState(id: ElementId, expected: string): void {
    const actual = this.load(id).row.state
    if (actual !== expected) {
      throw errors.preconditionFailed(
        `${formatElementId(id)} is in state ${JSON.stringify(actual)}, not ` +
          `the expected ${JSON.stringify(expected)}`,
      )
    }
  }

  /**
   * Checks an `EXPECT STATE` guard against an Assertion's lifecycle status.
   *
   * Distinct from {@link expectState}, which reads the *engine* state: an
   * Assertion can be epistemically retracted while its record is perfectly
   * active, and confusing the two would let a guard pass on the wrong question.
   */
  expectAssertionStatus(id: ElementId, expected: string): void {
    const element = this.load(id)
    if (element.kind !== 'Assertion') {
      throw errors.structuralReferenceInvalid(
        `${formatElementId(id)} is not an Assertion`,
      )
    }
    if (element.row.status !== expected) {
      throw errors.preconditionFailed(
        `${formatElementId(id)} is ${JSON.stringify(element.row.status)}, ` +
          `not the expected ${JSON.stringify(expected)}`,
      )
    }
  }

  /**
   * Commits everything staged, or reports what a dry run would have done.
   *
   * A dry run never establishes a durable cognitive commit (§69.3): it takes no
   * Space sequence and journals nothing. The caller runs it inside a
   * transaction it rolls back, so the shells go with it.
   */
  commit(idempotencyKey: string): Outcome {
    const pending = [...this.staged.entries()].filter(
      ([, staged]) => staged.changed,
    )
    const versionOf = (staged: Staged) =>
      staged.isNew ? 1 : staged.baseVersion + 1

    if (this.dryRun) {
      // A dry run leaves nothing behind. The caller usually wraps this in a
      // transaction it rolls back, but PREVIEW does not — and a shell that
      // survives is an id nobody can reach and nothing will reuse.
      this.discardShells()
      return this.outcome(
        'no_effect',
        null,
        null,
        pending.map(([id, staged]) => ({
          id,
          kind: staged.element.kind,
          op: staged.op,
          version: versionOf(staged),
        })),
      )
    }

    if (pending.length === 0) {
      // No sequence is taken, because nothing happened: a Space clock that
      // ticks for a no-op makes every `CHANGES SINCE` cursor report a change
      // that is not there.
      return this.outcome('no_effect', null, null, [])
    }

    const seq = this.store.nextSeq(this.cx.space)
    const committedAt = nowTime()
    const changes: ChangeEntry[] = []
    const written = new Set<string>()

    for (const [id, staged] of pending) {
      const version = versionOf(staged)
      const row = staged.element.row
      row.space = this.cx.space
      row.version = version
      row.seq = seq
      row.updated_at = committedAt
      row.updated_tx = this.cx.tx_id
      row.origin = this.cx.origin
      if (staged.isNew) {
        row.created_at = committedAt
        row.created_tx = this.cx.tx_id
      }
      if (row.state === '' || row.state === State.PENDING) {
        row.state = State.ACTIVE
      }
      changes.push(this.store.put(staged.element, staged.op, this.cx.tx_id))
      written.add(id)
    }

    // A shell nobody staged is a handle that was declared and never filled in —
    // a planning bug rather than data, so it is removed instead of committed
    // half-formed.
    this.discardUnwritten(written)

    this.store.putTransaction({
      tx_id: this.cx.tx_id,
      space: this.cx.space,
      seq,
      snapshot_seq: this.snapshotSeq,
      committed_at: committedAt,
      status: 'committed',
      transaction_class: 'cognitive',
      idempotency_key: idempotencyKey,
      request_digest: '',
      semantic_plan_digest: '',
      result_digest: '',
      schema_environment_version: this.env.version,
      result: { handles: this.handles() } as Json,
      changes,
    })

    return this.outcome('committed', seq, committedAt, changes)
  }

  /**
   * Removes the shells this run minted.
   *
   * Called on the paths that do not commit. `transactionSync` would roll them
   * back anyway; doing it explicitly keeps a `PREVIEW` that runs outside one
   * from leaving rubbish behind.
   */
  discardShells(): void {
    this.discardUnwritten(new Set())
  }

  private discardUnwritten(written: ReadonlySet<string>): void {
    for (const id of this.shells) {
      if (!written.has(formatElementId(id))) {
        this.store.removeShell(id)
      }
    }
    this.shells.length = 0
  }

  private outcome(
    status: 'committed' | 'no_effect',
    seq: number | null,
    committedAt: string | null,
    changes: ChangeEntry[],
  ): Outcome {
    return {
      status,
      tx_id: this.cx.tx_id,
      space_id: this.cx.space,
      space_seq: seq,
      snapshot_seq: this.snapshotSeq,
      committed_at: committedAt,
      schema_environment_version: this.env.version,
      handles: this.handles(),
      changes,
      warnings: this.warnings,
    }
  }
}
