/**
 * The one place a read reaches an element.
 *
 * Every pattern, filter, projection and aggregate loads through `Context.load`,
 * and that single choke point is where the Governance plane decides element
 * visibility. Returning `null` there means the element is not in this caller's
 * query universe (§104) — it is not matched, not counted, does not affect
 * ranking, and asking for it by id answers the same as asking for one that was
 * never written. That last part is deliberate: a distinguishable "exists but
 * hidden" is exactly the existence leak §103 is about, and filtering results
 * afterwards would produce one.
 *
 * The field mask is applied here too, to the *cached view*, for a reason worth
 * stating: a mask that only narrowed the projection list would still let
 * `FILTER(?c.attributes.salary > 200000)` answer the question it was meant to
 * refuse, because which rows come back is itself the disclosure.
 *
 * It is also where the query budget lives. A traversal that fans out has to be
 * stopped by something that counts, not by hoping the shape of the data is
 * reasonable.
 */

import { errors } from '../errors.js'
import type { AuthContext, EffectiveAuthority } from '../governance/index.js'
import { redactView, spaceResource } from '../governance/index.js'
import { formatElementId, type ElementId, type ElementKind } from '../id.js'
import type { JsonMap } from '../json.js'
import type { SchemaEnvironment } from '../schema/index.js'
import { State, TABLES, type Element, type Store } from '../store/index.js'
import { render } from '../view.js'

/** How much work one read may do before it is refused. */
export interface Budget {
  /** Elements loaded. */
  loads: number
  /** Candidate rows examined. */
  scans: number
}

/** The ceilings a single query runs under. */
export const LIMITS = {
  /** Distinct elements one query may load. */
  loads: 50_000,
  /** Rows one query may examine across all its patterns. */
  scans: 200_000,
  /** Intermediate solutions one query may hold. */
  solutions: 50_000,
} as const

export class Context {
  readonly store: Store
  readonly env: SchemaEnvironment
  readonly space: string
  readonly budget: Budget = { loads: 0, scans: 0 }
  /** What the caller may see here, resolved once for the whole read. */
  readonly authority: EffectiveAuthority
  /** Who the caller is. */
  readonly auth: AuthContext
  /**
   * The past coordinate this read is bound to, or `null` for the present.
   *
   * Every read in this context answers at the same coordinate: a query whose
   * patterns disagreed about *when* they were reading would join two different
   * Brains together and report the result as one.
   */
  readonly asOf: number | null

  /**
   * Whether `_system.origin` may be returned at all (§110).
   *
   * Space-scoped and decided once: engine origin is operational information
   * about the deployment rather than about any one element, so a caller either
   * may see who writes here or may not.
   */
  readonly readOrigin: boolean

  private readonly elements = new Map<string, Element | null>()
  private readonly views = new Map<string, JsonMap>()
  private readonly reconstructed = new Map<ElementKind, Element[]>()
  private governedResultLimit: number | null

  constructor(
    store: Store,
    env: SchemaEnvironment,
    space: string,
    authority: EffectiveAuthority,
    auth: AuthContext,
    asOf: number | null = null,
  ) {
    this.store = store
    this.env = env
    this.space = space
    this.authority = authority
    this.auth = auth
    this.asOf = asOf
    this.governedResultLimit = authority.authorize(
      'read',
      spaceResource(),
      auth,
    ).constraints.max_results
    this.readOrigin = isPermittedRead(authority, auth)
  }

  /** Whether this read is bound to a past coordinate. */
  get historical(): boolean {
    return this.asOf !== null
  }

  /** Loads an element, or `null` when it is not in this caller's universe. */
  load(id: ElementId): Element | null {
    const key = formatElementId(id)
    const cached = this.elements.get(key)
    if (cached !== undefined) return cached

    this.spend('loads', 1)
    const found =
      this.asOf === null
        ? this.store.load(id)
        : this.store.elementAt(this.space, id, this.asOf)
    const visible = this.admit(key, found)
    this.elements.set(key, visible)
    return visible
  }

  /** The rendered Core view of an element, computed once per query. */
  view(id: ElementId): JsonMap | null {
    const key = formatElementId(id)
    const cached = this.views.get(key)
    if (cached !== undefined) return cached
    if (this.load(id) === null) return null
    return this.views.get(key) ?? null
  }

  /** Caches an element the caller already has, so a scan pays for it once. */
  remember(element: Element): ElementId {
    const id: ElementId = { kind: element.kind, seq: element.row.id }
    const key = formatElementId(id)
    if (!this.elements.has(key)) {
      this.elements.set(key, this.admit(key, element))
    }
    return id
  }

  /**
   * Whether this caller's authority reaches every element in the Space.
   *
   * A Space-wide answer — a count, a total — is only honest when it is: a caller
   * whose Grant is narrowed must not be told how many elements exist outside it
   * (§106). Answered from the authority rather than by scanning, because the
   * point is to avoid producing the number at all.
   */
  readsWholeSpace(): boolean {
    return this.authority.readsWholeSpace(this.auth)
  }

  /**
   * Every element of one kind that existed at this read's coordinate.
   *
   * Only meaningful for a historical read, and it is a scan by necessity: the
   * indexes describe the present, and `{state: "active"}` today says nothing
   * about what was active at sequence 41. Charged to the same budget as
   * everything else, so a historical read of an enormous Space refuses rather
   * than stalls.
   *
   * The elements are remembered on the way out, so a later `view` answers from
   * the coordinate rather than re-reading the present. They still go through the
   * visibility check: a past coordinate is not a way around the present's
   * authorization, because the read is happening now, by this caller.
   *
   * Computed once per kind per query. Every historical pattern, every tuple
   * candidate and every functional-rival lookup asks for the same coordinate —
   * a query is bound to exactly one — so re-scanning the log per call would be
   * quadratic in the solution count and would charge the budget again each
   * time, refusing an ordinary query for work it did not need to do.
   */
  reconstruct(kind: ElementKind): Element[] {
    if (this.asOf === null) return []
    const cached = this.reconstructed.get(kind)
    if (cached !== undefined) return cached
    const elements = this.store.elementsAt(this.space, kind, this.asOf)
    this.spend('scans', elements.length)
    const visible: Element[] = []
    for (const element of elements) {
      const id = this.remember(element)
      if (this.load(id) !== null) visible.push(element)
    }
    this.reconstructed.set(kind, visible)
    return visible
  }

  /**
   * Applies the read decision to one element, caching its redacted view.
   *
   * Returns `null` for an element this caller may not read, and caches the
   * **redacted** view for one it may — so a `FILTER` or an `ORDER BY` on a
   * masked field sees what the projection would, rather than being able to probe
   * the value through row membership (§109).
   */
  private admit(key: string, element: Element | null): Element | null {
    if (element === null || element.row.space !== this.space) return null
    const constraints = this.authority.mayRead(element, this.auth)
    if (constraints === null) return null
    if (constraints.max_results !== null) {
      this.governedResultLimit =
        this.governedResultLimit === null
          ? constraints.max_results
          : Math.min(this.governedResultLimit, constraints.max_results)
    }
    const view = render(element)
    redactView(view, constraints, this.readOrigin)
    this.views.set(key, view)
    return element
  }

  /** The tightest result cap carried by an authority used by this read. */
  resultLimit(): number | null {
    return this.governedResultLimit
  }

  /** The SQL table one kind lives in. */
  table(kind: ElementKind): string {
    return TABLES[kind]
  }

  /**
   * Charges the budget, refusing rather than answering slowly.
   *
   * A query that would take a minute is not a slow query on a Durable Object,
   * it is a request that never returns: the isolate has a CPU ceiling and every
   * other caller of this Nexus is queued behind it.
   */
  spend(what: keyof Budget, amount: number): void {
    this.budget[what] += amount
    if (this.budget[what] > LIMITS[what]) {
      throw errors.resourceExhausted(
        `this query examined more than ${LIMITS[what]} ${what}; narrow the ` +
          `patterns or page it with LIMIT and CURSOR`,
      )
    }
  }

  /** The lifecycle state a pattern matches when it names none. */
  static readonly DEFAULT_STATE = State.ACTIVE
}

function isPermittedRead(authority: EffectiveAuthority, auth: AuthContext): boolean {
  const decision = authority.authorize('read_raw_origin', spaceResource(), auth)
  return decision.decision === 'allow' || decision.decision === 'allow_with_constraints'
}
