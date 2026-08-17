/**
 * The one place a read reaches an element.
 *
 * Every pattern, filter, projection and aggregate loads through `Context.load`.
 * Keeping that single choke point is what will let the Governance plane decide
 * element visibility in one place rather than in every caller — returning
 * nothing there means the element is simply not in this caller's query
 * universe (§104), which is a different and safer thing than filtering results
 * afterwards.
 *
 * It is also where the query budget lives. A traversal that fans out has to be
 * stopped by something that counts, not by hoping the shape of the data is
 * reasonable.
 */

import { errors } from '../errors.js'
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

  private readonly elements = new Map<string, Element | null>()
  private readonly views = new Map<string, JsonMap>()

  constructor(store: Store, env: SchemaEnvironment, space: string) {
    this.store = store
    this.env = env
    this.space = space
  }

  /** Loads an element, or `null` when it is not in this caller's universe. */
  load(id: ElementId): Element | null {
    const key = formatElementId(id)
    const cached = this.elements.get(key)
    if (cached !== undefined) return cached

    this.spend('loads', 1)
    const element = this.store.load(id)
    const visible =
      element !== null && element.row.space === this.space ? element : null
    this.elements.set(key, visible)
    return visible
  }

  /** The rendered Core view of an element, computed once per query. */
  view(id: ElementId): JsonMap | null {
    const key = formatElementId(id)
    const cached = this.views.get(key)
    if (cached !== undefined) return cached
    const element = this.load(id)
    if (element === null) return null
    const view = render(element)
    this.views.set(key, view)
    return view
  }

  /** Caches an element the caller already has, so a scan pays for it once. */
  remember(element: Element): ElementId {
    const id: ElementId = { kind: element.kind, seq: element.row.id }
    const key = formatElementId(id)
    if (!this.elements.has(key)) {
      this.elements.set(key, element.row.space === this.space ? element : null)
    }
    return id
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
