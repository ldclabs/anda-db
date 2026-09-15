import { projectionPolicyAt } from '../control.js'
import { dependencyValidity, isDerived } from '../projection/dependency.js'
import { baseline } from '../projection/policy.js'
import { projectionBasis } from '../projection/index.js'
import { nowTime } from '../time.js'
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
import {
  isPermitted,
  redactView,
  spaceResource,
  toIdentityOnly,
} from '../governance/index.js'
import {
  elementIdEquals,
  formatElementId,
  tryParseElementId,
  type ElementId,
  type ElementKind,
} from '../id.js'
import { isJsonMap, type Json, type JsonMap } from '../json.js'
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
  validAt = nowTime()
  projectionPolicy = baseline()
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
   * Whether `_system.origin` may be returned at all (§29).
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
    try { this.projectionPolicy = projectionPolicyAt(store, space, asOf ?? store.currentSeq(space), {}) } catch { /* Raw history remains readable; derived status is unavailable. */ }
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
  load(id: ElementId, validate = true): Element | null {
    const key = formatElementId(id)
    let element = this.elements.get(key)
    if (element === undefined) {
      this.spend('loads', 1)
      const found = this.asOf === null ? this.store.load(id) : this.store.elementAt(this.space, id, this.asOf)
      element = this.admit(key, found)
      this.elements.set(key, element)
    }
    if (validate && element && (isDerived(element) || this.store.controlAt(this.space, `identity_review/${key}`, this.asOf ?? this.store.currentSeq(this.space)))) {
      const view = this.views.get(key)
      if (view && isJsonMap(view._system)) view._system.dependency_validity = dependencyValidity(this, element, this.projectionPolicy, this.validAt)
    }
    this.filterReferenceAudit(key)
    return element
  }

  /** Hide both spellings of an audited reference unless both are readable. */
  private filterReferenceAudit(key: string): void {
    const view = this.views.get(key),
      system = view && isJsonMap(view._system) ? view._system : null,
      bindings = system?.input_references
    if (!system || !Array.isArray(bindings)) return
    system.input_references = bindings.filter((value) => {
      if (!isJsonMap(value)) return false
      return ['supplied', 'resolved'].every((name) => {
        const reference = tryParseElementId(String(value[name] ?? ''))
        if (!reference) return false
        const row =
          this.asOf === null
            ? this.store.load(reference)
            : this.store.elementAt(this.space, reference, this.asOf)
        return (
          !!row &&
          row.row.space === this.space &&
          this.authority.mayRead(row, this.auth)?.content === true
        )
      })
    })
  }

  /** The rendered Core view of an element, computed once per query. */
  view(id: ElementId): JsonMap | null {
    const key = formatElementId(id)
    const cached = this.views.get(key)
    if (cached !== undefined) return cached
    if (this.load(id) === null) return null
    return this.views.get(key) ?? null
  }

  /**
   * Makes an explicitly declared, resolved mutation output readable inside its
   * transaction. This isolated copy goes through ordinary read authorization
   * and redaction; it never publishes the draft or adds it to an unbound scan.
   * The caller must resolve the output's fields before seeding it.
   */
  seedElement(id: ElementId, element: Element): void {
    if (this.historical || id.kind !== element.kind || id.seq !== element.row.id) {
      throw errors.internalError('a mutation output does not belong to this live query context')
    }
    const key = formatElementId(id)
    const copy = structuredClone(element)
    if (copy.row.space === '') copy.row.space = this.space
    // PENDING is an internal reservation state, not the output's logical state
    // once its fields are resolved. Only this private read copy is activated.
    if (copy.row.state === State.PENDING) copy.row.state = State.ACTIVE
    this.views.delete(key)
    this.elements.set(key, this.admit(key, copy))
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
   * (§88.6). Answered from the authority rather than by scanning, because the
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
   * Returns `null` for an element this caller may not *discover*, and caches
   * the **redacted** view for one it may — so a `FILTER` or an `ORDER BY` on a
   * masked field sees what the projection would, rather than being able to probe
   * the value through row membership (§29.2).
   *
   * An element the caller may discover but not read comes back with its
   * identity and nothing else (§29.2). It still exists, is still counted and
   * can still be cited; what it says stays closed.
   */
  private admit(key: string, element: Element | null): Element | null {
    if (element === null || element.row.space !== this.space) return null
    const visibility = this.authority.mayRead(element, this.auth)
    if (visibility === null) return null
    const { constraints } = visibility
    if (constraints.max_results !== null) {
      this.governedResultLimit =
        this.governedResultLimit === null
          ? constraints.max_results
          : Math.min(this.governedResultLimit, constraints.max_results)
    }
    const view = render(element)
    if ((element.kind === 'Assertion' && element.row.mode === 'inferred') ||
        (element.kind === 'Concept' && (['SkillRevision', 'Insight', 'WorkingState'].some((name) => element.row.schema_ref.endsWith('/' + name)) || Object.keys(element.row.structural).some((name) => name.endsWith('/derived_from'))))) {
      (view._system as JsonMap).dependency_validity = {
        status: 'unverifiable', action_eligible: false,
        reasons: ['recursive dependency validation is unavailable'],
        basis: projectionBasis(this, this.projectionPolicy, this.validAt),
      }
    }
    if (element.kind === 'Proposition') {
      // §43.2: the binding keeps both views. `subject` / `object` are the
      // stored endpoints; `canonical_subject` / `canonical_object` follow
      // `merged_into` to the identity that survived — at this read's
      // coordinate, so a coordinate before the merge resolves nothing through
      // it (§48.1).
      view.canonical_subject = this.canonicalEndpoint(element.row.subject as Json)
      view.canonical_object = this.canonicalEndpoint(element.row.object as Json)
    }
    if (visibility.content) {
      redactView(view, constraints, this.readOrigin)
    } else {
      toIdentityOnly(view)
    }
    this.views.set(key, view)
    return element
  }

  /**
   * The Concept a local Concept reference resolves to after merges (§12.3).
   *
   * Follows `merged_into` to its fixpoint through this read's own loads, so
   * the chain is read at the read's coordinate and stops at an element this
   * caller may not discover — naming one it may not would be the existence
   * leak §30.4 forbids. Bounded, because a corrupt chain must hang nothing.
   */
  canonicalOf(id: ElementId): ElementId {
    let cursor = id
    for (let hop = 0; hop < 64; hop += 1) {
      if (cursor.kind !== 'Concept') return cursor
      const element = this.load(cursor)
      if (element === null || element.kind !== 'Concept') return cursor
      if (element.row.merged_into === '') return cursor
      const next = tryParseElementId(element.row.merged_into)
      if (next === null || elementIdEquals(next, cursor)) return cursor
      cursor = next
    }
    return cursor
  }

  /**
   * Every Concept whose `merged_into` chain ends at the same identity as
   * this one (§43.2): the canonical target and everything merged into it,
   * transitively. What a raw Proposition pattern's endpoint matches through.
   */
  canonicalCluster(id: ElementId): ElementId[] {
    const canonical = this.canonicalOf(id)
    if (canonical.kind !== 'Concept') return [canonical]
    const out: ElementId[] = [canonical]
    const seen = new Set<string>([formatElementId(canonical)])
    let frontier = [canonical]
    while (frontier.length > 0) {
      const next: ElementId[] = []
      for (const target of frontier) {
        for (const source of this.mergedInto(target)) {
          const key = formatElementId(source)
          if (seen.has(key)) continue
          seen.add(key)
          // Only a Concept this caller may discover joins the cluster: the
          // pattern then matches what it may see, and nothing else.
          if (this.load(source) === null) continue
          out.push(source)
          next.push(source)
        }
      }
      frontier = next
    }
    return out
  }

  /** The Concepts whose `merged_into` names one target, at this read's coordinate. */
  private mergedInto(target: ElementId): ElementId[] {
    const named = formatElementId(target)
    if (this.historical) {
      return this.reconstruct('Concept')
        .filter((element) => element.kind === 'Concept' && element.row.merged_into === named)
        .map((element) => ({ kind: 'Concept', seq: element.row.id }) as ElementId)
    }
    const rows = this.store.sql
      .exec<{ id: number }>(
        'SELECT id FROM concepts WHERE space = ? AND merged_into = ? ORDER BY id',
        this.space,
        named,
      )
      .toArray()
    this.spend('scans', rows.length)
    return rows.map((row) => ({ kind: 'Concept', seq: row.id }) as ElementId)
  }

  canonicalEndpoint(endpoint: Json): Json {
    if (!isJsonMap(endpoint) || typeof endpoint.id !== 'string') return endpoint
    const id = tryParseElementId(endpoint.id)
    if (id === null || id.kind !== 'Concept') return endpoint
    const canonical = this.canonicalOf(id)
    return elementIdEquals(canonical, id) ? endpoint : { id: formatElementId(canonical) }
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
  return isPermitted(
    authority.authorize('read_raw_origin', spaceResource(), auth).decision,
  )
}
