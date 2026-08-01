/**
 * TypeScript mirror of the `anda_kip` AST.
 *
 * These types describe the JSON that `rs/anda_kip_wasm` emits — serde's
 * default externally-tagged representation of the Rust enums in
 * `rs/anda_kip/src/ast.rs`. They are hand-written, which means they can drift
 * from the Rust definitions.
 *
 * Two things keep that drift from becoming a silent correctness problem:
 *
 *  1. Every `switch` over a tag in `src/exec/` ends in a `default:` that
 *     throws `internalError("unhandled ...")`, so an unknown variant fails
 *     loudly at the exact clause instead of being skipped.
 *  2. `test/ast-coverage.test.ts` parses a corpus of KIP commands and asserts
 *     that every tag appearing in the output is one this file declares.
 *
 * When the grammar gains a variant, both will point at it.
 */

/** A JSON value as produced by `serde_json`. */
export type Json =
  | null
  | boolean
  | number
  | string
  | Json[]
  | { [key: string]: Json }

/** `ast::Value` — a KIP literal. Serialized as an externally-tagged enum. */
export type KipValue =
  | 'Null'
  | { Bool: boolean }
  | { Number: number }
  | { String: string }
  | { Array: KipValue[] }
  | { Object: Record<string, KipValue> }

/** Top-level parsed command. */
export type Command =
  | { Kql: KqlQuery }
  | { Kml: KmlStatement }
  | { Meta: MetaCommand }

// ---------------------------------------------------------------------------
// KQL
// ---------------------------------------------------------------------------

export interface KqlQuery {
  find_clause: FindClause
  where_clauses: WhereClause[]
  order_by: OrderByItem[] | null
  limit: number | null
  cursor: string | null
}

export interface FindClause {
  expressions: FindExpression[]
}

/** `?var` optionally followed by a dot path, e.g. `?d.attributes.risk`. */
export interface DotPathVar {
  var: string
  path: string[]
}

export type FindExpression =
  | { Variable: DotPathVar }
  | { Aggregation: { func: AggregationFunction; var: DotPathVar; distinct: boolean } }

export type AggregationFunction = 'Count' | 'Sum' | 'Avg' | 'Min' | 'Max'

export interface OrderByItem {
  variable: DotPathVar
  direction: OrderDirection
  aggregation: AggregationFunction | null
}

export type OrderDirection = 'Asc' | 'Desc'

export type WhereClause =
  | { Concept: { variable: string; matcher: ConceptMatcher } }
  | { Proposition: { variable: string | null; matcher: PropositionMatcher } }
  | { Filter: { expression: FilterExpression } }
  | { Not: WhereClause[] }
  | { Optional: WhereClause[] }
  | { Union: WhereClause[] }

export type ConceptMatcher =
  | { ID: string }
  | { Type: string }
  | { Name: string }
  | { Object: { type: string; name: string } }

export type PropositionMatcher =
  | { ID: string }
  | {
      Object: {
        subject: TargetTerm
        predicate: PredTerm
        object: TargetTerm
      }
    }

/**
 * One endpoint of a proposition pattern.
 *
 * `Proposition` is the meta-statement case: an endpoint may itself be a
 * proposition pattern, which is how KIP expresses statements about
 * statements. It nests arbitrarily deep.
 */
export type TargetTerm =
  | { Variable: string }
  | { Concept: ConceptMatcher }
  | { Proposition: PropositionMatcher }

export type PredTerm =
  | { Variable: string }
  | { Literal: string }
  | { Alternative: string[] }
  | { MultiHop: { predicate: string; min: number; max: number | null } }

export type FilterExpression =
  | {
      Comparison: {
        left: FilterOperand
        operator: ComparisonOperator
        right: FilterOperand
      }
    }
  | {
      Logical: {
        left: FilterExpression
        operator: LogicalOperator
        right: FilterExpression
      }
    }
  | { Not: FilterExpression }
  | { Function: { func: FilterFunction; args: FilterOperand[] } }

export type FilterOperand =
  | { Variable: DotPathVar }
  | { Literal: KipValue }
  | { List: KipValue[] }

export type ComparisonOperator =
  | 'Equal'
  | 'NotEqual'
  | 'LessThan'
  | 'GreaterThan'
  | 'LessEqual'
  | 'GreaterEqual'

export type LogicalOperator = 'And' | 'Or'

export type FilterFunction =
  | 'Contains'
  | 'StartsWith'
  | 'EndsWith'
  | 'Regex'
  /** `IN(?expr, [a, b])` — membership. A function, not a comparison operator. */
  | 'In'
  | 'IsNull'
  | 'IsNotNull'

// ---------------------------------------------------------------------------
// KML
// ---------------------------------------------------------------------------

export type KmlStatement =
  | { Upsert: UpsertBlock[] }
  | { Update: UpdateStatement }
  | { Merge: MergeStatement }
  | { Delete: DeleteStatement }

export interface UpsertBlock {
  items: UpsertItem[]
  metadata: Record<string, Json> | null
}

export type UpsertItem =
  | { Concept: ConceptBlock }
  | { Proposition: PropositionBlock }

export interface ConceptBlock {
  handle: string | null
  concept: ConceptMatcher
  set_attributes: Record<string, Json> | null
  set_propositions: SetProposition[] | null
  metadata: Record<string, Json> | null
  /** `EXPECT VERSION <n>` — optimistic guard, absent when not written. */
  expect_version?: number | null
}

export interface SetProposition {
  predicate: string
  object: TargetTerm
  metadata: Record<string, Json> | null
}

export interface PropositionBlock {
  handle: string | null
  proposition: PropositionMatcher
  set_attributes: Record<string, Json> | null
  metadata: Record<string, Json> | null
  expect_version?: number | null
}

export interface UpdateStatement {
  target: string
  set_attributes: [string, UpdateValue][] | null
  set_metadata: [string, UpdateValue][] | null
  where_clauses: WhereClause[]
  limit: number | null
}

/**
 * An UPDATE right-hand side: either a literal or an arithmetic expression over
 * the target's *own* fields. The parser rejects references to any other
 * variable (`check_update_expr_targets`), which is what lets each element be
 * updated from its own row without a join.
 */
export type UpdateValue = { Json: Json } | { Expr: UpdateExpr }

/** An arithmetic node inside an UPDATE right-hand side. */
export type UpdateExpr = {
  Function: { func: UpdateFunction; args: UpdateOperand[] }
}

export type UpdateFunction = 'Add' | 'Mul' | 'Clamp' | 'Coalesce'

/** Operands nest: `CLAMP(MUL(?c.attributes.x, 0.5), 0.0, 1.0)`. */
export type UpdateOperand =
  | { Variable: DotPathVar }
  | { Number: number }
  | { Function: { func: UpdateFunction; args: UpdateOperand[] } }

export interface MergeStatement {
  source: string
  target: string
  where_clauses: WhereClause[]
}

export type DeleteStatement =
  | {
      DeleteAttributes: {
        attributes: string[]
        target: string
        where_clauses: WhereClause[]
      }
    }
  | {
      DeleteMetadata: {
        keys: string[]
        target: string
        where_clauses: WhereClause[]
      }
    }
  | { DeletePropositions: { target: string; where_clauses: WhereClause[] } }
  | { DeleteConcept: { target: string; where_clauses: WhereClause[] } }

// ---------------------------------------------------------------------------
// META
// ---------------------------------------------------------------------------

export type MetaCommand =
  | { Describe: DescribeTarget }
  | { Search: SearchCommand }
  | { Export: ExportCommand }

export type DescribeTarget =
  | 'Primer'
  | 'Domains'
  | { ConceptTypes: { limit: number | null; cursor: string | null } }
  | { ConceptType: string }
  | { PropositionTypes: { limit: number | null; cursor: string | null } }
  | { PropositionType: string }

export interface SearchCommand {
  target: SearchTarget
  term: string
  in_type: string | null
  mode: SearchMode | null
  threshold: number | null
  limit: number | null
}

export type SearchTarget = 'Concept' | 'Proposition'

export type SearchMode = 'Keyword' | 'Semantic' | 'Hybrid'

export interface ExportCommand {
  target: string
  where_clauses: WhereClause[]
  limit: number | null
  cursor: string | null
}

// ---------------------------------------------------------------------------
// Narrowing helpers
// ---------------------------------------------------------------------------

/** Returns the single tag of an externally-tagged enum object. */
export function tagOf(value: object): string {
  const keys = Object.keys(value)
  if (keys.length !== 1) {
    throw new Error(
      `expected an externally-tagged enum with exactly one key, got ${keys.length}: ${keys.join(', ')}`,
    )
  }
  return keys[0]!
}

/** True when the enum value is the given unit variant (a bare string). */
export function isUnit<T extends string>(
  value: unknown,
  variant: T,
): value is T {
  return value === variant
}
