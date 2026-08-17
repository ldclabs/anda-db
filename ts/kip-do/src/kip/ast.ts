/**
 * The executable KIP 2.0 AST this engine runs.
 *
 * The definitions live in `@ldclabs/kip-lang`, which produces them: `lower` is
 * what collapses a syntax tree into these shapes, so the types and the code
 * that builds them cannot drift apart. They are re-exported under their local
 * names — kip-lang prefixes the ones that collide with a syntax-tree node —
 * so `src/exec/` imports its AST from one place.
 *
 * There are no runtime narrowing helpers here on purpose. `lower` closes every
 * enum before the executor sees it: an unknown filter function, a predicate
 * path where an exact predicate is required, an UPDATE expression reading a
 * foreign variable, `ASSERT` sugar still un-desugared — all are rejected there.
 * A `switch` over these tags is therefore total, and TypeScript checks it.
 *
 * The shape is the wire form of `anda_kip`'s Rust AST (serde's default
 * externally-tagged encoding), which is what lets `test/parser-oracle.test.ts`
 * compare the two engines field for field.
 */

export type {
  // Root
  Command,
  BoundValue,
  KipValue,
  Scalar,
  SymbolRef,
  ElementRef,
  // Shared terms
  DotPathVar,
  PathStep,
  PredAtom,
  PredTerm,
  PredPathAtom,
  HopRange,
  ExecTerm as Term,
  ObjectMatcher,
  MatchValue,
  PropositionMatcher,
  // KQL
  KqlQuery,
  FindClause,
  FindExpression,
  AggregationFunction,
  AsOf,
  OrderByItem,
  OrderDirection,
  ExecWhereClause as WhereClause,
  BeliefTarget,
  FilterExpression,
  FilterOperand,
  FilterFunction,
  ComparisonOperator,
  LogicalOperator,
  // KML
  ExecKmlStatement as KmlStatement,
  ExecMutationClause as MutationClause,
  ConceptCreate,
  ConceptUpsert,
  RecordCreate,
  EnsureProposition,
  FacetAssignment,
  FacetUnset,
  StructuralEdge,
  ExecStructuralRemoval as StructuralRemoval,
  Assignments,
  MutationValue,
  UpdateExpr,
  UpdateFunction,
  ExecUpdateStatement as UpdateStatement,
  ExecUpdateAction as UpdateAction,
  RetractAssertion,
  SupersedeAssertion,
  CorrectEvidence,
  TransitionActivity,
  SetRetention,
  RemovalStatement,
  ExecPurgeStatement as PurgeStatement,
  MergeConcept,
  // META
  MetaCommand,
  DescribeTarget,
  ListCommand,
  ListTarget,
  SearchCommand,
  SearchTarget,
  VerifyTarget,
  ValidateCommand,
  ValidateTarget,
  PreviewCommand,
  HistoryCommand,
  ChangesCommand,
  ExportCapsuleCommand,
} from '@ldclabs/kip-lang'

import type { PropositionMatcher } from '@ldclabs/kip-lang'

/**
 * `(subject, predicate, object)` — the structural form of a Proposition.
 *
 * kip-lang builds this type but does not export it by name, so it is recovered
 * from the matcher that carries it. Deriving it beats redeclaring it: a field
 * added upstream arrives here, and one renamed upstream fails to compile
 * instead of quietly describing a shape that no longer exists.
 */
export type PropositionTriple = Extract<
  PropositionMatcher,
  { Tuple: unknown }
>['Tuple']
