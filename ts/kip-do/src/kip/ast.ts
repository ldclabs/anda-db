/**
 * The executable KIP AST this engine runs.
 *
 * The definitions live in `@ldclabs/kip-lang`, which produces them: `lower`
 * is what collapses a syntax tree into these shapes, so the types and the code
 * that builds them cannot drift apart. They are re-exported under their local
 * names — kip-lang prefixes the ones that collide with a syntax-tree node —
 * so `src/exec/` imports its AST from one place.
 *
 * There are no runtime narrowing helpers here on purpose. `lower` closes every
 * enum before the executor sees it, so a `switch` over these tags is total and
 * TypeScript checks it; the old `tagOf`/`isUnit` pair existed only because the
 * AST used to arrive as untyped JSON.
 *
 * The shape is the wire form of `anda_kip`'s Rust AST (serde's default
 * externally-tagged encoding), which is what lets `test/parser-oracle.test.ts`
 * compare the two engines field for field.
 */

export type {
  Json,
  KipValue,
  Command,
  KqlQuery,
  FindClause,
  DotPathVar,
  FindExpression,
  AggregationFunction,
  OrderByItem,
  OrderDirection,
  ExecWhereClause as WhereClause,
  ExecConceptMatcher as ConceptMatcher,
  PropositionMatcher,
  TargetTerm,
  PredTerm,
  FilterExpression,
  FilterOperand,
  ComparisonOperator,
  LogicalOperator,
  FilterFunction,
  KmlStatement,
  ExecUpsertBlock as UpsertBlock,
  UpsertItem,
  ExecConceptBlock as ConceptBlock,
  SetProposition,
  ExecPropositionBlock as PropositionBlock,
  ExecUpdateStatement as UpdateStatement,
  UpdateValue,
  UpdateExpr,
  UpdateFunction,
  ExecMergeStatement as MergeStatement,
  ExecDeleteStatement as DeleteStatement,
  MetaCommand,
  DescribeTarget,
  SearchCommand,
  SearchTarget,
  SearchMode,
  ExportCommand,
} from '@ldclabs/kip-lang'
