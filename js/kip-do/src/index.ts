/**
 * `@ldclabs/kip-do` — a KIP knowledge graph on SQLite-backed Durable Objects.
 *
 * One KIP database per Durable Object. See README.md for deployment and for
 * the list of known divergences from the Rust reference engine.
 */

export { KipDatabase, type KipDatabaseEnv } from './durable-object.js'
export {
  CognitiveNexus,
  normalizeScore,
  parseOffsetCursor,
  type KipResponse,
  type NexusOptions,
  type TransactionRunner,
} from './nexus.js'

export {
  KipError,
  KIP_ERROR_CODES,
  KIP_ERROR_HINTS,
  KIP_ERROR_NAMES,
  type KipErrorCode,
  type KipErrorJSON,
} from './errors.js'

export {
  parseKip,
  parseKipBatch,
  parserVersion,
} from './kip/parser.js'
export type * from './kip/ast.js'

export {
  compareEntityID,
  conceptID,
  conceptNode,
  formatEntityID,
  parseEntityID,
  propositionID,
  propositionLink,
  tryParseEntityID,
  type Concept,
  type ConceptID,
  type EntityID,
  type JsonMap,
  type LinkProperties,
  type Proposition,
  type PropositionID,
} from './entity.js'

export {
  AlinkTokenizer,
  SimpleTokenizer,
  extractJsonText,
  MAX_TEXTS_PER_BATCH,
  type FetcherLike,
  type Tokenizer,
  type TokenizeResult,
} from './tokenizer.js'

export {
  SCHEMA_STATEMENTS,
  SCHEMA_VERSION,
  applySchema,
  metaGet,
  metaSet,
} from './schema.js'

export { Store } from './store.js'
export {
  MAX_BOUND_PARAMS,
  MAX_VALUE_BYTES,
  checkParamCount,
  checkValueSize,
  idSet,
} from './sql.js'
