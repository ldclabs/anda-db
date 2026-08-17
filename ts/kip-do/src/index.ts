/**
 * `@ldclabs/kip-do` — a KIP 2.0 knowledge graph on SQLite-backed Durable
 * Objects. One Cognitive Nexus per Durable Object.
 *
 * The engine is being rebuilt for KIP 2.0. The 1.x executor was deleted rather
 * than ported: 2.0 is a different data model — a Proposition existing is not
 * the Proposition being true — and a renamed 1.x engine would have been a
 * worse lie than an absent one. It is recoverable from the branch history.
 *
 * What is exported here is what exists. As each stage lands, its surface
 * joins this list; nothing is re-exported ahead of the code that implements it.
 */

export {
  KipError,
  KIP_ERROR_CODES,
  KIP_ERROR_REGISTRY,
  errors,
  type ErrorFactories,
  type KipErrorCategory,
  type KipErrorCode,
  type KipErrorJSON,
  type KipErrorSpec,
  type KipRetryClass,
  type RetryInfo,
} from './errors.js'

export {
  canonicalJson,
  isJsonArray,
  isJsonMap,
  jsonEquals,
  parseJson,
  type Json,
  type JsonMap,
} from './json.js'

export {
  parseKip,
  parseKipAll,
  parseKipBatch,
  parserVersion,
  specRevision,
} from './kip/parser.js'
export type * from './kip/ast.js'

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
  MAX_BOUND_PARAMS,
  MAX_VALUE_BYTES,
  checkParamCount,
  checkValueSize,
  encodeJson,
  ftsQuote,
  idSet,
} from './sql.js'
