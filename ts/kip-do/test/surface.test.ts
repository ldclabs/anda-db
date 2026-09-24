import { describe, expect, it } from 'vitest'
import * as kipDo from '../src/index.js'
import * as schema from '../src/schema/index.js'

/**
 * The package's published interface, written down.
 *
 * A barrel is the one file where a mistake costs nothing at build time and
 * everything later: re-exporting an internal helper does not fail a test, does
 * not fail a typecheck, and quietly turns that helper's shape into a promise
 * the next refactor breaks. `export *` makes it worse — a new symbol in
 * `store/` joins the published surface without anyone writing a line.
 *
 * So the list lives here rather than only in the barrel's own text. Adding to
 * it is deliberate and one line; leaking into it is a failing test.
 *
 * Only runtime values are checked. Types are erased before this file runs, and
 * a type that escapes costs a caller nothing it cannot ignore — where a leaked
 * *value* is a function somebody will call.
 */
const PUBLIC_SURFACE = [
  'ALL_PERMISSIONS',
  'ANONYMOUS_PRINCIPAL',
  'ANY_SPACE',
  'BASELINE_ID',
  'BASELINE_VERSION',
  'BUNDLED_PACKAGES',
  'COGNITIVE_MEMORY',
  'COGNITIVE_MEMORY_ID',
  'COGNITIVE_MEMORY_VERSION',
  'CORE_PACKAGE',
  'CORE_PACKAGE_ID',
  'CORE_PACKAGE_REF',
  'CognitiveNexus',
  'DEFAULT_SPACE',
  'ELEMENT_KINDS',
  'EffectiveAuthority',
  'GovernanceStore',
  'KIP_ERROR_CODES',
  'KIP_ERROR_REGISTRY',
  'KIP_VERSION',
  'KipDatabase',
  'KipError',
  'PERMISSIONS',
  'SYSTEM_PRINCIPAL',
  'SchemaEnvironment',
  'Session',
  'State',
  'Store',
  'TABLES',
  'TIME_MAX',
  'TIME_MIN',
  'UNREACHABLE_SEQ',
  'actorKey',
  'anonymousAuth',
  'approvalId',
  'assurance',
  'authStrength',
  'authority',
  'authorityCeiling',
  'baseline',
  'beliefToJson',
  'bindingClass',
  'bindingId',
  'canonicalJson',
  'capabilities',
  'classification',
  'compareElementId',
  'contentDigest',
  'delegationId',
  'describePermission',
  'detailed',
  'effectivePurpose',
  'elementId',
  'elementIdEquals',
  'errors',
  'evaluateBinaryRule',
  'familyOf',
  'forecast',
  'formatElementId',
  'formatPackageRef',
  'formatSymbolRef',
  'formatTime',
  'govStatus',
  'grantId',
  'isAlwaysAudited',
  'isJsonArray',
  'isJsonMap',
  'isPermission',
  'isPermitted',
  'jsonEquals',
  'kindOfTag',
  'mergeRequestContext',
  'normalizeTime',
  'nowTime',
  'packageRefOf',
  'parseElementId',
  'parseElementIdOfKind',
  'parseJson',
  'parseKip',
  'parseKipAll',
  'parseKipBatch',
  'parsePackage',
  'parsePackageRef',
  'parsePermission',
  'parseSymbolRef',
  'parseTime',
  'parserVersion',
  'policyFromSettings',
  'principalAuth',
  'principalClass',
  'project',
  'purposeAssurance',
  'receiptOf',
  'requirePermitted',
  'slotToJson',
  'spaceResource',
  'specRevision',
  'subjectDigest',
  'systemAuth',
  'tagOf',
  'tryParseElementId',
]

/**
 * Internals that used to be exported and must not come back.
 *
 * Strictly weaker than the exact list above — anything that leaks fails there
 * first — and it is here for the failure message, not the coverage. A barrel
 * edit that re-exports `decodeRow` fails the equality check with a hundred-line
 * diff; it fails this one with the word `decodeRow`. Each name is also a
 * specific promise the package should not be making: how a row is encoded, what
 * the DDL says, how text is segmented, how many parameters SQLite will bind.
 */
const MUST_STAY_INTERNAL = [
  // the SQL codec and the schema it encodes for
  'TABLE_SPECS',
  'decodeRow',
  'insertStatement',
  'updateStatement',
  'specOf',
  'SCHEMA_STATEMENTS',
  'SCHEMA_VERSION',
  'applySchema',
  'configureSql',
  'metaGet',
  'metaSet',
  // the query-shaping guards
  'MAX_BOUND_PARAMS',
  'MAX_VALUE_BYTES',
  'checkParamCount',
  'checkValueSize',
  'encodeJson',
  'ftsQuote',
  'idSet',
  // segmentation, which is the FTS index's private vocabulary
  'segment',
  'segmentToText',
  'segmenterMark',
  'extractJsonText',
  'MAX_QUERY_TOKENS',
  // the search index, maintained inside `Store.put`
  'indexElement',
  'rebuildSearch',
  'searchIndex',
  'SEARCHABLE',
  'SEARCH_TABLES',
  // the executors a Session drives
  'executeKml',
  'executeKql',
  'executeMeta',
  'tryExecuteKml',
  'Transaction',
  'KqlContextState',
  // per-element governance machinery
  'redactView',
  'toIdentityOnly',
  'stagePurge',
  'quarantine',
  'release',
  'classify',
  'elevateAuthority',
  'kmlPermissions',
  'kqlPermissions',
  'metaPermissions',
  'clausePermissions',
  // cursors and views
  'pageToken',
  'pageCursorFromToken',
  'snapshotToken',
  'coordinateFromToken',
  'render',
  'readPath',
]

describe('the published interface', () => {
  it('exports exactly what a host needs, and nothing below it', () => {
    expect(Object.keys(kipDo).sort()).toEqual(PUBLIC_SURFACE)
  })

  it('names the door, when one is left open', () => {
    const leaked = MUST_STAY_INTERNAL.filter((name) => name in kipDo)
    expect(leaked).toEqual([])
  })

  it('is reachable and callable, not merely declared', () => {
    // A spot check that the barrel re-exports live bindings rather than names
    // that happen to typecheck: one value from each door.
    expect(typeof kipDo.KipDatabase).toBe('function')
    expect(kipDo.SYSTEM_PRINCIPAL).toBe('kip:principal:system')
    expect(kipDo.KIP_VERSION).toMatch(/^2\./)
    expect(kipDo.parserVersion()).toBeTypeOf('string')
  })

  it('keeps the Schema Package vocabulary behind its own door', () => {
    // `@ldclabs/kip-do/schema` is where a package author works, so the symbol
    // grammar and the validators live there and are deliberately not on the
    // root — the root carries only what a host needs to install and activate.
    expect(schema).toHaveProperty('parseSymbolRef')
    expect(schema).toHaveProperty('validateAttributes')
    expect(schema).toHaveProperty('conceptTypeDef')
    expect(kipDo).not.toHaveProperty('validateAttributes')
    expect(kipDo).not.toHaveProperty('conceptTypeDef')
  })
})
