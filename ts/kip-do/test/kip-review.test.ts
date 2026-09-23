import { expect, it } from 'vitest'
import { parseKip } from '../src/kip/parser.js'
import { checkEnvelope } from '../src/request.js'

it('does not apply Assertion registries to attributes or Facets', () => {
  for (const source of [
    'CREATE CONCEPT ?c { TYPE "Settings" SET ATTRIBUTES {mode:"dark",stance:"wide",confidence:42} }',
    'UPDATE :c SET FACET "RenderOptions" {mode:"dark",confidence:42}',
    'CREATE CONCEPT ?c { SET STRUCTURAL {("evidence", :e) {role:"custom"}} }',
  ]) expect(() => parseKip(source)).not.toThrow()
  expect(() => parseKip('CREATE ASSERTION ?a {SET FIELDS {mode:"dark"}}')).toThrow()
})

it('checks proposition handles and keeps NOT bindings local', () => {
  for (const source of [
    'ENSURE PROPOSITION ?p (?missing, "prefers", :value)',
    'ASSERT (?missing, "prefers", :value) {by: :me, mode:"stated"}',
    'ENSURE PROPOSITION ?p (:s, "p", (:nested,"q",?missing))',
    'UPDATE ?x SET ATTRIBUTES {score:1} WHERE {NOT {?x {}}}',
  ]) expect(() => parseKip(source)).toThrow()
  for (const source of [
    'MUTATE {ENSURE PROPOSITION ?p (?later,"p",:value) CREATE CONCEPT ?later {}}',
    'UPDATE ?x SET ATTRIBUTES {score:1} WHERE {?x {} NOT {?x {name:"hidden"}}}',
  ]) expect(() => parseKip(source)).not.toThrow()
})

it('rejects malformed extension critical flags', () => {
  const space = { id: 'default', row: () => ({ seq: 3, schema_environment_version: 1 }) }
  for (const value of [true, { critical: 'true' }, { critical: null }]) {
    expect(() => checkEnvelope({ kip: '2.0', operations: [{command:'DESCRIBE PROTOCOL'}], extensions: {'vendor/test':value} }, space)).toThrow()
  }
})
