import { env, runInDurableObject } from 'cloudflare:test'
import { describe, it, expect } from 'vitest'
import { CognitiveNexus } from '../src/nexus.js'
import { COGNITIVE_MEMORY, type SchemaPackage } from '../src/schema/index.js'
import { validatePackageContracts, validateValue } from '../src/schema/contracts.js'
import { parseCanonicalJson, canonicalJson, type JsonMap } from '../src/json.js'
import { parseElementId } from '../src/id.js'

const SETUP = `MUTATE {
 CREATE CONCEPT ?a { TYPE "Person" NAME "Ada" }
 CREATE CONCEPT ?p { TYPE "Preference" NAME "tea" }
 ENSURE PROPOSITION ?claim (?a, "prefers", ?p)
 CREATE EVIDENCE ?source { SET FIELDS {evidence_class: "observation", payload: "source", observed_at: "2026-09-07T00:00:00.000Z", content_digest: "sha256:41cf6794ba4200b839c53531555f0f3998df4cbb01a4d5cb0b94e3ca5e23947d"} }
}`
const BELIEF = 'FIND(?b) WHERE { ?p PROPOSITION (id: "P-1") ?b BELIEF (?p) }'
const DERIVE = `MUTATE {
 CREATE ASSERTION ?a { SET FIELDS {proposition: "P-1", asserted_by: "C-1", stance: "support", mode: "inferred", confidence: 0.9, asserted_at: "2026-09-07T00:00:00.000Z"} SET STRUCTURAL {("evidence", "E-1") {role: "support"}} }
 CREATE ACTIVITY ?work {
  SET FIELDS {activity_class: "semantic_consolidation", status: "completed"}
  SET FACET "DependencyBasis" {basis_seq: :seq, groups: [{role: "all_of", pins: [{id: "E-1", version: :version}]}], policy_basis: :basis}
  SET STRUCTURAL {("inputs", "E-1") ("outputs", ?a)}
 }
}`

describe('Cognitive Consistency d6e3a45', () => {
  it('checks real dependency pins before Maintenance and captures committed versions', async () => {
    await runInDurableObject(env.KIP_DB.getByName('new-dependency-contract'), (_instance, state) => {
      const nexus = CognitiveNexus.connect(state.storage)
      nexus.activatePackages([COGNITIVE_MEMORY])
      nexus.execute(SETUP)
      const capsule = nexus.describe('EXPORT CAPSULE ?c WHERE { ?c CONCEPT {name: "Ada"} }')
      validateValue({ $ref: 'urn:kip:2.0:schema:capsule' }, capsule)
      const basis = (nexus.query(BELIEF) as JsonMap[])[0]!.basis as JsonMap
      validateValue({ $ref: 'urn:kip:2.0:schema:projection#/$defs/ProjectionBasis' }, basis)
      nexus.execute(DERIVE, { seq: basis.snapshot_seq!, basis, version: 1 })
      validateValue({ $ref: 'urn:kip:2.0:schema:capsule' }, nexus.describe('EXPORT CAPSULE ?a WHERE { ?a ASSERTION {} }'))
      expect((nexus.query(BELIEF) as JsonMap[])[0]!.status).toBe('accepted')
      const activity = nexus.store.load(parseElementId('X-1'))!
      expect((activity.row.origin._kip_runtime as JsonMap).output_versions).toEqual({ 'A-1': 1 })
      nexus.execute('CREATE EVIDENCE ?new { SET FIELDS {evidence_class: "observation", payload: "corrected"} }')
      nexus.execute('TRANSITION "E-1" TO "corrected" BY "E-2"')
      const validity = (nexus.query('FIND(?a._system.dependency_validity) WHERE { ?a ASSERTION {} }') as JsonMap[])[0]!
      expect(validity.status).toBe('needs_review')
      expect(validity.action_eligible).toBe(false)
      expect((nexus.query(BELIEF) as JsonMap[])[0]!.status).not.toBe('accepted')
    })
  })
  it('refuses fabricated pins without committing half a derivation', async () => {
    await runInDurableObject(env.KIP_DB.getByName('new-invalid-pin'), (_instance, state) => {
      const nexus = CognitiveNexus.connect(state.storage)
      nexus.activatePackages([COGNITIVE_MEMORY]); nexus.execute(SETUP)
      const basis = (nexus.query(BELIEF) as JsonMap[])[0]!.basis as JsonMap
      expect(() => nexus.execute(DERIVE, { seq: basis.snapshot_seq!, basis, version: 99 })).toThrow()
      expect(nexus.query('FIND(?a) WHERE { ?a ASSERTION {} }')).toEqual([])
    })
  })
  it('keeps any_of valid while an independent alternative remains current', async () => {
    await runInDurableObject(env.KIP_DB.getByName('new-any-of'), (_instance, state) => {
      const nexus = CognitiveNexus.connect(state.storage)
      nexus.activatePackages([COGNITIVE_MEMORY]); nexus.execute(SETUP)
      nexus.execute('CREATE EVIDENCE ?second { SET FIELDS {evidence_class:"observation",payload:"independent"} }')
      const basis = (nexus.query(BELIEF) as JsonMap[])[0]!.basis as JsonMap
      nexus.execute(`MUTATE {
        CREATE CONCEPT ?summary {TYPE "Insight" SET ATTRIBUTES {summary:"supported by either source"}}
        CREATE ACTIVITY ?work {
          SET FIELDS {activity_class:"semantic_consolidation",status:"completed"}
          SET FACET "DependencyBasis" {basis_seq: :seq,policy_basis: :basis,groups:[{role:"any_of",pins:[{id:"E-1",version:1},{id:"E-2",version:1}]}]}
          SET STRUCTURAL {("inputs","E-1") ("inputs","E-2") ("outputs",?summary)}
        }
      }`, { seq: basis.snapshot_seq!, basis })
      nexus.execute('CREATE EVIDENCE ?new {SET FIELDS {evidence_class:"observation",payload:"correction"}}')
      nexus.execute('TRANSITION "E-1" TO "corrected" BY "E-3"')
      expect(nexus.query('FIND(?c._system.dependency_validity.status) WHERE {?c CONCEPT {type:"Insight"}}')).toEqual(['current'])
    })
  })
  it('does not let terminal audit records acquire retrospective read pins', async () => {
    await runInDurableObject(env.KIP_DB.getByName('terminal-dependency'), (_instance, state) => {
      const nexus = CognitiveNexus.connect(state.storage)
      nexus.activatePackages([COGNITIVE_MEMORY]); nexus.execute(SETUP)
      nexus.execute('CREATE ACTIVITY ?audit {SET FIELDS {activity_class:"semantic_consolidation",status:"completed"} SET STRUCTURAL {("inputs","E-1") ("outputs","C-1")}}')
      const basis = (nexus.query(BELIEF) as JsonMap[])[0]!.basis as JsonMap
      expect(() => nexus.execute('UPDATE "X-1" SET FACET "DependencyBasis" {basis_seq: :seq,policy_basis: :basis,groups:[{role:"all_of",pins:[{id:"E-1",version:1}]}]}', { seq: basis.snapshot_seq!, basis })).toThrow('terminal record')
    })
  })
  it('does not let a warm catalog supply a missing transitive schema pin', () => {
    validatePackageContracts(COGNITIVE_MEMORY)
    const missing = structuredClone(COGNITIVE_MEMORY) as SchemaPackage
    const pins = missing.manifest!.validation_schemas as JsonMap[]
    missing.manifest!.validation_schemas = pins.filter((pin) => !String(pin.id).startsWith('https:'))
    expect(() => validatePackageContracts(missing)).toThrow('unpinned schema resource')
    const bad = structuredClone(COGNITIVE_MEMORY)
    ;(bad.manifest!.validation_schemas as JsonMap[])[0]!.content_digest = 'sha256:' + '0'.repeat(64)
    expect(() => validatePackageContracts(bad)).toThrow('digest mismatch')
  })
  it('uses strict JSON before source digits and duplicate keys are lost', () => {
    for (const raw of ['9007199254740992', '9007199254740993.0', '1e-400', '{"a":1,"\\u0061":2}', '"\\ud800"', '\ufeffnull']) expect(() => parseCanonicalJson(raw)).toThrow()
    expect(canonicalJson(parseCanonicalJson('[-0,1.0,0.000001,1e-7]'))).toBe('[0,1,0.000001,1e-7]')
  })
})
