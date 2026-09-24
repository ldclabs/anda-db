import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import { CognitiveNexus } from '../src/nexus.js'
import { COGNITIVE_MEMORY, SchemaEnvironment, emptyLock } from '../src/schema/index.js'

const PROFILE = 'kip://profiles/cognitive-memory'
const PACKAGE = `${PROFILE}@2.0.0`
const QUERY = 'FIND(?p.name) WHERE { ?p CONCEPT {type: "Human"} }'

describe('immutable Schema Environments', () => {
  it('keeps caller edits out of cached queries until the lock is activated', async () => {
    await runInDurableObject(env.KIP_DB.getByName('schema-lock-ownership'), (_instance, state) => {
      const nexus = CognitiveNexus.connect(state.storage)
      nexus.activatePackages([COGNITIVE_MEMORY])
      nexus.execute('CREATE CONCEPT ?p { TYPE "Person" NAME "Ada" }')
      const lock = structuredClone(nexus.environment().lock)
      lock.aliases.Human = `${PACKAGE}/Person`
      const original = nexus.ensureSchema(lock)
      const seq = nexus.store.currentSeq(nexus.space)

      // Reuse the host's configuration object to prepare its next activation.
      // No Human lookup has warmed the symbol cache yet.
      lock.aliases.Human = `${PACKAGE}/Skill`
      expect(nexus.environment()).toBe(original)
      expect(nexus.query(QUERY)).toEqual(['Ada'])
      expect(nexus.store.currentSeq(nexus.space)).toBe(seq)
      expect(CognitiveNexus.connect(state.storage).query(QUERY)).toEqual(['Ada'])

      const next = nexus.ensureSchema(lock)
      expect(next.version).toBe(original.version + 1)
      expect(nexus.query(QUERY)).toEqual([])
      expect(nexus.environmentAt(nexus.space, original.version).resolveSymbolText('ConceptType', 'Human', 'read'))
        .toBe(`${PACKAGE}/Person`)
    })
  })

  it('protects nested values exposed by a cached environment', async () => {
    await runInDurableObject(env.KIP_DB.getByName('schema-environment-returns'), (_instance, state) => {
      const nexus = CognitiveNexus.connect(state.storage)
      const resolved = nexus.activatePackages([COGNITIVE_MEMORY])
      nexus.execute('CREATE CONCEPT ?p { TYPE "Person" NAME "Ada" }')

      expect(() => { resolved.lock.states[PROFILE] = 'quarantined' }).toThrow(TypeError)
      expect(() => { Object.assign(resolved, { lock: emptyLock() }) }).toThrow(TypeError)
      const artifact = resolved.artifact(PACKAGE)!
      expect(() => { delete artifact.definitions!.concept_types!.Person }).toThrow(TypeError)
      const symbol = resolved.resolveSymbol('ConceptType', 'Person', 'read')
      expect(() => { symbol.name = 'Skill' }).toThrow(TypeError)
      expect(() => { symbol.package.version.major = 99 }).toThrow(TypeError)
      expect(() => {
        resolved.definitionPackage(symbol)!.definitions!.concept_types!.Person!.description = 'changed'
      }).toThrow(TypeError)

      expect(nexus.environment()).toBe(resolved)
      expect(nexus.query('FIND(?p.name) WHERE { ?p CONCEPT {type: "Person"} }')).toEqual(['Ada'])
    })
  })

  it('owns package snapshots without freezing the host artifacts', () => {
    const artifact = structuredClone(COGNITIVE_MEMORY)
    const lock = emptyLock()
    lock.packages[PROFILE] = '2.0.0'
    lock.states[PROFILE] = 'active'
    const resolved = SchemaEnvironment.resolve(1, lock, new Map([[PACKAGE, artifact]]))

    delete artifact.definitions!.concept_types!.Person
    expect(resolved.resolveSymbolText('ConceptType', 'Person', 'read')).toBe(`${PACKAGE}/Person`)
    lock.states[PROFILE] = 'quarantined'
    expect(resolved.state(PROFILE)).toBe('active')
  })
})
