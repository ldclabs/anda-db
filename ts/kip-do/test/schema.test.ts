import { describe, expect, it } from 'vitest'
import * as validators from '../src/schema/validate.js'
import {
  COGNITIVE_MEMORY,
  COGNITIVE_MEMORY_ID,
  COGNITIVE_MEMORY_VERSION,
  CORE_PACKAGE,
  CORE_PACKAGE_ID,
  CORE_PACKAGE_REF,
  SchemaEnvironment,
  Validation,
  compareVersion,
  defines,
  emptyLock,
  facetDef,
  formatPackageRef,
  formatSymbolRef,
  formatVersion,
  isQualified,
  packageRefOf,
  parsePackage,
  parsePackageRef,
  parseSymbolRef,
  parseVersion,
  predicateDef,
  structuralFieldDef,
  symbols,
  validateAttributeMutability,
  validateAttributes,
  validateFacet,
  validateStructural,
  version,
  type SchemaLock,
  type SchemaPackage,
} from '../src/schema/index.js'

const CM_REF = 'kip://profiles/cognitive-memory@2.0.0'

/** A lock that activates Core and the bundled profile. */
function activeLock(): SchemaLock {
  const lock = emptyLock()
  lock.packages[CORE_PACKAGE_ID] = '2.0.0'
  lock.states[CORE_PACKAGE_ID] = 'active'
  lock.packages[COGNITIVE_MEMORY_ID] = COGNITIVE_MEMORY_VERSION
  lock.states[COGNITIVE_MEMORY_ID] = 'active'
  return lock
}

const available = new Map<string, SchemaPackage>([
  [CORE_PACKAGE_REF, CORE_PACKAGE],
  [CM_REF, COGNITIVE_MEMORY],
])

const env = (lock = activeLock()) => SchemaEnvironment.resolve(1, lock, available)

/** A second package that also defines `Person`, to force ambiguity. */
const RIVAL: SchemaPackage = {
  format: 'KIP-Schema-Package',
  manifest: {
    package_id: 'kip://rival',
    version: '1.0.0',
    package_ref: 'kip://rival@1.0.0',
  },
  definitions: {
    concept_types: { Person: { kind: 'ConceptType' } },
  },
}

describe('symbol identity', () => {
  it('round-trips a symbol through its canonical form', () => {
    const text = 'kip://profiles/cognitive-memory@2.0.0/has_step'
    const symbol = parseSymbolRef(text)
    expect(formatSymbolRef(symbol)).toBe(text)
    expect(symbol.name).toBe('has_step')
    // The package path's own slashes must not be mistaken for the symbol
    // separator.
    expect(symbol.package.packageId).toBe('kip://profiles/cognitive-memory')
    expect(formatPackageRef(symbol.package)).toBe(CM_REF)
  })

  it('never accepts a version range as an exact reference', () => {
    // Spec §13: an element bound to `@latest` would change meaning when someone
    // else publishes, with no transaction in between.
    for (const bad of ['2', '2.x', '^2.0', 'latest', '2.0', '2.0.0.0', '+2.0.0', '2.0.0-']) {
      expect(() => parseVersion(bad), bad).toThrow()
    }
    for (const bad of ['kip://core', 'core@2.0.0', 'kip://@2.0.0', 'kip://core@latest']) {
      expect(() => parsePackageRef(bad), bad).toThrow()
    }
  })

  it('sorts a pre-release below its own release', () => {
    const rc = parseVersion('2.0.0-rc1')
    expect(formatVersion(rc)).toBe('2.0.0-rc1')
    expect(compareVersion(rc, version(2, 0, 0))).toBeLessThan(0)
    expect(compareVersion(version(1, 9, 9), rc)).toBeLessThan(0)
    expect(compareVersion(version(2, 0, 1), version(2, 1, 0))).toBeLessThan(0)
  })

  it('tells a local name apart from a canonical one', () => {
    // Spec §19: local names are model-facing sugar and must be resolved before
    // execution; a canonical reference is already resolved.
    expect(isQualified('kip://core@2.0.0/Assertion')).toBe(true)
    expect(isQualified('Person')).toBe(false)
  })

  it('needs both a version and a name', () => {
    for (const bad of [
      'kip://core@2.0.0',
      'kip://core@2.0.0/',
      'kip://core@2.0.0/a/b',
      'kip://core/Assertion',
    ]) {
      expect(() => parseSymbolRef(bad), bad).toThrow()
    }
  })
})

describe('the bundled profile', () => {
  it('is the package its constants name', () => {
    // A host builds its Schema Lock from these strings, and a lock naming a
    // package that is not installed refuses to activate at all.
    const ref = packageRefOf(COGNITIVE_MEMORY)
    expect(ref.packageId).toBe(COGNITIVE_MEMORY_ID)
    expect(formatVersion(ref.version)).toBe(COGNITIVE_MEMORY_VERSION)
  })

  it('carries the vocabulary the conformance fixtures use', () => {
    expect(defines(COGNITIVE_MEMORY, 'ConceptType', 'Person')).toBe(true)
    expect(defines(COGNITIVE_MEMORY, 'PredicateType', 'prefers')).toBe(true)
    expect(defines(COGNITIVE_MEMORY, 'Facet', 'MnemonicState')).toBe(true)
    expect(defines(COGNITIVE_MEMORY, 'StructuralField', 'has_step')).toBe(true)
    expect(symbols(COGNITIVE_MEMORY, 'ConceptType')).toContain('Experience')
  })

  it('rejects an artifact whose two spellings of its identity disagree', () => {
    // Such an artifact could be installed under a name its symbols do not
    // claim, so it is refused rather than reconciled.
    expect(() =>
      parsePackage(
        JSON.stringify({
          manifest: {
            package_id: 'kip://a',
            version: '1.0.0',
            package_ref: 'kip://b@1.0.0',
          },
        }),
      ),
    ).toThrowError(/calls itself/)
    expect(() => parsePackage('not json')).toThrowError(/readable Schema Package/)
  })
})

describe('symbol resolution', () => {
  it('resolves a bare local name to one exact symbol', () => {
    expect(env().resolveSymbolText('ConceptType', 'Person', 'write')).toBe(
      `${CM_REF}/Person`,
    )
    expect(env().resolveSymbolText('PredicateType', 'prefers', 'write')).toBe(
      `${CM_REF}/prefers`,
    )
  })

  it('refuses a type no active package defines, rather than inventing it', () => {
    // A data mutation never creates a schema definition.
    expect(() =>
      env().resolveSymbol('ConceptType', 'Spaceship', 'write'),
    ).toThrowError(/no active Schema Package defines/)
  })

  it('cannot type a Concept with only Core activated', () => {
    // Core carries the element kinds and the open registries and declares no
    // Concept types at all — define-before-use working as intended.
    expect(() =>
      SchemaEnvironment.coreOnly().resolveSymbol('ConceptType', 'Person', 'write'),
    ).toThrowError(/no active Schema Package defines/)
  })

  it('names the candidates when a local name is ambiguous', () => {
    // Spec §184: tell the Agent how to recover, by name.
    const lock = activeLock()
    lock.packages['kip://rival'] = '1.0.0'
    lock.states['kip://rival'] = 'active'
    const withRival = SchemaEnvironment.resolve(
      1,
      lock,
      new Map([...available, ['kip://rival@1.0.0', RIVAL]]),
    )
    expect(() =>
      withRival.resolveSymbol('ConceptType', 'Person', 'write'),
    ).toThrowError(/defined by more than one active package/)
    // An exact reference is the way out, and it still works.
    expect(
      withRival.resolveSymbolText('ConceptType', `${CM_REF}/Person`, 'write'),
    ).toBe(`${CM_REF}/Person`)
  })

  it('resolves an alias to an exact symbol and then checks it like any other', () => {
    const lock = activeLock()
    lock.aliases.Human = `${CM_REF}/Person`
    lock.aliases.Broken = 'not-a-symbol'
    expect(env(lock).resolveSymbolText('ConceptType', 'Human', 'write')).toBe(
      `${CM_REF}/Person`,
    )
    expect(() =>
      env(lock).resolveSymbol('ConceptType', 'Broken', 'write'),
    ).toThrowError(/is not a symbol/)
    // An alias that points at a real symbol of the wrong kind is still wrong.
    expect(() =>
      env(lock).resolveSymbol('PredicateType', 'Human', 'write'),
    ).toThrowError(/defines no predicate/)
  })

  it('keeps reading a blocked package while refusing new writes to it', () => {
    // Data already bound to a blocked package does not stop meaning what it
    // meant, and refusing to resolve it would make the incident unauditable.
    const lock = activeLock()
    lock.states[COGNITIVE_MEMORY_ID] = 'blocked'
    expect(
      env(lock).resolveSymbolText('ConceptType', `${CM_REF}/Person`, 'read'),
    ).toBe(`${CM_REF}/Person`)
    expect(() =>
      env(lock).resolveSymbol('ConceptType', `${CM_REF}/Person`, 'write'),
    ).toThrowError(/cannot bind new data/)
    // A blocked package also stops answering bare local names, because a local
    // name is the caller not saying which package they meant.
    expect(() =>
      env(lock).resolveSymbol('ConceptType', 'Person', 'read'),
    ).toThrowError(/no active Schema Package defines/)
  })

  it('keeps a quarantined package out of resolution entirely', () => {
    const lock = activeLock()
    lock.states[COGNITIVE_MEMORY_ID] = 'quarantined'
    expect(() =>
      env(lock).resolveSymbol('ConceptType', `${CM_REF}/Person`, 'read'),
    ).toThrowError(/quarantined/)
  })

  it('binds new writes to the write default when one is set', () => {
    // §80: reads keep resolving the version the data was written against.
    const lock = activeLock()
    lock.packages['kip://rival'] = '1.0.0'
    lock.states['kip://rival'] = 'active'
    lock.write_defaults['kip://rival'] = '1.0.0'
    const resolved = SchemaEnvironment.resolve(
      1,
      lock,
      new Map([...available, ['kip://rival@1.0.0', RIVAL]]),
    )
    expect(resolved.packageRef('kip://rival', 'write')).toBe('kip://rival@1.0.0')
  })

  it('refuses to exist when its lock names an artifact it does not have', () => {
    // An environment that resolves some of its own lock is worse than one that
    // refuses to exist: the failure would surface as a missing symbol
    // somewhere unrelated.
    const lock = activeLock()
    lock.packages['kip://absent'] = '1.0.0'
    expect(() => SchemaEnvironment.resolve(1, lock, available)).toThrowError(
      /artifact is not installed/,
    )
  })

  it('unions the open registries across active packages', () => {
    // Open means a package may add to a registry, not that anything goes.
    const registry = env().registry('activity_classes')
    expect(registry.has('inference')).toBe(true)
    expect(registry.has('not_a_class')).toBe(false)
    expect(env().registry('stances')).toEqual(
      new Set(['support', 'reject', 'uncertain']),
    )
  })
})

describe('package validation', () => {
  it('rejects a member a closed Facet never declared', () => {
    // Spec §60 and §240.31: a Facet is a validated namespaced extension, not
    // the untyped metadata bag KIP 1.x had.
    const def = facetDef(COGNITIVE_MEMORY, 'MnemonicState')
    expect(def).toBeDefined()
    expect(validateFacet('f', def!, { memory_strength: 0.7 }).valid).toBe(true)

    const smuggled = validateFacet('f', def!, {
      memory_strength: 0.7,
      classification: 'public',
    })
    expect(smuggled.violations.map((v) => v.code)).toEqual([
      'SCHEMA_UNKNOWN_FIELD',
    ])
    expect(smuggled.violations[0]?.path).toMatch(/classification$/)
  })

  it('enforces a declared range, and reports a wrong type only once', () => {
    const def = facetDef(COGNITIVE_MEMORY, 'MnemonicState')!
    expect(
      validateFacet('f', def, { salience: 1.5 }).violations.map((v) => v.code),
    ).toEqual(['SCHEMA_RANGE_VIOLATION'])
    // A string where a number belongs is a type mismatch, and a value of the
    // wrong type cannot meaningfully be range-checked as well.
    expect(
      validateFacet('f', def, { salience: 'high' }).violations.map((v) => v.code),
    ).toEqual(['SCHEMA_TYPE_MISMATCH'])
  })

  it('treats a declared type list as a union', () => {
    const spec = { fields: { at: { type: ['timestamp', 'null'] } } }
    expect(validateAttributes('t', spec, { at: null }).valid).toBe(true)
    expect(validateAttributes('t', spec, { at: '2026-01-01' }).valid).toBe(true)
    expect(validateAttributes('t', spec, { at: 7 }).valid).toBe(false)
  })

  it('accepts a value it cannot validate rather than inventing a failure', () => {
    // An unrecognized type name means a package this engine does not fully
    // understand, not data that is wrong.
    const spec = { fields: { x: { type: 'geo_point' } } }
    expect(validateAttributes('t', spec, { x: 'anything' }).valid).toBe(true)
  })

  it('reports a missing required field and an unknown one in a closed set', () => {
    const spec = {
      open: false,
      fields: { name: { type: 'string', required: true } },
    }
    expect(
      validateAttributes('t', spec, { other: 1 }).violations.map((v) => v.code),
    ).toEqual(['SCHEMA_REQUIRED_MISSING', 'SCHEMA_UNKNOWN_FIELD'])
    expect(validateAttributes('t', { open: true, fields: {} }, { any: 1 }).valid).toBe(
      true,
    )
  })

  it('refuses a change to an immutable attribute but allows establishing it', () => {
    // Immutability is a statement about a transition, not about a value.
    const spec = { fields: { born: { type: 'string', mutable: false } } }
    expect(
      validateAttributeMutability('t', spec, {}, { born: '1999' }).valid,
    ).toBe(true)
    expect(
      validateAttributeMutability('t', spec, { born: '1999' }, { born: '2000' })
        .violations.map((v) => v.code),
    ).toEqual(['SCHEMA_IMMUTABLE_FIELD'])
  })

  it('checks structural cardinality and uniqueness', () => {
    const def = { cardinality: { min: 1, max: 2 }, unique: true }
    expect(validateStructural('s', def, ['a']).valid).toBe(true)
    expect(
      validateStructural('s', def, []).violations.map((v) => v.code),
    ).toEqual(['SCHEMA_CARDINALITY_VIOLATION'])
    expect(
      validateStructural('s', def, ['a', 'b', 'c']).violations.map((v) => v.code),
    ).toEqual(['SCHEMA_CARDINALITY_VIOLATION'])
    expect(
      validateStructural('s', def, ['a', 'a']).violations.map((v) => v.code),
    ).toEqual(['SCHEMA_DUPLICATE_REFERENCE'])
  })

  it('does not turn a functional predicate into a write rejection', () => {
    // §95, §240.28: a functional predicate with two competing objects is a
    // contested belief the Nexus has to be able to store in order to report it.
    // The bundled profile leaves `prefers` non-functional and open-world, so
    // an absent claim there is unknown and never false (§51)…
    expect(predicateDef(COGNITIVE_MEMORY, 'prefers')?.open_world).toBe(true)
    // …and whatever a package declares, this layer offers no validator that
    // could act on it. The absence is the contract, so it is asserted here
    // rather than left to be noticed.
    expect(Object.keys(validators).filter((n) => n.startsWith('validate'))).toEqual([
      'validateAttributes',
      'validateAttributeMutability',
      'validateFacet',
      'validateStructural',
    ])
  })

  it('reads order as order, never as causality', () => {
    // §66: `has_step` being ordered says step 3 follows step 2, never that it
    // was caused by it — so nothing here derives one from the other.
    const def = structuralFieldDef(COGNITIVE_MEMORY, 'has_step')
    expect(def?.ordered).toBe(true)
    expect(validateStructural('s', def!, ['a', 'b']).valid).toBe(true)
  })

  it('turns a failed validation into one error naming every path', () => {
    const failed = new Validation().push({
      code: 'SCHEMA_TYPE_MISMATCH',
      schema_ref: 't',
      path: 'attributes.x',
      message: 'expected string, got number',
      severity: 'error',
    })
    expect(failed.valid).toBe(false)
    expect(() => failed.throwIfInvalid()).toThrowError(/attributes\.x/)
    // A warning is worth reporting and is not a reason to refuse.
    const warned = new Validation().push({
      code: 'X',
      schema_ref: 't',
      path: 'p',
      message: 'm',
      severity: 'warning',
    })
    expect(warned.valid).toBe(true)
    expect(warned.throwIfInvalid().warnings).toHaveLength(1)
  })
})
