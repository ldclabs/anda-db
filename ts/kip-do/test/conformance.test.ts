import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import { CognitiveNexus } from '../src/nexus.js'
import { KipError } from '../src/errors.js'
import type { Json, JsonMap } from '../src/json.js'
import { parseKip } from '../src/kip/parser.js'
import { COGNITIVE_MEMORY, type SchemaPackage } from '../src/schema/index.js'
import { CASE_COUNT, FIXTURES, type Case, type Fixture } from './conformance/fixtures.generated.js'
import { sameResult } from './conformance/normalize.js'

/**
 * The cross-engine KIP 2.0 conformance suite.
 *
 * These are the same fixtures `rs/anda_cognitive_nexus/tests/conformance.rs`
 * runs, byte for byte. A case that passes in one engine and fails in the other
 * is a divergence report, which is the whole reason the fixtures are plain data
 * rather than either engine's tests.
 *
 * Cases that exercise something this engine has not built are **reported, not
 * silently skipped**: the summary below names them, so the gap has a size. A
 * suite that quietly passed by skipping would say the two engines agree.
 */
async function runFixture(fixture: Fixture): Promise<Outcome[]> {
  const stub = env.KIP_DB.getByName(`conf-${fixture.name}`)
  return runInDurableObject(stub, (_instance, state) => {
    const nexus = CognitiveNexus.connect(state.storage)
    // The Cognitive Memory Profile is always available; a fixture may add
    // packages of its own, which is how it declares the vocabulary its cases
    // need without depending on what some other fixture installed.
    nexus.activatePackages([
      COGNITIVE_MEMORY,
      ...((fixture.packages ?? []) as SchemaPackage[]),
    ])
    for (const setup of fixture.setup ?? []) nexus.execute(setup)
    return fixture.cases.map((testCase) => runCase(nexus, testCase))
  })
}

type Outcome =
  | { kind: 'pass'; name: string }
  | { kind: 'fail'; name: string; detail: string }
  | { kind: 'unbuilt'; name: string; detail: string }

/** Runs one command whichever language it is, and flattens the outcome. */
function execute(
  nexus: CognitiveNexus,
  command: string,
  params: JsonMap,
): { result: Json } | { error: KipError } {
  try {
    const parsed = parseKip(command)
    if ('Kql' in parsed) return { result: nexus.find(parsed.Kql, params) as Json }
    if ('Meta' in parsed) return { result: nexus.describe(command, params) }
    const outcome = nexus.mutate(parsed.Kml, params)
    return { result: { handles: outcome.handles } as Json }
  } catch (err) {
    return { error: KipError.from(err) }
  }
}

function runCase(nexus: CognitiveNexus, testCase: Case): Outcome {
  const outcome = execute(
    nexus,
    testCase.command,
    (testCase.params ?? {}) as JsonMap,
  )
  const expectedError = testCase.expect.error

  if ('error' in outcome) {
    // A capability this engine has not built is a different fact from a wrong
    // answer, and the summary keeps them apart.
    if (
      outcome.error.code === 'UnsupportedCapability' &&
      expectedError !== 'UnsupportedCapability'
    ) {
      return {
        kind: 'unbuilt',
        name: testCase.name,
        detail: outcome.error.message,
      }
    }
    if (expectedError === undefined) {
      return {
        kind: 'fail',
        name: testCase.name,
        detail: `expected a result, got ${outcome.error.code}: ${outcome.error.message}`,
      }
    }
    return outcome.error.code === expectedError
      ? { kind: 'pass', name: testCase.name }
      : {
          kind: 'fail',
          name: testCase.name,
          detail: `expected ${expectedError}, got ${outcome.error.code}: ${outcome.error.message}`,
        }
  }

  if (expectedError !== undefined) {
    return {
      kind: 'fail',
      name: testCase.name,
      detail: `expected ${expectedError}, got a result: ${JSON.stringify(outcome.result)}`,
    }
  }
  if (testCase.expect.result === undefined) {
    return { kind: 'pass', name: testCase.name }
  }
  return sameResult(
    outcome.result,
    testCase.expect.result as Json,
    testCase.ordered === true,
  )
    ? { kind: 'pass', name: testCase.name }
    : {
        kind: 'fail',
        name: testCase.name,
        detail: `expected ${JSON.stringify(testCase.expect.result)}, got ${JSON.stringify(outcome.result)}`,
      }
}

/**
 * Outcomes accumulated across the per-fixture tests.
 *
 * Each fixture runs exactly once: its cases share one accumulating database
 * (a mutation case sets up state a later read case queries), so running a
 * fixture twice would replay its setup against a Space that already has it.
 */
const UNBUILT: string[] = []
let PASSED = 0

describe('KIP 2.0 conformance', () => {
  it('runs the same fixtures the reference engine runs', () => {
    // A shrinking suite is a silent loss of coverage; the generator reads the
    // fixture directory, so a bad path shows up here first.
    expect(FIXTURES).toHaveLength(7)
    expect(CASE_COUNT).toBe(62)
  })

  for (const fixture of FIXTURES) {
    it(fixture.name, async () => {
      const outcomes = await runFixture(fixture)
      for (const outcome of outcomes) {
        if (outcome.kind === 'pass') PASSED += 1
        else if (outcome.kind === 'unbuilt') {
          UNBUILT.push(`${fixture.name} / ${outcome.name}`)
        }
      }
      const failures = outcomes.filter((o) => o.kind === 'fail')
      expect(
        failures.map((f) => `${f.name}: ${'detail' in f ? f.detail : ''}`),
        `${fixture.name} disagrees with the reference engine`,
      ).toEqual([])
    })
  }

  it('accounts for every case, as passed or as not built', () => {
    // Nothing falls between the two: a case that fails for a reason other than
    // "not built" already failed its fixture above, so reaching here means the
    // whole suite is accounted for.
    expect(PASSED + UNBUILT.length).toBe(CASE_COUNT)

    // Every remaining gap is the same one — `AS OF`, the historical read path.
    // Listed by name rather than counted, so closing it has to be acknowledged
    // here and a *new* gap cannot hide inside a number that happens to match.
    expect(UNBUILT).toEqual([
      'governance / and the content is gone from the past as well as from the present',
      'history / the claim is active at the coordinate its transaction produced (seq 1)',
      'history / and the earlier coordinate still says active: history is not rewritten',
      'history / a coordinate before anything existed is empty, not an error',
      'history / a coordinate the Space has not reached is refused, never rounded to the present',
      'history / an unknown transaction names no coordinate',
    ])
  })
})
