import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import type { KipResponse } from '../src/durable-object.js'
import type { Json, JsonMap } from '../src/json.js'
import type { SchemaPackage } from '../src/schema/index.js'
import type { ConformanceKipDatabase } from './worker.js'
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
 * Every case goes through the **request envelope**, the same way the reference
 * harness builds a `Request` and hands it to the `Executor`. An earlier version
 * of this file called `nexus.find` and `nexus.mutate` directly, which proved
 * the two engines agreed about everything except the layer a client actually
 * talks to — and the envelope is where this engine's own gaps turned out to be.
 *
 * Cases that exercise something this engine has not built are **reported, not
 * silently skipped**: the summary below names them, so the gap has a size. A
 * suite that quietly passed by skipping would say the two engines agree.
 */
async function runFixture(fixture: Fixture): Promise<Outcome[]> {
  const stub = env.KIP_CONFORMANCE_DB.getByName(`conf-${fixture.name}`)

  // A fixture may declare packages of its own, which is how it names the
  // vocabulary its cases need without depending on what some other fixture
  // installed. Installing one is a host decision, so it does not go through
  // the envelope: no command can install a Schema Package, by design.
  const packages = (fixture.packages ?? []) as SchemaPackage[]
  if (packages.length > 0) {
    await runInDurableObject(stub, (instance: ConformanceKipDatabase) =>
      instance.activateFixturePackages(packages),
    )
  }

  for (const setup of fixture.setup ?? []) {
    const outcome = await post(stub, setup, {})
    if ('error' in outcome) {
      throw new Error(
        `${fixture.name}: setup failed with ${outcome.error.code}: ${outcome.error.message}\n${setup}`,
      )
    }
  }

  const outcomes: Outcome[] = []
  for (const testCase of fixture.cases) outcomes.push(await runCase(stub, testCase))
  return outcomes
}

type Outcome =
  | { kind: 'pass'; name: string }
  | { kind: 'fail'; name: string; detail: string }
  | { kind: 'unbuilt'; name: string; detail: string }

type Flat =
  | { result: Json }
  | { error: { code: string; message: string } }

/**
 * Sends one command as a single-operation KIP request and flattens the answer.
 *
 * A KML operation answers with a receipt rather than a result; no fixture
 * asserts on a receipt's contents (they are engine truth — ids, sequences,
 * timestamps), so it flattens to `null`, and what the case is really pinning is
 * that the mutation was accepted at all.
 */
async function post(
  stub: DurableObjectStub<ConformanceKipDatabase>,
  command: string,
  params: JsonMap,
): Promise<Flat> {
  const response = await stub.fetch('https://kip-conformance/', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      kip: '2.0',
      operations: [{ command, parameters: params }],
    }),
  })
  const envelope = (await response.json()) as KipResponse

  // A malformed envelope fails at the top level; a command fails in its result.
  if (envelope.error !== undefined) {
    return { error: { code: envelope.error.code, message: envelope.error.message } }
  }
  const first = envelope.results[0]
  if (first === undefined) {
    return { error: { code: 'InternalError', message: 'the response carried no result' } }
  }
  if (first.error !== undefined) {
    return { error: { code: first.error.code, message: first.error.message } }
  }
  return { result: first.result === undefined ? null : first.result }
}

async function runCase(
  stub: DurableObjectStub<ConformanceKipDatabase>,
  testCase: Case,
): Promise<Outcome> {
  const outcome = await post(
    stub,
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
    expect(FIXTURES).toHaveLength(8)
    expect(CASE_COUNT).toBe(80)
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

    // Empty, and it is the list rather than a count that says so: a *new* gap
    // cannot hide inside a number that happens to match, and closing the last
    // one had to be acknowledged here by deleting its name.
    expect(UNBUILT).toEqual([])
  })
})
