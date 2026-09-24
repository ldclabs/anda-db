import { env, runInDurableObject } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import type { KipResponse } from '../src/durable-object.js'
import type { Json, JsonMap } from '../src/json.js'
import type { SchemaPackage } from '../src/schema/index.js'
import type { ConformanceKipDatabase } from './worker.js'
import { CASE_COUNT, FIXTURES, type Case, type Fixture } from './conformance/fixtures.generated.js'
import { sameResult } from './conformance/normalize.js'

/**
 * The KIP 2.0 engine suite.
 *
 * KIP's `conformance/engine-suite/`, copied byte for byte into
 * `fixtures/kip-conformance-2.0/` — the same fixtures
 * `rs/anda_cognitive_nexus/tests/conformance.rs` runs, compared the way KIP's
 * own runner compares them. A case that passes in one engine and fails in the
 * other is a divergence report, which is the whole reason the fixtures are
 * plain data rather than either engine's tests.
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
  await runInDurableObject(stub, (instance: ConformanceKipDatabase) =>
    instance.activateFixturePackages(packages),
  )

  // A setup step may capture raw result values into parameters for later
  // steps and every case: actual ids, versions and bases, never invented ones.
  const captured: JsonMap = {}
  for (const setup of fixture.setup ?? []) {
    const step = typeof setup === 'string' ? { command: setup } : setup
    const outcome = await post(stub, step.command, { ...captured, ...(step.params ?? {}) } as JsonMap)
    if ('error' in outcome) {
      throw new Error(
        `${fixture.name}: setup failed with ${outcome.error.code}: ${outcome.error.message}\n${step.command}`,
      )
    }
    for (const [name, path] of Object.entries('capture' in step ? step.capture ?? {} : {})) {
      const value = pointer(outcome.result, path)
      if (value === undefined) throw new Error(`${fixture.name}: setup result is missing ${path}`)
      captured[name] = value
    }
  }

  const outcomes: Outcome[] = []
  for (const testCase of fixture.cases) outcomes.push(await runCase(stub, testCase, captured))
  return outcomes
}

/** A JSON Pointer into a raw result; `undefined` when the path is missing. */
function pointer(value: Json, path: string): Json | undefined {
  if (path === '') return value
  let current: Json | undefined = value
  for (const segment of path.slice(1).split('/')) {
    const key = segment.replace(/~1/g, '/').replace(/~0/g, '~')
    if (current === null || typeof current !== 'object') return undefined
    current = Array.isArray(current) ? current[Number(key)] : (current as JsonMap)[key]
    if (current === undefined) return undefined
  }
  return current
}

/**
 * `expect.result_contains` (KIP runner): objects match member by member, an
 * expected array needs a matching actual row per expected row, and scalars
 * match exactly. Extra members and rows are allowed.
 */
function contains(actual: unknown, expected: unknown): boolean {
  if (Array.isArray(expected)) {
    return Array.isArray(actual) && expected.every((row) => actual.some((a) => contains(a, row)))
  }
  if (expected !== null && typeof expected === 'object') {
    return actual !== null && typeof actual === 'object' && !Array.isArray(actual) &&
      Object.entries(expected).every(([key, value]) =>
        Object.hasOwn(actual, key) && contains((actual as Record<string, unknown>)[key], value))
  }
  return actual === expected
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
  extra: Record<string, unknown> = {},
): Promise<Flat> {
  const response = await stub.fetch('https://kip-conformance/', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      kip: '2.0',
      operations: [{ command, parameters: params }],
      ...extra,
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
  captured: JsonMap,
): Promise<Outcome> {
  const outcome = await post(
    stub,
    testCase.command,
    { ...captured, ...(testCase.params ?? {}) } as JsonMap,
    testCase.envelope ?? {},
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
  const pass =
    (testCase.expect.result === undefined ||
      sameResult(outcome.result, testCase.expect.result as Json, testCase.ordered === true)) &&
    (testCase.expect.result_contains === undefined ||
      contains(outcome.result, testCase.expect.result_contains))
  return pass
    ? { kind: 'pass', name: testCase.name }
    : {
        kind: 'fail',
        name: testCase.name,
        detail: `expected ${JSON.stringify(testCase.expect)}, got ${JSON.stringify(outcome.result)}`,
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
const PENDING: string[] = []
let PASSED = 0

describe('KIP 2.0 conformance', () => {
  it('runs the same fixtures the reference engine runs', () => {
    // A shrinking suite is a silent loss of coverage; the generator reads the
    // fixture directory, so a bad path shows up here first.
    expect(FIXTURES.length).toBeGreaterThanOrEqual(22)
    expect(CASE_COUNT).toBeGreaterThanOrEqual(388)
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
      const failures = outcomes
        .filter((o) => o.kind === 'fail')
        .map((f) => `${f.name}: ${'detail' in f ? f.detail : ''}`)
      // A pending fixture has been verified by no engine yet (KIP engine-suite
      // README): its failures are reported, and its passes are new evidence.
      if (fixture.status === 'pending_engine') {
        PENDING.push(...failures.map((f) => `${fixture.name} / ${f}`))
        return
      }
      expect(failures, `${fixture.name} disagrees with the reference engine`).toEqual([])
    })
  }

  it('accounts for every case, as passed or as not built', () => {
    // Nothing falls between the two: a case that fails for a reason other than
    // "not built" already failed its fixture above, so reaching here means the
    // whole suite is accounted for.
    expect(PASSED + UNBUILT.length + PENDING.length).toBe(CASE_COUNT)
    expect(PENDING, 'pending-engine fixtures this engine does not pass yet').toEqual([])

    // The list rather than a count, so a *new* gap cannot hide inside a number
    // that happens to match.
    expect(UNBUILT).toEqual([])
  })
})
