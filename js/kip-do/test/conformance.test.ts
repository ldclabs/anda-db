import { env } from 'cloudflare:test'
import { describe, expect, it } from 'vitest'
import fixtures from './conformance/fixtures.js'
import {
  type FixtureCase,
  type Json,
  canonical,
  normalize,
  validateFixture,
} from './conformance/normalize.js'
import type { TestKipDatabase } from './worker.js'

/**
 * Runs the cross-engine conformance fixtures from `fixtures/kip-conformance/`.
 *
 * The same files drive `rs/anda_cognitive_nexus/tests/conformance.rs`. A case
 * that passes here and fails there — or the reverse — is a divergence between
 * the two KIP engines, which is exactly what this suite exists to surface.
 *
 * Fixtures are bundled through `conformance/fixtures.ts` rather than read from
 * disk, because tests run inside workerd where there is no filesystem.
 */

let counter = 0

async function execute(
  stub: DurableObjectStub<TestKipDatabase>,
  command: string,
): Promise<{ result?: Json; next_cursor?: string | null; error?: any }> {
  return (await stub.executeKip(command)) as any
}

describe.each(fixtures)('$name', (raw) => {
  const fixture = validateFixture(raw, raw.name)

  // One database per fixture: cases run in order and their effects accumulate,
  // so a KML case can set up state a later KQL case queries.
  const stubPromise = (async () => {
    const stub = env.KIP_DB.getByName(`conformance-${fixture.name}-${counter++}`)
    for (const command of fixture.setup ?? []) {
      const response = await execute(stub, command)
      if (response.error) {
        throw new Error(
          `setup failed for fixture "${fixture.name}": ` +
            `${response.error.code} ${response.error.message}\n  ${command}`,
        )
      }
    }
    return stub
  })()

  for (const testCase of fixture.cases) {
    const skipReason = testCase.skip?.ts
    const title = skipReason
      ? `${testCase.name} [assertions skipped: ${skipReason}]`
      : testCase.name

    // A skipped case still executes; only its assertions are dropped. Cases
    // accumulate state, so not running one would leave this engine's database
    // in a different state from the other engine's and silently invalidate
    // every later case.
    it(title, async () => {
      const stub = await stubPromise
      const response = await execute(stub, testCase.command)
      if (skipReason) return

      if (testCase.expect.error) {
        if (!response.error) {
          throw new Error(
            `expected ${testCase.expect.error.code} but the command succeeded ` +
              `with ${JSON.stringify(response.result)}`,
          )
        }
        expect(response.error.code).toBe(testCase.expect.error.code)
        if (testCase.expect.error.message) {
          expect(response.error.message).toContain(testCase.expect.error.message)
        }
        return
      }

      if (response.error) {
        throw new Error(
          `expected a result but got ${response.error.code}: ${response.error.message}`,
        )
      }

      const ordered = testCase.ordered ?? false
      const actual = normalize(response.result as Json, ordered)
      const wanted = normalize(testCase.expect.result as Json, ordered)
      // Compare canonical encodings so a key-order difference between the
      // engines is never mistaken for a semantic one.
      expect(canonical(actual)).toBe(canonical(wanted))

      if ('next_cursor' in testCase.expect) {
        expect(response.next_cursor ?? null).toBe(
          testCase.expect.next_cursor ?? null,
        )
      }
    })
  }
})

describe('fixture hygiene', () => {
  it('bundles at least one fixture', () => {
    // A bundling mistake would produce an empty, silently green suite.
    expect(fixtures.length).toBeGreaterThan(0)
  })

  it('validates every fixture and has no duplicate names', () => {
    const names = new Set<string>()
    for (const fixture of fixtures) {
      validateFixture(fixture, fixture.name)
      expect(names.has(fixture.name)).toBe(false)
      names.add(fixture.name)
    }
  })

  it('records a reason for every skip', () => {
    // A bare `skip: {}` would quietly drop coverage.
    for (const fixture of fixtures) {
      for (const testCase of fixture.cases as FixtureCase[]) {
        if (!testCase.skip) continue
        const reasons = [testCase.skip.rust, testCase.skip.ts].filter(Boolean)
        expect(
          reasons.length,
          `${fixture.name}/${testCase.name} has an empty skip`,
        ).toBeGreaterThan(0)
      }
    }
  })
})
