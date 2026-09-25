import { describe, expect, it } from 'vitest'
import { checkEnvelope } from '../src/request.js'

const space = { id: 'default', row: () => ({ seq: 3, schema_environment_version: 1 }) }
const read = { command: 'DESCRIBE PROTOCOL' }

describe('the request envelope', () => {
  it('requires the protocol version', () => {
    expect(() => checkEnvelope({ operations: [read] }, space)).toThrowError(
      /which version it speaks/,
    )
    expect(() => checkEnvelope({ kip: '1.0', operations: [read] }, space)).toThrowError(
      /declares "1.0"/,
    )
  })

  it('refuses a duplicated op_id, and a batch with no mode', () => {
    expect(() =>
      checkEnvelope(
        {
          kip: '2.0',
          execution: { mode: 'sequence' },
          operations: [{ ...read, op_id: 'a' }, { ...read, op_id: 'a' }],
        },
        space,
      ),
    ).toThrowError(/appears twice/)
    expect(() => checkEnvelope({ kip: '2.0', operations: [read, read] }, space)).toThrowError(
      /must declare execution.mode/,
    )
  })

  it('checks preconditions against the Space it is given', () => {
    expect(() =>
      checkEnvelope({ kip: '2.0', preconditions: { space_seq: 4 }, operations: [read] }, space),
    ).toThrowError(/expects default at sequence 4, and it is at 3/)
    expect(() =>
      checkEnvelope({ kip: '2.0', preconditions: { space_seq: 3 }, operations: [read] }, space),
    ).not.toThrow()
  })

  it('fails a critical extension and carries a non-critical one', () => {
    expect(() =>
      checkEnvelope(
        { kip: '2.0', extensions: { 'acme/x': { critical: true } }, operations: [read] },
        space,
      ),
    ).toThrowError(/critical/)
    expect(() =>
      checkEnvelope(
        { kip: '2.0', extensions: { 'acme/x': { critical: false } }, operations: [read] },
        space,
      ),
    ).not.toThrow()
  })

  it('takes compatibility_profile as a non-empty string', () => {
    expect(() =>
      checkEnvelope({ kip: '2.0', compatibility_profile: '', operations: [read] }, space),
    ).toThrowError(/non-empty string/)
    expect(() =>
      checkEnvelope({ kip: '2.0', compatibility_profile: 'kip-1', operations: [read] }, space),
    ).not.toThrow()
  })
})

it('refuses an atomic batch as an unsupported capability, and an isolation level as unsupported isolation', () => {
  // §75.3: `atomic` is the `atomic_batch` capability; a runtime that does not
  // advertise it refuses with UnsupportedCapability, as the Rust engine does.
  // An isolation level it cannot provide is a different refusal (§32.2).
  const refusal = (execution: Record<string, unknown>): unknown => {
    try {
      checkEnvelope({ kip: '2.0', execution, operations: [read, read] } as never, space)
    } catch (error) {
      return (error as { code?: unknown }).code
    }
    return undefined
  }
  expect(refusal({ mode: 'atomic' })).toBe('UnsupportedCapability')
  expect(refusal({ mode: 'sequence', isolation: 'snapshot' })).toBe('UnsupportedIsolation')
  expect(refusal({ mode: 'sequence', isolation: 'serializable' })).toBeUndefined()
})

it('refuses envelope dry runs before any operation can commit', () => {
  expect(() => checkEnvelope({kip: '2.0', options: {dry_run: true}, operations: [read]}, space)).toThrowError(/PREVIEW KML/)
  expect(() => checkEnvelope({kip: '2.0', options: {dry_run: false}, operations: [read]}, space)).not.toThrow()
})

describe('an ingestion context is one Evidence per entry (§71.1)', () => {
  const entry = { key: 'msg', evidence_class: 'user_statement', payload: 'I prefer dark mode.' }
  const write = (name: string) => ({ command: `CREATE CONCEPT ?c { TYPE "T" NAME "${name}" }` })
  const define = { command: 'DEFINE CONCEPT TYPE "Instrument" {description: "x"}' }
  const batch = (
    mode: string,
    operations: { command: string }[],
    evidence: Record<string, unknown>[] = [entry],
  ) => ({ kip: '2.0', execution: { mode }, ingest: { evidence }, operations }) as never

  it('refuses two write transactions unless every entry carries a client_key', () => {
    for (const mode of ['sequence', 'independent']) {
      expect(() => checkEnvelope(batch(mode, [write('a'), write('b')]), space)).toThrowError(
        /opens 2 write transactions .* "msg" has no client_key/,
      )
      expect(() =>
        checkEnvelope(batch(mode, [write('a'), write('b')], [{ ...entry, client_key: 'message:1' }]), space),
      ).not.toThrow()
    }
  })

  it('counts neither reads nor a standalone DEFINE as a transaction to mint into', () => {
    expect(() => checkEnvelope(batch('sequence', [read, write('a')]), space)).not.toThrow()
    expect(() => checkEnvelope(batch('sequence', [define, write('a')]), space)).not.toThrow()
    const alone = { kip: '2.0', ingest: { evidence: [entry] }, operations: [define] } as never
    expect(() => checkEnvelope(alone, space)).toThrowError(/other than a standalone DEFINE/)
  })
})
