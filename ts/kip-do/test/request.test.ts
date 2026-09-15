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

it('refuses envelope dry runs before any operation can commit', () => {
  expect(() => checkEnvelope({kip: '2.0', options: {dry_run: true}, operations: [read]}, space)).toThrowError(/PREVIEW KML/)
  expect(() => checkEnvelope({kip: '2.0', options: {dry_run: false}, operations: [read]}, space)).not.toThrow()
})
