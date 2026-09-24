/**
 * Capabilities a host answers for (§67.4).
 *
 * Three registry names describe what sits *around* a Nexus rather than the
 * Nexus itself: `memory_interface` is a Brain binding (Memory Interface §2),
 * `durable_brain_runtime` and `receiver_fencing` are the Brain Runtime
 * companion's workers and receivers. This engine implements none of them, so a
 * raw Nexus answers `false` for each — a raw Nexus MUST NOT advertise a binding
 * or level its connected Brain cannot serve (§2).
 *
 * A host that does serve them says so once, through
 * `CognitiveNexus.setHostCapabilities`. `DESCRIBE CAPABILITIES`, `DESCRIBE
 * PRIMER` and every request's `requires` block then answer with the
 * declaration, which is checked against what this engine can actually carry.
 * Mirrors `rs/anda_cognitive_nexus/src/meta/host.rs`.
 */

import type { Json, JsonMap } from '../json.js'
import { errors } from '../errors.js'
import { memoryBundles } from '../schema/contracts.js'

/** The registry names a host, not this engine, answers for. */
export const HOST_NAMES = ['memory_interface', 'durable_brain_runtime', 'receiver_fencing'] as const

/** A Memory Interface level (`profiles/memory-bundles.json`). */
export type MemoryBundleName = 'memory_basic' | 'memory_experience' | 'memory_learning'

/** What a deployment advertising `memory_interface` publishes (§2). */
export interface MemoryDescriptor {
  kip_memory: '2.0'
  bundles: MemoryBundleName[]
  default_scope?: { task_ref?: string; context_refs?: string[] }
  default_budget: { max_output_tokens: number; deadline_ms: number; tokenizer?: string }
  tokenizer: string
  minimum_response_tokens: number
  default_space?: { id?: string; uri?: string }
}

/** A host's declaration of the capabilities around this Nexus. */
export interface HostCapabilities {
  memory_interface?: MemoryDescriptor
  durable_brain_runtime?: boolean
  receiver_fencing?: boolean
}

/** The conformance levels this engine claims (§89); `capabilities()` reports them. */
export const CONFORMANCE_PROFILES: readonly string[] = ['KIP-Core']

const LEVEL_DEPENDENCY: Record<MemoryBundleName, MemoryBundleName | undefined> = {
  memory_basic: undefined,
  memory_experience: 'memory_basic',
  memory_learning: 'memory_experience',
}

const positive = (value: unknown): boolean =>
  typeof value === 'number' && Number.isSafeInteger(value) && value > 0

/**
 * Checks a host declaration against this engine: each advertised level must
 * run on a conformance level this engine claims and every capability it adds
 * must be supported; `receiver_fencing` sits on top of `durable_brain_runtime`.
 */
export function validateHostCapabilities(
  host: HostCapabilities,
  supported: (name: string) => boolean,
): void {
  const descriptor = host.memory_interface
  if (descriptor !== undefined) {
    if (descriptor.kip_memory !== '2.0') {
      throw errors.constraintViolation('descriptor kip_memory must be "2.0"')
    }
    const bundles = descriptor.bundles
    if (!Array.isArray(bundles) || !bundles.includes('memory_basic')) {
      throw errors.constraintViolation('a Memory Interface descriptor advertises at least memory_basic')
    }
    if (new Set(bundles).size !== bundles.length) {
      throw errors.constraintViolation('a descriptor names each level once')
    }
    const registry = memoryBundles() as unknown as Record<string, JsonMap>
    for (const bundle of bundles) {
      const entry = registry[bundle]
      if (!entry || !(bundle in LEVEL_DEPENDENCY)) {
        throw errors.unsupportedCapability(`${String(bundle)} is not a level of the vendored bundle registry`)
      }
      const dependency = LEVEL_DEPENDENCY[bundle]
      if (dependency !== undefined && !bundles.includes(dependency)) {
        throw errors.unsupportedCapability(`${bundle} must advertise its dependency ${dependency}`)
      }
      const level = entry.nexus_level
      if (typeof level === 'string' && !CONFORMANCE_PROFILES.includes(level)) {
        throw errors.unsupportedCapability(`${bundle} runs on ${level}, which this engine does not claim`)
      }
      for (const capability of (entry.capabilities as string[] | undefined) ?? []) {
        if (!supported(capability)) {
          throw errors.unsupportedCapability(`${bundle} requires ${capability}, which this engine does not support`)
        }
      }
    }
    const budget = descriptor.default_budget
    if (!budget || !positive(budget.max_output_tokens) || !positive(budget.deadline_ms)) {
      throw errors.constraintViolation('default_budget names max_output_tokens and deadline_ms')
    }
    if (typeof descriptor.tokenizer !== 'string' || descriptor.tokenizer === '' ||
      !positive(descriptor.minimum_response_tokens)) {
      throw errors.constraintViolation('a descriptor names its tokenizer and a positive minimum_response_tokens')
    }
  }
  if (host.receiver_fencing === true && host.durable_brain_runtime !== true) {
    throw errors.unsupportedCapability(
      'receiver_fencing is a further capability on top of durable_brain_runtime',
    )
  }
}

/** The host's answer for one of {@link HOST_NAMES}; `undefined` for any other name. */
export function hostState(host: HostCapabilities, name: string): boolean | undefined {
  switch (name) {
    case 'memory_interface': return host.memory_interface !== undefined
    case 'durable_brain_runtime': return host.durable_brain_runtime === true
    case 'receiver_fencing': return host.receiver_fencing === true
    default: return undefined
  }
}

/**
 * The registry value `DESCRIBE CAPABILITIES` reports for a host name: the
 * descriptor itself for an advertised binding, otherwise the boolean.
 */
export function hostRegistryValue(host: HostCapabilities, name: string): Json | undefined {
  if (name === 'memory_interface' && host.memory_interface !== undefined) {
    return structuredClone(host.memory_interface) as unknown as Json
  }
  return hostState(host, name)
}
