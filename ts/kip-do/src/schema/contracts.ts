/** KIP §20.5. Only verified, pinned resources can satisfy a contract. */
import { canonicalJson, isJsonMap, type Json, type JsonMap } from '../json.js'
import { errors } from '../errors.js'
import { sha256Text } from '../digest.js'
import { facetDef, type SchemaPackage } from './package.js'
import type { SchemaEnvironment } from './env.js'
import { SCHEMA_DOCUMENTS, SCHEMA_DIGESTS, VALUE_VALIDATORS, MEMORY_BUNDLE_MANIFEST } from './contracts.generated.js'

const documents = structuredClone(SCHEMA_DOCUMENTS) as unknown as Record<string, JsonMap>
const digests: Record<string, string> = { ...SCHEMA_DIGESTS }
type Validator = ((value: unknown) => boolean) & { errors?: unknown }
const validators = VALUE_VALIDATORS as Record<string, Validator>

export function verifyArtifact(artifact: SchemaPackage): void {
  canonicalJson(artifact)
  const profile = artifact.integrity?.digest_profile
  if (!profile) return
  if (profile !== 'kip-jcs-safe-v1') throw errors.unsupportedCapability('previous draft artifact numeric contracts require explicit migration')
  const { integrity, ...covered } = artifact
  if (integrity?.content_digest !== 'sha256:' + sha256Text(canonicalJson(covered))) throw errors.digestMismatch('Schema Package digest does not match its canonical bytes')
}

function checkReferences(value: unknown, owner: string, locked: Set<string>): void {
  if (Array.isArray(value)) { value.forEach((v) => checkReferences(v, owner, locked)); return }
  if (!isJsonMap(value)) return
  if (typeof value.$id === 'string' && value.$id !== owner) throw errors.unsupportedCapability('nested schema resource IDs require an explicit resource loader')
  for (const keyword of ['$ref', '$dynamicRef']) {
    const ref = value[keyword]
    if (typeof ref !== 'string') continue
    const [resource, fragment = ''] = ref.split('#')
    const id = resource || owner
    if (!locked.has(id)) throw errors.unsupportedCapability(`unpinned schema resource ${id}`)
    let selected: unknown = documents[id]
    if (fragment) {
      if (!fragment.startsWith('/')) throw errors.unsupportedCapability(`unsupported schema anchor ${ref}`)
      for (const token of fragment.slice(1).split('/')) {
        selected = isJsonMap(selected) ? selected[token.replace(/~1/g, '/').replace(/~0/g, '~')] : undefined
      }
      if (selected === undefined) throw errors.unsupportedCapability(`unresolved schema reference ${ref}`)
    }
  }
  Object.entries(value).filter(([key]) => !['const', 'enum', 'default', 'examples'].includes(key)).forEach(([, v]) => checkReferences(v, owner, locked))
}

const checked = new Set<string>()
export function validatePackageContracts(package_: SchemaPackage): void {
  const key = sha256Text(canonicalJson(package_))
  if (checked.has(key)) return
  const pins = package_.manifest?.validation_schemas
  const locked = new Set<string>()
  if (pins !== undefined) {
    if (!Array.isArray(pins)) throw errors.typeMismatch('validation_schemas must be a list')
    for (const pin of pins) {
      if (!isJsonMap(pin) || typeof pin.id !== 'string') throw errors.typeMismatch('schema pin needs id')
      if (!Object.hasOwn(documents, pin.id)) throw errors.unsupportedCapability(`validation schema unavailable: ${pin.id}`)
      if (digests[pin.id] !== pin.content_digest) throw errors.digestMismatch(`validation schema digest mismatch: ${pin.id}`)
      if (locked.has(pin.id)) throw errors.constraintViolation('duplicate validation schema pin')
      locked.add(pin.id)
    }
  }
  for (const id of locked) checkReferences(documents[id], id, locked)
  function visit(value: unknown): void {
    if (Array.isArray(value)) { value.forEach(visit); return }
    if (!isJsonMap(value)) return
    for (const [name, child] of Object.entries(value)) {
      if (name === 'value_schema') {
        checkReferences(child, '', locked)
        if (!validators[canonicalJson(child)]) throw errors.unsupportedCapability('value_schema is not in the static validator catalog')
      } else visit(child)
    }
  }
  visit(package_.definitions)
  checked.add(key)
}

export function validateValue(schema: Json, value: Json): void {
  const validate = validators[canonicalJson(schema)]
  if (!validate) throw errors.unsupportedCapability('value_schema is not in the static validator catalog')
  if (!validate(value)) throw errors.constraintViolation('value_schema validation failed: ' + JSON.stringify(validate.errors), validate.errors as Json)
}

export { SCHEMA_DOCUMENTS, SCHEMA_DIGESTS }

/** Validate the final transaction state, including mutations through UPDATE/UNSET. */
export function validateRecord(env: SchemaEnvironment, view: JsonMap, before: JsonMap | null): void {
  canonicalJson(view)
  const terminal = (v: JsonMap) => ['completed', 'failed', 'cancelled'].includes(String(v.status))
  for (const [name, value] of Object.entries((view.facets ?? {}) as JsonMap)) {
    if (name === 'kip://profiles/cognitive-memory@2.1.0/OutcomeRecord' && isJsonMap(before?.facets) && before.facets[name] !== undefined && canonicalJson(before.facets[name]) !== canonicalJson(value)) throw errors.immutableField('attached OutcomeRecord is immutable, including previously absent optional members')
    const symbol = env.resolveSymbol('Facet', name, 'write')
    const pkg = env.definitionPackage(symbol)
    const def = pkg && facetDef(pkg, symbol.name)
    if (!def) throw errors.schemaSymbolNotFound(name)
    if (def.value_schema !== undefined) validateValue(def.value_schema as Json, value)
    if (before && terminal(before) && (def.attachment || name.endsWith('/DependencyBasis')) && canonicalJson((before.facets as JsonMap)?.[name]) !== canonicalJson(value)) throw errors.immutableField('terminal record Facets are immutable, including previously absent records')
    const attachment = def.attachment as JsonMap | undefined
    if (attachment) {
      if (Array.isArray(attachment.activity_classes) && !attachment.activity_classes.includes(view.activity_class ?? null)) throw errors.constraintViolation(`${name} is attached to the wrong Activity class`)
      if (attachment.terminal_only === true && !terminal(view)) throw errors.constraintViolation(`${name} requires a terminal Activity`)
    }
  }
  if (before && terminal(before)) {
    for (const [name, value] of Object.entries((before.facets ?? {}) as JsonMap)) {
      const symbol = env.resolveSymbol('Facet', name, 'read')
      const pkg = env.definitionPackage(symbol)
      const def = pkg && facetDef(pkg, symbol.name)
      if ((def?.attachment || name.endsWith('/DependencyBasis')) && canonicalJson((view.facets as JsonMap)?.[name]) !== canonicalJson(value)) throw errors.immutableField('terminal record Facets are immutable')
    }
  }
  const name = String(view.schema_ref ?? '')
  const attributes = view.attributes as JsonMap | undefined
  if (name.startsWith('kip://profiles/cognitive-memory@2.1.0/')) {
    if (name.endsWith('/SkillRevision')) {
      const { behavior_digest, ...behavior } = attributes ?? {}
      if (behavior_digest !== 'sha256:' + sha256Text(canonicalJson(behavior))) throw errors.digestMismatch('SkillRevision behavior_digest must cover the immutable behavior fields')
    }
  }
}

export interface MemoryBundle {
  requires: string[]
  capabilities: string[]
  guarantees: string[]
  contract: string
  validated_standing: boolean
  required_kml?: string[]
}

export function memoryBundles(): Record<string, MemoryBundle> {
  return structuredClone(MEMORY_BUNDLE_MANIFEST.bundles) as unknown as Record<string, MemoryBundle>
}

/** Type availability never establishes an Agent-to-Brain binding. */
export function validateMemoryBundles(declared: ReadonlySet<string>, bindingAvailable: boolean, capabilityAvailable: (name: string) => boolean): void {
  if (!declared.size) return
  if (!bindingAvailable) throw errors.unsupportedCapability('memory bundles require an available Brain binding')
  const registry = memoryBundles()
  for (const name of declared) {
    const bundle = registry[name]
    if (name !== 'memory_interface' && !bundle) throw errors.unsupportedCapability(`unknown memory bundle ${name}`)
    for (const dependency of name === 'memory_interface' ? ['memory_basic'] : bundle!.requires) {
      if (!declared.has(dependency)) throw errors.unsupportedCapability(`${name} must advertise dependency ${dependency}`)
    }
    for (const capability of bundle?.capabilities ?? []) {
      if (!capabilityAvailable(capability)) throw errors.unsupportedCapability(`${name} requires ${capability}`)
    }
  }
}

export function pinnedPlane(planes: JsonMap, name: string): Json | undefined {
  if (['attributes', 'structural', 'retention'].includes(name)) return planes[name]
  if (name.startsWith('facets.') && name.length > 7) return (planes.facets as JsonMap | undefined)?.[name.slice(7)] ?? 0
  return undefined
}

export const digest = (value: Json): string => `sha256:${sha256Text(canonicalJson(value))}`

export function normalizeRecordRefs(name: string, members: JsonMap): void {
  if (!name.startsWith('kip://profiles/cognitive-memory@2.1.0/')) return
  const paths: Record<string,string[]> = {
    DependencyBasis:['groups.*.pins.*.id','policy_basis.context_refs.*'],
    DecisionRecord:['retrieved_refs.*','used_refs.*','applied_revisions.*','basis.context_refs.*'],
    AttemptRecord:['decision_ref','applied_revisions.*','trial_ref'], OutcomeRecord:['attempt_ref'],
    TrialRecord:['revision_refs.*','baseline_attempt_refs.*','baseline_outcome_refs.*','basis.context_refs.*'],
    EvaluationRecord:['trial_ref','revision_refs.*','attempt_refs.*','outcome_refs.*','missing_attempt_refs.*','excluded_samples.*.ref'],
    TrialState:['trial_ref','revision_ref'],GradingState:['revision_ref','evaluation_ref'],ErasurePlan:['source_event_refs.*','targets.*.ref'],
  }
  function visit(value: Json, path: string[]): Json {
    if (!path.length) return isJsonMap(value) && Object.keys(value).length === 1 && typeof value.id === 'string' && /^[CPAEX]-[1-9][0-9]*$/.test(value.id) ? value.id : value
    const [first,...rest]=path
    if (first === '*' && Array.isArray(value)) return value.map((v)=>visit(v,rest))
    if (isJsonMap(value) && first! in value) value[first!]=visit(value[first!]!,rest)
    return value
  }
  for (const path of paths[name.split('/').at(-1)!] ?? []) visit(members,path.split('.'))
}
