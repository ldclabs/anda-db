import { CognitiveNexus, SYSTEM_PRINCIPAL } from '../../src/nexus.js'
import { COGNITIVE_MEMORY } from '../../src/schema/index.js'
import { principalAuth } from '../../src/governance/index.js'
import { digest } from '../../src/schema/contracts.js'
import { parseElementId } from '../../src/id.js'
import type { JsonMap } from '../../src/json.js'
import type {
  PreparedWatchPage,
  WatchEvaluation,
  WakeRecord,
} from '../../src/attention/types.js'
import { OPTIONS } from './options.js'
export const pin = (id: string) => ({ id, digest: digest({ id }) })
export const expiry = () => new Date(Date.now() + 120000).toISOString()
export const version = (n: CognitiveNexus, ref: string) =>
  n.store.load(parseElementId(ref))!.row.version
export function fresh(storage: DurableObjectStorage) {
  const n = CognitiveNexus.connect(storage)
  n.activatePackages([COGNITIVE_MEMORY, OPTIONS])
  return n
}
export function fixture(
  storage: DurableObjectStorage,
  semantic = false,
  binding = false,
) {
  const n = fresh(storage),
    s = n.systemSession()
  if (semantic || binding)
    s.setAttentionConfig(0, {
      scope: { space_id: n.space, space_instance: 'test-instance' },
      pins: {
        policy: pin('policy'),
        evaluator: semantic ? pin('evaluator') : null,
        binding: binding ? pin('executor') : null,
      },
    })
  const target = n.execute('CREATE CONCEPT ?t {TYPE "Person" NAME "before"}')
    .handles.t!
  const watch = n.execute(
    'CREATE CONCEPT ?w {TYPE "Watch" SET ATTRIBUTES {watch_class:"delta",summary:"track",condition: :condition,status:"disarmed"}}',
    {
      condition: {
        element: target,
        ops: ['update'],
        ...(semantic ? { text: 'meaningful update' } : {}),
      },
    },
  ).handles.w!
  const armed = s.armWatch(watch, 1),
    generation = Number((armed.watch as JsonMap).arm_generation)
  n.execute('UPDATE :target SET FIELDS {name:"after"}', { target })
  const matchedSeq = n.store.currentSeq(n.space)
  const advance = () => s.advanceWatch(watch, version(n, watch), generation)
  return { n, s, target, watch, armed, generation, matchedSeq, advance }
}
export const wakeOf = (result: JsonMap) => result.wake as unknown as WakeRecord
export const evaluation = (
  page: PreparedWatchPage,
  key: string,
  result: 'match' | 'no_match' | 'unknown',
): WatchEvaluation => ({
  evaluation_key: key,
  evaluator: page.evaluator,
  judgments: page.candidates.map((c) => ({
    candidate_id: c.id,
    result,
    rationale: 'independent host evaluation',
  })),
})
export function principal(
  n: CognitiveNexus,
  id: string,
  actions: string[],
  fields?: string[],
) {
  n.store.governance.ensurePrincipal({ principal_id: id })
  n.store.governance.createGrant(
    {
      space_id: n.space,
      grantee_principal: id,
      actions,
      ...(fields ? { constraints: { fields } } : {}),
    },
    SYSTEM_PRINCIPAL,
  )
  return n.session(principalAuth(id))
}
