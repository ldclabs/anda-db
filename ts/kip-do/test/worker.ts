import { DurableObject } from 'cloudflare:workers'

/**
 * Test harness object.
 *
 * `wrangler.jsonc` names a SQLite-backed Durable Object class, and the workers
 * test pool boots the whole Worker before it runs any file — including the
 * parser tests, which touch no storage at all. So this class has to exist for
 * the suite to start.
 *
 * It is a placeholder while the KIP 2.0 engine is being rebuilt: the 1.x
 * `KipDatabase` was deleted with the rest of the 1.x executor, and the 2.0 one
 * does not exist yet. The engine's own tests will replace this with the real
 * class rather than adding a second one beside it.
 */
export class TestKipDatabase extends DurableObject {}

export interface Env {
  KIP_DB: DurableObjectNamespace<TestKipDatabase>
}

export default {
  async fetch(): Promise<Response> {
    return new Response('kip-do test harness')
  },
}
