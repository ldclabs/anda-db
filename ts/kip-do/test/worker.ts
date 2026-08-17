import { KipDatabase } from '../src/index.js'

/**
 * Test harness object.
 *
 * The real class, not a stand-in: the engine's own tests reach into the object
 * with `runInDurableObject` and build their own `CognitiveNexus`, but the HTTP
 * surface has to be exercised as a host would deploy it.
 */
export class TestKipDatabase extends KipDatabase {}

export interface Env {
  KIP_DB: DurableObjectNamespace<TestKipDatabase>
}

export default {
  async fetch(): Promise<Response> {
    return new Response('kip-do test harness')
  },
}
