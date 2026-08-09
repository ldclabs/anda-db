import { KipDatabase } from '../src/index.js'

/**
 * Test harness object. Uses the package's default tokenizer (SimpleTokenizer)
 * so the suite has no external dependency; the Chinese-segmentation tests
 * inject a stub tokenizer instead.
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
