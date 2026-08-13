import { KipDatabase, type KipResponse } from '../src/index.js'

/**
 * Test harness object. Uses the package's default tokenizer (SimpleTokenizer)
 * so the suite has no external dependency; the Chinese-segmentation tests
 * inject a stub tokenizer instead.
 */
export class TestKipDatabase extends KipDatabase {}

/**
 * Calls the one RPC method used by tests without asking TypeScript to expand
 * Cloudflare's recursive Durable Object RPC mapped type over the whole class.
 */
export function executeTestKip(
  stub: DurableObjectStub<TestKipDatabase>,
  command: string,
): Promise<KipResponse> {
  return (
    stub as unknown as {
      executeKip(command: string): Promise<KipResponse>
    }
  ).executeKip(command)
}

export interface Env {
  KIP_DB: DurableObjectNamespace<TestKipDatabase>
}

export default {
  async fetch(): Promise<Response> {
    return new Response('kip-do test harness')
  },
}
