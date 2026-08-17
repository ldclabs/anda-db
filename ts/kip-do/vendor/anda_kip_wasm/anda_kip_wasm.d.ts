/* tslint:disable */
/* eslint-disable */

/**
 * Dumps the complete KIP error taxonomy as JSON.
 *
 * `scripts/codegen-errors.mjs` turns this into `src/errors.generated.ts` so
 * the TypeScript engine cannot drift from the Rust definitions. Transcribing
 * the registry's codes, categories, retry classes and agent-facing hints by
 * hand is exactly the kind of task that looks done and is subtly wrong — a
 * wrong `hint` degrades the agent's self-correction loop silently, and a wrong
 * `retry` class turns a lost write into a duplicated one, neither with a test
 * to catch it.
 *
 * The registry is enumerated from [`KipErrorCode::ALL`], so a code added to
 * `anda_kip` appears here without anyone remembering to add it.
 */
export function error_catalog(): string;

/**
 * Parses a KIP command (KQL, KML or META) into its AST.
 *
 * Returns a JSON string of either `{"ok": <Command>}` or
 * `{"error": {code, name, message, hint}}`. A `Result`-shaped envelope is
 * used instead of a thrown exception because wasm-bindgen's error path
 * stringifies through JS, which would lose the structured code/name/hint
 * that the whole KIP error contract rests on.
 */
export function parse(input: string): string;

/**
 * Parses a batch of commands in one call.
 *
 * A KIP request may carry a `commands` array (the multi-statement KML form),
 * and each crossing of the JS/WASM boundary costs a string copy in both
 * directions. Batching keeps that cost proportional to the payload rather
 * than to the number of statements.
 *
 * Input is a JSON array of strings; output is a JSON array of the same
 * envelopes [`parse`] returns, positionally aligned with the input. A
 * payload that does not decode as an array of strings yields a one-element
 * array carrying the decode error, so the output is an array in every case
 * and a consumer's `.map` never explodes on a bare object.
 */
export function parse_batch(inputs_json: string): string;

/**
 * Round-trips a command through parse and re-serialization.
 *
 * Used by the conformance harness to assert that the TypeScript AST mirror
 * in `src/kip/ast.ts` stays structurally aligned with the Rust definitions:
 * the harness parses with WASM, reconstructs the value in TS, and compares.
 */
export function parse_to_command_type(input: string): string;

/**
 * Returns the grammar version this module was built from.
 */
export function parser_version(): string;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly error_catalog: (a: number) => void;
    readonly parse: (a: number, b: number, c: number) => void;
    readonly parse_batch: (a: number, b: number, c: number) => void;
    readonly parse_to_command_type: (a: number, b: number, c: number) => void;
    readonly parser_version: (a: number) => void;
    readonly __wbindgen_add_to_stack_pointer: (a: number) => number;
    readonly __wbindgen_export: (a: number, b: number, c: number) => void;
    readonly __wbindgen_export2: (a: number, b: number) => number;
    readonly __wbindgen_export3: (a: number, b: number, c: number, d: number) => number;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 *
 * @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
 *
 * @returns {InitOutput}
 */
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 *
 * @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
 *
 * @returns {Promise<InitOutput>}
 */
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
