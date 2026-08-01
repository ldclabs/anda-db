/**
 * Wrangler and miniflare resolve a `.wasm` import to a compiled
 * `WebAssembly.Module`. TypeScript has no built-in knowledge of that, so the
 * shape is declared here rather than asserted at each import site.
 */
declare module '*.wasm' {
  const module: WebAssembly.Module
  export default module
}
