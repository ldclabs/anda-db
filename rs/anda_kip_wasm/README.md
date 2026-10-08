# anda_kip_wasm

`anda_kip_wasm` compiles the [`anda_kip`](../anda_kip) parser to WebAssembly so
it can serve as the **oracle** in a differential test. It is not published.

The TypeScript engine [`@ldclabs/kip-do`](../../ts/kip-do) parses KIP with
`@ldclabs/kip-lang`, a native TypeScript implementation. Nothing structural
forces the two grammars to agree on what a command means, so
`ts/kip-do/test/parser-oracle.test.ts` compares them field for field over a
corpus harvested from the shared conformance fixtures and the KIP command
strings in the Rust sources and tests. `anda_kip` is pure computation with no
I/O, so it compiles to `wasm32-unknown-unknown` unchanged and gives the
reference answer.

The module is a committed test dependency of `kip-do`
(`ts/kip-do/vendor/anda_kip_wasm/`), not part of its published package.

## Exported functions

The boundary is one string in, one JSON string out, which keeps the ABI
stable across `wasm-bindgen` versions and the payload easy to inspect when
the parsers disagree.

| Function                       | Returns                                                                                 |
| ------------------------------ | --------------------------------------------------------------------------------------- |
| `parse(input)`                 | `{"ok": <Command>}` or `{"error": {code, name, message, hint}}`                         |
| `parse_batch(inputs_json)`     | an array of `parse` envelopes for a JSON array of commands                              |
| `parser_version()`             | the grammar version this module was built from                                          |
| `error_catalog()`              | the Core Error Registry from `KipErrorCode::ALL`, the source of `kip-do`'s generated table |
| `parse_to_command_type(input)` | a parse and re-serialization round trip, for AST structure checks                       |

## Building

This crate is a separate Cargo workspace: a `cdylib` target would make the
root `cargo test --workspace` link a dynamic library for nothing. Build it
through `kip-do`, which needs [wasm-pack](https://rustwasm.github.io/wasm-pack/)
and the `wasm32-unknown-unknown` target:

```bash
cd ts/kip-do
pnpm run build:oracle-wasm
```

The script writes `--target web` output, which Cloudflare Workers can load
with `initSync`, into `ts/kip-do/vendor/anda_kip_wasm/`. Rebuild and commit
it, followed by `pnpm run codegen`, whenever the Rust grammar or error
registry changes; that run is where a divergence between the two KIP engines
is meant to surface.

## License

MIT. See [LICENSE](../../LICENSE).
