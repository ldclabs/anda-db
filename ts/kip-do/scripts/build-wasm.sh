#!/usr/bin/env bash
#
# Rebuilds the KIP grammar WASM module from `rs/anda_kip_wasm` into
# `vendor/anda_kip_wasm/`.
#
# The output is committed but NOT shipped: it is the oracle
# `test/parser-oracle.test.ts` compares this engine's TypeScript parser
# against. Run this whenever the `anda_kip` grammar changes and commit the
# result with the Rust change — that run is where a divergence between the two
# KIP engines is meant to surface.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
pkg_root="$(dirname "$here")"
crate_dir="$(cd "$pkg_root/../../rs/anda_kip_wasm" && pwd)"
out_dir="$pkg_root/vendor/anda_kip_wasm"

if ! command -v wasm-pack >/dev/null 2>&1; then
  echo "error: wasm-pack is required (cargo install wasm-pack)" >&2
  exit 1
fi

# `--target web` emits `initSync(module)`, which is the shape Cloudflare
# Workers need: wrangler and miniflare resolve a `.wasm` import to a
# `WebAssembly.Module`, which is handed straight to `initSync`. The
# `bundler` target instead emits a bare `import * as wasm from "./x.wasm"`
# that only a wasm-aware bundler resolves, and neither wrangler nor vite does.
echo "building anda_kip_wasm -> $out_dir"
wasm-pack build "$crate_dir" \
  --target web \
  --release \
  --out-dir "$out_dir" \
  --out-name anda_kip_wasm

# wasm-pack writes a package.json and .gitignore describing a standalone npm
# package. Neither is wanted here: this directory is a test fixture inside an
# existing package, and the .gitignore would exclude the very artifact we
# need to commit.
rm -f "$out_dir/package.json" "$out_dir/.gitignore" "$out_dir/README.md"

echo "wasm size: $(du -h "$out_dir/anda_kip_wasm_bg.wasm" | cut -f1)"
