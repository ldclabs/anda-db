# Cross-implementation fixtures

## `kip_lang_ast.json`

Command → executable AST pairs produced by the reference toolkit
[`@ldclabs/kip-lang`](https://github.com/ldclabs/KIP/tree/main/packages/kip-lang),
which is the other implementation of KIP 2.0's language layer. `anda_kip` must
decode every command in this file to a byte-identical AST: that is what the
claim "field-for-field compatible with `exec-ast.ts`" means, and a divergence is
a real interoperability bug rather than a style difference.

The corpus covers every KQL pattern family, every KML mutation family and every
META statement family, plus the executable examples from `KIPSyntax.md`.

### Regenerating

From a checkout of the KIP repository with `packages/kip-lang` built:

```bash
node -e '
  const fs = require("fs");
  import("./dist/index.js").then((m) => {
    const cases = JSON.parse(fs.readFileSync(FIXTURE, "utf8")).cases;
    const out = cases.map(({ command }) => ({
      command,
      ast: m.lower(m.parse(command).ast),
    }));
    fs.writeFileSync(FIXTURE, JSON.stringify({ ...meta, cases: out }, null, 1));
  });
'
```

Run it from `packages/kip-lang`, pointing `FIXTURE` at this file. Add new
commands to `cases` first with any `ast`; the regeneration overwrites it with
the reference output. Then run `cargo test -p anda_kip --test kip_lang_parity`.
