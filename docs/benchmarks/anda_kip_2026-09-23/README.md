# Anda KIP protocol review benchmarks — 2026-09-23

Compare baseline `91f6a0e` with this SDK repair. These are local parser/request
preparation measurements, not Nexus transaction or storage throughput.

## Method

The same [driver](driver.rs) was compiled in two standalone temporary Cargo
projects, each depending on the corresponding `anda_kip` path. Both used Rust
1.98.1 and Cargo's ordinary release profile (`opt-level=3`, no workspace size
profile). Input construction is outside the timed loop; parsing, validation,
AST creation and destruction are included. Each workload has three warm-up
iterations, followed by 20 iterations for bulk KML or 20,000 iterations for
KQL and request preparation. Three process pairs ran sequentially, alternating
baseline/current order. The table reports the median of those three averages;
all raw observations are in [timings.csv](timings.csv).

| Workload | Baseline µs | Updated µs | Baseline / updated |
| --- | ---: | ---: | ---: |
| 100 CREATE clauses | 442.712 | 98.196 | 4.51× |
| 500 CREATE clauses | 8,316.444 | 521.031 | 15.96× |
| 1,000 CREATE clauses | 31,802.283 | 1,091.656 | 29.13× |
| 2,000 CREATE clauses | 125,837.352 | 2,013.244 | 62.50× |
| Simple KQL query | 4.893 | 3.366 | 1.45× |
| Prepare request with 64 KiB ingest payload | 6.663 | 1.251 | 5.33× |

The bulk workload is one MUTATE command, below the existing 256 KiB input
ceiling even at 2,000 clauses. The main change removes a full handle-set clone
per clause. Text parsing no longer materializes an intermediate JSON AST just
to revalidate numbers already checked by the lexer. Ordinary clauses lower
without boxed closures or one-item result vectors; ASSERT consumes its members.
Request preparation validates JSON fields in place and reuses the parsed
commands for its ingestion check.

External ASTs still go through the numeric/depth gate and shared shape checks.
Raw JSON ingestion still rejects duplicate keys and lossy number tokens. No
parser budget, mutation atomicity, or persisted data format was relaxed for
these measurements. The TypeScript SQL fix preserves explicit JSON nulls in
new writes; it does not rewrite existing stored rows.

## Reproduction and regression coverage

The permanent Criterion workloads use the same inputs:

```sh
cargo bench -p anda_kip --bench protocol --profile release-speed
```

For the paired numbers above, place `driver.rs` at `src/main.rs` in a standalone
Cargo package with edition 2024 and `anda_kip = { path = "<checkout>/rs/anda_kip" }`.
Build with `cargo build --release`, run each binary three times, and take each
workload's median. Do not compare the workspace's default size-optimized
release profile against the standalone speed profile.

Validation completed for this change:

- Rust workspace, all features: 1,777 tests passed; one existing fixture
  generator remained ignored.
- TypeScript: typecheck and 531 tests passed, including the rebuilt WASM parser
  oracle and shared conformance corpus.
- Workspace all-feature check, all-target/all-feature Clippy with warnings
  denied, formatting, agent-document consistency and changed-document links.

The SDK adds 11 focused regression tests covering external Capsule and Core
wire values, null preservation, AST/text admission, Core vocabulary scope,
mutation references, extensions, portable numbers and per-operation receipts.
Nexus adds an ingestion/readback regression; TypeScript covers the matching
parser, extension and empty-payload behavior.
