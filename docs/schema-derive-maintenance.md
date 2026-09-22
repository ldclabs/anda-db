# Schema / derive Review Checklist and Implementation Report

[中文版](schema-derive-maintenance.zh.md)

Review baseline: `c2a7077`. Completion date: 2026-09-06.

Import paths for public types and document CBOR serialization layouts remain fully backward-compatible. Schemas incorporate upgrade history metadata; legacy serialized formats remain readable. When legacy databases upgrade for the first time, the core database scans raw documents and pending mutation records, reconstructing field assignment history prior to allocating new field indices.

- [x] **1. F32 JSON roundtrip.** Identifies read-back values using real JSON formatting and parsing semantics, preserving exact CBOR extension pathways; 200,000 bit-pattern samples with a fixed seed verify bitwise roundtrips for finite F32 values under both default parsing and `float_roundtrip` configurations.
- [x] **2. Reassignment of legacy field indices.** Rejects direct allocation when history is indeterminate; `SchemaHistoryRecovery` derives high-water marks from raw data only when watermarks are absent, preventing trusted high-water marks from being artificially elevated by foreign fields. Collection upgrades scan unregistered documents and replay-verified mutation intent images, persisting upgraded schemas prior to write operations in open callbacks.
- [x] **3. Multi-version nested field reuse.** Persists tombstone deletion paths, rejecting subsequent reuse of identical field names; covers arrays, Option, wildcard maps, and upgrades following JSON/CBOR schema overrides.
- [x] **4. Open map upgrades.** Transitions between open maps and fixed-field maps require explicit migrations, preventing invalid values under tightened constraints and avoiding inadvertent exposure of previously retired keys when opened.
- [x] **5. FieldEntry validation entry points.** `SchemaBuilder::add_field` re-validates names and types of deserialized entries, leaving builder state unmodified on failure; validates schema watermark upper bounds.
- [x] **6. JSON validation.** Rejects raw Bytes, CBOR Tags, non-text keys, and their nested representations; required JSON fields distinguish missing keys from explicit JSON nulls.
- [x] **7. Recursive types.** Direct recursion in known containers is rejected at compile time; introduces `try_field_type()` with construction guards, allowing indirect recursion and recursive type aliases to terminate via error returns. Retains the legacy non-failing `field_type()` path for compatibility.
- [x] **8. `_id` attribute enforcement.** Validates integer CBOR keys and `flatten` attributes before executing early return branches; adds compile-fail UI test cases.
- [x] **9. Reserved key collisions.** Fixed struct fields disallow `"*"` and `i64::MIN` to avoid collisions with wildcard representations.
- [x] **10. Borrowed map keys.** Key inference supports references, slices, and transparent wrappers; includes real document write and persistence roundtrip tests for `&str` and `&[u8]`.
- [x] **11. Non-finite float JSON output.** Human-readable serializers reject Infinity to prevent silent coercion to `null`; CBOR retains infinite float representations.
- [x] **12. Duplicate key rejection.** Document field indices, FieldType Maps, and upgrade history Maps share unified duplicate key detection; JSON conversion explicitly rejects duplicate object keys.
- [x] **13. JSON conversion depth.** In-memory CBOR -> JSON conversion enforces depth limits identical to surrounding containers; `Option` in the derive DSL does not consume container depth quota.
- [x] **14. Canonical value fast paths.** `Scalar`, `Text`, `Bytes`, and `Vector` retain data directly; `coerce` on canonical `Vector` bypasses intermediate CBOR array construction, while non-canonical arrays fall back to existing coercion rules.
- [x] **15. JSON ownership and read traversal.** Moves strings, keys, and JSON subtrees; document reads combine pruning, normalization, and type checking into a single pass, followed by a per-field complexity check that preserves original document state on failure.
- [x] **16. Memory allocations.** Scalar complexity checks avoid heap allocations; sequence capacity hints utilize upper bounds; CBOR vector conversion preallocates by known length. Vector reads retain iterator-based implementations measured faster under the release profile.
- [x] **17. Module decomposition.** Refactored `field.rs` into a public facade, separating definitions, keys, values, metadata, budgets, and tests. `FieldType` derives `Debug`; repetitive `From` conversions utilize compact helper macros.
- [x] **18. Macro parsing and diagnostics.** DSL normalizes whitespace once; rejects malformed helper attributes, container `field_type` overrides, and Serde `tag`/`into` attributes that alter container layouts.
- [x] **19. Testing and documentation.** Added cross-path, cross-version, and failure-atomicity test coverage; enabled doctests across both crates; verified README examples; updated dependency versions and deprecated APIs.

Implementations and regressions:

- [Runtime regressions](../rs/anda_db_schema/tests/regressions.rs), [Recursive types](../rs/anda_db_schema/tests/recursive_types.rs), [Borrowed keys](../rs/anda_db_schema/tests/borrowed_keys.rs)
- [Upgrade history and recovery](../rs/anda_db_schema/src/schema/history.rs), [Collection upgrade integration](../rs/anda_db/src/collection.rs)
- [Macro compile-fail cases](../rs/anda_db_derive/tests/ui/), [Performance benchmarks](../rs/anda_db_schema/benches/values.rs)

## Performance Verification

Criterion benchmarks executed serially across baseline and updated implementations on identical hardware using the workspace bench/release profile. 20 samples, 0.4s warmup, 1s measurement. Values represent central estimates; benchmarks measure `coerce` or post-decode document construction/validation, excluding input setup, CBOR decoding, and storage I/O, and cannot be extrapolated directly to overall database speedup.

| Scenario | Baseline | Updated |
| --- | --- | ---: |
| Scalar coerce | 42.37 ns | 9.74 ns |
| Scalar document assembly | 167.06 ns | 67.21 ns |
| 1536-dim canonical Vector coerce | 6.30 µs | 9.55 ns |
| 1536-dim vector document assembly | 6.83 µs | 6.71 µs |
| 100-item JSON coerce | 5.87 µs | 0.80 µs |
| 100-item JSON document assembly | 4.41 µs | 1.28 µs |
| 100-item text array coerce | 3.72 µs | 1.16 µs |
| 100-item text array document assembly | 1.99 µs | 1.22 µs |

Allocation profiling (post-input construction): 1536-dim canonical Vector allocations dropped from 12 (57,360 total requested bytes) to 0; 100-item JSON dropped from 114 (19,142 bytes) to 2 (2,424 bytes). Metrics track cumulative requested allocations, not peak memory.

```sh
cargo bench -p anda_db_schema --bench values -- --warm-up-time 0.4 --measurement-time 1 --sample-size 20 --noplot
```

## Verification Commands and Compatibility

```sh
cargo check --workspace --all-features
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test -p anda_db_schema --test regressions --features serde_json/float_roundtrip finite_f32_json_roundtrip_preserves_bits
cargo fmt -p anda_db_schema -p anda_db_derive -p anda_db -- --check
```

Workspace tests include legacy format fixture verification, collection history scans, 15 `trybuild` compile-fail cases, and documentation tests. Fixture generation tests remain ignored by design; historical fixtures were not overwritten.

Both crates pass validation on Rust 1.89. Rust 1.88 checks are rejected due to `cbor2 1.1.4` specifying `rust-version = 1.89`, reflecting an existing dependency constraint relative to the workspace MSRV.

Custom storage implementations must supply unpruned raw documents and pending mutation images to the history recovery engine under exclusive writer lock; partial scans cannot guarantee safe field index allocation. Core collections adhere to this workflow automatically. The infallible `field_type()` interface panics on invalid declarations; callers requiring error handling should use `try_field_type()`, while derived `schema()` implementations propagate nested construction errors.
