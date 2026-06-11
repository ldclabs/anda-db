# Testing Standards

AndaDB promises durability and consistency, so its test suite must cover two
dimensions ordinary software tests ignore: **the machine can lose power at any
moment**, and **any operation sequence must behave like the spec**. The suite
is organised in layers; every layer runs in normal `cargo test` / CI unless
noted otherwise.

## Layers

### 1. Functional correctness (unit + integration)

Conventional tests for every public API and error branch, plus a regression
test for every fixed bug. Lives in each crate's `src/` and `tests/`. The
broadest entry point is `rs/anda_db/tests/coverage_public_api.rs`.

### 2. Crash consistency and fault injection

The crash model for object storage: each individual `put` is atomic, but a
sequence of puts/deletes can be interrupted anywhere.

- [`anda_object_store::fault::FaultStore`](../rs/anda_object_store/src/fault.rs)
  wraps any `ObjectStore` and injects faults: power failure after the N-th
  mutation, targeted per-path failures, torn writes. It also logs every
  mutation that reaches the backend, so tests can assert write ordering.
- `rs/anda_db/tests/crash_recovery.rs` replays a deterministic workload and
  simulates a power failure after **every** possible mutation, then reboots
  and checks the durability contract:
  - the database always reopens (never bricked);
  - documents acknowledged by a successful `flush` are intact and indexed;
  - documents touched after the last ack are in one of the states their
    mutation history allows — never a corrupt third state;
  - the database accepts writes after recovery.
  It also guards the flush invariants (e.g. `flush_metadata` must not advance
  `last_saved_version`), clean errors on transient read faults, and
  no-panic behaviour on corrupted objects.

When you change the flush/recovery path, this is the suite that must stay
green — and if you discover a new invariant, encode it here.

### 3. Model-based property tests

Random operation sequences run against both the real component and a
trivially-correct reference model; all observable behaviour must match.

- `rs/anda_db_btree/tests/proptest_model.rs` — B-Tree vs `std::BTreeMap`:
  mutation results, point queries, arbitrary nested range queries, unique-key
  semantics, and a flush/load round-trip, with tiny buckets to exercise
  splitting.
- `rs/anda_db_tfs/tests/proptest_model.rs` — BM25 vs a naive inverted index:
  exact retrieval sets for term queries, boolean query set algebra, score
  sanity, and a persistence round-trip.

Failures shrink to a minimal reproducing sequence; commit that sequence as a
regression test.

### 4. Fuzzing (parsers, untrusted input)

The KIP parsers are exposed to external input through `anda_db_server`; the
invariant is "always terminate with a `Result`, never panic".

- `rs/anda_kip/tests/proptest_parser.rs` — always-on fuzz subset: arbitrary
  unicode, mutated valid statements and mutated knowledge capsules, plus the
  `quote_str`/`unquote_str` round-trip.
- `rs/anda_kip/fuzz/` — `cargo fuzz` targets for open-ended coverage-guided
  runs (nightly only, see its README). Turn every crash it finds into a
  regression test.

### 5. Quantified quality metrics (approximate indexes)

HNSW is approximate: "returns results" is not "returns good results".
`rs/anda_db_hnsw/tests/recall.rs` computes exact ground truth by brute force
over deterministic vectors and asserts `recall@10` floors — on a fresh index,
after deletions, and after a persistence round-trip. If you tune index
parameters or the graph algorithm, these floors are the contract.

### 6. On-disk format compatibility

`rs/anda_db/tests/fixtures/v<MAJOR>_<MINOR>/` holds complete database
directories written by released versions, committed to the repository.
`rs/anda_db/tests/format_compat.rs` opens every fixture and verifies
documents, all three index types and extensions are intact. Breaking a
fixture means breaking existing users' data: add a migration path or revert.
After an intentional, compatible format change, regenerate the current
version's fixture:

```bash
cargo test -p anda_db --test format_compat -- --ignored generate
git add rs/anda_db/tests/fixtures
```

### 7. Coverage (dashboard, not a gate)

```bash
make coverage        # summary in the terminal
make coverage-html   # HTML report
```

CI uploads an lcov artifact. Coverage points at untested branches; it is not
a quality target by itself — fault injection and randomized testing find more
bugs than cases written to satisfy a percentage.

## Checklist for new features

- New public API → functional tests (layer 1).
- Touches flush/recovery/storage layout → crash-consistency coverage
  (layer 2) and, if the format changed, a fixture regeneration (layer 6).
- New data structure with clear semantics → a reference model property test
  (layer 3).
- Parses untrusted input → fuzz coverage (layer 4).
- Approximate/heuristic behaviour → a quantified metric with a floor
  (layer 5).
