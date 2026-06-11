# anda_kip fuzzing

Coverage-guided fuzzing of the KIP parsers (KQL / KML / META / JSON), which
are exposed to untrusted input through `anda_db_server`. The invariant under
test: parsers always terminate and return a `Result` — no panics, no hangs,
no unbounded memory.

The always-on subset of these checks runs in normal CI via
`rs/anda_kip/tests/proptest_parser.rs`. This directory is for open-ended
fuzzing sessions.

## Usage

Requires nightly and [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz):

```bash
cargo install cargo-fuzz

cd rs/anda_kip
# Seed with the shipped knowledge capsules for deep grammar coverage:
cargo +nightly fuzz run fuzz_kml fuzz/corpus/fuzz_kml capsules
cargo +nightly fuzz run fuzz_kql
cargo +nightly fuzz run fuzz_meta
cargo +nightly fuzz run fuzz_kip
```

Found crashes are minimized into `fuzz/artifacts/<target>/`; turn every fix
into a regression test in `tests/proptest_parser.rs`.
