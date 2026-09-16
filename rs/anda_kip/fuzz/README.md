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
cargo +nightly fuzz run fuzz-kml
cargo +nightly fuzz run fuzz-kql
cargo +nightly fuzz run fuzz-meta
cargo +nightly fuzz run fuzz-kip
```

The binary target names use hyphens; the Rust source files keep underscores.
When upgrading an existing checkout, rename each `fuzz/corpus/fuzz_<language>`
directory to `fuzz/corpus/fuzz-<language>` to retain its accumulated inputs. If
both directories already exist, keep both and pass the old corpus as an extra
input, for example:

```bash
cargo +nightly fuzz run fuzz-kml fuzz/corpus/fuzz-kml fuzz/corpus/fuzz_kml
```

Found crashes are minimized into `fuzz/artifacts/<target>/`; turn every fix
into a regression test in `tests/proptest_parser.rs`.
