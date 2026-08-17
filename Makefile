BUILD_ENV := rust
KIP_FUZZ_RUNS ?= 1000
KIP_FUZZ_ARGS ?= -runs=$(KIP_FUZZ_RUNS)
KIP_FUZZ_TARGETS ?= fuzz_kip fuzz_kql fuzz_kml fuzz_meta

.PHONY: build-wasm build-did lint fix test test-all test-full test-ts test-anda-db-snapshots test-anda-db-format-compat test-kip-fuzz test-py coverage coverage-html sync-agents-doc check-agents-doc

lint: check-agents-doc
	@cargo fmt
	@cargo clippy --all-targets --all-features

# CLAUDE.md is the source of the shared agent instructions; AGENTS.md is a
# byte-identical copy so non-Claude harnesses read the same document.
sync-agents-doc:
	@cp CLAUDE.md AGENTS.md
	@echo "AGENTS.md regenerated from CLAUDE.md"

check-agents-doc:
	@cmp -s CLAUDE.md AGENTS.md || \
		(echo "AGENTS.md has drifted from CLAUDE.md; run 'make sync-agents-doc'" >&2; exit 1)

fix:
	@cargo fmt --all
	@cargo clippy --fix --workspace --tests

test:
	@cargo test --workspace --all-features --exclude anda_cognitive_nexus_py -- --nocapture

# The Rust half, which is what the `test` CI job runs. `ts/kip-do` has its own
# job because it needs a Node toolchain and no Rust one.
test-all: test test-anda-db-snapshots test-kip-fuzz

test-full: test-all test-ts

# The second KIP 2.0 engine: typecheck, unit tests, the shared conformance
# suite and the differential parser oracle. Mirrors the `typescript` CI job.
test-ts:
	@command -v pnpm >/dev/null || (echo "pnpm is required: corepack enable pnpm" >&2; exit 1)
	@cd ts/kip-do && pnpm run typecheck && pnpm run test

test-anda-db-snapshots:
	@cargo test -p anda_db --test format_compat -- --nocapture

test-anda-db-format-compat: test-anda-db-snapshots

test-kip-fuzz:
	@command -v cargo-fuzz >/dev/null || (echo "cargo-fuzz is required: cargo install cargo-fuzz" >&2; exit 1)
	@set -e; for target in $(KIP_FUZZ_TARGETS); do \
		echo "Running KIP fuzz target $$target with args: $(KIP_FUZZ_ARGS)"; \
		(cd rs/anda_kip && cargo +nightly fuzz run $$target -- $(KIP_FUZZ_ARGS)); \
	done

# The Python binding is not a default workspace member: uncomment
# `py/anda_cognitive_nexus_py` in the root Cargo.toml `members` first.
# It is pinned to pyo3 0.20 (CPython 3.7-3.12); override PYO3_PYTHON when the
# `python3` on PATH is newer, e.g. `make test-py PYO3_PYTHON=python3.12`.
test-py:
	@grep -q '^  "py/anda_cognitive_nexus_py",' Cargo.toml || \
		(echo 'uncomment "py/anda_cognitive_nexus_py" in the root Cargo.toml [workspace] members first' >&2; exit 1)
	@$(if $(PYO3_PYTHON),PYO3_PYTHON=$(PYO3_PYTHON) ,)cargo test -p anda_cognitive_nexus_py --lib

# Coverage is a dashboard, not a gate: use it to find untested branches in
# core code paths. Requires `cargo install cargo-llvm-cov`.
coverage:
	@cargo llvm-cov --workspace --all-features --exclude anda_cognitive_nexus_py

coverage-html:
	@cargo llvm-cov --workspace --all-features --exclude anda_cognitive_nexus_py --html --open
