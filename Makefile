BUILD_ENV := rust
KIP_FUZZ_RUNS ?= 1000
KIP_FUZZ_ARGS ?= -runs=$(KIP_FUZZ_RUNS)
KIP_FUZZ_TARGETS ?= fuzz_kip fuzz_kql fuzz_kml fuzz_meta

.PHONY: build-wasm build-did lint fix test test-all test-full test-anda-db-snapshots test-anda-db-format-compat test-kip-fuzz test-py coverage coverage-html

lint:
	@cargo fmt
	@cargo clippy --all-targets --all-features

fix:
	@cargo clippy --fix --workspace --tests

test:
	@cargo test --workspace --all-features --exclude anda_cognitive_nexus_py -- --nocapture

test-all: test test-anda-db-snapshots test-kip-fuzz

test-full: test-all

test-anda-db-snapshots:
	@cargo test -p anda_db --test format_compat -- --nocapture

test-anda-db-format-compat: test-anda-db-snapshots

test-kip-fuzz:
	@command -v cargo-fuzz >/dev/null || (echo "cargo-fuzz is required: cargo install cargo-fuzz" >&2; exit 1)
	@set -e; for target in $(KIP_FUZZ_TARGETS); do \
		echo "Running KIP fuzz target $$target with args: $(KIP_FUZZ_ARGS)"; \
		(cd rs/anda_kip && cargo +nightly fuzz run $$target -- $(KIP_FUZZ_ARGS)); \
	done

test-py:
	@cargo test -p anda_cognitive_nexus_py --lib

# Coverage is a dashboard, not a gate: use it to find untested branches in
# core code paths. Requires `cargo install cargo-llvm-cov`.
coverage:
	@cargo llvm-cov --workspace --all-features --exclude anda_cognitive_nexus_py

coverage-html:
	@cargo llvm-cov --workspace --all-features --exclude anda_cognitive_nexus_py --html --open
