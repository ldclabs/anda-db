BUILD_ENV := rust

.PHONY: build-wasm build-did

lint:
	@cargo fmt
	@cargo clippy --all-targets --all-features

fix:
	@cargo clippy --fix --workspace --tests

test:
	@cargo test --workspace --all-features --exclude anda_cognitive_nexus_py -- --nocapture

test-py:
	@cargo test -p anda_cognitive_nexus_py --lib

# Coverage is a dashboard, not a gate: use it to find untested branches in
# core code paths. Requires `cargo install cargo-llvm-cov`.
coverage:
	@cargo llvm-cov --workspace --all-features --exclude anda_cognitive_nexus_py

coverage-html:
	@cargo llvm-cov --workspace --all-features --exclude anda_cognitive_nexus_py --html --open