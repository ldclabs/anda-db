# Anda Python Bindings (anda_cognitive_nexus_py)

This crate provides the official Python bindings for the Anda engine, allowing Python applications to interact with an agent's cognitive nexus (Anda DB) using the Knowledge Interaction Protocol (KIP).

This bridge is built using [`PyO3`](https://pyo3.rs/) and packaged using [`maturin`](https://www.maturin.rs/), enabling high-performance, in-process communication between Python and the core Rust engine.

It speaks **KIP 2.0**. Two consequences for anyone porting 1.x code:

- **A type is not graph state.** There is no `$ConceptType` node to write before
  using a type: types come from an immutable Schema Package, and
  `PyAndaDB.create` activates the bundled cognitive-memory profile (`Person`,
  `Preference`, `Event`, `Experience`, …) in the default MemorySpace.
- **`response` is the KIP 2.0 response envelope**, not a bare result:
  `{"kip", "status", "results": [{"status", "result", "error"}], "receipt", …}`.
  A command that fails reports on its own result entry; the request-level
  `error` is for a failure of the envelope itself.

---

## Prerequisites

Before you begin, ensure you have the following tools installed on your system:

-   **Rust Toolchain:** Installed via `rustup`. ([Installation Guide](https://www.rust-lang.org/tools/install))
-   **Python:** 3.8 – 3.12. The bindings are built on `pyo3` 0.20 (the last line
    that `pyo3-asyncio` supports), which refuses interpreters newer than 3.12.
    If the `python3` on your PATH is newer, select a supported one with
    `PYO3_PYTHON=python3.12`.
-   **uv:** A fast Python installer and resolver. ([Installation Guide](https://github.com/astral-sh/uv))

## Rust Lib Verification

This crate is **not** a default member of the Rust workspace (it links against a
Python interpreter). Uncomment `py/anda_cognitive_nexus_py` in the `members`
array of the repository root `Cargo.toml` before running any `cargo` command
against it, and comment it back out afterwards.

```bash
git clone REPO_URL
cd anda-db
# edit Cargo.toml: uncomment "py/anda_cognitive_nexus_py" under [workspace] members
cargo check -p anda_cognitive_nexus_py
cargo test --package anda_cognitive_nexus_py -- tests::test_execute_kip_in_mem --show-output
cargo run -p anda_cognitive_nexus_py --example test_kip_stateful_execution
cargo test -p anda_cognitive_nexus_py --doc
```

`make test-py` wraps the `--lib` run and checks the member is uncommented first.

## Python Development Setup

These instructions will guide you through setting up a local development environment to work on the `anda_cognitive_nexus_py` bindings.

All commands should be run from the **root of the `anda` repository**.

**1. Create Virtual Environment**

First, create and activate a Python virtual environment. This isolates our dependencies.

```bash
cd py/anda_cognitive_nexus_py
# Create the virtual environment
uv venv

# Activate the environment (Linux/macOS)
source .venv/bin/activate

# On Windows (cmd.exe), use:
# .venv\Scripts\activate.bat
```

**2. Install & Build for Development**

Next, use `maturin` to build the Rust crate and install it as an editable package in your virtual environment. The `develop` command compiles the Rust code and links it to your environment, so changes in the Rust code are available after recompiling without needing to reinstall.

```bash
uv pip install -r tests_py/requirements.txt
# This command will compile the Rust code and install the `anda` package
maturin develop
```

After this step, the `anda_cognitive_nexus_py` module is available to be imported in any Python script run from this activated environment.

To build a release wheel, use the `release-py` profile so panics unwind into
Python exceptions instead of aborting the interpreter:

```bash
maturin build --profile release-py
```

## Running Tests

Tests for the Python bindings are located in the `tests_py/` directory and use the `pytest` framework.

To run the tests, execute the following command from the project root:

```bash
# Make sure your virtual environment is activated
pytest --cache-clear
# find . -type d -name "__pycache__" -exec rm -rf {} +

# pytest -s --log-cli-level=INFO tests_py/
pytest -v tests_py/

# Test a single case with debug level log
export RUST_LOG=debug
pytest -s -k test_create_success
```

You should see an output indicating that all tests have passed.

## Basic Usage Example

To quickly verify your setup, you can run the following Python script:

```python
# main.py
import anda_cognitive_nexus_py as anda

# This is the "hello world" function currently implemented
result = anda.sum_as_string(10, 20)

print(f"Calling the Rust-powered 'sum_as_string(10, 20)' function...")
print(f"Result: {result}")

assert result == "30"

print("Successfully received a response from the Rust library!")
```

Run it with:

```bash
python main.py
```

---

## Creating a Database and Executing a KIP Command (New API)

The API now exposes configuration and enums as Python classes, not dicts or strings. Construct configs using `AndaDbConfig` and `StoreLocationType` directly:

```python
import anda_cognitive_nexus_py as anda

# Construct the config using Python classes (not dicts)
config = anda.AndaDbConfig(
	store_location_type=anda.StoreLocationType.InMem,  # Use enum variant as a class attribute
	store_location="",
	db_name="test_db",
	db_desc="Test database",
	meta_cache_capacity=10000
)

# Create the database (async)
import asyncio
async def main():
	db = await anda.PyAndaDB.create(config)
	try:
		# Record an attributed claim. The Proposition states the tuple; the
		# Assertion is what commits to it, with a stance, a mode and a
		# confidence. Nothing here says the claim is true.
		await db.execute_kip("""
			MUTATE {
				CREATE CONCEPT ?alice { TYPE "Person" NAME :who }
				CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark mode" }
				ASSERT ?a (?alice, "prefers", ?dark) {
					by: ?alice, mode: "stated", confidence: 0.9
				}
			}
			""",
			parameters={"who": "Alice"},
		)

		# Read the claims back — who claimed what, with how much confidence.
		result = await db.execute_kip(
			"""
			FIND(?person.name, ?thing.name, ?a.confidence)
			WHERE {
				?p PROPOSITION (?person, "prefers", ?thing)
				?a ASSERTION {proposition: ?p}
			}
			ORDER BY ?a.confidence DESC
			"""
		)
		print(result["response"]["results"][0]["result"])
	finally:
		# Flush pending data to storage; required for file-backed stores.
		await db.close()

asyncio.run(main())
```

**Notes:**
- `StoreLocationType` and other enums are exposed as Python classes, not as `enum.Enum`. Use `anda.StoreLocationType.InMem` (not a string or dict).
- Parameters are bound structurally into value positions, never interpolated
  into the command text — `:who` is data, not code.
- A read returns raw claims. What is *currently believed* is projected from
  Assertions under a policy (`BELIEF` / `BELIEF SLOT`) and is never stored.
- See the Python tests in `tests_py/` for more usage examples.
