# Anda Python Bindings (anda_cognitive_nexus_py)

This crate provides the official Python bindings for the Anda engine, allowing Python applications to interact with an agent's cognitive nexus (Anda DB) using the Knowledge Interaction Protocol (KIP).

This bridge is built using [`PyO3`](https://pyo3.rs/) and packaged using [`maturin`](https://www.maturin.rs/), enabling high-performance, in-process communication between Python and the core Rust engine.

---

## Prerequisites

Before you begin, ensure you have the following tools installed on your system:

-   **Rust Toolchain:** Installed via `rustup`. ([Installation Guide](https://www.rust-lang.org/tools/install))
-   **Python:** Version 3.8 or higher.
-   **uv:** A fast Python installer and resolver. ([Installation Guide](https://github.com/astral-sh/uv))

## Rust Lib Verification
```bash
git clone REPO_URL
cd anda_db
cargo check -p anda_cognitive_nexus_py
cargo test --package anda_cognitive_nexus_py -- tests::test_execute_kip_in_mem --show-output
cargo run --example test_kip_stateful_execution
cargo test -p anda_cognitive_nexus_py --doc
```

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

After this step, the `anda` module is available to be imported in any Python script run from this activated environment.

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
import anda

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
import anda

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
	# Execute a KIP command (async)
	result = await db.execute_kip("KIP_COMMAND_STRING")
	print(result)  # result is a Python dict, not a JSON string

asyncio.run(main())
```

**Notes:**
- `StoreLocationType` and other enums are exposed as Python classes, not as `enum.Enum`. Use `anda.StoreLocationType.InMem` (not a string or dict).
- See the Python tests in `tests_py/` for more usage examples.
