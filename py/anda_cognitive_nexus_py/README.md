# Anda Python Bindings (anda_cognitive_nexus_py)

This crate provides the official Python bindings for the Anda engine, allowing Python applications to interact with an agent's cognitive nexus (Anda DB) using the Knowledge Interaction Protocol (KIP).

This bridge is built using [`PyO3`](https://pyo3.rs/) and packaged using [`maturin`](https://www.maturin.rs/), enabling high-performance, in-process communication between Python and the core Rust engine.

It speaks **KIP 2.0**. Three consequences for anyone porting 1.x code:

- **A type is not graph state.** There is no `$ConceptType` node to write before
  using a type: types come from an immutable Schema Package, and
  `PyAndaDB.create` activates the bundled cognitive-memory profile (`Person`,
  `Preference`, `Event`, `Experience`, …) in the default MemorySpace.
- **`response` is the KIP 2.0 response envelope**, not a bare result:
  `{"kip", "status", "results": [{"status", "result", "error"}], "receipt", …}`.
  A command that fails reports on its own result entry; the request-level
  `error` is for a failure of the envelope itself.
- **A Proposition existing is not the same as it being true.** A read returns
  raw claims: who asserted what, with what confidence. What is *currently
  believed* is projected from those Assertions under a named policy
  (`BELIEF` / `BELIEF SLOT`) and is never stored.

---

## Prerequisites

Before you begin, ensure you have the following tools installed on your system:

-   **Rust Toolchain:** Installed via `rustup`. ([Installation Guide](https://www.rust-lang.org/tools/install))
-   **Python:** 3.10 – 3.12. The bindings are built on `pyo3` 0.20 (the last line
    that `pyo3-asyncio` supports), which refuses interpreters newer than 3.12.
    If the `python3` on your PATH is newer, install a supported one and point
    the build at it:

    ```bash
    uv python install 3.12
    export PYO3_PYTHON="$(uv python find 3.12)"
    ```

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
export PYO3_PYTHON="$(uv python find 3.12)"
cargo check -p anda_cognitive_nexus_py
cargo test -p anda_cognitive_nexus_py --lib
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
cd py
# Create the virtual environment on a supported interpreter
uv venv --python 3.12

# Activate the environment (Linux/macOS)
source .venv/bin/activate

# On Windows (cmd.exe), use:
# .venv\Scripts\activate.bat
```

**2. Install & Build for Development**

Next, use `maturin` to build the Rust crate and install it as an editable package in your virtual environment. The `develop` command compiles the Rust code and links it to your environment, so changes in the Rust code are available after recompiling without needing to reinstall.

```bash
uv pip install -r anda_cognitive_nexus_py/tests_py/requirements.txt
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

```bash
# Make sure your virtual environment is activated
pytest -v anda_cognitive_nexus_py/tests_py/

# Test a single case with debug level log
export RUST_LOG=debug
pytest -s -k test_create_success
```

## Quick Check

```python
import anda_cognitive_nexus_py as anda

# The protocol version a request envelope's `kip` member must carry.
print(anda.kip_version())  # '2.0'
```

What the *engine* behind the binding supports is a separate question with its
own answer — ask it with `DESCRIBE CAPABILITIES`, which reports the conformance
profiles and capability names as structured data.

---

## Creating a Database and Executing a KIP Command

Configuration and enums are Python classes, not dicts or strings. Construct
configs using `AndaDbConfig` and `StoreLocationType` directly:

```python
import asyncio
import anda_cognitive_nexus_py as anda

config = anda.AndaDbConfig(
	store_location_type=anda.StoreLocationType.InMem,  # Use enum variant as a class attribute
	store_location="",
	db_name="test_db",
	db_desc="Test database",
	meta_cache_capacity=10000,
	# schema_packages=None activates the bundled Cognitive Memory Profile.
)

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
- See the Python tests in `tests_py/` for more usage examples.

---

## The Read-Only Path

`execute_kip_readonly` accepts KQL and META — including `VERIFY`, `VALIDATE`,
`PREVIEW`, `HISTORY`, `CHANGES` and `EXPORT CAPSULE` — and refuses anything that
changes state:

```python
refused = await db.execute_kip_readonly(
	'CREATE CONCEPT ?c { TYPE "Person" NAME "Mallory" }'
)
refused["response"]["error"]["code"]  # 'ReadonlyViolation'
```

The refusal is decided on what the command *parses as*, never on a label a
caller attached to it. That is what makes it safe to hand this method a command
an untrusted prompt composed: no field in the request can talk a write past the
boundary.

---

## The Full Request Envelope

`execute_kip` covers one command. Everything else in KIP 2.0's envelope lives on
`execute_request`, which takes the envelope as a dict and returns the response
envelope — the same shape the HTTP server speaks:

```python
response = await db.execute_request({
	"kip": "2.0",
	"request_id": "req-1",
	# A MemorySpace is named, never inferred from conversation context.
	"space": {"id": "kip:space:default"},
	"execution": {
		"mode": "sequence",          # or "independent"
		"on_error": "stop",
		"idempotency_key": "onboarding:alice",
	},
	# Observed material becomes Evidence straight from the envelope, instead of
	# being re-typed by a model inside KML text.
	"ingest": {
		"evidence": [{
			"key": "msg",
			"evidence_class": "user_statement",
			"payload": "I prefer dark mode.",
			"observed_at": "2026-08-14T01:00:00Z",
		}]
	},
	"operations": [
		{"op_id": "write", "command": """
			ASSERT (:alice, "prefers", :dark_mode) {
				by: :alice, mode: "stated", evidence: :msg
			}
		"""},
		{"op_id": "read", "command": 'FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} }'},
	],
	"parameters": {"alice": "...", "dark_mode": "..."},
	"options": {"deadline_ms": 10000},
})

response["status"]                        # 'succeeded' | 'partial' | 'failed' | 'outcome_unknown'
[r["op_id"] for r in response["results"]] # ['write', 'read']
response["results"][0]["receipt"]["tx_id"]
```

Unlike `execute_kip`, this returns the bare envelope with no `"type"` beside it:
a request whose operations are a read and a write has no single language, and
`results[]` correlates by `op_id`.

`execute_request_readonly` is the same thing on the read-only path. One
state-changing operation fails the whole request; the reads beside it are not
served either, so a caller cannot mistake a half-served request for a served
one.

A malformed envelope is *answered*, not raised: the response carries the same
registered error code an engine would have returned, so a caller needs one
recovery path rather than two.

---

## Host Operations

Two things are deliberately **not** KIP commands, because letting a prompt reach
them would be the bug. They are host decisions, and the host is your Python
process:

```python
# Importing another Brain's cognition (§39, §41). Capsule bytes are not
# destination mutation authority: everything is re-validated against this
# Space's Schema Environment and re-authorized under its Governance.
report = await db.import_capsule(capsule, isolate=False)
report["identity_map"]  # source id -> destination id; a re-import is idempotent

# Putting Schema Packages in force (§20.10). The lock names *exactly* these
# packages, so include the baseline when you still want it.
env = await db.install_schema_packages([
	anda.COGNITIVE_MEMORY_PROFILE,
	my_own_package_json,
])
env["schema_environment_version"]  # what a client pins in `preconditions`
```

`isolate=True` imports into quarantine for review rather than into ordinary
recall state — cognition held out of use without claiming its author took it
back.

Export goes the other way and *is* a command, because reading is not the
dangerous direction: `EXPORT CAPSULE ?a WHERE { ... }` through `execute_kip` or
`execute_kip_readonly`.

---

## Known Limits

Reported honestly rather than approximated:

- **`execution.mode: "atomic"` is refused** with `UnsupportedCapability`. One
  transaction, one snapshot, read-your-writes and all-or-none commit are
  properties this runner cannot provide by running operations one at a time, and
  faking them would tell a caller their writes were atomic when they were not.
- **The binding runs as the system Principal.** That is the embedded case: one
  process, one owner, and the process *is* the owner. It is a real
  authorization through the same Governance path, not a bypass — a Space whose
  policy denies something denies it here too. A host serving more than one
  caller must authenticate them itself and is not served by this binding.
- **Only the default MemorySpace exists.** `space` in the envelope is honoured
  and a named Space must already exist; this binding has no API to create
  another one.
- The engine's own gaps — semantic `SEARCH`, Capsule signatures, the `restore`
  import mode, `DESCRIBE TRUST` — are reported by `DESCRIBE CAPABILITIES` as
  structured data rather than discovered by triggering an error.
