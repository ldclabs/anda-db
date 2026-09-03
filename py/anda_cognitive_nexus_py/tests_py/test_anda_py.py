"""Python-level tests for the KIP 2.0 binding.

`execute_kip` returns `{"type": PyCommandType, "response": <envelope>}`, where
the envelope is the KIP 2.0 response (§81): a `status`, one `results[]` entry
per operation, and — for an envelope-level failure — a request-level `error`.
An ordinary command failure reports on its own result, so tests that want the
error look at `results[0]["error"]`.
"""

import pytest
from anda_cognitive_nexus_py import PyCommandType, PyAndaDB, StoreLocationType, AndaDbConfig


def operation_error(response):
    """The error of a single-operation response, wherever it was reported."""
    if response.get("error"):
        return response["error"]
    results = response.get("results") or []
    return results[0].get("error") if results else None


@pytest.mark.asyncio
async def test_create_success():
    db_config = AndaDbConfig(
      StoreLocationType.InMem,
      "",
      "test_db"
    )
    db = await PyAndaDB.create(db_config)
    assert isinstance(db, PyAndaDB)

@pytest.mark.asyncio
async def test_create_invalid_config():
    db_config = AndaDbConfig(
        StoreLocationType.LocalFile,
        "",  # Invalid: required for Local_file
        "bad_db"
    )
    with pytest.raises(RuntimeError):
        await PyAndaDB.create(db_config)

@pytest.mark.asyncio
async def test_execute_kip_success():
    db_config = AndaDbConfig(
      StoreLocationType.InMem,
      "",
      "test_db",
      "Test_DB",
      10000
    )
    db = await PyAndaDB.create(db_config)
    command = 'FIND(?x) WHERE { ?x CONCEPT {type: "Person"} }'
    result = await db.execute_kip(command)
    assert isinstance(result, dict)
    assert type(result["type"]).__name__ == "PyCommandType"
    assert result["type"] == PyCommandType.Kql
    response = result["response"]
    assert response["kip"] == "2.0"
    assert response["status"] == "succeeded"
    # Nothing has been written, and an empty memory answers with an empty
    # result — not with an error.
    assert response["results"][0]["result"] == []

@pytest.mark.asyncio
async def test_execute_kip_invalid_command():
    db_config = AndaDbConfig(
      StoreLocationType.InMem,
      "",
      "test_db",
      "Test_DB",
      10000
    )
    db = await PyAndaDB.create(db_config)
    result = await db.execute_kip("INVALID_COMMAND")
    assert result["type"] == PyCommandType.Unknown
    response = result["response"]
    assert response["status"] == "failed"
    error = operation_error(response)
    assert error["code"] == "InvalidSyntax"
    # The hint is the agent-facing recovery instruction; without it a model
    # cannot correct itself from the error alone.
    assert error["hint"]
    assert error["retry"]["class"] == "requires_different_input"

@pytest.mark.asyncio
async def test_execute_kip_invalid_parameters_error_message():
    """A syntactically invalid command fails with a registered code, not a
    stringified panic."""
    db_config = AndaDbConfig(
        StoreLocationType.InMem,
        "",
        "test_db_error_msg",
        "desc",
        10000
    )
    db = await PyAndaDB.create(db_config)
    bad_command = 'FIND( WHERE { ?x CONCEPT {type: "Person"} }'  # missing ')'
    result = await db.execute_kip(bad_command)
    assert result["type"] == PyCommandType.Unknown
    error = operation_error(result["response"])
    assert error is not None, result["response"]
    assert error["code"] == "InvalidSyntax"
    assert isinstance(error["message"], str) and error["message"]

def test_andadbconfig_type_validation():
    # db_name should be a string, not an int
    with pytest.raises(TypeError):
        AndaDbConfig(StoreLocationType.InMem, '', 123)
    # store_location_type should be a StoreLocationType, not a string
    with pytest.raises(TypeError):
        AndaDbConfig("in_mem", '', 'test_db')
    # meta_cache_capacity should be an int or None, not a string
    with pytest.raises(TypeError):
        AndaDbConfig(StoreLocationType.InMem, '', 'test_db', 'desc', "not_an_int")
    # db_desc should be a string or None, not a list
    with pytest.raises(TypeError):
        AndaDbConfig(StoreLocationType.InMem, '', 'test_db', ["not", "a", "string"])

@pytest.mark.asyncio
async def test_execute_kip_non_json_parameters_raise_value_error():
    """
    Parameters that have no JSON equivalent must raise ValueError —
    never panic/abort the interpreter.
    """
    db_config = AndaDbConfig(StoreLocationType.InMem, "", "test_db_bad_params")
    db = await PyAndaDB.create(db_config)
    command = 'FIND(?x) WHERE { ?x CONCEPT {type: :t} }'
    # unsupported value type
    with pytest.raises(ValueError):
        await db.execute_kip(command, parameters={"t": object()})
    # non-finite float
    with pytest.raises(ValueError):
        await db.execute_kip(command, parameters={"t": float("nan")})
    # non-string key
    with pytest.raises(ValueError):
        await db.execute_kip(command, parameters={1: "x"})

@pytest.mark.asyncio
async def test_execute_kip_nested_parameters():
    """Nested JSON-compatible parameters (lists, tuples, dicts) are accepted,
    and reach the graph as data rather than as command text."""
    db_config = AndaDbConfig(StoreLocationType.InMem, "", "test_db_nested_params")
    db = await PyAndaDB.create(db_config)
    written = await db.execute_kip(
        'CREATE CONCEPT ?c { TYPE "Person" NAME :who '
        "SET ATTRIBUTES { aliases: :aliases, description: :description } }",
        parameters={
            "who": "Alice",
            "aliases": ["Ally", "Al"],
            "description": "written through bound parameters",
        },
    )
    assert written["response"]["status"] == "succeeded", written["response"]
    assert written["type"] == PyCommandType.Kml

    found = await db.execute_kip(
        'FIND(?c.name, ?c.attributes.aliases) WHERE { ?c CONCEPT {type: "Person", name: :who} }',
        parameters={"who": "Alice"},
    )
    assert found["response"]["status"] == "succeeded", found["response"]
    assert found["response"]["results"][0]["result"] == [["Alice", ["Ally", "Al"]]]

@pytest.mark.asyncio
async def test_dry_run_commits_nothing():
    """A dry run validates without establishing a durable commit (§69.3)."""
    db_config = AndaDbConfig(StoreLocationType.InMem, "", "test_db_dry_run")
    db = await PyAndaDB.create(db_config)
    validated = await db.execute_kip(
        'CREATE CONCEPT ?c { TYPE "Person" NAME "Ghost" }', dry_run=True
    )
    assert validated["response"]["status"] == "succeeded", validated["response"]

    found = await db.execute_kip(
        'FIND(?c.name) WHERE { ?c CONCEPT {type: "Person", name: "Ghost"} }'
    )
    assert found["response"]["results"][0]["result"] == []

@pytest.mark.asyncio
async def test_close_is_idempotent():
    db_config = AndaDbConfig(StoreLocationType.InMem, "", "test_db_close")
    db = await PyAndaDB.create(db_config)
    result = await db.execute_kip("DESCRIBE PRIMER")
    assert result["type"] == PyCommandType.Meta
    assert result["response"]["status"] == "succeeded"
    assert await db.close() is None
    # second close is a no-op
    assert await db.close() is None

@pytest.mark.asyncio
async def test_pyandadb_thread_safety_and_async():
    """
    Test that PyAndaDB can be used safely from multiple async tasks (and threads, if supported).
    This test launches several concurrent create/execute_kip operations and checks for correct results and no panics.
    """
    import concurrent.futures
    import asyncio

    async def create_and_query(idx):
        db_config = AndaDbConfig(
            StoreLocationType.InMem,
            "",
            f"test_db_{idx}",
            f"desc_{idx}",
            10000
        )
        db = await PyAndaDB.create(db_config)
        command = f'FIND(?x) WHERE {{ ?x CONCEPT {{name: "person-{idx}"}} }}'
        result = await db.execute_kip(command)
        assert result["response"]["status"] == "succeeded", result["response"]
        return result

    # Run several tasks concurrently in asyncio
    results = await asyncio.gather(*(create_and_query(i) for i in range(5)))
    assert len(results) == 5
    for res in results:
        assert isinstance(res, dict)
        assert "type" in res
        assert "response" in res

    # Optionally, test thread safety by running in a ThreadPoolExecutor
    def thread_entry(idx):
        return asyncio.run(create_and_query(idx))

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        thread_results = list(executor.map(thread_entry, range(3)))
    assert len(thread_results) == 3
    for res in thread_results:
        assert isinstance(res, dict)
        assert "type" in res
        assert "response" in res

# ---------------------------------------------------------------------------
# The read-only path (§76)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_execute_kip_readonly_refuses_a_write():
    """A write sent to the read-only path is refused, and does not happen.

    The refusal is decided on what the command parses as, never on a label a
    caller attached to it (§73.1, §88.3).
    """
    db = await PyAndaDB.create(AndaDbConfig(StoreLocationType.InMem, "", "test_db_readonly"))

    refused = await db.execute_kip_readonly(
        'CREATE CONCEPT ?c { TYPE "Person" NAME "Mallory" }'
    )
    # Classified as the write it is, then refused for being one.
    assert refused["type"] == PyCommandType.Kml
    assert refused["response"]["status"] == "failed"
    assert operation_error(refused["response"])["code"] == "ReadonlyViolation"

    # The state-capable path finds nothing, so nothing was committed.
    found = await db.execute_kip(
        'FIND(?c.name) WHERE { ?c CONCEPT {type: "Person", name: "Mallory"} }'
    )
    assert found["response"]["results"][0]["result"] == []


@pytest.mark.asyncio
async def test_execute_kip_readonly_serves_reads_with_bound_parameters():
    db = await PyAndaDB.create(AndaDbConfig(StoreLocationType.InMem, "", "test_db_readonly_ok"))
    written = await db.execute_kip(
        'CREATE CONCEPT ?c { TYPE "Person" NAME :who }', parameters={"who": "Alice"}
    )
    assert written["response"]["status"] == "succeeded", written["response"]

    found = await db.execute_kip_readonly(
        'FIND(?c.name) WHERE { ?c CONCEPT {type: "Person", name: :who} }',
        parameters={"who": "Alice"},
    )
    assert found["type"] == PyCommandType.Kql
    assert found["response"]["results"][0]["result"] == ["Alice"]

    primer = await db.execute_kip_readonly("DESCRIBE PRIMER")
    assert primer["type"] == PyCommandType.Meta
    assert primer["response"]["status"] == "succeeded"


# ---------------------------------------------------------------------------
# The request envelope (§71, §75, §81)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_execute_request_carries_space_ingest_and_a_sequence():
    """The whole envelope reaches the engine.

    A named MemorySpace (§5.5, never inferred), several operations under a
    declared execution mode (§75), and Evidence minted from the transport
    envelope rather than re-typed inside KML text (§71.1, §88.12).
    """
    db = await PyAndaDB.create(AndaDbConfig(StoreLocationType.InMem, "", "test_db_envelope"))

    response = await db.execute_request({
        "kip": "2.0",
        "request_id": "req-1",
        "space": {"id": "kip:space:default"},
        "execution": {"mode": "sequence", "on_error": "stop"},
        "ingest": {
            "evidence": [{
                "key": "msg",
                "evidence_class": "user_statement",
                "payload": "I prefer dark mode.",
                "media_type": "text/plain",
                "observed_at": "2026-08-14T01:00:00Z",
            }]
        },
        "operations": [
            {
                "op_id": "write",
                "command": """
                    MUTATE {
                        CREATE CONCEPT ?alice { TYPE "Person" NAME :who }
                        CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark mode" }
                        ASSERT ?a (?alice, "prefers", ?dark) {
                            by: ?alice, mode: "stated", confidence: 0.9, evidence: :msg
                        }
                    }
                """,
            },
            {
                "op_id": "read",
                "command": 'FIND(?c.name) WHERE { ?c CONCEPT {type: "Person", name: :who} }',
            },
        ],
        "parameters": {"who": "Alice"},
    })

    # The bare response envelope, with no "type" beside it: a request whose
    # operations are a read and a write has no single language.
    assert response["kip"] == "2.0"
    assert response["request_id"] == "req-1"
    assert response["status"] == "succeeded", response
    assert [r["op_id"] for r in response["results"]] == ["write", "read"]
    # `sequence`: the later operation observed the earlier one's commit.
    assert response["results"][1]["result"] == ["Alice"]
    # Each state-changing operation gets its own Receipt; the top-level slot is
    # reserved for `atomic` (§75.2).
    assert response["results"][0]["receipt"]["tx_id"]


@pytest.mark.asyncio
async def test_execute_request_readonly_refuses_a_batch_containing_a_write():
    db = await PyAndaDB.create(
        AndaDbConfig(StoreLocationType.InMem, "", "test_db_envelope_readonly")
    )

    response = await db.execute_request_readonly({
        "kip": "2.0",
        "execution": {"mode": "independent"},
        "operations": [
            {"op_id": "read", "command": "DESCRIBE PRIMER"},
            # Labelled a read; it is a write, and the parse is what decides.
            {
                "op_id": "write",
                "language": "KML",
                "command": 'CREATE CONCEPT ?c { TYPE "Person" NAME "Mallory" }',
            },
        ],
    })

    assert response["error"]["code"] == "ReadonlyViolation", response
    # The read beside the write was not served either, so a caller cannot
    # mistake a half-served request for a served one.
    assert all(r.get("op_id") is None for r in response["results"])


@pytest.mark.asyncio
async def test_execute_request_answers_a_malformed_envelope():
    """A protocol question gets a protocol answer, not an exception."""
    db = await PyAndaDB.create(AndaDbConfig(StoreLocationType.InMem, "", "test_db_bad_envelope"))

    for envelope, code in [
        # Not this protocol version.
        ({"kip": "1.0", "operations": [{"command": "DESCRIBE PRIMER"}]},
         "UnsupportedProtocolVersion"),
        # A member no KIP 2.0 envelope has. Accepted silently, it would read as
        # a setting that took effect.
        ({"kip": "2.0", "operations": [{"command": "DESCRIBE PRIMER"}], "readonly": True},
         "InvalidRequestEnvelope"),
        # Several operations and no declared mode: whether an earlier commit
        # survives a later failure is not an engine default (§75.4).
        ({"kip": "2.0", "operations": [
            {"command": "DESCRIBE PRIMER"}, {"command": "DESCRIBE PROTOCOL"}]},
         "InvalidRequestEnvelope"),
    ]:
        response = await db.execute_request(envelope)
        assert response["status"] == "failed", envelope
        assert response["error"]["code"] == code, response


@pytest.mark.asyncio
async def test_atomic_execution_is_refused_rather_than_approximated():
    """§75.4: a batch is not a transaction, and will not be presented as one."""
    db = await PyAndaDB.create(AndaDbConfig(StoreLocationType.InMem, "", "test_db_atomic"))

    response = await db.execute_request({
        "kip": "2.0",
        "execution": {"mode": "atomic"},
        "operations": [
            {"command": 'CREATE CONCEPT ?a { TYPE "Person" NAME "A" }'},
            {"command": 'CREATE CONCEPT ?b { TYPE "Person" NAME "B" }'},
        ],
    })
    assert response["error"]["code"] == "UnsupportedCapability", response


# ---------------------------------------------------------------------------
# Host operations: Capsules (§37-§41) and Schema Packages (§20)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_import_capsule_carries_cognition_into_another_nexus():
    """Export is a META command; import is a host decision, not a KIP one."""
    source = await PyAndaDB.create(AndaDbConfig(StoreLocationType.InMem, "", "test_db_capsule_src"))
    written = await source.execute_kip("""
        MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark mode" }
            ASSERT ?a (?alice, "prefers", ?dark) {
                by: ?alice, mode: "stated", confidence: 0.9
            }
        }
    """)
    assert written["response"]["status"] == "succeeded", written["response"]

    exported = await source.execute_kip('EXPORT CAPSULE ?a WHERE { ?a ASSERTION {} }')
    assert exported["response"]["status"] == "succeeded", exported["response"]
    capsule = exported["response"]["results"][0]["result"]

    destination = await PyAndaDB.create(
        AndaDbConfig(StoreLocationType.InMem, "", "test_db_capsule_dst")
    )
    report = await destination.import_capsule(capsule)
    assert report["imported"] is True
    # The source-to-destination identity map is what makes a re-import
    # idempotent rather than a second copy.
    assert report["identity_map"]

    found = await destination.execute_kip(
        'FIND(?c.name, ?a.confidence) WHERE {'
        '  ?c CONCEPT {type: "Person"}'
        '  ?p PROPOSITION (?c, "prefers", ?pref)'
        '  ?a ASSERTION {proposition: ?p}'
        '}'
    )
    assert found["response"]["results"][0]["result"] == [["Alice", 0.9]]


@pytest.mark.asyncio
async def test_import_capsule_rejects_something_that_is_not_a_capsule():
    db = await PyAndaDB.create(AndaDbConfig(StoreLocationType.InMem, "", "test_db_bad_capsule"))
    with pytest.raises(RuntimeError):
        await db.import_capsule({"not": "a capsule"})


@pytest.mark.asyncio
async def test_install_schema_packages_puts_exactly_that_lock_in_force():
    """§20.9: the Schema Lock names exactly the packages given."""
    import anda_cognitive_nexus_py as anda

    db = await PyAndaDB.create(AndaDbConfig(StoreLocationType.InMem, "", "test_db_schema"))

    # Re-activating the lock already in force must not mint a new Schema
    # Environment version: that would invalidate every client pinning the old
    # one for no change at all (§20.8, §35.4).
    first = await db.install_schema_packages([anda.COGNITIVE_MEMORY_PROFILE])
    second = await db.install_schema_packages([anda.COGNITIVE_MEMORY_PROFILE])
    assert first["schema_environment_version"] == second["schema_environment_version"]
    assert "kip://profiles/cognitive-memory" in second["lock"]["packages"]

    # Activating nothing leaves the Core Package alone in force, and Core
    # declares no Concept types at all.
    await db.install_schema_packages([])
    refused = await db.execute_kip('CREATE CONCEPT ?c { TYPE "Person" NAME "Nobody" }')
    assert refused["response"]["status"] == "failed", refused["response"]


@pytest.mark.asyncio
async def test_a_space_can_be_created_without_the_bundled_profile():
    """`schema_packages=[]` is a legitimate choice, and an unusable ontology.

    It is what a host picks when it installs its own schema later; a Space with
    only the Core Package can hold Assertions about types it does not have, and
    cannot create a Concept.
    """
    db = await PyAndaDB.create(
        AndaDbConfig(StoreLocationType.InMem, "", "test_db_no_profile", None, None, [])
    )
    refused = await db.execute_kip('CREATE CONCEPT ?c { TYPE "Person" NAME "Nobody" }')
    assert refused["response"]["status"] == "failed", refused["response"]


@pytest.mark.asyncio
async def test_import_capsule_can_quarantine_instead_of_recalling():
    """`isolate=True` imports for review, not into ordinary recall (§39.2).

    Quarantine holds cognition out of use without claiming its author took it
    back, so a flag that failed to route would put unreviewed cognition where a
    host meant it to be held.
    """
    source = await PyAndaDB.create(
        AndaDbConfig(StoreLocationType.InMem, "", "test_db_capsule_iso_src")
    )
    await source.execute_kip("""
        MUTATE {
            CREATE CONCEPT ?alice { TYPE "Person" NAME "Alice" }
            CREATE CONCEPT ?dark { TYPE "Preference" NAME "Dark mode" }
            ASSERT ?a (?alice, "prefers", ?dark) {
                by: ?alice, mode: "stated", confidence: 0.9
            }
        }
    """)
    exported = await source.execute_kip('EXPORT CAPSULE ?a WHERE { ?a ASSERTION {} }')
    capsule = exported["response"]["results"][0]["result"]

    recalled = await PyAndaDB.create(
        AndaDbConfig(StoreLocationType.InMem, "", "test_db_capsule_recall")
    )
    quarantined = await PyAndaDB.create(
        AndaDbConfig(StoreLocationType.InMem, "", "test_db_capsule_quarantine")
    )
    await recalled.import_capsule(capsule, isolate=False)
    await quarantined.import_capsule(capsule, isolate=True)

    read = 'FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} }'
    # The ordinary import is in recall; the isolated one is held out of it, so
    # the two destinations must not answer the same read the same way.
    assert (await recalled.execute_kip(read))["response"]["results"][0]["result"] == ["Alice"]
    assert (await quarantined.execute_kip(read))["response"]["results"][0]["result"] == []


@pytest.mark.asyncio
async def test_an_idempotency_key_reaches_the_engine():
    """§34: the same key on the same work replays instead of writing twice."""
    db = await PyAndaDB.create(AndaDbConfig(StoreLocationType.InMem, "", "test_db_idempotency"))

    def write():
        return {
            "kip": "2.0",
            "execution": {"mode": "sequence", "idempotency_key": "onboarding:alice"},
            "operations": [
                {"command": 'CREATE CONCEPT ?c { TYPE "Person" NAME "Alice" }'},
            ],
        }

    first = await db.execute_request(write())
    assert first["status"] == "succeeded", first
    # Echoed so a client holding `outcome_unknown` can recover by key (§81).
    assert first["execution"]["idempotency_key"] == "onboarding:alice"

    second = await db.execute_request(write())
    assert second["status"] == "succeeded", second

    # One Concept, not two: the second request replayed the first transaction
    # rather than doing the work again.
    found = await db.execute_kip('FIND(?c.name) WHERE { ?c CONCEPT {type: "Person"} }')
    assert found["response"]["results"][0]["result"] == ["Alice"]
    assert (
        first["results"][0]["receipt"]["tx_id"] == second["results"][0]["receipt"]["tx_id"]
    ), (first["results"][0]["receipt"], second["results"][0]["receipt"])
