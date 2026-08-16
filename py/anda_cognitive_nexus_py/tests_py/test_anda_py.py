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
