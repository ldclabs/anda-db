import json
import pytest
import asyncio
from anda_cognitive_nexus_py import PyCommandType, PyAndaDB, StoreLocationType, AndaDbConfig

@pytest.mark.asyncio
async def test_create_success():
    # db_config = {
    #     "store_location_type": StoreLocationType.InMem,
    #     "store_location": "",
    #     "db_name": "test_db",
    #     "db_desc": "Test DB",
    #     "meta_cache_capacity": 10000
    # }
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
    command = "FIND(?x) WHERE { ?x {type: 'TestType'} }"
    result = await db.execute_kip(command)
    assert isinstance(result, dict)
    assert "type" in result
    assert "response" in result
    # type should be PyCommandType
    assert type(result["type"]).__name__ == "PyCommandType"
    # response should be a dict or list (depending on command)
    assert isinstance(result["response"], dict)

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
    bad_command = "INVALID_COMMAND"
    result = await db.execute_kip(bad_command)
    assert isinstance(result, dict)
    assert "type" in result
    assert "response" in result
    # type should be PyCommandType
    assert type(result["type"]).__name__ == "PyCommandType"
    # response should contain error info (implementation dependent)
    assert isinstance(result["response"], dict)
    # Optionally inspect for error keys/messages
    if isinstance(result["response"], dict):
        assert result["type"] == PyCommandType.Unknown
        assert len(result["response"]) and "error" in result["response"]

@pytest.mark.asyncio
async def test_execute_kip_invalid_parameters_error_message():
    """
    Test that execute_kip returns a user-friendly error message for invalid command/parameters.
    """
    db_config = AndaDbConfig(
        StoreLocationType.InMem,
        "",
        "test_db_error_msg",
        "desc",
        10000
    )
    db = await PyAndaDB.create(db_config)
    # Pass a syntactically invalid command
    bad_command = "FIND( WHERE { ?x {type: 'TestType'} }"  # missing closing parenthesis
    result = await db.execute_kip(bad_command)
    assert isinstance(result, dict)
    assert "type" in result
    assert "response" in result
    # Should indicate error in type or response
    assert result["type"] == PyCommandType.Unknown or result["type"].__class__.__name__ == "PyCommandType"
    assert isinstance(result["response"], dict)
    # Check for user-friendly error message
    assert any(
        k in result["response"] for k in ("error", "message", "detail")
    ), f"No error key found in response: {result['response']}"
    # Optionally, check that the error message is not empty and is user-friendly
    error_msg = result["response"].get("error") or result["response"].get("message") or result["response"].get("detail")
    assert error_msg and isinstance(error_msg, dict)
    error_str = json.dumps(error_msg)
    assert "error" in error_str.lower()

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
        command = f"FIND(?x) WHERE {{ ?x {{type: 'TestType{idx}'}} }}"
        result = await db.execute_kip(command)
        assert isinstance(result, dict)
        assert "type" in result
        assert "response" in result
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

