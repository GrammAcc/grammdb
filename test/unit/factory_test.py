import pytest

import grammdb


async def test_caching(fixt_schema):
    db1 = grammdb.db_factory(fixt_schema)
    db2 = grammdb.db_factory(fixt_schema)
    assert db1() is db1()
    assert db2() is db2()
    assert db1() is not db2()


async def test_interface_is_satisfied(fixt_schema):
    db1 = grammdb.db_factory(fixt_schema)
    assert hasattr(db1(), "get_schema")
    assert hasattr(db1(), "get_engine")
    assert hasattr(db1(), "set_engine")
    assert hasattr(db1(), "new_connection")


async def test_schema_is_set(fixt_schema):
    db1 = grammdb.db_factory(fixt_schema)
    assert db1().get_schema() is fixt_schema


async def test_uninitialized_engine_error(fixt_schema):
    db1 = grammdb.db_factory(fixt_schema)
    with pytest.raises(AssertionError, match="No engine found"):
        db1().get_engine()
