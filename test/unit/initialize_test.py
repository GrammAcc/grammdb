from test.helpers import does_not_raise

import grammdb


async def test_initialize_adds_timestamps(fixt_schema, fixt_conn_string):
    database = grammdb.db_factory(fixt_schema)
    await grammdb.init_db(database(), fixt_conn_string)
    for table in fixt_schema.get_metadata().tables.values():
        assert "created_at" in table.c
        assert "updated_at" in table.c


async def test_no_engine_error_after_initialize(fixt_schema, fixt_conn_string):
    database = grammdb.db_factory(fixt_schema)
    await grammdb.init_db(database(), fixt_conn_string)
    with does_not_raise():
        database().get_engine()


async def test_initialize_can_be_called_multiple_times(fixt_schema, fixt_conn_string):
    database = grammdb.db_factory(fixt_schema)
    with does_not_raise():
        await grammdb.init_db(database(), fixt_conn_string)
        await grammdb.init_db(database(), fixt_conn_string)
        await grammdb.init_db(database(), fixt_conn_string)


async def test_initialize_passes_on_engine_options(fixt_schema, fixt_conn_string):
    database = grammdb.db_factory(fixt_schema)
    await grammdb.init_db(database(), fixt_conn_string)
    assert database().get_engine().pool._max_overflow == 10
    await grammdb.init_db(database(), fixt_conn_string, max_overflow=0)
    assert database().get_engine().pool._max_overflow == 0


async def test_initialize_can_override_default_engine_options(fixt_schema, fixt_conn_string):
    database = grammdb.db_factory(fixt_schema)
    await grammdb.init_db(database(), fixt_conn_string)
    assert database().get_engine().pool.size() == 10
    await grammdb.init_db(database(), fixt_conn_string, pool_size=20)
    assert database().get_engine().pool.size() == 20
