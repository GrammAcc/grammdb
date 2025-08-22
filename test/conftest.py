import os
from types import SimpleNamespace

import pytest
import sqlalchemy as sa

import grammdb

sqlite_connection_string = "sqlite+aiosqlite://"
postgres_connection_string = "postgresql+asyncpg://postgres:postgres@localhost:5432"

exclude_postgres = os.environ.get("NO_DOCKER", False)

conn_strings = (
    [sqlite_connection_string]
    if exclude_postgres
    else [sqlite_connection_string, postgres_connection_string]
)
conn_string_ids = ["sqlite"] if exclude_postgres else ["sqlite", "postgres"]


@pytest.fixture(params=conn_strings, ids=conn_string_ids)
async def fixt_conn_string(request):
    return request.param


class MockSchema:
    _metadata = sa.MetaData()

    def get_metadata(self):
        return self._metadata

    Table1 = sa.Table(
        "mock_table",
        _metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("col", sa.Text),
    )
    Table2 = sa.Table(
        "mock_table2",
        _metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("col", sa.Text),
    )


@pytest.fixture
async def fixt_schema():
    return MockSchema()


@pytest.fixture
async def fixt_from(fixt_schema):
    def from_func():
        return sa.select(fixt_schema.Table1)

    return SimpleNamespace(table1=from_func)


@pytest.fixture
async def fixt_where(fixt_schema):
    def where_func(col_val):
        def _(selectable):
            return selectable.where(fixt_schema.Table1.c.col == col_val)

        return _

    return SimpleNamespace(col_is=where_func)


@pytest.fixture
async def fixt_sqlite_db(fixt_schema):
    database = grammdb.db_factory(fixt_schema)
    await grammdb.init_db(database(), sqlite_connection_string, drop_tables=True)
    return database


@pytest.fixture
async def fixt_postgres_db(fixt_schema):
    database = grammdb.db_factory(fixt_schema)
    try:
        await grammdb.init_db(database(), postgres_connection_string, drop_tables=True)
        return database
    except OSError:
        assert (
            False
        ), "Could not connect to postgres database. Did you forget to run docker compose up?"


@pytest.fixture
async def fixt_db(fixt_schema, fixt_conn_string):
    database = grammdb.db_factory(fixt_schema)
    try:
        await grammdb.init_db(database(), fixt_conn_string, drop_tables=True)
        return database
    except OSError:
        assert (
            False
        ), "Could not connect to postgres database. Did you forget to run docker compose up?"
