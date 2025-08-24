import pytest
import sqlalchemy as sa

import grammdb


class MockSchema:
    _metadata = sa.MetaData()

    def get_metadata(self):
        return self._metadata

    Lone = sa.Table(
        "lone",
        _metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("unique_col", sa.Text, unique=True),
        sa.Column("not_null_col", sa.Text, nullable=False, default="not_null_value"),
        sa.Column("check_col", sa.Text, sa.CheckConstraint("check_col <> 'invalid'")),
    )
    Primary = sa.Table(
        "primary",
        _metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("related_id", sa.Integer, sa.ForeignKey("related.id")),
    )
    Related = sa.Table(
        "related",
        _metadata,
        sa.Column("id", sa.Integer, primary_key=True),
    )


async def test_unique_constraint_error(fixt_conn_string):
    schema = MockSchema()
    database = grammdb.db_factory(schema)
    await grammdb.init_db(database(), fixt_conn_string, drop_tables=True)
    vals = {"id": 1, "unique_col": "unique_value", "check_col": "valid"}
    vals2 = {"id": 2, "unique_col": "unique_value", "check_col": "valid"}
    async with grammdb.connection_ctx(database()) as conn:
        await conn.execute(grammdb.insert(into=schema.Lone, **vals))
        with pytest.raises(grammdb.exceptions.UniqueConstraintError):
            with grammdb.exceptions.constraint_error():
                await conn.execute(grammdb.insert(into=schema.Lone, **vals2))


async def test_check_constraint_error(fixt_conn_string):
    schema = MockSchema()
    database = grammdb.db_factory(schema)
    await grammdb.init_db(database(), fixt_conn_string, drop_tables=True)
    vals = {"id": 1, "unique_col": "unique_value", "check_col": "invalid"}
    async with grammdb.connection_ctx(database()) as conn:
        with pytest.raises(grammdb.exceptions.CheckConstraintError):
            with grammdb.exceptions.constraint_error():
                await conn.execute(grammdb.insert(into=schema.Lone, **vals))


async def test_foreign_key_constraint_error(fixt_conn_string):
    schema = MockSchema()
    database = grammdb.db_factory(schema)
    await grammdb.init_db(database(), fixt_conn_string, drop_tables=True)
    vals = {"id": 1, "related_id": 1000}
    async with grammdb.connection_ctx(database()) as conn:
        if fixt_conn_string.startswith("sqlite"):
            await conn.execute(sa.text("PRAGMA foreign_keys = ON;"))

        with pytest.raises(grammdb.exceptions.ForeignKeyConstraintError):
            with grammdb.exceptions.constraint_error():
                await conn.execute(grammdb.insert(into=schema.Primary, **vals))


async def test_not_null_constraint_error(fixt_conn_string):
    schema = MockSchema()
    database = grammdb.db_factory(schema)
    await grammdb.init_db(database(), fixt_conn_string, drop_tables=True)
    vals = {"id": 1, "not_null_col": None, "unique_col": "unique_value", "check_col": "valid"}
    async with grammdb.connection_ctx(database()) as conn:
        with pytest.raises(grammdb.exceptions.NotNullConstraintError):
            with grammdb.exceptions.constraint_error():
                await conn.execute(grammdb.insert(into=schema.Lone, **vals))
