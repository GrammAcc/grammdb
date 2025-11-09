from test.helpers import does_not_raise

import sqlalchemy as sa

import grammdb


async def test_insert(fixt_schema, fixt_db):
    async with grammdb.connection_ctx(fixt_db()) as conn:
        sut = grammdb.insert(into=fixt_schema.Table1, col="test_value")
        await conn.execute(sut)
        with does_not_raise():
            res = (await conn.execute(sa.select(fixt_schema.Table1))).one()
        assert res.col == "test_value"


async def test_insert_many(fixt_schema, fixt_db):
    async with grammdb.connection_ctx(fixt_db()) as conn:
        sut = grammdb.insert_many(
            into=fixt_schema.Table1, values=[{"col": "test_value1"}, {"col": "test_value2"}]
        )
        await conn.execute(sut)
        with does_not_raise():
            res = (await conn.execute(sa.select(fixt_schema.Table1))).all()
        assert res[0].col == "test_value1"
        assert res[1].col == "test_value2"


async def test_select_star(fixt_schema, fixt_db, fixt_from):
    async with grammdb.connection_ctx(fixt_db()) as conn:
        await conn.execute(sa.insert(fixt_schema.Table1).values(col="test_value"))
        sut = grammdb.select("*", fixt_from.table1())
        with does_not_raise():
            res = (await conn.execute(sut)).one()
        assert res.col == "test_value"


async def test_select_column(fixt_schema, fixt_db, fixt_from):
    async with grammdb.connection_ctx(fixt_db()) as conn:
        await conn.execute(sa.insert(fixt_schema.Table1).values(col="test_value"))
        sut = grammdb.select("col", fixt_from.table1())
        with does_not_raise():
            res = (await conn.execute(sut)).scalar_one()
        assert res == "test_value"


async def test_select_where(fixt_schema, fixt_db, fixt_from, fixt_where):
    async with grammdb.connection_ctx(fixt_db()) as conn:
        await conn.execute(sa.insert(fixt_schema.Table1).values(col="test_value1"))
        await conn.execute(sa.insert(fixt_schema.Table1).values(col="test_value2"))
        sut = grammdb.select("col", fixt_from.table1(), fixt_where.col_is("test_value2"))
        with does_not_raise():
            res = (await conn.execute(sut)).scalar_one()
        assert res == "test_value2"


async def test_update(fixt_schema, fixt_db, fixt_where):
    async with grammdb.connection_ctx(fixt_db()) as conn:
        await conn.execute(sa.insert(fixt_schema.Table1).values(col="test_value"))
        sut = grammdb.update(
            fixt_schema.Table1, fixt_where.col_is("test_value"), col="test_value_updated"
        )
        await conn.execute(sut)
        res = (await conn.execute(sa.select(fixt_schema.Table1))).one()
        assert res.col == "test_value_updated"


async def test_delete(fixt_schema, fixt_db, fixt_where):
    async with grammdb.connection_ctx(fixt_db()) as conn:
        await conn.execute(sa.insert(fixt_schema.Table1).values(col="test_value1"))
        await conn.execute(sa.insert(fixt_schema.Table1).values(col="test_value2"))
        sut = grammdb.delete(fixt_schema.Table1, fixt_where.col_is("test_value1"))
        await conn.execute(sut)
        res = (await conn.execute(sa.select(fixt_schema.Table1))).all()
        assert len(res) == 1
        assert res[0].col == "test_value2"
