import sqlalchemy as sa

import grammdb


async def test_rollback(fixt_schema, fixt_db):
    tr = await grammdb.start_transaction(fixt_db())
    select_stmt = grammdb.select("*", sa.select(fixt_schema.Table1))
    before_insert = (await tr.execute(select_stmt)).all()
    assert len(before_insert) == 0
    insert_stmt = grammdb.insert(into=fixt_schema.Table1)
    await tr.execute(insert_stmt)
    after_insert = (await tr.execute(select_stmt)).all()
    assert len(after_insert) == 1
    await grammdb.rollback_transaction(tr)
    tr2 = await grammdb.start_transaction(fixt_db())
    after_rollback = (await tr2.execute(select_stmt)).all()
    assert len(after_rollback) == 0
    await grammdb.rollback_transaction(tr2)


async def test_close(fixt_schema, fixt_db):
    tr = await grammdb.start_transaction(fixt_db())
    select_stmt = grammdb.select("*", sa.select(fixt_schema.Table1))
    before_insert = (await tr.execute(select_stmt)).all()
    assert len(before_insert) == 0
    insert_stmt = grammdb.insert(into=fixt_schema.Table1)
    await tr.execute(insert_stmt)
    after_insert = (await tr.execute(select_stmt)).all()
    assert len(after_insert) == 1
    await grammdb.close_transaction(tr)
    tr2 = await grammdb.start_transaction(fixt_db())
    after_close = (await tr2.execute(select_stmt)).all()
    assert len(after_close) == 0
    await grammdb.close_transaction(tr2)


async def test_commit(fixt_schema, fixt_db):
    tr = await grammdb.start_transaction(fixt_db())
    select_stmt = grammdb.select("*", sa.select(fixt_schema.Table1))
    before_insert = (await tr.execute(select_stmt)).all()
    assert len(before_insert) == 0
    insert_stmt = grammdb.insert(into=fixt_schema.Table1)
    await tr.execute(insert_stmt)
    after_insert = (await tr.execute(select_stmt)).all()
    assert len(after_insert) == 1
    await grammdb.commit_transaction(tr)
    tr2 = await grammdb.start_transaction(fixt_db())
    after_commit = (await tr2.execute(select_stmt)).all()
    assert len(after_commit) == 1
    await grammdb.commit_transaction(tr2)


async def test_rollback_ctx(fixt_schema, fixt_db):
    async with grammdb.connection_ctx(fixt_db()) as tr:
        select_stmt = grammdb.select("*", sa.select(fixt_schema.Table1))
        before_insert = (await tr.execute(select_stmt)).all()
        assert len(before_insert) == 0
        insert_stmt = grammdb.insert(into=fixt_schema.Table1)
        await tr.execute(insert_stmt)
        after_insert = (await tr.execute(select_stmt)).all()
        assert len(after_insert) == 1
        await grammdb.rollback_transaction(tr)
    async with grammdb.connection_ctx(fixt_db()) as tr:
        after_rollback = (await tr.execute(select_stmt)).all()
        assert len(after_rollback) == 0
        await grammdb.rollback_transaction(tr)


async def test_close_ctx(fixt_schema, fixt_db):
    async with grammdb.connection_ctx(fixt_db()) as tr:
        select_stmt = grammdb.select("*", sa.select(fixt_schema.Table1))
        before_insert = (await tr.execute(select_stmt)).all()
        assert len(before_insert) == 0
        insert_stmt = grammdb.insert(into=fixt_schema.Table1)
        await tr.execute(insert_stmt)
        after_insert = (await tr.execute(select_stmt)).all()
        assert len(after_insert) == 1
        await grammdb.close_transaction(tr)
    async with grammdb.connection_ctx(fixt_db()) as tr:
        after_close = (await tr.execute(select_stmt)).all()
        assert len(after_close) == 0
        await grammdb.close_transaction(tr)


async def test_commit_ctx(fixt_schema, fixt_db):
    async with grammdb.connection_ctx(fixt_db()) as tr:
        select_stmt = grammdb.select("*", sa.select(fixt_schema.Table1))
        before_insert = (await tr.execute(select_stmt)).all()
        assert len(before_insert) == 0
        insert_stmt = grammdb.insert(into=fixt_schema.Table1)
        await tr.execute(insert_stmt)
        after_insert = (await tr.execute(select_stmt)).all()
        assert len(after_insert) == 1
        await grammdb.commit_transaction(tr)
    async with grammdb.connection_ctx(fixt_db()) as tr:
        after_commit = (await tr.execute(select_stmt)).all()
        assert len(after_commit) == 1
        await grammdb.commit_transaction(tr)


async def test_ctx_auto_close(fixt_schema, fixt_db):
    async with grammdb.connection_ctx(fixt_db()) as tr:
        assert fixt_db().get_engine().pool.checkedout() == 1
        select_stmt = grammdb.select("*", sa.select(fixt_schema.Table1))
        before_insert = (await tr.execute(select_stmt)).all()
        assert len(before_insert) == 0
        insert_stmt = grammdb.insert(into=fixt_schema.Table1)
        await tr.execute(insert_stmt)
        after_insert = (await tr.execute(select_stmt)).all()
        assert len(after_insert) == 1
    async with grammdb.connection_ctx(fixt_db()) as tr:
        assert fixt_db().get_engine().pool.checkedout() == 1
        after_rollback = (await tr.execute(select_stmt)).all()
        assert len(after_rollback) == 0

    assert fixt_db().get_engine().pool.checkedout() == 0
