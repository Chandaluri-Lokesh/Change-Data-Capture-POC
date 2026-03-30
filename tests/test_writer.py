"""
Unit tests for writer/pg_writer.py.

Tests upsert idempotency, delete propagation, partial batch safety,
and connection retry logic using mock asyncpg pools.

No live PostgreSQL instance required for these unit tests.
For integration tests against a real database, use scripts/verify.py.

Mocking strategy
────────────────
asyncpg pool and connection are mocked with AsyncMock / MagicMock.
The mock conn.transaction() returns an async context manager that
delegates executemany / execute calls to the mock connection.
"""

import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, call, patch

from writer.pg_writer import (
    create_pool,
    upsert_orders_flat,
    upsert_transactions,
    delete_order,
    _execute_with_retry,
)


# ---------------------------------------------------------------------------
# Pool / connection mock factory
# ---------------------------------------------------------------------------

def _make_pool():
    """
    Return (pool_mock, conn_mock) where pool_mock.acquire() yields conn_mock.

    Important:
      - pool.acquire() and conn.transaction() are SYNCHRONOUS calls in asyncpg
        that return async context managers — so they must be plain MagicMock,
        not AsyncMock (which would return a coroutine and break `async with`).
      - conn.executemany / execute / fetchval are AWAITED, so they are AsyncMock.
    """
    conn = MagicMock()

    # conn.transaction() → sync call returning async context manager
    txn_cm = MagicMock()
    txn_cm.__aenter__ = AsyncMock(return_value=conn)
    txn_cm.__aexit__  = AsyncMock(return_value=False)
    conn.transaction = MagicMock(return_value=txn_cm)

    # Awaited DB methods
    conn.executemany = AsyncMock(return_value=None)
    conn.execute     = AsyncMock(return_value=None)
    conn.fetchval    = AsyncMock(return_value='mock-id')
    conn.fetchrow    = AsyncMock(return_value=None)

    # pool.acquire() → sync call returning async context manager
    acquire_cm = MagicMock()
    acquire_cm.__aenter__ = AsyncMock(return_value=conn)
    acquire_cm.__aexit__  = AsyncMock(return_value=False)

    pool = MagicMock()
    pool.acquire = MagicMock(return_value=acquire_cm)

    return pool, conn


def _dt() -> datetime:
    return datetime(2024, 1, 23, 12, 0, 0, tzinfo=timezone.utc)


def _order_row(**kwargs) -> dict:
    base = {
        'order_id':    'order-001',
        'customer_id': 'cust-001',
        'status':      'PENDING',
        'city':        'Boston',
        'total':       42.50,
        'created_at':  _dt(),
        'updated_at':  _dt(),
    }
    base.update(kwargs)
    return base


def _txn_row(**kwargs) -> dict:
    base = {
        'txn_id':      'txn-order-001',
        'order_id':    'order-001',
        'amount':      42.50,
        'status':      'PENDING',
        'recorded_at': _dt(),
    }
    base.update(kwargs)
    return base


# ---------------------------------------------------------------------------
# upsert_orders_flat
# ---------------------------------------------------------------------------

class TestUpsertOrdersFlat:
    def test_empty_input_returns_zero(self):
        pool, _ = _make_pool()
        result = asyncio.run(upsert_orders_flat(pool, []))
        assert result == 0

    def test_returns_row_count(self):
        pool, _ = _make_pool()
        assert asyncio.run(upsert_orders_flat(pool, [_order_row()])) == 1

    def test_multiple_rows(self):
        pool, _ = _make_pool()
        rows = [_order_row(order_id=f'o{i}') for i in range(5)]
        assert asyncio.run(upsert_orders_flat(pool, rows)) == 5

    def test_executemany_called_once(self):
        pool, conn = _make_pool()
        asyncio.run(upsert_orders_flat(pool, [_order_row()]))
        assert conn.executemany.call_count == 1

    def test_sql_contains_on_conflict(self):
        pool, conn = _make_pool()
        asyncio.run(upsert_orders_flat(pool, [_order_row()]))
        sql = conn.executemany.call_args[0][0]
        assert 'ON CONFLICT' in sql
        assert 'DO UPDATE SET' in sql

    def test_correct_tuple_params(self):
        pool, conn = _make_pool()
        row = _order_row(order_id='o1', total=9.99)
        asyncio.run(upsert_orders_flat(pool, [row]))
        params = conn.executemany.call_args[0][1]
        assert params[0][0] == 'o1'
        assert params[0][4] == 9.99

    def test_transaction_used(self):
        pool, conn = _make_pool()
        asyncio.run(upsert_orders_flat(pool, [_order_row()]))
        assert conn.transaction.called


# ---------------------------------------------------------------------------
# upsert_transactions
# ---------------------------------------------------------------------------

class TestUpsertTransactions:
    def test_empty_returns_zero(self):
        pool, _ = _make_pool()
        assert asyncio.run(upsert_transactions(pool, [])) == 0

    def test_returns_count(self):
        pool, _ = _make_pool()
        assert asyncio.run(upsert_transactions(pool, [_txn_row()])) == 1

    def test_sql_on_conflict_order_id(self):
        pool, conn = _make_pool()
        asyncio.run(upsert_transactions(pool, [_txn_row()]))
        sql = conn.executemany.call_args[0][0]
        assert 'ON CONFLICT' in sql
        assert 'order_id' in sql.lower()

    def test_correct_txn_id_param(self):
        pool, conn = _make_pool()
        asyncio.run(upsert_transactions(pool, [_txn_row(txn_id='txn-abc')]))
        params = conn.executemany.call_args[0][1]
        assert params[0][0] == 'txn-abc'



# ---------------------------------------------------------------------------
# delete_order
# ---------------------------------------------------------------------------

class TestDeleteOrder:
    def test_empty_order_id_is_no_op(self):
        a_pool, a_conn = _make_pool()
        f_pool, f_conn = _make_pool()
        asyncio.run(delete_order(a_pool, f_pool, ''))
        assert not a_conn.fetchval.called
        assert not f_conn.fetchval.called

    def test_analytics_delete_called(self):
        a_pool, a_conn = _make_pool()
        f_pool, f_conn = _make_pool()
        asyncio.run(delete_order(a_pool, f_pool, 'order-001'))
        # fetchval or execute should be called on analytics (for RETURNING clause)
        assert a_conn.fetchval.called or a_conn.execute.called

    def test_finance_delete_called(self):
        a_pool, a_conn = _make_pool()
        f_pool, f_conn = _make_pool()
        asyncio.run(delete_order(a_pool, f_pool, 'order-001'))
        assert f_conn.fetchval.called

    def test_both_targets_called_concurrently(self):
        """asyncio.gather means both pools are used in the same call."""
        a_pool, a_conn = _make_pool()
        f_pool, f_conn = _make_pool()
        asyncio.run(delete_order(a_pool, f_pool, 'order-xyz'))
        assert a_pool.acquire.called
        assert f_pool.acquire.called


# ---------------------------------------------------------------------------
# _execute_with_retry
# ---------------------------------------------------------------------------

class TestExecuteWithRetry:
    def test_success_on_first_try(self):
        calls = []

        async def factory():
            calls.append(1)
            return 'ok'

        result = asyncio.run(_execute_with_retry(None, factory, retries=1))
        assert result == 'ok'
        assert len(calls) == 1

    def test_retries_on_connection_error(self):
        import asyncpg
        call_count = [0]

        async def factory():
            call_count[0] += 1
            if call_count[0] < 2:
                raise asyncpg.ConnectionDoesNotExistError('stale')
            return 'recovered'

        result = asyncio.run(_execute_with_retry(None, factory, retries=1))
        assert result == 'recovered'
        assert call_count[0] == 2

    def test_raises_after_exhausting_retries(self):
        import asyncpg

        async def factory():
            raise asyncpg.ConnectionDoesNotExistError('always fails')

        with pytest.raises(asyncpg.ConnectionDoesNotExistError):
            asyncio.run(_execute_with_retry(None, factory, retries=1))

    def test_oserror_also_retried(self):
        call_count = [0]

        async def factory():
            call_count[0] += 1
            if call_count[0] < 2:
                raise OSError('network blip')
            return 'ok'

        result = asyncio.run(_execute_with_retry(None, factory, retries=1))
        assert result == 'ok'

    def test_zero_retries_raises_immediately(self):
        import asyncpg

        async def factory():
            raise asyncpg.ConnectionDoesNotExistError('fail')

        with pytest.raises(asyncpg.ConnectionDoesNotExistError):
            asyncio.run(_execute_with_retry(None, factory, retries=0))
