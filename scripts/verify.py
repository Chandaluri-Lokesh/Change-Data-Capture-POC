"""
End-to-end verification suite for the CDC pipeline.

Tests
─────
1. Count parity        — MongoDB orders count ≈ Postgres orders_flat count
2. Insert latency      — new order appears in analytics + finance within SLA (2 s)
3. Update propagation  — status change reflected in Postgres within MAX_WAIT_S
4. Delete propagation  — deleted order disappears from both Postgres targets
5. Duplicate safety    — same order re-delivered → only 1 row in Postgres (upsert idempotency)

Each test prints  ✓ [PASS] or  ✗ [FAIL] with details.
A final summary shows pass/fail counts.

Prerequisites
─────────────
- MongoDB, Kafka, Debezium connector, kafka_consumer.py must all be running.
- Run from the project root: python scripts/verify.py

Edge cases
──────────
- Count parity: allow up to PARITY_TOLERANCE in-flight events.
  Re-run after pausing the generator if the diff is larger.
- Latency probe: polls Postgres every POLL_INTERVAL_MS up to MAX_WAIT_S.
- Tests use isolated ObjectIds so they don't interfere with real data.
- Cleanup: test documents are deleted from MongoDB after each test.
"""

import asyncio
import asyncpg
import logging
import os
import sys
import time
from bson import ObjectId
from datetime import timezone
from pymongo import MongoClient
from dotenv import load_dotenv

_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
_src  = os.path.join(_root, 'src')
for p in (_root, _src):
    if p not in sys.path:
        sys.path.insert(0, p)

load_dotenv(dotenv_path=os.path.join(_root, '.env'))
logging.basicConfig(level=logging.WARNING)

MONGO_URI        = os.getenv('MONGO_URI', 'mongodb://localhost:27017/?replicaSet=rs0')
MAX_WAIT_S       = 10          # max seconds to wait for propagation
POLL_INTERVAL_S  = 0.1         # how often to poll Postgres (100 ms)
PARITY_TOLERANCE = 5           # allowed count difference (in-flight events)

RESULTS: list = []


def result(name: str, passed: bool, detail: str = '') -> None:
    RESULTS.append((name, passed))
    mark   = 'OK' if passed else 'FAIL'
    line   = f"  [{mark}] {name}"
    if detail:
        line += f": {detail}"
    print(line)


# ---------------------------------------------------------------------------
# Pool helpers
# ---------------------------------------------------------------------------

async def _analytics_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        host=os.getenv('POSTGRES_ANALYTICS_HOST', 'localhost'),
        port=int(os.getenv('POSTGRES_ANALYTICS_PORT', '5434')),
        user=os.getenv('POSTGRES_ANALYTICS_USER', 'postgres'),
        password=os.getenv('POSTGRES_ANALYTICS_PASSWORD', 'password'),
        database=os.getenv('POSTGRES_ANALYTICS_DB', 'analytics'),
        min_size=1, max_size=3,
    )


async def _finance_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        host=os.getenv('POSTGRES_FINANCE_HOST', 'localhost'),
        port=int(os.getenv('POSTGRES_FINANCE_PORT', '5433')),
        user=os.getenv('POSTGRES_FINANCE_USER', 'postgres'),
        password=os.getenv('POSTGRES_FINANCE_PASSWORD', 'password'),
        database=os.getenv('POSTGRES_FINANCE_DB', 'finance'),
        min_size=1, max_size=3,
    )


async def _poll_row(
    pool: asyncpg.Pool,
    table: str,
    key_col: str,
    key_val: str,
    predicate=None,
    timeout: float = MAX_WAIT_S,
) -> tuple[bool, int]:
    """
    Poll Postgres every POLL_INTERVAL_S until a matching row exists (or predicate holds).

    Returns:
        (found: bool, elapsed_ms: int)
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    f"SELECT * FROM {table} WHERE {key_col} = $1", key_val
                )
            if row and (predicate is None or predicate(row)):
                elapsed_ms = int((timeout - (deadline - time.monotonic())) * 1000)
                return True, elapsed_ms
        except Exception:
            pass
        await asyncio.sleep(POLL_INTERVAL_S)
    return False, int(timeout * 1000)


async def _row_gone(
    pool: asyncpg.Pool,
    table: str,
    key_col: str,
    key_val: str,
    timeout: float = MAX_WAIT_S,
) -> tuple[bool, int]:
    """
    Poll until the row is absent from the table.

    Returns:
        (gone: bool, elapsed_ms: int)
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    f"SELECT 1 FROM {table} WHERE {key_col} = $1", key_val
                )
            if row is None:
                elapsed_ms = int((timeout - (deadline - time.monotonic())) * 1000)
                return True, elapsed_ms
        except Exception:
            pass
        await asyncio.sleep(POLL_INTERVAL_S)
    return False, int(timeout * 1000)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

async def test_count_parity(mongo_db, a_pool: asyncpg.Pool) -> None:
    """MongoDB orders count should match Postgres orders_flat within tolerance."""
    mongo_count = mongo_db['orders'].count_documents({})
    async with a_pool.acquire() as conn:
        pg_count = await conn.fetchval("SELECT COUNT(*) FROM orders_flat")
    diff = abs(mongo_count - (pg_count or 0))
    result(
        'Count parity',
        diff <= PARITY_TOLERANCE,
        f"mongo={mongo_count} pg={pg_count} diff={diff} tolerance={PARITY_TOLERANCE}",
    )


async def test_insert_latency(
    mongo_db, a_pool: asyncpg.Pool, f_pool: asyncpg.Pool
) -> str:
    """Insert test order → measure propagation latency to both Postgres targets."""
    oid = ObjectId()
    order = {
        '_id':         oid,
        'customer_id': 'verify-latency-test',
        'status':      'PENDING',
        'city':        'VerifyCity',
        'total':       1.11,
    }
    mongo_db['orders'].insert_one(order)
    oid_str = str(oid)

    found_a, lat_a = await _poll_row(a_pool, 'orders_flat', 'order_id', oid_str)
    found_f, lat_f = await _poll_row(f_pool, 'transactions', 'order_id', oid_str)

    result('Insert -> analytics',    found_a, f"latency={lat_a} ms")
    result('Insert -> finance',      found_f, f"latency={lat_f} ms")
    result(
        'Latency SLA (< 2 000 ms)',
        lat_a < 2000 if found_a else False,
        f"{lat_a} ms vs 2 000 ms SLA",
    )
    return oid_str


async def test_update_propagation(
    mongo_db, a_pool: asyncpg.Pool, order_id: str
) -> None:
    """Update order status → verify Postgres reflects the new value."""
    new_status = 'SHIPPED'
    mongo_db['orders'].update_one(
        {'_id': ObjectId(order_id)},
        {'$set': {'status': new_status}},
    )
    found, elapsed = await _poll_row(
        a_pool, 'orders_flat', 'order_id', order_id,
        predicate=lambda row: row['status'] == new_status,
    )
    result('Update propagation', found, f"status={new_status!r} elapsed={elapsed} ms")


async def test_delete_propagation(
    mongo_db, a_pool: asyncpg.Pool, f_pool: asyncpg.Pool, order_id: str
) -> None:
    """Delete order in MongoDB → verify it disappears from both Postgres targets."""
    mongo_db['orders'].delete_one({'_id': ObjectId(order_id)})

    gone_a, ela = await _row_gone(a_pool, 'orders_flat',  'order_id', order_id)
    gone_f, elf = await _row_gone(f_pool, 'transactions', 'order_id', order_id)

    result('Delete -> analytics propagation', gone_a, f"elapsed={ela} ms")
    result('Delete -> finance propagation',   gone_f, f"elapsed={elf} ms")


async def test_duplicate_safety(
    mongo_db, a_pool: asyncpg.Pool, f_pool: asyncpg.Pool
) -> None:
    """Insert the same order twice → verify only 1 row in each Postgres target."""
    oid   = ObjectId()
    order = {
        '_id':         oid,
        'customer_id': 'verify-dup-safety',
        'status':      'PENDING',
        'city':        'DupCity',
        'total':       9.99,
    }
    oid_str = str(oid)

    # First write
    mongo_db['orders'].insert_one(order)
    # Wait for first propagation
    await _poll_row(a_pool, 'orders_flat', 'order_id', oid_str)

    # Re-deliver the same document (replace_one triggers another Debezium event)
    mongo_db['orders'].replace_one({'_id': oid}, order)
    await asyncio.sleep(3)  # allow second event to propagate

    async with a_pool.acquire() as conn:
        count_a = await conn.fetchval(
            "SELECT COUNT(*) FROM orders_flat WHERE order_id = $1", oid_str
        )
    async with f_pool.acquire() as conn:
        count_f = await conn.fetchval(
            "SELECT COUNT(*) FROM transactions WHERE order_id = $1", oid_str
        )

    result('Duplicate safety (analytics)', count_a == 1, f"rows={count_a}")
    result('Duplicate safety (finance)',   count_f == 1, f"rows={count_f}")

    # Cleanup test document
    mongo_db['orders'].delete_one({'_id': oid})


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

async def main() -> None:
    print("\n=== CDC Pipeline Verification Suite ===\n")

    # MongoDB
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        client.admin.command('ping')
        db = client['mydb']
        print("[OK] MongoDB connected")
    except Exception as exc:
        print(f"[FAIL] MongoDB connection failed: {exc}")
        return

    # Analytics Postgres
    try:
        a_pool = await _analytics_pool()
        print("[OK] Analytics Postgres connected (port 5434)")
    except Exception as exc:
        print(f"[FAIL] Analytics Postgres failed: {exc}")
        client.close()
        return

    # Finance Postgres
    try:
        f_pool = await _finance_pool()
        print("[OK] Finance Postgres connected (port 5433)")
    except Exception as exc:
        print(f"[FAIL] Finance Postgres failed: {exc}")
        await a_pool.close()
        client.close()
        return

    print("\n--- Tests ---\n")

    await test_count_parity(db, a_pool)

    order_id = await test_insert_latency(db, a_pool, f_pool)
    if order_id:
        await test_update_propagation(db, a_pool, order_id)
        await test_delete_propagation(db, a_pool, f_pool, order_id)

    await test_duplicate_safety(db, a_pool, f_pool)

    await a_pool.close()
    await f_pool.close()
    client.close()

    # Summary
    total  = len(RESULTS)
    passed = sum(1 for _, ok in RESULTS if ok)
    print(f"\n{'-' * 42}")
    print(f"  {passed}/{total} passed")
    if passed < total:
        print("\n  Failed tests:")
        for name, ok in RESULTS:
            if not ok:
                print(f"    - {name}")
    else:
        print("  All tests passed!")
    print()


if __name__ == '__main__':
    asyncio.run(main())
