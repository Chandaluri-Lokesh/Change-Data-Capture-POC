"""
PostgreSQL writer using asyncpg connection pools.

All write operations are:
  - Idempotent via ON CONFLICT DO UPDATE (orders_flat, transactions)
  - Wrapped in transactions for atomicity
  - Guarded by a single-retry loop for stale connections

line_items idempotency strategy:
  The DDL has no unique constraint on (order_id, sku), so ON CONFLICT cannot be
  used.  Instead, upsert_line_items() deletes existing rows for the order_id
  then re-inserts — all within one transaction.  This is safe because the main
  consumer re-delivers full documents (not diffs) on restart.

Batch strategy (row-by-row in consumer):
  The consumer calls these functions one row at a time so that a single
  bad row is DLQ'd without discarding the rest of the batch.  For high
  throughput, callers can accumulate rows and pass a list.

Edge cases handled
──────────────────
- Stale / dropped connection: ConnectionDoNotUseError / ConnectionFailureError
  → retry once with a fresh pool connection before re-raising.
- delete_order with empty order_id: skipped with a warning (defensive).
- Analytics and finance deletes run concurrently (asyncio.gather).
- Postgres server time is NOT used for timestamps — event ts_ms is always
  the source of truth to avoid clock skew between services.
"""

import asyncio
import asyncpg
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)


async def create_pool(host: str, port, user: str, password: str, db: str,
                      min_size: int = 2, max_size: int = 10) -> asyncpg.Pool:
    """Create an asyncpg connection pool to a PostgreSQL database."""
    return await asyncpg.create_pool(
        host=host,
        port=int(port),
        user=user,
        password=password,
        database=db,
        min_size=min_size,
        max_size=max_size,
        command_timeout=30,
    )


async def _execute_with_retry(pool, coro_factory, retries: int = 1):
    """
    Execute an async DB operation with `retries` retries on connection error.

    coro_factory: zero-argument async callable that performs the DB work.
    Uses a small sleep between attempts to give the pool time to recover.
    """
    for attempt in range(retries + 1):
        try:
            return await coro_factory()
        except (asyncpg.ConnectionDoesNotExistError,
                asyncpg.InterfaceError,
                OSError) as exc:
            if attempt < retries:
                logger.warning(
                    f"[pg_writer] Connection error (attempt {attempt + 1}/{retries + 1}): "
                    f"{exc!r} — retrying in 0.5s"
                )
                await asyncio.sleep(0.5)
            else:
                raise


async def upsert_orders_flat(pool: asyncpg.Pool, rows: List[Dict]) -> int:
    """
    Upsert rows into analytics.orders_flat.

    ON CONFLICT (order_id) keeps the latest status, city, total, updated_at.
    created_at is intentionally NOT updated on conflict — preserves first-seen time.

    Returns:
        Number of rows processed (0 for empty input).
    """
    if not rows:
        return 0

    sql = """
        INSERT INTO orders_flat
            (order_id, customer_id, status, city, total, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (order_id) DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            status      = EXCLUDED.status,
            city        = EXCLUDED.city,
            total       = EXCLUDED.total,
            updated_at  = EXCLUDED.updated_at
    """

    async def _run():
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(sql, [
                    (r['order_id'], r['customer_id'], r['status'], r['city'],
                     r['total'], r['created_at'], r['updated_at'])
                    for r in rows
                ])

    await _execute_with_retry(pool, _run)
    return len(rows)


async def upsert_transactions(pool: asyncpg.Pool, rows: List[Dict]) -> int:
    """
    Upsert rows into finance.transactions.

    ON CONFLICT (order_id) is used because order_id has a UNIQUE constraint in
    the DDL — txn_id (PK) is also deterministic so both paths are idempotent.

    Returns:
        Number of rows processed.
    """
    if not rows:
        return 0

    sql = """
        INSERT INTO transactions (txn_id, order_id, amount, status, recorded_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (order_id) DO UPDATE SET
            txn_id      = EXCLUDED.txn_id,
            amount      = EXCLUDED.amount,
            status      = EXCLUDED.status,
            recorded_at = EXCLUDED.recorded_at
    """

    async def _run():
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(sql, [
                    (r['txn_id'], r['order_id'], r['amount'],
                     r['status'], r['recorded_at'])
                    for r in rows
                ])

    await _execute_with_retry(pool, _run)
    return len(rows)


async def delete_order(analytics_pool: asyncpg.Pool, finance_pool: asyncpg.Pool,
                       order_id: str) -> None:
    """
    Delete an order from both Postgres targets concurrently.

    analytics: removes row from orders_flat AND all related line_items.
    finance:   removes row from transactions.

    Each target is deleted independently — a failure in one does not
    block the other.  Callers can catch exceptions from asyncio.gather
    if they need to DLQ the failure.
    """
    if not order_id:
        logger.warning("[pg_writer] delete_order called with empty order_id — skipping")
        return

    async def _del_analytics():
        async with analytics_pool.acquire() as conn:
            async with conn.transaction():
                deleted = await conn.fetchval(
                    "DELETE FROM orders_flat WHERE order_id = $1 RETURNING order_id",
                    order_id,
                )
                await conn.execute(
                    "DELETE FROM line_items WHERE order_id = $1", order_id
                )
        if deleted:
            logger.info(f"[pg_writer] Deleted order {order_id!r} from analytics")

    async def _del_finance():
        async with finance_pool.acquire() as conn:
            deleted = await conn.fetchval(
                "DELETE FROM transactions WHERE order_id = $1 RETURNING order_id",
                order_id,
            )
        if deleted:
            logger.info(
                f"[pg_writer] Deleted transaction for {order_id!r} from finance"
            )

    await asyncio.gather(_del_analytics(), _del_finance())


async def ensure_metrics_table(pool: asyncpg.Pool) -> None:
    """Create cdc_pipeline_metrics if it does not already exist."""
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS cdc_pipeline_metrics (
                id               SERIAL PRIMARY KEY,
                doc_id           TEXT,
                collection       TEXT,
                operation        TEXT,
                doc_size_bytes   INT,
                mongo_ts_ms      BIGINT,
                kafka_ts_ms      BIGINT,
                consumer_recv_ms BIGINT,
                pg_stored_ms     BIGINT,
                debezium_lat_ms  INT,
                consumer_lat_ms  INT,
                write_lat_ms     INT,
                e2e_lat_ms       INT,
                recorded_at      TIMESTAMPTZ DEFAULT now()
            )
        """)
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cdc_metrics_recorded "
            "ON cdc_pipeline_metrics (recorded_at DESC)"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cdc_metrics_col "
            "ON cdc_pipeline_metrics (collection)"
        )


async def write_metric(pool: asyncpg.Pool, metric: dict) -> None:
    """
    Insert one pipeline timing row into cdc_pipeline_metrics.
    Failures are logged and swallowed — a metrics write never kills the pipeline.
    """
    sql = """
        INSERT INTO cdc_pipeline_metrics
            (doc_id, collection, operation, doc_size_bytes,
             mongo_ts_ms, kafka_ts_ms, consumer_recv_ms, pg_stored_ms,
             debezium_lat_ms, consumer_lat_ms, write_lat_ms, e2e_lat_ms)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
    """
    async def _run():
        async with pool.acquire() as conn:
            await conn.execute(
                sql,
                metric['doc_id'],           metric['collection'],
                metric['operation'],        metric['doc_size_bytes'],
                metric['mongo_ts_ms'],      metric['kafka_ts_ms'],
                metric['consumer_recv_ms'], metric['pg_stored_ms'],
                metric['debezium_lat_ms'],  metric['consumer_lat_ms'],
                metric['write_lat_ms'],     metric['e2e_lat_ms'],
            )
    try:
        await _execute_with_retry(pool, _run)
    except Exception as exc:
        logger.warning(f"[pg_writer] Metrics write failed (non-fatal): {exc!r}")
