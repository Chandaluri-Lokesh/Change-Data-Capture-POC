"""
PostgreSQL writer using asyncpg connection pools.

Generic functions
─────────────────
upsert_table(pool, table, rows, upsert_key_cols)
    INSERT … ON CONFLICT (upsert_key_cols) DO UPDATE
    Works for any table; driven entirely by the mapping engine output.

delete_cascade(pool, table, pk_col, pk_val)
    DELETE FROM table WHERE pk_col = pk_val
    Child rows are cleaned up by ON DELETE CASCADE FK constraints.

Metrics helpers (retained)
──────────────────────────
ensure_metrics_table(pool)   — CREATE TABLE IF NOT EXISTS cdc_pipeline_metrics
write_metric(pool, metric)   — INSERT one latency row (non-fatal on failure)

Pool helper
───────────
create_pool(host, port, user, password, db) → asyncpg.Pool

All write operations are idempotent (ON CONFLICT DO UPDATE).
Stale connections are retried once with a 0.5 s back-off.
"""

import asyncio
import logging
from typing import List, Dict, Optional

import asyncpg

logger = logging.getLogger(__name__)


async def create_pool(
    host: str, port, user: str, password: str, db: str,
    min_size: int = 2, max_size: int = 10,
) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        host=host, port=int(port), user=user,
        password=password, database=db,
        min_size=min_size, max_size=max_size,
        command_timeout=30,
    )


async def _execute_with_retry(pool, coro_factory, retries: int = 1):
    for attempt in range(retries + 1):
        try:
            return await coro_factory()
        except (asyncpg.ConnectionDoesNotExistError,
                asyncpg.InterfaceError, OSError) as exc:
            if attempt < retries:
                logger.warning(
                    f"[pg_writer] Connection error (attempt {attempt+1}/{retries+1}): "
                    f"{exc!r} — retrying in 0.5 s"
                )
                await asyncio.sleep(0.5)
            else:
                raise


# ---------------------------------------------------------------------------
# Generic upsert — driven by mapping engine
# ---------------------------------------------------------------------------

async def upsert_table(
    pool: asyncpg.Pool,
    table_name: str,
    rows: List[Dict],
    upsert_key_cols: List[str],
) -> int:
    """
    Generic idempotent upsert for any P2P table.

    Columns are inferred from the first row's keys.
    Internal helper keys (prefixed with '_') are stripped.
    If all columns are in the upsert key (no SET columns), DO NOTHING is used.

    Returns:
        Number of rows processed.
    """
    if not rows:
        return 0

    all_cols = [c for c in rows[0].keys() if not c.startswith('_')]
    if not all_cols:
        return 0

    non_key = [c for c in all_cols if c not in upsert_key_cols]
    placeholders  = ', '.join(f'${i+1}' for i in range(len(all_cols)))
    cols_sql      = ', '.join(f'"{c}"' for c in all_cols)
    conflict_sql  = ', '.join(f'"{c}"' for c in upsert_key_cols)

    if non_key:
        update_sql = ', '.join(f'"{c}" = EXCLUDED."{c}"' for c in non_key)
        sql = (
            f'INSERT INTO {table_name} ({cols_sql}) VALUES ({placeholders}) '
            f'ON CONFLICT ({conflict_sql}) DO UPDATE SET {update_sql}'
        )
    else:
        sql = (
            f'INSERT INTO {table_name} ({cols_sql}) VALUES ({placeholders}) '
            f'ON CONFLICT ({conflict_sql}) DO NOTHING'
        )

    data = [tuple(r.get(c) for c in all_cols) for r in rows]

    async def _run():
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(sql, data)

    await _execute_with_retry(pool, _run)
    return len(rows)


# ---------------------------------------------------------------------------
# Generic cascading delete — driven by mapping engine
# ---------------------------------------------------------------------------

async def delete_cascade(
    pool: asyncpg.Pool,
    table_name: str,
    pk_col: str,
    pk_val: str,
) -> None:
    """
    Delete the parent row; child rows are removed by FK ON DELETE CASCADE.
    No-op if pk_val is falsy.
    """
    if not pk_val:
        logger.warning(f"[pg_writer] delete_cascade called with empty pk_val for {table_name} — skipping")
        return
    async with pool.acquire() as conn:
        deleted = await conn.fetchval(
            f'DELETE FROM {table_name} WHERE "{pk_col}" = $1 RETURNING "{pk_col}"',
            pk_val,
        )
        if deleted:
            logger.info(f"[pg_writer] Deleted {table_name} row {pk_val!r} (cascade)")


# ---------------------------------------------------------------------------
# Metrics table (analytics DB)
# ---------------------------------------------------------------------------

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
    """Insert one pipeline timing row. Failures are logged and swallowed."""
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
