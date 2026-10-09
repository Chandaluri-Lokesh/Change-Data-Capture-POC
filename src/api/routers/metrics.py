"""
GET  /api/metrics/summary   — aggregated latency stats + table row counts
GET  /api/metrics/recent    — last N rows from cdc_pipeline_metrics
WS   /ws/metrics            — WebSocket: streams new metric rows every 2 s
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Optional

import asyncpg
from fastapi import APIRouter, Request, WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)
router = APIRouter()

P2P_TABLES = [
    'rfq', 'rfq_line_items', 'rfq_invited_vendors',
    'purchase_orders', 'po_line_items',
    'asns', 'asn_line_items',
    'grns', 'grn_line_items',
    'invoices', 'invoice_line_items',
]


@router.get('/summary')
async def metrics_summary(request: Request):
    """Aggregated latency stats + table row counts."""
    pool: asyncpg.Pool = request.app.state.pg_pool

    async with pool.acquire() as conn:
        latency = await conn.fetchrow("""
            SELECT
                COUNT(*)                          AS total_events,
                AVG(e2e_lat_ms)::NUMERIC(10,1)    AS avg_e2e_ms,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY e2e_lat_ms)
                                                  AS p95_e2e_ms,
                MAX(e2e_lat_ms)                   AS max_e2e_ms,
                AVG(debezium_lat_ms)::NUMERIC(10,1) AS avg_debezium_ms,
                AVG(write_lat_ms)::NUMERIC(10,1)  AS avg_write_ms,
                COUNT(*) FILTER (WHERE operation = 'c')  AS inserts,
                COUNT(*) FILTER (WHERE operation = 'u')  AS updates,
                COUNT(*) FILTER (WHERE operation = 'd')  AS deletes
            FROM cdc_pipeline_metrics
            WHERE recorded_at > now() - INTERVAL '10 minutes'
        """)

        counts = {}
        for table in P2P_TABLES:
            try:
                cnt = await conn.fetchval(f'SELECT COUNT(*) FROM {table}')
                counts[table] = int(cnt or 0)
            except Exception:
                counts[table] = -1

    return {
        'latency': dict(latency) if latency else {},
        'table_counts': counts,
        'generated_at': datetime.now(timezone.utc).isoformat(),
    }


@router.get('/recent')
async def metrics_recent(request: Request, limit: int = 50):
    """Last N rows from cdc_pipeline_metrics."""
    pool: asyncpg.Pool = request.app.state.pg_pool
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, doc_id, collection, operation, doc_size_bytes,
                   debezium_lat_ms, consumer_lat_ms, write_lat_ms, e2e_lat_ms,
                   recorded_at
            FROM cdc_pipeline_metrics
            ORDER BY recorded_at DESC
            LIMIT $1
        """, limit)
    return [dict(r) for r in rows]


@router.get('/collections')
async def events_by_collection(request: Request):
    """Event count per collection in the last hour."""
    pool: asyncpg.Pool = request.app.state.pg_pool
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT collection,
                   COUNT(*) AS events,
                   AVG(e2e_lat_ms)::NUMERIC(10,1) AS avg_e2e_ms
            FROM cdc_pipeline_metrics
            WHERE recorded_at > now() - INTERVAL '1 hour'
            GROUP BY collection
            ORDER BY events DESC
        """)
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# WebSocket — real-time metric stream
# ---------------------------------------------------------------------------

@router.websocket('/ws')
async def metrics_ws(websocket: WebSocket):
    """
    WebSocket endpoint. Polls cdc_pipeline_metrics every 2 s and pushes
    rows newer than the last seen id to all connected clients.
    """
    await websocket.accept()
    pool: asyncpg.Pool = websocket.app.state.pg_pool
    last_id: int = 0

    try:
        # Send last 10 rows immediately on connect
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, doc_id, collection, operation, e2e_lat_ms,
                       debezium_lat_ms, write_lat_ms, recorded_at
                FROM cdc_pipeline_metrics
                ORDER BY id DESC LIMIT 10
            """)
        if rows:
            last_id = rows[0]['id']
            payload = [_row_to_dict(r) for r in reversed(rows)]
            await websocket.send_text(json.dumps({'type': 'init', 'data': payload}))

        while True:
            await asyncio.sleep(2)
            try:
                async with pool.acquire() as conn:
                    new_rows = await conn.fetch("""
                        SELECT id, doc_id, collection, operation, e2e_lat_ms,
                               debezium_lat_ms, consumer_lat_ms, write_lat_ms, recorded_at
                        FROM cdc_pipeline_metrics
                        WHERE id > $1
                        ORDER BY id ASC
                        LIMIT 50
                    """, last_id)
                if new_rows:
                    last_id = new_rows[-1]['id']
                    payload = [_row_to_dict(r) for r in new_rows]
                    await websocket.send_text(json.dumps({'type': 'update', 'data': payload}))
            except Exception as exc:
                logger.warning(f"[metrics_ws] DB poll error: {exc}")

    except WebSocketDisconnect:
        logger.debug("[metrics_ws] Client disconnected")
    except Exception as exc:
        logger.warning(f"[metrics_ws] Unexpected error: {exc}")


def _row_to_dict(row) -> dict:
    d = dict(row)
    if isinstance(d.get('recorded_at'), datetime):
        d['recorded_at'] = d['recorded_at'].isoformat()
    return d
