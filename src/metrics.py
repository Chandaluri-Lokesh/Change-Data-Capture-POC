"""
Real-time CLI observability dashboard for the CDC pipeline.

Refreshes every INTERVAL seconds and prints:
  - Consumer lag per topic + partition (Kafka watermarks vs committed offsets)
  - PostgreSQL row counts: orders_flat, line_items, transactions
  - Connector status if Kafka Connect REST API is reachable

Also provides CDCJsonFormatter — a structured JSON log formatter
that the kafka_consumer can attach to its logger for per-event audit lines.

Run
───
  python src/metrics.py

Edge cases
──────────
- Kafka unreachable: lag shown as N/A; does not crash.
- Postgres unreachable: counts shown as N/A; does not crash.
- Kafka Connect unreachable: connector status shown as N/A; does not crash.
- All network calls have short timeouts so the dashboard stays responsive.
"""

import asyncio
import asyncpg
import json
import logging
import os
import sys
from datetime import datetime, timezone

_src = os.path.abspath(os.path.dirname(__file__))
if _src not in sys.path:
    sys.path.insert(0, _src)

import requests
from confluent_kafka import Consumer, TopicPartition
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

BROKER       = os.getenv('KAFKA_BROKER_URL', 'localhost:9092')
CONNECT_URL  = os.getenv('KAFKA_CONNECT_REST_URL', 'http://localhost:8083')
GROUP_ID     = 'poc-pipeline-consumer'
TOPICS       = ['poc.mydb.orders', 'poc.mydb.products']
INTERVAL     = 10   # seconds between refreshes


# ---------------------------------------------------------------------------
# Structured JSON log formatter (for use by kafka_consumer.py)
# ---------------------------------------------------------------------------

class CDCJsonFormatter(logging.Formatter):
    """
    Emit one JSON object per log line with standard CDC fields.

    Standard fields always present:
      ts, level, logger, message

    Optional CDC fields (attach via logger.info(..., extra={...})):
      op_type, collection, order_id, latency_ms, target_db, status
    """

    CDC_EXTRA_FIELDS = (
        'op_type', 'collection', 'order_id',
        'latency_ms', 'target_db', 'status',
    )

    def format(self, record: logging.LogRecord) -> str:
        base = {
            'ts':      datetime.now(tz=timezone.utc).isoformat(),
            'level':   record.levelname,
            'logger':  record.name,
            'message': record.getMessage(),
        }
        for field in self.CDC_EXTRA_FIELDS:
            if hasattr(record, field):
                base[field] = getattr(record, field)
        return json.dumps(base)


# ---------------------------------------------------------------------------
# Kafka lag
# ---------------------------------------------------------------------------

def _fetch_lag(broker: str, group_id: str, topics: list) -> dict:
    """
    Returns {topic: {partition: lag}}.
    Returns {} if Kafka is unreachable (silently).
    """
    result: dict = {}
    try:
        consumer = Consumer({
            'bootstrap.servers':     broker,
            'group.id':              group_id,
            'enable.auto.commit':    False,
            'broker.address.family': 'v6',
        })
        for topic in topics:
            result[topic] = {}
            try:
                metadata = consumer.list_topics(topic=topic, timeout=3)
                if topic not in metadata.topics:
                    continue
                for pid in metadata.topics[topic].partitions:
                    tp = TopicPartition(topic, pid)
                    committed    = consumer.committed([tp], timeout=3)[0]
                    committed_off = (
                        committed.offset if committed and committed.offset >= 0 else 0
                    )
                    _, high = consumer.get_watermark_offsets(
                        tp, timeout=3, cached=False
                    )
                    result[topic][pid] = max(0, high - committed_off)
            except Exception:
                pass
        consumer.close()
    except Exception:
        pass
    return result


# ---------------------------------------------------------------------------
# Postgres row counts
# ---------------------------------------------------------------------------

async def _fetch_pg_counts() -> dict:
    """
    Returns a dict with row counts per table.
    Values are None if the target database is unreachable.
    """
    counts = {
        'analytics.orders_flat': None,
        'analytics.line_items':  None,
        'finance.transactions':  None,
    }

    async def _count_analytics():
        try:
            pool = await asyncpg.create_pool(
                host=os.getenv('POSTGRES_ANALYTICS_HOST', 'localhost'),
                port=int(os.getenv('POSTGRES_ANALYTICS_PORT', '5434')),
                user=os.getenv('POSTGRES_ANALYTICS_USER', 'postgres'),
                password=os.getenv('POSTGRES_ANALYTICS_PASSWORD', 'password'),
                database=os.getenv('POSTGRES_ANALYTICS_DB', 'analytics'),
                min_size=1, max_size=1, command_timeout=5,
            )
            async with pool.acquire() as conn:
                counts['analytics.orders_flat'] = await conn.fetchval(
                    "SELECT COUNT(*) FROM orders_flat"
                )
                counts['analytics.line_items'] = await conn.fetchval(
                    "SELECT COUNT(*) FROM line_items"
                )
            await pool.close()
        except Exception:
            pass

    async def _count_finance():
        try:
            pool = await asyncpg.create_pool(
                host=os.getenv('POSTGRES_FINANCE_HOST', 'localhost'),
                port=int(os.getenv('POSTGRES_FINANCE_PORT', '5433')),
                user=os.getenv('POSTGRES_FINANCE_USER', 'postgres'),
                password=os.getenv('POSTGRES_FINANCE_PASSWORD', 'password'),
                database=os.getenv('POSTGRES_FINANCE_DB', 'finance'),
                min_size=1, max_size=1, command_timeout=5,
            )
            async with pool.acquire() as conn:
                counts['finance.transactions'] = await conn.fetchval(
                    "SELECT COUNT(*) FROM transactions"
                )
            await pool.close()
        except Exception:
            pass

    await asyncio.gather(_count_analytics(), _count_finance())
    return counts


# ---------------------------------------------------------------------------
# Connector status
# ---------------------------------------------------------------------------

def _fetch_connector_status(connect_url: str) -> dict:
    """
    Returns {connector_name: {state, tasks}} from Kafka Connect REST API.
    Returns {} if unreachable.
    """
    result: dict = {}
    try:
        names_resp = requests.get(f"{connect_url}/connectors", timeout=3)
        if names_resp.status_code != 200:
            return result
        for name in names_resp.json():
            try:
                status = requests.get(
                    f"{connect_url}/connectors/{name}/status", timeout=3
                ).json()
                result[name] = {
                    'state': status.get('connector', {}).get('state', 'UNKNOWN'),
                    'tasks': [
                        {'id': t['id'], 'state': t['state']}
                        for t in status.get('tasks', [])
                    ],
                }
            except Exception:
                pass
    except Exception:
        pass
    return result


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

async def run_metrics():
    print(
        f"\nCDC Pipeline Metrics  --  refreshes every {INTERVAL}s  (Ctrl+C to stop)\n"
    )

    while True:
        now = datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        print(f"{'-' * 62}")
        print(f"  {now}\n")

        # ── Consumer lag ──────────────────────────────────────────────────
        lag_data  = _fetch_lag(BROKER, GROUP_ID, TOPICS)
        total_lag = 0
        print("  Consumer Lag:")
        if lag_data:
            for topic, parts in lag_data.items():
                for pid, lag in parts.items():
                    print(f"    {topic}[{pid}]  lag={lag:,}")
                    total_lag += lag
            print(f"    -- total lag: {total_lag:,}")
        else:
            print("    Kafka unreachable — N/A")

        # ── Postgres row counts ───────────────────────────────────────────
        counts = await _fetch_pg_counts()
        print("\n  PostgreSQL Row Counts:")
        for table, count in counts.items():
            val = f"{count:,}" if count is not None else "N/A (unreachable)"
            print(f"    {table:<40} {val}")

        # ── Connector status ──────────────────────────────────────────────
        conn_status = _fetch_connector_status(CONNECT_URL)
        print("\n  Connector Status:")
        if conn_status:
            for name, info in conn_status.items():
                task_states = ', '.join(
                    f"task[{t['id']}]={t['state']}" for t in info['tasks']
                )
                print(f"    {name:<30} {info['state']}  {task_states}")
        else:
            print("    Kafka Connect unreachable — N/A")

        print()
        await asyncio.sleep(INTERVAL)


if __name__ == '__main__':
    try:
        asyncio.run(run_metrics())
    except KeyboardInterrupt:
        print("\nMetrics stopped.")
