"""
Main Kafka consumer — orchestrates the full CDC pipeline.

Pipeline per message
────────────────────
1. Batch poll from Kafka (up to BATCH_SIZE messages, POLL_TIMEOUT seconds wait)
2. For each message:
   a. Parse Debezium envelope → ParsedEvent          (debezium_parser)
   b. Validate required fields / detect schema drift  (schema_guard)
   c. Route to (db_target, table, row) targets        (router)
   d. Write to Postgres (row-by-row error handling)   (pg_writer)
3. Manual offset commit after the full batch is processed
   (success rows written + failed rows DLQ'd)

Offset commit strategy
──────────────────────
enable.auto.commit=False.  Commit only after the entire batch is either
written to Postgres or sent to the DLQ.  This means on restart we replay
at most one batch — all writes are idempotent (ON CONFLICT DO UPDATE) so
replaying is safe.

Row-by-row error handling
─────────────────────────
A bad row goes to DLQ without blocking the rest of the batch.
This is better granularity than rolling back all 100 rows for one failure.

Edge cases
──────────
- Tombstone (null value): silently skipped.
- Poison pill / parse error: DLQ + commit (partition is not blocked).
- Consumer rebalance mid-batch: uncommitted offsets cause re-delivery;
  idempotent upserts absorb duplicates safely.
- Lag spike on restart: batch drain handles backlog; lag metric is visible
  via src/metrics.py.
- KafkaError._PARTITION_EOF: informational, not an error — logged at DEBUG.
- Graceful shutdown on KeyboardInterrupt: closes consumer, pools, DLQ producer.

Run
───
  PYTHONPATH=src python src/consumer/kafka_consumer.py
"""

import asyncio
import logging
import os
import sys
import time

# Ensure src/ is importable when running this script directly
_src = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _src not in sys.path:
    sys.path.insert(0, _src)

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

import transformer.debezium_parser as debezium_parser
import transformer.router as router
import transformer.schema_guard as schema_guard
from writer.pg_writer import (
    create_pool,
    ensure_metrics_table,
    upsert_orders_flat,
    upsert_transactions,
    delete_order,
    write_metric,
)
from consumer.dlq import create_dlq_producer, send_to_dlq

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s [%(name)s] %(message)s',
)
logger = logging.getLogger(__name__)

BROKER       = os.getenv('KAFKA_BROKER_URL', 'localhost:9092')
TOPICS       = ['poc.mydb.orders']
GROUP_ID     = 'poc-pipeline-consumer'
BATCH_SIZE   = 100
POLL_TIMEOUT = 1.0   # seconds; consumer.consume() blocks up to this long


# ---------------------------------------------------------------------------
# Pool creation
# ---------------------------------------------------------------------------

async def _create_pools():
    analytics_pool = await create_pool(
        host=os.getenv('POSTGRES_ANALYTICS_HOST', 'localhost'),
        port=os.getenv('POSTGRES_ANALYTICS_PORT', '5434'),
        user=os.getenv('POSTGRES_ANALYTICS_USER', 'postgres'),
        password=os.getenv('POSTGRES_ANALYTICS_PASSWORD', 'password'),
        db=os.getenv('POSTGRES_ANALYTICS_DB', 'analytics'),
    )
    finance_pool = await create_pool(
        host=os.getenv('POSTGRES_FINANCE_HOST', 'localhost'),
        port=os.getenv('POSTGRES_FINANCE_PORT', '5433'),
        user=os.getenv('POSTGRES_FINANCE_USER', 'postgres'),
        password=os.getenv('POSTGRES_FINANCE_PASSWORD', 'password'),
        db=os.getenv('POSTGRES_FINANCE_DB', 'finance'),
    )
    return analytics_pool, finance_pool


# ---------------------------------------------------------------------------
# Single-message processor
# ---------------------------------------------------------------------------

async def _process_message(
    msg, analytics_pool, finance_pool, dlq_producer,
    kafka_ts_ms: int, doc_size_bytes: int, consumer_recv_ms: int,
) -> bool:
    """
    Process one Kafka message through the full pipeline.

    Returns True on success (or tombstone skip), False when the event
    was DLQ'd.  Either way the caller should commit the offset.
    """
    topic     = msg.topic()
    partition = msg.partition()
    offset    = msg.offset()
    raw_value = msg.value()
    raw_key   = msg.key()

    # ── Parse ────────────────────────────────────────────────────────────────
    try:
        event = debezium_parser.parse(topic, raw_value, raw_key)
    except ValueError as exc:
        logger.error(f"[consumer] Parse error on {topic}[{partition}]@{offset}: {exc}")
        send_to_dlq(dlq_producer, topic, partition, offset, raw_value or b'', str(exc))
        return False

    # ── Tombstone ─────────────────────────────────────────────────────────────
    if event.op == 'tombstone':
        logger.debug(
            f"[consumer] Tombstone on {topic}[{partition}]@{offset} — skipped"
        )
        return True

    # ── Schema validation ────────────────────────────────────────────────────
    if not schema_guard.validate(event):
        send_to_dlq(
            dlq_producer, topic, partition, offset,
            raw_value or b'', 'schema_validation_failed',
        )
        return False

    # ── Route ────────────────────────────────────────────────────────────────
    routed = router.route(event)
    if not routed:
        logger.debug(
            f"[consumer] No route for op={event.op!r} "
            f"collection={event.collection!r} — skipped"
        )
        return True

    # ── Write (row-by-row error handling) ─────────────────────────────────────
    success = True
    for (db_target, table, row) in routed:
        try:
            if row.get('_delete'):
                await delete_order(analytics_pool, finance_pool, row['order_id'])

            elif db_target == 'analytics' and table == 'orders_flat':
                await upsert_orders_flat(analytics_pool, [row])

            elif db_target == 'finance' and table == 'transactions':
                await upsert_transactions(finance_pool, [row])

        except Exception as exc:
            logger.error(
                f"[consumer] Write error {db_target}.{table} "
                f"op={event.op!r} doc_id={event.doc_id!r}: {exc}"
            )
            send_to_dlq(
                dlq_producer, topic, partition, offset,
                raw_value or b'', str(exc),
            )
            success = False
            # Continue processing remaining routes — partial writes are
            # better than dropping the entire event.

    pg_stored_ms = int(time.time() * 1000)
    if event.ts_ms > 0 and kafka_ts_ms > 0:
        await write_metric(analytics_pool, {
            'doc_id':           event.doc_id,
            'collection':       event.collection,
            'operation':        event.op,
            'doc_size_bytes':   doc_size_bytes,
            'mongo_ts_ms':      event.ts_ms,
            'kafka_ts_ms':      kafka_ts_ms,
            'consumer_recv_ms': consumer_recv_ms,
            'pg_stored_ms':     pg_stored_ms,
            'debezium_lat_ms':  max(0, kafka_ts_ms    - event.ts_ms),
            'consumer_lat_ms':  max(0, consumer_recv_ms - kafka_ts_ms),
            'write_lat_ms':     max(0, pg_stored_ms   - consumer_recv_ms),
            'e2e_lat_ms':       max(0, pg_stored_ms   - event.ts_ms),
        })

    logger.info(
        f"[consumer] op={event.op!r} collection={event.collection!r} "
        f"doc_id={event.doc_id!r} ts_ms={event.ts_ms} routes={len(routed)}"
    )
    return success


# ---------------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------------

async def run_consumer():
    logger.info("[consumer] Creating PostgreSQL connection pools...")
    analytics_pool, finance_pool = await _create_pools()
    logger.info("[consumer] PostgreSQL pools ready (analytics=5434, finance=5433)")
    await ensure_metrics_table(analytics_pool)
    logger.info("[consumer] Metrics table ready.")

    dlq_producer = create_dlq_producer(BROKER)

    consumer = Consumer({
        'bootstrap.servers':    BROKER,
        'group.id':             GROUP_ID,
        'auto.offset.reset':    'earliest',
        'enable.auto.commit':   False,
        'max.poll.interval.ms': 300_000,
        'session.timeout.ms':   30_000,
        'broker.address.family': 'v6',
    })
    consumer.subscribe(TOPICS)
    logger.info(f"[consumer] Subscribed to {TOPICS} as group '{GROUP_ID}'")

    events_consumed = 0
    events_written  = 0
    events_dlq      = 0

    try:
        while True:
            msgs = consumer.consume(num_messages=BATCH_SIZE, timeout=POLL_TIMEOUT)
            if not msgs:
                continue

            batch_ok  = 0
            batch_dlq = 0
            batch_real = 0  # messages that were not Kafka-level errors

            for msg in msgs:
                if msg.error():
                    err = msg.error()
                    if err.code() == KafkaError._PARTITION_EOF:
                        logger.debug(
                            f"[consumer] Partition EOF on "
                            f"{msg.topic()}[{msg.partition()}]"
                        )
                        continue
                    logger.error(f"[consumer] Kafka error: {err}")
                    continue

                batch_real += 1
                events_consumed += 1
                _kafka_ts_ms      = msg.timestamp()[1]
                _doc_size_bytes   = len(msg.value() or b'')
                _consumer_recv_ms = int(time.time() * 1000)
                ok = await _process_message(
                    msg, analytics_pool, finance_pool, dlq_producer,
                    _kafka_ts_ms, _doc_size_bytes, _consumer_recv_ms,
                )
                if ok:
                    batch_ok   += 1
                    events_written += 1
                else:
                    batch_dlq  += 1
                    events_dlq += 1

            # Only commit when we actually consumed real messages; committing
            # with no stored offsets raises _NO_OFFSET (e.g. all-error batch).
            if batch_real > 0:
                consumer.commit(asynchronous=False)
                logger.info(
                    f"[consumer] Batch committed: ok={batch_ok} dlq={batch_dlq} "
                    f"| totals consumed={events_consumed} written={events_written} "
                    f"dlq={events_dlq}"
                )

    except KeyboardInterrupt:
        logger.info("[consumer] Shutdown signal received — closing gracefully...")
    finally:
        consumer.close()
        await analytics_pool.close()
        await finance_pool.close()
        dlq_producer.flush(timeout=10)
        logger.info("[consumer] Shutdown complete")


if __name__ == '__main__':
    asyncio.run(run_consumer())
