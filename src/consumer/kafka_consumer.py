"""
Main Kafka consumer — orchestrates the full CDC pipeline for P2P documents.

Pipeline per message
────────────────────
1. Batch poll from Kafka (up to BATCH_SIZE messages, POLL_TIMEOUT seconds wait)
2. For each message:
   a. Parse Debezium envelope → ParsedEvent          (debezium_parser)
   b. Validate required fields / detect schema drift  (schema_guard)
   c. Route → (table, rows, upsert_key) targets       (router → mapping_engine)
   d. Write to Postgres generically                   (pg_writer.upsert_table / delete_cascade)
   e. Write to Neo4j (node merge + relationships)     (neo4j_writer.run_ops)
3. Manual offset commit after the full batch

Topics consumed
───────────────
  poc.mydb.rfqs
  poc.mydb.purchase_orders
  poc.mydb.asns
  poc.mydb.grns
  poc.mydb.invoices

Run
───
  PYTHONPATH=src python src/consumer/kafka_consumer.py
"""

import asyncio
import logging
import os
import time

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

import transformer.debezium_parser as debezium_parser
import transformer.router as router
import transformer.schema_guard as schema_guard
from writer.pg_writer import (
    create_pool,
    ensure_metrics_table,
    upsert_table,
    delete_cascade,
    write_metric,
)
from writer.neo4j_writer import create_driver, run_ops, apply_constraints
from consumer.dlq import create_dlq_producer, send_to_dlq

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s [%(name)s] %(message)s',
)
logger = logging.getLogger(__name__)

BROKER       = os.getenv('KAFKA_BROKER_URL', 'localhost:9092')
TOPICS       = [
    'poc.mydb.rfqs',
    'poc.mydb.purchase_orders',
    'poc.mydb.asns',
    'poc.mydb.grns',
    'poc.mydb.invoices',
]
GROUP_ID     = 'p2p-pipeline-consumer'
BATCH_SIZE   = 100
POLL_TIMEOUT = 1.0


# ---------------------------------------------------------------------------
# Connection setup
# ---------------------------------------------------------------------------

async def _create_pg_pool():
    return await create_pool(
        host=os.getenv('POSTGRES_ANALYTICS_HOST', 'localhost'),
        port=os.getenv('POSTGRES_ANALYTICS_PORT', '5432'),
        user=os.getenv('POSTGRES_ANALYTICS_USER', 'postgres'),
        password=os.getenv('POSTGRES_ANALYTICS_PASSWORD', 'postgres'),
        db=os.getenv('POSTGRES_ANALYTICS_DB', 'analytics'),
    )


async def _create_neo4j_driver():
    return await create_driver(
        uri=os.getenv('NEO4J_URI',           'bolt://localhost:7687'),
        user=os.getenv('NEO4J_USER',         'neo4j'),
        password=os.getenv('NEO4J_PASSWORD', 'password'),
        database=os.getenv('NEO4J_DATABASE', None),
    )


async def _try_connect_neo4j():
    """Attempt to connect to Neo4j; returns driver on success, None on failure."""
    try:
        driver = await _create_neo4j_driver()
        await apply_constraints(driver)
        logger.info("[consumer] Neo4j (re)connected successfully.")
        return driver
    except Exception as exc:
        logger.warning(f"[consumer] Neo4j unavailable ({exc!r}) — graph writes skipped.")
        return None


# ---------------------------------------------------------------------------
# Single-message processor
# ---------------------------------------------------------------------------

async def _process_message(
    msg, pg_pool, neo4j_driver, dlq_producer,
    kafka_ts_ms: int, doc_size_bytes: int, consumer_recv_ms: int,
) -> tuple:
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
        return False, False

    # ── Tombstone ─────────────────────────────────────────────────────────────
    if event.op == 'tombstone':
        logger.debug(f"[consumer] Tombstone on {topic}[{partition}]@{offset} — skipped")
        return True, False

    # ── Schema validation ────────────────────────────────────────────────────
    if not schema_guard.validate(event):
        send_to_dlq(
            dlq_producer, topic, partition, offset,
            raw_value or b'', 'schema_validation_failed',
        )
        return False, False

    # ── Route (Postgres) ─────────────────────────────────────────────────────
    pg_routes = router.route(event)
    if not pg_routes and event.op != 'tombstone':
        logger.debug(
            f"[consumer] No Postgres route for collection={event.collection!r} "
            f"op={event.op!r} — skipped"
        )

    # ── Write to Postgres ─────────────────────────────────────────────────────
    success = True
    for (table_name, rows, upsert_key) in pg_routes:
        try:
            if rows and rows[0].get('_delete'):
                pk_col = router.engine().get_primary_key(event.collection)
                await delete_cascade(pg_pool, table_name, pk_col, event.doc_id)
            else:
                await upsert_table(pg_pool, table_name, rows, upsert_key)
        except Exception as exc:
            logger.error(
                f"[consumer] PG write error {table_name} "
                f"op={event.op!r} doc_id={event.doc_id!r}: {exc}"
            )
            send_to_dlq(
                dlq_producer, topic, partition, offset,
                raw_value or b'', str(exc),
            )
            success = False

    # ── Write to Neo4j ────────────────────────────────────────────────────────
    neo4j_failed = False
    neo4j_ops = router.get_neo4j_ops(event)
    if neo4j_ops and neo4j_driver:
        try:
            await run_ops(neo4j_driver, neo4j_ops)
        except Exception as exc:
            logger.error(
                f"[consumer] Neo4j write error op={event.op!r} "
                f"doc_id={event.doc_id!r}: {exc}"
            )
            neo4j_failed = True

    # ── Metrics ───────────────────────────────────────────────────────────────
    pg_stored_ms = int(time.time() * 1000)
    if event.ts_ms > 0 and kafka_ts_ms > 0:
        try:
            await write_metric(pg_pool, {
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
        except Exception as exc:
            logger.warning(f"[consumer] Metric write failed for doc_id={event.doc_id!r}: {exc}")

    logger.info(
        f"[consumer] op={event.op!r} collection={event.collection!r} "
        f"doc_id={event.doc_id!r} pg_routes={len(pg_routes)} neo4j_ops={len(neo4j_ops)}"
    )
    return success, neo4j_failed


# ---------------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------------

async def run_consumer():
    logger.info("[consumer] Starting P2P CDC consumer...")

    pg_pool = await _create_pg_pool()
    logger.info("[consumer] PostgreSQL pool ready")
    await ensure_metrics_table(pg_pool)

    neo4j_driver = await _try_connect_neo4j()

    dlq_producer = create_dlq_producer(BROKER)

    consumer = Consumer({
        'bootstrap.servers':    BROKER,
        'group.id':             GROUP_ID,
        'auto.offset.reset':    'earliest',
        'enable.auto.commit':   False,
        'max.poll.interval.ms': 300_000,
        'session.timeout.ms':   30_000,
        'broker.address.family': 'v4',
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

            # ── Attempt Neo4j reconnect if driver is down ─────────────────────
            if neo4j_driver is None:
                neo4j_driver = await _try_connect_neo4j()

            batch_ok        = 0
            batch_dlq       = 0
            batch_real      = 0
            batch_neo4j_err = False

            for msg in msgs:
                if msg.error():
                    err = msg.error()
                    if err.code() == KafkaError._PARTITION_EOF:
                        logger.debug(
                            f"[consumer] EOF on {msg.topic()}[{msg.partition()}]"
                        )
                        continue
                    logger.error(f"[consumer] Kafka error: {err}")
                    continue

                batch_real      += 1
                events_consumed += 1
                _kafka_ts_ms      = msg.timestamp()[1]
                _doc_size_bytes   = len(msg.value() or b'')
                _consumer_recv_ms = int(time.time() * 1000)

                ok, neo4j_failed = await _process_message(
                    msg, pg_pool, neo4j_driver, dlq_producer,
                    _kafka_ts_ms, _doc_size_bytes, _consumer_recv_ms,
                )
                if neo4j_failed:
                    batch_neo4j_err = True
                if ok:
                    batch_ok       += 1
                    events_written += 1
                else:
                    batch_dlq      += 1
                    events_dlq     += 1

            # ── If Neo4j failed this batch, close stale driver and reconnect ──
            if batch_neo4j_err:
                if neo4j_driver:
                    try:
                        await neo4j_driver.close()
                    except Exception:
                        pass
                neo4j_driver = await _try_connect_neo4j()

            if batch_real > 0:
                consumer.commit(asynchronous=False)
                logger.info(
                    f"[consumer] Batch committed: ok={batch_ok} dlq={batch_dlq} "
                    f"| totals consumed={events_consumed} written={events_written} "
                    f"dlq={events_dlq}"
                )

    except KeyboardInterrupt:
        logger.info("[consumer] Shutdown signal — closing gracefully...")
    finally:
        consumer.close()
        await pg_pool.close()
        if neo4j_driver:
            try:
                await neo4j_driver.close()
            except Exception:
                pass
        dlq_producer.flush(timeout=10)
        logger.info("[consumer] Shutdown complete")


if __name__ == '__main__':
    asyncio.run(run_consumer())
