"""
Dead Letter Queue (DLQ) producer.

Failed events are sent to the 'poc.dlq' Kafka topic with full metadata:
  source_topic, source_partition, source_offset, error, retry_count,
  failed_at (ISO-8601), raw_value (UTF-8 decoded, errors replaced).

Retry loop prevention
─────────────────────
retry_count tracks how many times this event has been re-queued.
When retry_count >= MAX_RETRIES (3), the event is written to
dlq.dead.log on disk instead of being re-produced to poc.dlq.
This prevents a poison pill from cycling the pipeline indefinitely.

The dlq_replay.py script increments retry_count when it re-produces
a DLQ event back to its source topic.

Edge cases
──────────
- DLQ producer failure: falls back to writing the payload to dlq.dead.log.
- dlq.dead.log write failure: logged at ERROR; payload is lost (last resort).
- producer.poll(0): triggers delivery callbacks non-blocking; avoids buffer fill.
"""

import json
import logging
import os
from datetime import datetime, timezone

from confluent_kafka import Producer

logger = logging.getLogger(__name__)

DLQ_TOPIC    = 'poc.dlq'
MAX_RETRIES  = 3

# Resolve dead letter log relative to project root
_project_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..')
)
DLQ_DEAD_LOG = os.path.join(_project_root, 'dlq.dead.log')


def create_dlq_producer(broker_url: str) -> Producer:
    """Create a Kafka Producer configured for reliable DLQ delivery."""
    return Producer({
        'bootstrap.servers':     broker_url,
        'acks':                  'all',
        'retries':               3,
        'retry.backoff.ms':      200,
        'broker.address.family': 'v4',
    })


def _on_delivery(err, msg):
    if err:
        logger.error(f"[dlq] Delivery to DLQ failed: {err}")
    else:
        logger.debug(
            f"[dlq] Delivered to {msg.topic()}[{msg.partition()}]@{msg.offset()}"
        )


def _write_dead_letter(payload: dict) -> None:
    """Append a dead-letter entry to the local log file (last-resort storage)."""
    try:
        with open(DLQ_DEAD_LOG, 'a', encoding='utf-8') as f:
            f.write(json.dumps(payload) + '\n')
    except OSError as exc:
        logger.error(f"[dlq] Cannot write to dead letter log: {exc}")


def send_to_dlq(
    producer: Producer,
    topic: str,
    partition: int,
    offset: int,
    raw_value: bytes,
    error: str,
    retry_count: int = 0,
) -> None:
    """
    Route a failed event to the DLQ Kafka topic (or to dlq.dead.log if
    retry_count has reached MAX_RETRIES).

    Args:
        producer:    Confluent Kafka Producer (shared instance).
        topic:       Source Kafka topic the event came from.
        partition:   Source partition number.
        offset:      Source offset number.
        raw_value:   Original raw bytes of the failed message value.
        error:       Human-readable error description.
        retry_count: Number of times this event has already been retried
                     (default 0 for first-time failures).
    """
    payload = {
        'source_topic':     topic,
        'source_partition': partition,
        'source_offset':    offset,
        'error':            str(error),
        'retry_count':      retry_count,
        'failed_at':        datetime.now(tz=timezone.utc).isoformat(),
        'raw_value': (
            raw_value.decode('utf-8', errors='replace') if raw_value else None
        ),
    }

    if retry_count >= MAX_RETRIES:
        logger.error(
            f"[dlq] {topic}[{partition}]@{offset} exceeded max retries "
            f"({MAX_RETRIES}) — writing to dlq.dead.log"
        )
        _write_dead_letter(payload)
        return

    try:
        producer.produce(
            DLQ_TOPIC,
            value=json.dumps(payload).encode('utf-8'),
            on_delivery=_on_delivery,
        )
        producer.poll(0)  # flush callbacks without blocking the consumer loop
        logger.warning(
            f"[dlq] Queued to poc.dlq: {topic}[{partition}]@{offset} "
            f"error={error!r} retry_count={retry_count}"
        )
    except Exception as exc:
        logger.error(f"[dlq] Failed to produce to DLQ ({exc!r}) — falling back to dead log")
        _write_dead_letter(payload)
