"""
DLQ Replay utility.

Consumes events from 'poc.dlq' and re-produces them to their original source
topic so the main consumer pipeline can re-process them.

Retry loop prevention
─────────────────────
Each DLQ envelope carries a 'retry_count' integer.
On replay, retry_count is incremented BEFORE re-producing.
If retry_count > MAX_RETRIES (3), the event is written to dlq.dead.log
instead of being re-queued — preventing infinite replay loops.

Usage
─────
  # Replay from where we left off (last committed DLQ offset)
  python scripts/dlq_replay.py

  # Replay everything from the beginning of poc.dlq
  python scripts/dlq_replay.py --from-beginning

  # Replay a specific number of messages only
  python scripts/dlq_replay.py --limit 50

Edge cases
──────────
- Malformed DLQ envelope: logged, committed, skipped.
- Missing source_topic in envelope: logged, committed, skipped.
- Empty raw_value: re-produced as empty bytes (downstream handles tombstone).
- Replay stops automatically when poc.dlq is fully consumed (no messages for
  two consecutive poll windows) unless --from-beginning is set.
"""

import argparse
import json
import logging
import os

from confluent_kafka import Consumer, KafkaError, Producer
from dotenv import load_dotenv

_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
load_dotenv(dotenv_path=os.path.join(_root, '.env'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
)
logger = logging.getLogger(__name__)

BROKER       = os.getenv('KAFKA_BROKER_URL', 'localhost:9092')
DLQ_TOPIC    = 'poc.dlq'
GROUP_ID     = 'poc-dlq-replay'
MAX_RETRIES  = 3
DLQ_DEAD_LOG = os.path.join(_root, 'dlq.dead.log')


def _write_dead_letter(payload: dict) -> None:
    try:
        with open(DLQ_DEAD_LOG, 'a', encoding='utf-8') as f:
            f.write(json.dumps(payload) + '\n')
    except OSError as exc:
        logger.error(f"[replay] Cannot write to dlq.dead.log: {exc}")


def run_replay(from_beginning: bool = False, limit: int = 0) -> None:
    consumer = Consumer({
        'bootstrap.servers':  BROKER,
        'group.id':           GROUP_ID,
        'enable.auto.commit': False,
        'auto.offset.reset':  'earliest' if from_beginning else 'latest',
    })
    consumer.subscribe([DLQ_TOPIC])

    producer = Producer({
        'bootstrap.servers': BROKER,
        'acks':              'all',
        'retries':           3,
    })

    start_msg = 'beginning' if from_beginning else 'latest committed offset'
    logger.info(f"[replay] Starting DLQ replay from {start_msg}...")

    replayed       = 0
    dead           = 0
    skipped        = 0
    empty_polls    = 0
    MAX_EMPTY_POLLS = 2  # stop after 2 consecutive empty polls (DLQ drained)

    try:
        while True:
            if limit and replayed >= limit:
                logger.info(f"[replay] Reached --limit {limit}")
                break

            msgs = consumer.consume(num_messages=50, timeout=2.0)

            if not msgs:
                empty_polls += 1
                if empty_polls >= MAX_EMPTY_POLLS:
                    logger.info("[replay] DLQ appears empty — stopping")
                    break
                continue
            empty_polls = 0  # reset on non-empty poll

            for msg in msgs:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"[replay] Kafka error: {msg.error()}")
                    continue

                # ── Parse DLQ envelope ────────────────────────────────────
                try:
                    payload = json.loads(msg.value().decode('utf-8'))
                except Exception as exc:
                    logger.error(
                        f"[replay] Cannot decode DLQ envelope at "
                        f"{msg.topic()}[{msg.partition()}]@{msg.offset()}: {exc}"
                    )
                    skipped += 1
                    consumer.commit(message=msg, asynchronous=False)
                    continue

                source_topic = payload.get('source_topic', '')
                raw_value    = payload.get('raw_value', '')
                retry_count  = int(payload.get('retry_count', 0)) + 1

                if not source_topic:
                    logger.warning(
                        f"[replay] DLQ envelope missing source_topic — skipping "
                        f"@{msg.offset()}"
                    )
                    skipped += 1
                    consumer.commit(message=msg, asynchronous=False)
                    continue

                # ── Max retries guard ─────────────────────────────────────
                if retry_count > MAX_RETRIES:
                    logger.error(
                        f"[replay] {source_topic} retry_count={retry_count} "
                        f"> MAX_RETRIES={MAX_RETRIES} → dead letter"
                    )
                    payload['retry_count'] = retry_count
                    _write_dead_letter(payload)
                    dead += 1
                    consumer.commit(message=msg, asynchronous=False)
                    continue

                # ── Re-produce to source topic ────────────────────────────
                raw_bytes = (
                    raw_value.encode('utf-8')
                    if isinstance(raw_value, str)
                    else b''
                )
                try:
                    producer.produce(
                        source_topic,
                        value=raw_bytes,
                        headers={'retry_count': str(retry_count).encode()},
                    )
                    replayed += 1
                    logger.info(
                        f"[replay] Re-produced to {source_topic!r} "
                        f"retry_count={retry_count}"
                    )
                except Exception as exc:
                    logger.error(
                        f"[replay] Failed to re-produce to {source_topic!r}: {exc}"
                    )
                    payload['retry_count'] = retry_count
                    _write_dead_letter(payload)
                    dead += 1

                consumer.commit(message=msg, asynchronous=False)

            producer.flush(timeout=5)

    except KeyboardInterrupt:
        logger.info("[replay] Interrupted by user")

    finally:
        consumer.close()
        producer.flush(timeout=10)
        logger.info(
            f"[replay] Complete — replayed={replayed} dead={dead} skipped={skipped}"
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Replay failed events from poc.dlq back to source topics'
    )
    parser.add_argument(
        '--from-beginning', action='store_true',
        help='Replay from the very start of poc.dlq (default: from last committed offset)',
    )
    parser.add_argument(
        '--limit', type=int, default=0,
        help='Stop after replaying this many messages (0 = no limit)',
    )
    args = parser.parse_args()
    run_replay(from_beginning=args.from_beginning, limit=args.limit)
