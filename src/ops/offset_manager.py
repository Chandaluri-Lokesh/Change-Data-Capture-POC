"""
Consumer offset management and lag monitoring.

Features
────────
show_lag()          — print per-partition committed offset, high watermark, and lag.
reset_to_earliest() — reset consumer group to the beginning of every partition (full replay).
reset_to_latest()   — reset to the end of every partition (skip backlog).

Offset audit log
─────────────────
Every committed offset is appended to a local SQLite file (offsets.db) so you
have a human-readable audit trail of where each consumer stopped.

Edge cases
──────────
- StaleResumeTokenException (oplog gap): if Kafka Connect has been down longer
  than the MongoDB oplog retention window (~24 h on small deployments), Debezium
  re-snapshots.  The consumer receives a flood of op=r events.  Because all
  writes use ON CONFLICT DO UPDATE, replaying is safe but slow.  Monitor with
  show_lag() to see when the lag clears.

- Kafka topic retention: set retention.ms ≥ 604800000 (7 days) on CDC topics
  so consumer downtime does not invalidate committed offsets.  If the committed
  offset falls below the retention low-watermark, the consumer resets to earliest
  automatically (auto.offset.reset=earliest is set in kafka_consumer.py).

- reset_to_earliest / reset_to_latest: the consumer group MUST be stopped before
  resetting.  If the consumer is running, the committed offsets will be
  overwritten on the next poll.

Run
───
  python src/ops/offset_manager.py lag
  python src/ops/offset_manager.py reset-earliest
  python src/ops/offset_manager.py reset-latest
"""

import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone

_src = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _src not in sys.path:
    sys.path.insert(0, _src)

from confluent_kafka import Consumer, TopicPartition
from dotenv import load_dotenv

load_dotenv(
    dotenv_path=os.path.join(os.path.dirname(__file__), '..', '..', '.env')
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
)
logger = logging.getLogger(__name__)

BROKER   = os.getenv('KAFKA_BROKER_URL', 'localhost:9092')
GROUP_ID = 'poc-pipeline-consumer'
TOPICS   = ['poc.mydb.orders', 'poc.mydb.products']

_project_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..')
)
OFFSET_DB = os.path.join(_project_root, 'offsets.db')


# ---------------------------------------------------------------------------
# SQLite audit log
# ---------------------------------------------------------------------------

def _init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(OFFSET_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS offset_audit (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            ts        TEXT    NOT NULL,
            group_id  TEXT    NOT NULL,
            topic     TEXT    NOT NULL,
            partition INTEGER NOT NULL,
            committed INTEGER NOT NULL
        )
    """)
    conn.commit()
    return conn


def log_committed_offset(group_id: str, topic: str, partition: int, offset: int):
    """Append a committed offset record to the SQLite audit log."""
    try:
        conn = _init_db()
        conn.execute(
            "INSERT INTO offset_audit (ts, group_id, topic, partition, committed) "
            "VALUES (?, ?, ?, ?, ?)",
            (datetime.now(tz=timezone.utc).isoformat(), group_id, topic, partition, offset),
        )
        conn.commit()
        conn.close()
    except sqlite3.Error as exc:
        logger.warning(f"[offset_manager] SQLite write error: {exc}")


# ---------------------------------------------------------------------------
# Lag monitoring
# ---------------------------------------------------------------------------

def show_lag(
    broker: str = BROKER,
    group_id: str = GROUP_ID,
    topics: list = None,
) -> dict:
    """
    Print a consumer lag table and return lag data as a dict.

    Returns:
        {topic: {partition: lag}}
    """
    if topics is None:
        topics = TOPICS

    consumer = Consumer({
        'bootstrap.servers': broker,
        'group.id':          group_id,
        'enable.auto.commit': False,
    })

    lag_data: dict = {}

    print(f"\n  {'Topic':<30} {'Par':>4} {'Committed':>12} {'HWM':>12} {'Lag':>10}")
    print('  ' + '-' * 70)

    for topic in topics:
        lag_data[topic] = {}
        try:
            metadata = consumer.list_topics(topic=topic, timeout=5)
            if topic not in metadata.topics:
                print(f"  {topic}: topic not found in Kafka")
                continue

            topic_total = 0
            for pid in sorted(metadata.topics[topic].partitions.keys()):
                tp = TopicPartition(topic, pid)

                committed    = consumer.committed([tp], timeout=5)[0]
                committed_off = committed.offset if committed and committed.offset >= 0 else 0

                low, high = consumer.get_watermark_offsets(tp, timeout=5, cached=False)
                lag       = max(0, high - committed_off)
                lag_data[topic][pid] = lag
                topic_total += lag

                print(
                    f"  {topic:<30} {pid:>4} {committed_off:>12} "
                    f"{high:>12} {lag:>10}"
                )
            print(
                f"  {'  total lag [' + topic + ']:' :<60} {topic_total:>10}"
            )
        except Exception as exc:
            logger.error(f"[offset_manager] Error fetching lag for {topic!r}: {exc}")

    print()
    consumer.close()
    return lag_data


# ---------------------------------------------------------------------------
# Offset reset
# ---------------------------------------------------------------------------

def _reset(broker: str, group_id: str, topics: list, position: str) -> None:
    """
    Internal offset reset.

    position: 'earliest' | 'latest'
    WARNING: The consumer group must NOT be running during reset.
    """
    consumer = Consumer({
        'bootstrap.servers': broker,
        'group.id':          group_id,
        'enable.auto.commit': False,
        'auto.offset.reset': position,
    })

    all_tps: list[TopicPartition] = []
    for topic in topics:
        try:
            metadata = consumer.list_topics(topic=topic, timeout=5)
        except Exception as exc:
            logger.error(f"[offset_manager] Cannot fetch metadata for {topic!r}: {exc}")
            continue

        if topic not in metadata.topics:
            logger.warning(f"[offset_manager] Topic {topic!r} not found — skipping")
            continue

        for pid in metadata.topics[topic].partitions:
            all_tps.append(TopicPartition(topic, pid))

    if not all_tps:
        logger.error("[offset_manager] No partitions found — nothing to reset")
        consumer.close()
        return

    consumer.assign(all_tps)

    if position == 'earliest':
        consumer.seek_to_beginning(*all_tps)
        for tp in all_tps:
            tp.offset = 0
    else:
        for tp in all_tps:
            _, high = consumer.get_watermark_offsets(tp, timeout=5, cached=False)
            tp.offset = high
            consumer.seek(tp)

    consumer.commit(offsets=all_tps, asynchronous=False)

    for tp in all_tps:
        logger.info(
            f"[offset_manager] Reset {group_id} "
            f"{tp.topic}[{tp.partition}] → {position} (offset={tp.offset})"
        )
        log_committed_offset(group_id, tp.topic, tp.partition, tp.offset)

    consumer.close()


def reset_to_earliest(
    broker: str = BROKER,
    group_id: str = GROUP_ID,
    topics: list = None,
) -> None:
    """Reset consumer group to earliest — replays all events from the beginning."""
    _reset(broker, group_id, topics or TOPICS, 'earliest')


def reset_to_latest(
    broker: str = BROKER,
    group_id: str = GROUP_ID,
    topics: list = None,
) -> None:
    """Reset consumer group to latest — skips all backlogged events."""
    _reset(broker, group_id, topics or TOPICS, 'latest')


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'lag'

    if cmd == 'lag':
        show_lag()

    elif cmd == 'reset-earliest':
        print(
            "\nWARNING: This will replay ALL events from the beginning.\n"
            "         The consumer must be stopped first.\n"
        )
        if input("Type 'yes' to confirm: ").strip().lower() == 'yes':
            reset_to_earliest()
        else:
            print("Aborted.")

    elif cmd == 'reset-latest':
        print(
            "\nWARNING: This will skip all backlogged events.\n"
            "         The consumer must be stopped first.\n"
        )
        if input("Type 'yes' to confirm: ").strip().lower() == 'yes':
            reset_to_latest()
        else:
            print("Aborted.")

    else:
        print(
            "Usage: python src/ops/offset_manager.py "
            "[lag | reset-earliest | reset-latest]"
        )
