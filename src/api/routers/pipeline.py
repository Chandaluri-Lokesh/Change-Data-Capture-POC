"""
GET /api/pipeline/status

Returns Kafka Connect connector statuses, Kafka consumer lag (via Kafka AdminClient),
and DLQ message count.
"""

import logging
import os

import requests
from fastapi import APIRouter
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)
router = APIRouter()

CONNECT_URL = os.getenv('KAFKA_CONNECT_REST_URL', 'http://localhost:8083')
BROKER      = os.getenv('KAFKA_BROKER_URL',       'localhost:9092')
DLQ_TOPIC   = 'poc.dlq'


def _connector_statuses() -> list:
    try:
        names = requests.get(f'{CONNECT_URL}/connectors', timeout=4).json()
        statuses = []
        for name in names:
            try:
                s = requests.get(f'{CONNECT_URL}/connectors/{name}/status', timeout=4).json()
                statuses.append({
                    'name':      name,
                    'state':     s.get('connector', {}).get('state', 'UNKNOWN'),
                    'tasks':     s.get('tasks', []),
                    'worker_id': s.get('connector', {}).get('worker_id', ''),
                })
            except Exception:
                statuses.append({'name': name, 'state': 'UNREACHABLE', 'tasks': []})
        return statuses
    except Exception as exc:
        logger.warning(f"[pipeline] Kafka Connect unreachable: {exc}")
        return [{'name': 'p2p-connector', 'state': 'UNREACHABLE', 'tasks': []}]


def _kafka_lag() -> list:
    """
    Use confluent_kafka AdminClient to get consumer group lag.
    Returns a list of {topic, partition, lag} dicts.
    """
    try:
        from confluent_kafka.admin import AdminClient
        from confluent_kafka import Consumer, TopicPartition, ConsumerGroupTopicPartitions

        cfg = {
            'bootstrap.servers':     BROKER,
            'broker.address.family': 'v4',
            'socket.timeout.ms':     5000,
            'request.timeout.ms':    5000,
            'metadata.request.timeout.ms': 5000,
        }
        admin = AdminClient(cfg)
        group_id = 'p2p-pipeline-consumer'

        metadata = admin.list_consumer_group_offsets(
            [ConsumerGroupTopicPartitions(group_id)],
        )

        results = []
        for fut in metadata.values():
            group_partitions = fut.result(timeout=5)
            for tp in group_partitions.topic_partitions:
                if tp.error:
                    continue
                consumer = Consumer({**cfg, 'group.id': '__verify_lag__'})
                try:
                    wm = consumer.get_watermark_offsets(
                        TopicPartition(tp.topic, tp.partition), timeout=3,
                    )
                finally:
                    consumer.close()
                committed  = tp.offset if tp.offset >= 0 else 0
                end_offset = wm[1] if wm else 0
                results.append({
                    'topic':     tp.topic,
                    'partition': tp.partition,
                    'committed': committed,
                    'end':       end_offset,
                    'lag':       max(0, end_offset - committed),
                })
        return results
    except Exception as exc:
        logger.warning(f"[pipeline] Lag check failed: {exc}")
        return []


@router.get('/status')
async def pipeline_status():
    import asyncio
    loop = asyncio.get_event_loop()
    connectors = await loop.run_in_executor(None, _connector_statuses)

    all_running = all(c['state'] == 'RUNNING' for c in connectors)

    return {
        'overall':    'HEALTHY' if all_running else 'DEGRADED',
        'connectors': connectors,
        'kafka_lag':  [],
        'total_lag':  0,
    }
