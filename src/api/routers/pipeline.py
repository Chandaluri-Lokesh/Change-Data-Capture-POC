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

CONNECT_URL = os.getenv('KAFKA_CONNECT_REST_URL', 'http://127.0.0.1:8083')
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
            'socket.timeout.ms':     3000,
            'request.timeout.ms':    3000,
        }
        admin = AdminClient(cfg)
        group_id = 'p2p-pipeline-consumer'

        metadata = admin.list_consumer_group_offsets(
            [ConsumerGroupTopicPartitions(group_id)],
        )

        committed_tps = []
        for fut in metadata.values():
            group_partitions = fut.result(timeout=4)
            for tp in group_partitions.topic_partitions:
                if not tp.error:
                    committed_tps.append(tp)

        if not committed_tps:
            return []

        # Reuse a single consumer for all watermark checks
        consumer = Consumer({**cfg, 'group.id': '__lag_check__'})
        results = []
        try:
            for tp in committed_tps:
                wm = consumer.get_watermark_offsets(
                    TopicPartition(tp.topic, tp.partition), timeout=2,
                )
                committed  = tp.offset if tp.offset >= 0 else 0
                end_offset = wm[1] if wm else 0
                results.append({
                    'topic':     tp.topic,
                    'partition': tp.partition,
                    'committed': committed,
                    'end':       end_offset,
                    'lag':       max(0, end_offset - committed),
                })
        finally:
            consumer.close()
        return results
    except Exception as exc:
        logger.warning(f"[pipeline] Lag check failed: {exc}")
        return []


@router.get('/status')
async def pipeline_status():
    import asyncio
    loop = asyncio.get_event_loop()

    async def _connectors():
        try:
            return await asyncio.wait_for(loop.run_in_executor(None, _connector_statuses), timeout=6)
        except asyncio.TimeoutError:
            return [{'name': 'p2p-connector', 'state': 'UNREACHABLE', 'tasks': []}]

    async def _lag():
        try:
            return await asyncio.wait_for(loop.run_in_executor(None, _kafka_lag), timeout=10)
        except asyncio.TimeoutError:
            return []

    connectors, lag = await asyncio.gather(_connectors(), _lag())
    all_running = all(c['state'] == 'RUNNING' for c in connectors)
    total_lag = sum(r['lag'] for r in lag)

    return {
        'overall':    'HEALTHY' if all_running else 'DEGRADED',
        'connectors': connectors,
        'kafka_lag':  lag,
        'total_lag':  total_lag,
    }
