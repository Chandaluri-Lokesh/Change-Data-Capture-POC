"""
Kafka Connect connector manager for the P2P CDC pipeline.

Registers the p2p-connector (watches rfqs, purchase_orders, asns, grns, invoices).
Polls all connectors every 30 s and auto-restarts any FAILED connector/task.

Run
───
  python src/connector/manager.py
"""

import json
import logging
import os
import re
import time

import requests
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s')
logger = logging.getLogger(__name__)

CONNECT_URL    = os.getenv('KAFKA_CONNECT_REST_URL', 'http://localhost:8083')
CONNECTORS_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'connectors')

# Connectors to register on startup
CONNECTOR_FILES = ['p2p-connector.json']

_PLACEHOLDER_RE = re.compile(r'\$\{(\w+)\}')


def _expand_env(value: str) -> str:
    """Replace ${VAR} placeholders with values from the environment."""
    def _sub(m):
        var = m.group(1)
        resolved = os.getenv(var)
        if resolved is None:
            logger.warning(f"Env var '{var}' referenced in connector config is not set.")
            return m.group(0)
        return resolved
    return _PLACEHOLDER_RE.sub(_sub, value)


def _inject_env(config: dict) -> dict:
    """Recursively expand ${VAR} placeholders in all string values."""
    return {k: (_inject_env(v) if isinstance(v, dict) else _expand_env(v) if isinstance(v, str) else v)
            for k, v in config.items()}


def register_connector(json_file_name: str, retries: int = 3) -> None:
    path = os.path.join(CONNECTORS_DIR, json_file_name)
    if not os.path.exists(path):
        logger.error(f"Connector file not found: {path}")
        return

    with open(path, 'r') as f:
        config = json.load(f)
    config['config'] = _inject_env(config['config'])
    name = config['name']

    logger.info(f"Targeting connector '{name}'...")
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(f'{CONNECT_URL}/connectors/{name}', timeout=10)
            if resp.status_code == 200:
                logger.info(f"Connector '{name}' exists — updating config in-place...")
                r = requests.put(
                    f'{CONNECT_URL}/connectors/{name}/config',
                    json=config['config'], timeout=60,
                )
            else:
                logger.info(f"Connector '{name}' not found — registering new...")
                r = requests.post(f'{CONNECT_URL}/connectors', json=config, timeout=60)
            r.raise_for_status()
            wait_for_running(name)
            return
        except requests.exceptions.ConnectionError:
            logger.error(f"Cannot reach Kafka Connect at {CONNECT_URL}. Is it running?")
            return
        except requests.exceptions.Timeout:
            logger.warning(
                f"Request timed out registering '{name}' (attempt {attempt}/{retries})."
            )
            if attempt < retries:
                time.sleep(5)
        except requests.exceptions.HTTPError as exc:
            logger.error(f"HTTP error registering '{name}': {exc}")
            return
    logger.error(f"Failed to register '{name}' after {retries} attempts.")


def wait_for_running(name: str, retries: int = 15) -> bool:
    for _ in range(retries):
        try:
            resp = requests.get(
                f'{CONNECT_URL}/connectors/{name}/status', timeout=10
            ).json()
            conn_state = resp.get('connector', {}).get('state')
            tasks      = resp.get('tasks', [])
            if conn_state == 'RUNNING' and tasks and all(
                t.get('state') == 'RUNNING' for t in tasks
            ):
                logger.info(f"Connector '{name}' is RUNNING.")
                return True
            logger.info(
                f"Waiting for '{name}'… connector={conn_state} tasks={tasks}"
            )
        except Exception as exc:
            logger.warning(f"[wait_for_running] Poll error for '{name}': {exc}")
        time.sleep(3)
    logger.error(f"Connector '{name}' did not reach RUNNING state.")
    return False


def health_check(watch: bool = True) -> None:
    """Poll all connectors every 30 s; restart any that are FAILED."""
    logger.info("Starting connector health-check watchdog...")
    while True:
        try:
            resp = requests.get(f'{CONNECT_URL}/connectors', timeout=10)
            if resp.status_code != 200:
                logger.error("Failed to fetch connector list.")
                time.sleep(10)
                continue

            for name in resp.json():
                try:
                    status = requests.get(
                        f'{CONNECT_URL}/connectors/{name}/status', timeout=10
                    ).json()
                    if status.get('connector', {}).get('state') == 'FAILED':
                        logger.error(f"Connector '{name}' FAILED — restarting...")
                        requests.post(
                            f'{CONNECT_URL}/connectors/{name}/restart', timeout=10
                        )
                    for task in status.get('tasks', []):
                        if task.get('state') == 'FAILED':
                            tid   = task.get('id')
                            trace = task.get('trace', '')[:120]
                            logger.error(
                                f"Task {tid} of '{name}' FAILED — restarting. Trace: {trace}"
                            )
                            requests.post(
                                f'{CONNECT_URL}/connectors/{name}/tasks/{tid}/restart',
                                timeout=10,
                            )
                except Exception as exc:
                    logger.warning(f"Health-check error for '{name}': {exc}")
        except Exception as exc:
            logger.error(f"Watchdog error: {exc}")

        if not watch:
            break
        time.sleep(30)


def wait_for_connect_api(retries: int = 30) -> None:
    logger.info(f"Waiting for Kafka Connect REST API at {CONNECT_URL}...")
    for i in range(retries):
        try:
            if requests.get(CONNECT_URL, timeout=5).status_code == 200:
                logger.info("Kafka Connect REST API is UP.")
                return
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            pass
        logger.info(f"Still waiting… ({i+1}/{retries})")
        time.sleep(5)
    raise RuntimeError("Kafka Connect did not become available in time.")


if __name__ == '__main__':
    wait_for_connect_api()
    for f in CONNECTOR_FILES:
        register_connector(f)
    health_check(watch=True)
