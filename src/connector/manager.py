import os
import time
import json
import logging
import requests
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

CONNECT_URL = os.getenv("KAFKA_CONNECT_REST_URL", "http://localhost:8083")
CONNECTORS_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'connectors')

def register_connector(json_file_name):
    """
    Registers or updates a given Debezium Connector JSON to the Kafka Connect REST API.
    """
    path = os.path.join(CONNECTORS_DIR, json_file_name)
    with open(path, 'r') as f:
        config = json.load(f)
        
    name = config["name"]
    logging.info(f"Targeting Connector '{name}'...")
    
    # Check if it already exists
    try:
        resp = requests.get(f"{CONNECT_URL}/connectors/{name}")
        if resp.status_code == 200:
            logging.info(f"Connector '{name}' exists. Updating config IN-PLACE...")
            put_resp = requests.put(f"{CONNECT_URL}/connectors/{name}/config", json=config["config"])
            put_resp.raise_for_status()
        else:
            logging.info(f"Connector '{name}' NOT found. Registering anew...")
            post_resp = requests.post(f"{CONNECT_URL}/connectors", json=config)
            post_resp.raise_for_status()
            
        # Verify it booted up to RUNNING state properly
        wait_for_running(name)
    except requests.exceptions.ConnectionError:
        logging.error(f"Cannot reach Kafka Connect at {CONNECT_URL}. Is it down?")

def wait_for_running(name, retries=12):
    """
    Polls the Kafka Connect API waiting for both the Connector AND its Tasks to reach 'RUNNING'
    """
    for _ in range(retries):
        try:
            resp = requests.get(f"{CONNECT_URL}/connectors/{name}/status").json()
            connector_state = resp.get('connector', {}).get('state')
            tasks = resp.get('tasks', [])
            
            # Ensure the worker task itself didn't silently crash
            if connector_state == 'RUNNING' and tasks and all(t.get('state') == 'RUNNING' for t in tasks):
                logging.info(f"Connector + Tasks for '{name}' are RUNNING securely!")
                return True
                
            logging.warning(f"Waiting for '{name}'. Current state: connector={connector_state}, tasks={tasks}")
        except Exception as e:
            pass
            
        time.sleep(3)
    
    logging.error(f"Connector '{name}' failed to stabilize in RUNNING state!")
    return False

def health_check(watch=False):
    """
    Polls all existing connectors every 30 seconds and forcefully restarts them if they enter a FAILED state.
    """
    logging.info("Starting Active Health Check Watchdog...")
    while True:
        try:
            resp = requests.get(f"{CONNECT_URL}/connectors")
            if resp.status_code != 200:
                logging.error("Failed to fetch running connectors list!")
                time.sleep(10)
                continue
                
            connectors = resp.json()
            for name in connectors:
                status_resp = requests.get(f"{CONNECT_URL}/connectors/{name}/status").json()
                
                connector_state = status_resp.get('connector', {}).get('state')
                tasks = status_resp.get('tasks', [])
                
                # Check for overarching Connector failure
                if connector_state == 'FAILED':
                    logging.error(f"Connector '{name}' FAILED! Sending Restart API call...")
                    requests.post(f"{CONNECT_URL}/connectors/{name}/restart")
                    
                # Check for underlying Task failures (silent killer)
                for task in tasks:
                    if task.get('state') == 'FAILED':
                        task_id = task.get('id')
                        trace = task.get('trace', '')
                        logging.error(f"Task {task_id} for '{name}' FAILED! Trace: {trace[:100]}... Restarting...")
                        requests.post(f"{CONNECT_URL}/connectors/{name}/tasks/{task_id}/restart")
                        
        except Exception as e:
            logging.error(f"Watchdog error: {e}")
            
        if not watch:
            break
            
        time.sleep(30)

def pause_connector(name):
    """Utility to pause connector safely without dropping offsets."""
    requests.put(f"{CONNECT_URL}/connectors/{name}/pause")
    logging.info(f"Paused details for '{name}'")

def resume_connector(name):
    requests.put(f"{CONNECT_URL}/connectors/{name}/resume")
    logging.info(f"Resumed details for '{name}'")


def wait_for_connect_api(retries=30):
    """Wait for Kafka Connect REST API to become available before attempting registrations"""
    logging.info(f"Waiting for Kafka Connect REST API at {CONNECT_URL}...")
    for i in range(retries):
        try:
            resp = requests.get(CONNECT_URL)
            if resp.status_code == 200:
                logging.info("Kafka Connect REST API is UP!")
                return True
        except requests.exceptions.ConnectionError:
            pass
        logging.info(f"Still waiting for Connect API... ({i+1}/{retries})")
        time.sleep(5)
    raise Exception("Kafka Connect did not come up in time!")

if __name__ == "__main__":
    wait_for_connect_api()
    
    register_connector("orders-connector.json")
    register_connector("products-connector.json")
    
    # Run loop manually for pipeline observability
    health_check(watch=False)
