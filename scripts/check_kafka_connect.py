"""Verify Kafka Connect is reachable. Usage: python scripts/check_kafka_connect.py [url]"""
import sys
import requests

url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8083"
try:
    r = requests.get(f"{url}/connectors", timeout=5)
    r.raise_for_status()
    print("  [OK] Kafka Connect is reachable.")
except Exception as e:
    print(f"  [WARN] Kafka Connect not reachable: {e}")
    print("         Make sure Kafka + Kafka Connect are running in WSL first.")
    sys.exit(1)
