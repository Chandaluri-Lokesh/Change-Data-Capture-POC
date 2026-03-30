"""Idempotent MongoDB replica-set initialisation (called by launch.bat)."""
import sys
from pymongo import MongoClient
from pymongo.errors import OperationFailure

c = MongoClient(
    "mongodb://localhost:27018/?directConnection=true",
    serverSelectionTimeoutMS=5000,
)
try:
    c.admin.command("replSetGetStatus")
    print("  [OK] Replica set already initialised.")
except OperationFailure:
    c.admin.command(
        "replSetInitiate",
        {"_id": "rs0", "members": [{"_id": 0, "host": "172.20.96.1:27018"}]},
    )
    print("  [OK] Replica set initialised.")
except Exception as e:
    print(f"  [FAIL] {e}")
    sys.exit(1)
