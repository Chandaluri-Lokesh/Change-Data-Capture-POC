"""
Idempotent MongoDB replica-set initialisation.

Run once before starting the CDC pipeline:
  python scripts/init_replica_set.py

Reads MONGO_RS_HOST from .env (default: localhost:27018).
"""
import os
import sys

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import OperationFailure

_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
load_dotenv(dotenv_path=os.path.join(_root, '.env'))

RS_HOST = os.getenv('MONGO_RS_HOST', 'localhost:27018')

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
        {"_id": "rs0", "members": [{"_id": 0, "host": RS_HOST}]},
    )
    print(f"  [OK] Replica set initialised with host={RS_HOST!r}")
except Exception as e:
    print(f"  [FAIL] {e}")
    sys.exit(1)
