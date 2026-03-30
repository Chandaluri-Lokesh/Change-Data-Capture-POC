import os
import time
import random
import logging
from pymongo import MongoClient
from pymongo.errors import OperationFailure
from order_gen import generate_order

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Hardcoded direct connection for Windows host execution to avoid Docker Bridge DNS errors
DIRECT_LOCAL_URI = os.getenv("MONGO_DIRECT_URI", "mongodb://localhost:27018/?directConnection=true")
DB_NAME = "mydb"

def setup_mongodb():
    logging.info("Attempting direct connection to MongoDB to initialize Replica Set...")
    
    # Wait for Mongo to physically boot
    for _ in range(30):
        try:
            client = MongoClient(DIRECT_LOCAL_URI, serverSelectionTimeoutMS=2000)
            client.admin.command('ping')
            break
        except Exception:
            logging.info("Waiting for mongodb to accept connections...")
            time.sleep(2)

    try:
        # Check if already initialized
        client.admin.command('replSetGetStatus')
        logging.info("Replica Set is already initialized.")
    except OperationFailure:
        mongo_rs_host = os.getenv("MONGO_RS_HOST", "172.20.96.1:27018")
        logging.info(f"Initializing Replica Set natively to: {mongo_rs_host}...")
        # For Windows + WSL native mode, Debezium connects to Mongo via localhost.
        config = {
            "_id": "rs0",
            "members": [{"_id": 0, "host": mongo_rs_host}]
        }
        client.admin.command("replSetInitiate", config)
        time.sleep(5)

    db = client[DB_NAME]
    collections = db.list_collection_names()
    
    if "orders" not in collections:
        db.create_collection("orders", validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["customer_id", "status"],
                "properties": {
                    "customer_id": {"bsonType": "string"},
                    "status": {"enum": ["PENDING", "PAID", "SHIPPED", "CANCELLED"]},
                    "total": {"bsonType": "number"},
                    "city": {"bsonType": "string"}
                }
            }
        })
    if "customers" not in collections:
        db.create_collection("customers")
        
    logging.info("MongoDB Configured Perfectly from Windows Host!")

def run_simulation():
    setup_mongodb()
    
    # Connect directly to bypass replica set topology checks during inserts!
    client = MongoClient(DIRECT_LOCAL_URI)
    db = client[DB_NAME]
    orders_col = db['orders']

    logging.info("Seeding initial docs...")
    seeded_orders = [generate_order() for _ in range(10)]
    try: orders_col.insert_many(seeded_orders, ordered=False)
    except Exception: pass
    
    active_order_ids = [o['_id'] for o in seeded_orders]
    logging.info("Starting live streaming simulation loops right here on Windows!")
    
    while True:
        action = random.choices(
            ['insert', 'update', 'replace', 'delete'], 
            weights=[60, 20, 10, 10]
        )[0]
        try:
            if action == 'insert':
                new_order = generate_order()
                orders_col.update_one({'_id': new_order['_id']}, {'$set': new_order}, upsert=True)
                active_order_ids.append(new_order['_id'])
                logging.info(f"[INSERT] Order {new_order['_id']}")
            elif action == 'update' and active_order_ids:
                oid = random.choice(active_order_ids)
                new_status = random.choice(["PAID", "SHIPPED", "CANCELLED"])
                orders_col.update_one({'_id': oid}, {'$set': {'status': new_status}})
                logging.info(f"[UPDATE] Order {oid}")
            elif action == 'replace' and active_order_ids:
                oid = random.choice(active_order_ids)
                replacement = generate_order()
                replacement['_id'] = oid
                orders_col.replace_one({'_id': oid}, replacement)
                logging.info(f"[REPLACE] Order {oid}")
            elif action == 'delete' and active_order_ids:
                oid = random.choice(active_order_ids)
                active_order_ids.remove(oid)
                orders_col.delete_one({'_id': oid})
                logging.info(f"[DELETE] Order {oid}")
        except Exception as e:
            logging.error(f"Error during {action}: {e}")
            
        time.sleep(random.uniform(0.5, 2.0))

if __name__ == "__main__":
    run_simulation()
