# Change Data Capture (CDC) Pipeline — Developer Guide

**Document Type:** Technical Reference for Developers and Engineering Team
**Project:** Real-Time CDC Pipeline — MongoDB to PostgreSQL via Debezium and Kafka
**Date:** March 2026

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Tech Stack](#2-tech-stack)
3. [Repository Structure](#3-repository-structure)
4. [Prerequisites and Environment Setup](#4-prerequisites-and-environment-setup)
5. [Module-by-Module Reference](#5-module-by-module-reference)
   - 5.1 Data Generator
   - 5.2 Debezium Connector
   - 5.3 Debezium Parser
   - 5.4 Schema Guard
   - 5.5 Transformer
   - 5.6 Router
   - 5.7 PostgreSQL Writer
   - 5.8 Dead Letter Queue
   - 5.9 Kafka Consumer (Orchestrator)
   - 5.10 Dashboard
   - 5.11 Connector Manager
   - 5.12 Offset Manager
   - 5.13 Scripts
6. [Database Schemas](#6-database-schemas)
7. [Latency Calculations](#7-latency-calculations)
8. [Configuration Reference](#8-configuration-reference)
9. [How to Run](#9-how-to-run)
10. [Testing](#10-testing)
11. [Extending the Pipeline](#11-extending-the-pipeline)

---

## 1. System Overview

The pipeline implements the **Change Data Capture (CDC)** pattern. Instead of the application writing to multiple databases simultaneously, a separate process reads the source database's internal changelog and fans out changes to downstream targets.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DATA FLOW                                       │
│                                                                           │
│  MongoDB (rs0)                                                            │
│    └─ Change Stream (oplog)                                               │
│         └─ Debezium Connector (Kafka Connect :8083)                       │
│              └─ Apache Kafka broker (:9092)                               │
│                   └─ topic: poc.mydb.orders                               │
│                        └─ Python Consumer (kafka_consumer.py)             │
│                             ├─ parse   (debezium_parser.py)               │
│                             ├─ validate (schema_guard.py)                 │
│                             ├─ route   (router.py)                        │
│                             ├─ transform (transformer.py)                 │
│                             ├─ write   (pg_writer.py)                     │
│                             │    ├─ Analytics PG (:5434) → orders_flat    │
│                             │    └─ Finance PG   (:5433) → transactions   │
│                             ├─ metrics  (cdc_pipeline_metrics table)      │
│                             └─ DLQ      (dlq.py → poc.mydb.orders.dlq)   │
└─────────────────────────────────────────────────────────────────────────┘
```

### Key Design Decisions

| Decision | Choice | Reason |
|---|---|---|
| Serialisation format | JSON (not Avro) | Simpler for POC; no schema registry needed |
| Capture mode | `change_streams_update_full` | Delivers full document on every update, not a diff |
| Offset commit strategy | Manual, post-batch | At-least-once delivery; safe with idempotent upserts |
| Async I/O | asyncio + asyncpg | High-throughput, non-blocking PostgreSQL writes |
| Idempotency | ON CONFLICT DO UPDATE | Re-delivered events produce same result |
| Failure isolation | Row-by-row error handling | One bad row goes to DLQ; rest of batch continues |

---

## 2. Tech Stack

### Core Pipeline

| Technology | Version | Role |
|---|---|---|
| **MongoDB** | 6.x+ | Source database; replica set required for change streams |
| **Apache Kafka** | 2.13-4.2.0 (KRaft) | Message broker; no ZooKeeper |
| **Kafka Connect** | Bundled with Kafka | Debezium plugin host |
| **Debezium MongoDB Connector** | 2.x | CDC connector plugin |
| **Python** | 3.10–3.12 | Consumer, transformer, writer runtime |
| **PostgreSQL** | 14+ | Destination databases (Analytics + Finance) |

### Python Libraries

| Library | Version | Used In | Purpose |
|---|---|---|---|
| `confluent-kafka` | 2.3.0 | consumer, dlq, metrics, dashboard | Kafka client (wraps librdkafka C library) |
| `asyncpg` | 0.29.0 | pg_writer | Async PostgreSQL driver; connection pools |
| `pymongo` | 4.6.1 | simulator, dashboard | MongoDB client |
| `psycopg2-binary` | 2.9+ | dashboard | Sync PostgreSQL driver for Streamlit queries |
| `streamlit` | 1.35+ | dashboard | Live web dashboard framework |
| `python-dotenv` | 1.0.1 | all modules | `.env` file loading |
| `requests` | 2.31.0 | connector/manager, dashboard | Kafka Connect REST API |
| `Faker` | 22.5.0 | order_gen | Realistic synthetic test data |
| `fastavro` | 1.9.3 | (future) | Avro serialisation when Schema Registry is added |

### Infrastructure

| Tool | Role |
|---|---|
| WSL2 (Ubuntu) | Kafka and Kafka Connect run in Linux environment on Windows |
| Windows 11 | Host OS; MongoDB, PostgreSQL, Python run natively |
| `.env` file | Runtime configuration (ports, passwords, URLs) |

---

## 3. Repository Structure

```
poc-debezium/
├── src/
│   ├── consumer/
│   │   ├── kafka_consumer.py      # Main pipeline orchestrator
│   │   └── dlq.py                 # Dead Letter Queue producer
│   ├── transformer/
│   │   ├── debezium_parser.py     # Debezium envelope → ParsedEvent
│   │   ├── schema_guard.py        # Field presence validation
│   │   ├── transformer.py         # ParsedEvent → Postgres row dicts
│   │   └── router.py              # Routes events to (db, table, row) targets
│   ├── writer/
│   │   └── pg_writer.py           # asyncpg upsert/delete functions + metrics
│   ├── generator/
│   │   ├── order_gen.py           # Document factory functions
│   │   └── simulator.py           # Live MongoDB simulation loop
│   ├── connector/
│   │   └── manager.py             # Register/delete Debezium connectors via REST
│   ├── ops/
│   │   └── offset_manager.py      # Consumer lag and offset CLI tool
│   ├── ui/
│   │   └── dashboard.py           # Streamlit dashboard
│   └── metrics.py                 # Standalone CLI lag monitor
├── connectors/
│   └── orders-connector.json      # Debezium connector configuration
├── init/
│   └── postgres/
│       ├── 01_analytics.sql       # Analytics DB DDL (orders_flat)
│       ├── 02_finance.sql         # Finance DB DDL (transactions)
│       └── 03_metrics.sql         # Metrics table DDL
├── scripts/
│   ├── verify.py                  # Data reconciliation: MongoDB vs PG counts
│   ├── dlq_replay.py              # Re-publish DLQ events to source topic
│   ├── init_replica_set.py        # Bootstrap MongoDB replica set
│   └── check_kafka_connect.py     # Connector health check CLI
├── tests/                         # pytest test suite
├── setup.bat                      # First-time Windows setup script
├── launch.bat                     # Start all components
├── requirements.txt
└── .env                           # Runtime environment variables
```

---

## 4. Prerequisites and Environment Setup

### 4.1 Required Software

| Software | Version | Download |
|---|---|---|
| Python | 3.10–3.12 | python.org (3.14 NOT supported — C-extension wheels unavailable) |
| MongoDB Community | 6.x+ | mongodb.com |
| PostgreSQL | 14+ | postgresql.org |
| WSL2 (Ubuntu 22.04+) | — | Microsoft Store |
| Java (for Kafka in WSL) | 11 or 17 | `sudo apt install openjdk-17-jdk` |
| Kafka | 2.13-4.2.0 | kafka.apache.org |
| Debezium MongoDB Plugin | 2.x | debezium.io |

### 4.2 First-Time Setup (Windows)

```bat
# 1. Run setup.bat as Administrator
setup.bat

# This will:
#   - Verify Python 3.x is available
#   - Create .venv with all dependencies from requirements.txt
#   - Create mongodb-data/ directory
#   - Copy .env.example to .env (if .env doesn't exist)
#   - Apply PostgreSQL DDL to analytics and finance databases
```

### 4.3 WSL Kafka Setup (one-time)

```bash
# Inside WSL terminal:
bash setup_ubuntu_debezium.sh

# This installs:
#   - Java 17
#   - Kafka 2.13-4.2.0 with KRaft (no ZooKeeper)
#   - Debezium MongoDB Connector plugin into kafka/plugins/
```

### 4.4 Environment Variables (.env)

```ini
# MongoDB
MONGO_URI=mongodb://localhost:27018/?replicaSet=rs0
MONGO_DIRECT_URI=mongodb://localhost:27018/?directConnection=true
MONGO_RS_HOST=<WSL_IP>:27018

# Kafka
KAFKA_BROKER_URL=localhost:9092
KAFKA_CONNECT_REST_URL=http://localhost:8083

# PostgreSQL — Analytics (orders_flat, cdc_pipeline_metrics)
POSTGRES_ANALYTICS_HOST=localhost
POSTGRES_ANALYTICS_PORT=5434
POSTGRES_ANALYTICS_USER=postgres
POSTGRES_ANALYTICS_PASSWORD=password
POSTGRES_ANALYTICS_DB=analytics

# PostgreSQL — Finance (transactions)
POSTGRES_FINANCE_HOST=localhost
POSTGRES_FINANCE_PORT=5433
POSTGRES_FINANCE_USER=postgres
POSTGRES_FINANCE_PASSWORD=password
POSTGRES_FINANCE_DB=finance
```

### 4.5 librdkafka IPv6 Note

On Windows + WSL2, Kafka binds to both IPv4 and IPv6. The librdkafka client (used by confluent-kafka) tries IPv4 first, which fails (~2 second timeout), then falls back to IPv6. To avoid this delay, all Kafka client configs in this project explicitly set:

```python
'broker.address.family': 'v6'
```

This applies to: `kafka_consumer.py`, `dlq.py`, `metrics.py`, `dashboard.py`.

---

## 5. Module-by-Module Reference

---

### 5.1 Data Generator

**Files:** `src/generator/order_gen.py`, `src/generator/simulator.py`

**Purpose:** Generate realistic MongoDB documents and simulate a live order system with continuous inserts, updates, replaces, and deletes.

#### `order_gen.py`

```python
def generate_order() -> dict:
    # Returns a MongoDB document with fields:
    # _id: ObjectId(), customer_id: uuid4, status: "PENDING",
    # city: fake city name, total: random float 10.0–500.0
```

All order documents follow this schema. The `status` field is constrained by a MongoDB JSON Schema validator to: `["PENDING", "PAID", "SHIPPED", "CANCELLED"]`.

#### `simulator.py`

Runs a continuous loop with weighted random action selection:

```python
weights = [60, 20, 10, 10]  # insert, update, replace, delete
```

- **insert (60%):** Creates a new order using `generate_order()`, writes via `update_one(..., upsert=True)`
- **update (20%):** Picks a random active order ID, sets a new status
- **replace (10%):** Replaces the entire document with a freshly generated one (same `_id`)
- **delete (10%):** Deletes a random active order

Sleep between operations: `random.uniform(0.5, 2.0)` seconds.

**How to run:**
```bash
# From project root, with .venv active:
cd src/generator
python simulator.py
```

---

### 5.2 Debezium Connector

**File:** `connectors/orders-connector.json`

**Purpose:** Configuration for the Debezium MongoDB source connector deployed to Kafka Connect.

```json
{
  "name": "orders-connector",
  "config": {
    "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
    "mongodb.connection.string": "mongodb://<WSL_IP>:27018/?replicaSet=rs0",
    "topic.prefix": "poc",
    "database.include.list": "mydb",
    "collection.include.list": "mydb.orders",
    "snapshot.mode": "initial",
    "capture.mode": "change_streams_update_full",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
```

**Key configuration decisions:**

| Setting | Value | Effect |
|---|---|---|
| `capture.mode` | `change_streams_update_full` | On every update, Debezium delivers the **full document** (not just changed fields). Without this, `after` would be null for updates. |
| `snapshot.mode` | `initial` | On first start, Debezium reads all existing documents (snapshot), then switches to live change stream. |
| `schemas.enable` | `false` | Disables Kafka Connect's schema envelope. Messages are plain JSON — simpler for POC. |
| `topic.prefix` | `poc` | Kafka topic names become `poc.<database>.<collection>` → `poc.mydb.orders` |

**Debezium Envelope Structure** (what arrives in Kafka):

```json
{
  "op": "c",
  "ts_ms": 1711900000000,
  "after": "{\"_id\": {\"$oid\": \"abc123\"}, \"customer_id\": \"uuid\", \"status\": \"PENDING\", \"city\": \"London\", \"total\": 149.99}",
  "source": { ... }
}
```

- `op`: Operation code — `c` (create/insert), `u` (update), `d` (delete), `r` (snapshot read)
- `ts_ms`: Unix epoch milliseconds when MongoDB committed the change to its oplog — **Timestamp 1** in latency tracking
- `after`: Stringified JSON of the full document post-change (null for deletes)

**Tombstone messages:** After every delete, Debezium emits a second Kafka message with a null value (key only). This is used by Kafka for log compaction. The consumer detects `msg.value() is None` and skips silently.

**How to register the connector:**
```bash
python src/connector/manager.py register connectors/orders-connector.json
# OR via REST:
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @connectors/orders-connector.json
```

---

### 5.3 Debezium Parser

**File:** `src/transformer/debezium_parser.py`

**Purpose:** Converts raw Kafka message bytes into a typed `ParsedEvent` dataclass. Handles all Debezium op codes, BSON Extended JSON coercion, and tombstone detection.

#### ParsedEvent Dataclass

```python
@dataclass
class ParsedEvent:
    op: str            # 'c', 'u', 'd', 'r', 'tombstone', 'unknown'
    topic: str         # Full Kafka topic name
    collection: str    # Last segment of topic (e.g. 'orders')
    doc_id: str        # String form of MongoDB _id
    document: dict     # Full document (None for delete and tombstone)
    ts_ms: int         # MongoDB oplog timestamp in epoch milliseconds
    extra_fields: dict # Unknown fields (schema drift detection)
```

#### BSON Extended JSON Coercion (`_coerce_bson`)

MongoDB stores types not native to JSON. Debezium serialises them as Extended JSON objects. The parser recursively converts these:

| MongoDB Type | Wire Format | Python Result |
|---|---|---|
| ObjectId | `{"$oid": "abc123"}` | `"abc123"` (str) |
| Date | `{"$date": 1711900000000}` | `1711900000000` (int) |
| Date (long) | `{"$date": {"$numberLong": "ms"}}` | integer milliseconds |
| Decimal128 | `{"$numberDecimal": "149.99"}` | `149.99` (float) |
| Int64 | `{"$numberLong": "42"}` | `42` (int) |
| Int32 | `{"$numberInt": "42"}` | `42` (int) |

#### Key Parsing for Deletes (`_parse_key_for_id`)

For delete events, `after` is null — the document is gone. The document `_id` is only available in the Kafka message **key**. The key format is doubly-encoded JSON:

```
Kafka key bytes → decode UTF-8 → {"id": "{\"$oid\": \"abc123\"}"}
                                         └── this string is itself JSON
```

The parser handles both layers of JSON decoding and $oid coercion.

#### `parse()` Function Logic

```
msg_value is None?
  → return ParsedEvent(op='tombstone')

JSON decode msg_value → envelope dict
  op_raw = envelope['op']
  ts_ms  = envelope['ts_ms']

after_raw = envelope['after']
  if string → json.loads() → _coerce_bson() → document dict
  if dict   → _coerce_bson() directly (non-standard connector version)
  if None and op=='d' → doc_id from message key

return ParsedEvent(op, topic, collection, doc_id, document, ts_ms)
```

---

### 5.4 Schema Guard

**File:** `src/transformer/schema_guard.py`

**Purpose:** Validates that a `ParsedEvent` document contains the minimum required fields before routing. Routes events with missing required fields to the DLQ.

Required fields by collection:

```python
REQUIRED_FIELDS = {
    'orders': {'customer_id', 'status'},
}
```

`validate(event)` returns `True` if all required fields are present, `False` otherwise. Delete events (op='d') are not validated (document is None by definition).

---

### 5.5 Transformer

**File:** `src/transformer/transformer.py`

**Purpose:** Maps a `ParsedEvent` to one or more PostgreSQL row dictionaries. Contains one function per destination table.

#### `to_orders_flat(event) → dict | None`

Maps an orders event to an `analytics.orders_flat` row:

```python
{
    'order_id':    str(doc['_id']),           # MongoDB _id as string
    'customer_id': doc.get('customer_id'),    # UUID string
    'status':      doc.get('status'),         # PENDING | PAID | SHIPPED | CANCELLED
    'city':        doc.get('city'),           # String
    'total':       float(doc['total']),       # Numeric, None if missing
    'created_at':  datetime from ts_ms,       # UTC datetime
    'updated_at':  datetime from ts_ms,       # UTC datetime (same on every event)
}
```

Note: `created_at` is set from `ts_ms` on every event. The PostgreSQL upsert's `ON CONFLICT DO UPDATE` clause intentionally excludes `created_at` — so the very first event's timestamp is preserved as the creation time.

#### `to_transaction(event) → dict | None`

Maps an orders event to a `finance.transactions` row:

```python
{
    'txn_id':      f"txn-{order_id}",   # Deterministic — enables idempotent upsert
    'order_id':    order_id,
    'amount':      float(doc['total']),
    'status':      doc.get('status'),
    'recorded_at': datetime from ts_ms,
}
```

The `txn_id` is derived deterministically so that the same order always maps to the same `txn_id` — this makes `ON CONFLICT (order_id) DO UPDATE` safe for re-delivered events.

#### `_ts_to_utc(ts_ms) → datetime`

```python
def _ts_to_utc(ts_ms: int) -> datetime:
    if ts_ms:
        return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    return datetime.now(tz=timezone.utc)  # fallback if ts_ms is 0
```

Converts Debezium's `ts_ms` (Unix epoch milliseconds integer) to a Python timezone-aware UTC `datetime`. Falls back to `now()` if `ts_ms` is 0 to avoid the Unix epoch (1970-01-01) appearing in the database.

---

### 5.6 Router

**File:** `src/transformer/router.py`

**Purpose:** Given a `ParsedEvent`, returns a list of `(db_target, table, row_dict)` tuples that the consumer should write. Centralises all routing logic.

#### Routing Table

```
event.collection == 'orders':
  op in ('c', 'u', 'r'):
    → ('analytics', 'orders_flat', to_orders_flat(event))
    → ('finance',   'transactions', to_transaction(event))
  op == 'd':
    → ('analytics', 'orders_flat', {'order_id': id, '_delete': True})
    → ('finance',   'transactions', {'order_id': id, '_delete': True})

op in ('tombstone', 'unknown'):
  → []   # caller skips

unknown collection:
  → []   # logged at DEBUG
```

The `_delete: True` sentinel tells `pg_writer.py` to call `delete_order()` instead of an upsert.

---

### 5.7 PostgreSQL Writer

**File:** `src/writer/pg_writer.py`

**Purpose:** All PostgreSQL I/O. Uses `asyncpg` connection pools for non-blocking, high-throughput writes. All operations are idempotent.

#### Connection Pool

```python
async def create_pool(host, port, user, password, db,
                      min_size=2, max_size=10) -> asyncpg.Pool:
```

Two pools are created at startup: one for Analytics (port 5434), one for Finance (port 5433). Pool size: 2–10 connections each. `command_timeout=30` seconds per query.

#### Retry Wrapper (`_execute_with_retry`)

```python
async def _execute_with_retry(pool, coro_factory, retries=1):
    # On ConnectionDoesNotExistError, InterfaceError, or OSError:
    # waits 0.5s and retries once before re-raising.
```

This handles stale connections that the pool hasn't detected as dead yet.

#### `upsert_orders_flat(pool, rows)`

```sql
INSERT INTO orders_flat
    (order_id, customer_id, status, city, total, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (order_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id,
    status      = EXCLUDED.status,
    city        = EXCLUDED.city,
    total       = EXCLUDED.total,
    updated_at  = EXCLUDED.updated_at
-- Note: created_at is NOT updated on conflict — preserves original creation time
```

Uses `executemany()` inside a single transaction for batch efficiency. Accepts a list of row dicts; consumer calls it with a list of one for row-by-row error isolation.

#### `upsert_transactions(pool, rows)`

```sql
INSERT INTO transactions (txn_id, order_id, amount, status, recorded_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (order_id) DO UPDATE SET
    txn_id = EXCLUDED.txn_id,
    amount = EXCLUDED.amount,
    status = EXCLUDED.status,
    recorded_at = EXCLUDED.recorded_at
```

`order_id` has a UNIQUE constraint in the DDL, enabling `ON CONFLICT (order_id)`.

#### `delete_order(analytics_pool, finance_pool, order_id)`

Deletes concurrently from both databases using `asyncio.gather()`:

```python
await asyncio.gather(_del_analytics(), _del_finance())
# analytics: DELETE FROM orders_flat WHERE order_id = $1
# finance:   DELETE FROM transactions WHERE order_id = $1
```

Each deletion runs in its own transaction. A failure in one does not roll back the other — the caller handles partial failure by sending to DLQ.

#### `write_metric(pool, metric: dict)`

Inserts one row into `cdc_pipeline_metrics`. Any exception is logged and swallowed — a metrics write failure never kills the pipeline.

#### `ensure_metrics_table(pool)`

Creates `cdc_pipeline_metrics` and its indexes if they do not exist. Called once at consumer startup so the table auto-creates without requiring manual SQL migration.

---

### 5.8 Dead Letter Queue

**File:** `src/consumer/dlq.py`

**Purpose:** Publishes failed events to a Kafka DLQ topic so they can be inspected and replayed without blocking the main pipeline.

```python
def create_dlq_producer(broker: str) -> Producer:
    # Creates a confluent_kafka.Producer configured for DLQ use
    # 'broker.address.family': 'v6' for WSL2 IPv6 compatibility

def send_to_dlq(producer, topic, partition, offset, raw_value, error_reason):
    # Publishes raw_value to topic + '.dlq' with headers:
    #   'error': error_reason (bytes)
    #   'source_topic': topic (bytes)
    #   'source_partition': str(partition) (bytes)
    #   'source_offset': str(offset) (bytes)
```

DLQ topic naming: `{original_topic}.dlq` → `poc.mydb.orders.dlq`

Failed events that go to DLQ:
- Parse errors (malformed JSON)
- Schema validation failures (missing required fields)
- PostgreSQL write errors

---

### 5.9 Kafka Consumer (Orchestrator)

**File:** `src/consumer/kafka_consumer.py`

**Purpose:** Main pipeline process. Polls Kafka, orchestrates all pipeline stages, commits offsets, and records metrics.

#### Consumer Configuration

```python
Consumer({
    'bootstrap.servers':    BROKER,            # localhost:9092
    'group.id':             'poc-pipeline-consumer',
    'auto.offset.reset':    'earliest',        # Start from beginning if no offset
    'enable.auto.commit':   False,             # Manual commit after batch
    'max.poll.interval.ms': 300_000,           # 5 min max between polls
    'session.timeout.ms':   30_000,            # 30s heartbeat timeout
    'broker.address.family': 'v6',             # Force IPv6 (WSL2 networking)
})
```

#### Batch Processing Loop

```
while True:
    msgs = consumer.consume(num_messages=100, timeout=1.0)
    # timeout=1.0: blocks up to 1 second if no messages

    for msg in msgs:
        if msg.error():
            if PARTITION_EOF → skip (informational)
            else → log and skip

        kafka_ts_ms      = msg.timestamp()[1]      # Timestamp 2
        doc_size_bytes   = len(msg.value() or b'')
        consumer_recv_ms = int(time.time() * 1000) # Timestamp 3

        ok = await _process_message(msg, ...)
        # ok=True → written to PG or tombstone skip
        # ok=False → sent to DLQ

    if batch had real messages:
        consumer.commit(asynchronous=False)    # Synchronous commit = stronger guarantee
```

#### `_process_message()` Stages

```
1. debezium_parser.parse()  →  ParsedEvent  (or ValueError → DLQ)
2. tombstone check          →  skip if op='tombstone'
3. schema_guard.validate()  →  False → DLQ
4. router.route()           →  list of (db, table, row) or []
5. for each route:
     if _delete → delete_order()
     elif analytics/orders_flat → upsert_orders_flat()
     elif finance/transactions  → upsert_transactions()
     exception → DLQ + continue (don't block remaining routes)
6. pg_stored_ms = time.time() * 1000   # Timestamp 4
7. write_metric(analytics_pool, {...}) # All 4 timestamps + derived latencies
```

#### Offset Commit Strategy

`enable.auto.commit=False`. The consumer commits only after processing the **entire batch** — every message was either written to PG or sent to DLQ. This means:

- On crash: at most one batch is re-delivered
- Re-delivery is safe because all writes are idempotent (ON CONFLICT DO UPDATE)
- `asynchronous=False` means the commit is acknowledged by the broker before proceeding

---

### 5.10 Dashboard

**File:** `src/ui/dashboard.py`

**Purpose:** Streamlit web dashboard providing live visibility into all pipeline components, queue status, and performance metrics.

**Run:**
```bash
streamlit run src/ui/dashboard.py
```

#### Data Sources and Caching

All check functions use `@st.cache_data(ttl=5)` — cached for 5 seconds to avoid hammering services on every Streamlit re-render.

| Function | Source | What It Checks |
|---|---|---|
| `check_mongo()` | pymongo | Server info, replica set status, member states |
| `check_kafka()` | confluent-kafka Consumer | Broker reachability, CDC topic list |
| `check_connect()` | REST GET `/connectors?expand=status` | Connector and task states |
| `check_postgres()` | psycopg2 | `orders_flat` and `transactions` row counts |
| `check_lag()` | confluent-kafka Consumer | HWM, committed offset, lag per topic/partition |
| `check_metrics()` | psycopg2 | KPIs, time-series, and recent events from `cdc_pipeline_metrics` |

#### Lag Calculation

```python
lag = max(0, high_watermark_offset - committed_offset)
```

- `high_watermark_offset` (HWM): the next offset that will be written — total messages produced
- `committed_offset`: the last offset the consumer group acknowledged
- `lag`: messages produced but not yet consumed

#### Queue Status Math

```
total_produced  = sum(hwm for all partitions)       # Total in Kafka
total_committed = sum(committed for all partitions)  # Consumer has read
total_done      = cdc_pipeline_metrics row count      # Written to PG
pending         = total_produced - total_committed    # Not yet consumed
skipped         = total_committed - total_done        # Consumed but not written (tombstones)
```

#### Performance Metrics Queries

```sql
-- KPIs
SELECT
    COUNT(*)                                                    AS total_docs,
    ROUND(AVG(e2e_lat_ms))                                      AS avg_e2e_ms,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY e2e_lat_ms)) AS p50_ms,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY e2e_lat_ms)) AS p95_ms,
    ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY e2e_lat_ms)) AS p99_ms,
    ROUND(AVG(doc_size_bytes))                                  AS avg_bytes,
    ROUND(AVG(debezium_lat_ms))                                 AS avg_deb_ms,
    ROUND(AVG(consumer_lat_ms))                                 AS avg_con_ms,
    ROUND(AVG(write_lat_ms))                                    AS avg_wrt_ms,
    COUNT(*) FILTER (WHERE recorded_at > now() - interval '1 minute') AS tput_1m
FROM cdc_pipeline_metrics
```

`PERCENTILE_CONT` is PostgreSQL's ordered-set aggregate for computing exact percentiles over a sorted column.

**Catch-up time estimate:**
```python
eta_seconds = (total_lag / tput_1m) * 60
```
If lag = 500 events and throughput = 100 docs/min → eta = 300 seconds = 5 minutes.

---

### 5.11 Connector Manager

**File:** `src/connector/manager.py`

**Purpose:** CLI wrapper for the Kafka Connect REST API. Registers, deletes, and checks connector status.

```bash
python src/connector/manager.py register connectors/orders-connector.json
python src/connector/manager.py delete orders-connector
python src/connector/manager.py status orders-connector
python src/connector/manager.py list
```

Internally uses `requests` to POST/DELETE/GET `http://localhost:8083/connectors/...`.

---

### 5.12 Offset Manager

**File:** `src/ops/offset_manager.py`

**Purpose:** CLI tool for inspecting and managing Kafka consumer group offsets.

```bash
python src/ops/offset_manager.py lag      # Show lag per topic/partition
python src/ops/offset_manager.py reset    # Reset offsets to earliest (triggers replay)
```

Uses `confluent-kafka`'s `Consumer.list_topics()`, `committed()`, and `get_watermark_offsets()`.

---

### 5.13 Scripts

**Directory:** `scripts/`

| Script | Purpose | Usage |
|---|---|---|
| `verify.py` | Reconciles MongoDB vs PostgreSQL row counts; flags discrepancies | `python scripts/verify.py` |
| `dlq_replay.py` | Re-publishes DLQ messages to source topic for reprocessing | `python scripts/dlq_replay.py` |
| `init_replica_set.py` | Bootstraps MongoDB replica set (`rs0`) — run once | `python scripts/init_replica_set.py` |
| `check_kafka_connect.py` | Checks connector and task health via REST | `python scripts/check_kafka_connect.py` |

---

## 6. Database Schemas

### Analytics PostgreSQL (port 5434, database: `analytics`)

```sql
-- Orders (one row per MongoDB order document)
CREATE TABLE orders_flat (
    order_id    TEXT PRIMARY KEY,
    customer_id TEXT,
    status      TEXT,           -- PENDING | PAID | SHIPPED | CANCELLED
    city        TEXT,
    total       NUMERIC,
    created_at  TIMESTAMPTZ,
    updated_at  TIMESTAMPTZ
);

-- CDC pipeline timing metrics (one row per processed document)
CREATE TABLE cdc_pipeline_metrics (
    id               SERIAL PRIMARY KEY,
    doc_id           TEXT,
    collection       TEXT,
    operation        TEXT,       -- c | u | d | r
    doc_size_bytes   INT,        -- raw Kafka message size in bytes
    mongo_ts_ms      BIGINT,     -- Timestamp 1: MongoDB oplog commit time
    kafka_ts_ms      BIGINT,     -- Timestamp 2: Kafka broker assign time
    consumer_recv_ms BIGINT,     -- Timestamp 3: Python consumer poll time
    pg_stored_ms     BIGINT,     -- Timestamp 4: PostgreSQL write completion time
    debezium_lat_ms  INT,        -- kafka_ts_ms - mongo_ts_ms
    consumer_lat_ms  INT,        -- consumer_recv_ms - kafka_ts_ms
    write_lat_ms     INT,        -- pg_stored_ms - consumer_recv_ms
    e2e_lat_ms       INT,        -- pg_stored_ms - mongo_ts_ms
    recorded_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_cdc_metrics_recorded ON cdc_pipeline_metrics (recorded_at DESC);
CREATE INDEX idx_cdc_metrics_col      ON cdc_pipeline_metrics (collection);
```

### Finance PostgreSQL (port 5433, database: `finance`)

```sql
-- One financial transaction record per order
CREATE TABLE transactions (
    txn_id      TEXT PRIMARY KEY,   -- "txn-{order_id}" deterministic
    order_id    TEXT UNIQUE,        -- UNIQUE enables ON CONFLICT (order_id)
    amount      NUMERIC,
    status      TEXT,
    recorded_at TIMESTAMPTZ
);
```

---

## 7. Latency Calculations

Every document that completes the pipeline records four Unix epoch millisecond timestamps:

```
Timestamp 1 (mongo_ts_ms):      MongoDB oplog commit time
  └─ captured from: Debezium envelope field ts_ms

Timestamp 2 (kafka_ts_ms):      Kafka broker message timestamp
  └─ captured from: msg.timestamp()[1]  (librdkafka)

Timestamp 3 (consumer_recv_ms): Python consumer poll time
  └─ captured from: int(time.time() * 1000)  immediately after msg is dequeued

Timestamp 4 (pg_stored_ms):     PostgreSQL write completion time
  └─ captured from: int(time.time() * 1000)  after await upsert_* returns
```

**Derived latencies** (all values are `max(0, ...)` to guard against clock skew):

```
debezium_lat_ms  = kafka_ts_ms      - mongo_ts_ms
  └─ Time for Debezium to read oplog change and publish to Kafka

consumer_lat_ms  = consumer_recv_ms - kafka_ts_ms
  └─ Time message spent waiting in Kafka queue before consumer polled it

write_lat_ms     = pg_stored_ms     - consumer_recv_ms
  └─ Time to parse + validate + route + write to PostgreSQL

e2e_lat_ms       = pg_stored_ms     - mongo_ts_ms
  └─ Total pipeline latency: MongoDB commit → PostgreSQL storage
  └─ Invariant: e2e = debezium + consumer + write  (approximately; clock drift may cause tiny rounding)
```

**PostgreSQL statistical queries used in dashboard:**

```sql
-- P50 (median)
PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY e2e_lat_ms)

-- P95
PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY e2e_lat_ms)

-- P99
PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY e2e_lat_ms)
```

`PERCENTILE_CONT` uses linear interpolation between adjacent sorted values. For n=1 it returns the only value. For large n it is accurate to the dataset.

---

## 8. Configuration Reference

### Kafka Consumer Tuning

| Parameter | Value | Effect |
|---|---|---|
| `BATCH_SIZE` | 100 | Max messages per `consumer.consume()` call |
| `POLL_TIMEOUT` | 1.0 s | Max time to block waiting for messages |
| `max.poll.interval.ms` | 300,000 ms | Max time between polls before broker considers consumer dead |
| `session.timeout.ms` | 30,000 ms | Heartbeat timeout for group membership |

### PostgreSQL Pool Tuning

| Parameter | Value | Effect |
|---|---|---|
| `min_size` | 2 | Minimum idle connections kept open |
| `max_size` | 10 | Maximum concurrent connections |
| `command_timeout` | 30 s | Per-query timeout |

### Debezium Connector Tuning

| Parameter | Default | Notes |
|---|---|---|
| `snapshot.mode` | `initial` | Change to `never` to skip snapshot after first run |
| `capture.mode` | `change_streams_update_full` | Required for full document on updates |
| `poll.interval.ms` | 500 ms | How often Debezium polls the change stream |
| `max.batch.size` | 2048 | Max events per Debezium poll batch |

---

## 9. How to Run

### Full pipeline startup (Windows):

```bat
launch.bat
```

This script:
1. Activates `.venv`
2. Starts MongoDB (Docker or local service)
3. Starts Kafka broker in WSL (polls :9092 for readiness)
4. Starts Kafka Connect in WSL (polls :8083 for readiness)
5. Registers the orders Debezium connector via REST
6. Starts the Python consumer in a new terminal
7. Starts the data simulator in a new terminal
8. Starts the Streamlit dashboard in a new terminal

### Manual component startup (for debugging):

```bash
# 1. Activate virtualenv
.venv\Scripts\activate

# 2. Start Kafka in WSL
wsl bash -c "cd ~/kafka_2.13-4.2.0 && bin/kafka-server-start.sh config/server.properties"

# 3. Start Kafka Connect in WSL
wsl bash -c "cd ~/kafka_2.13-4.2.0 && bin/connect-distributed.sh config/connect-distributed.properties"

# 4. Register connector
python src/connector/manager.py register connectors/orders-connector.json

# 5. Start consumer
PYTHONPATH=src python src/consumer/kafka_consumer.py

# 6. Start simulator
cd src/generator && python simulator.py

# 7. Start dashboard
streamlit run src/ui/dashboard.py
```

### Useful one-liners:

```bash
# Check connector status
python scripts/check_kafka_connect.py

# Check consumer lag
python src/ops/offset_manager.py lag

# Verify MongoDB vs PG counts
python scripts/verify.py

# Replay DLQ events
python scripts/dlq_replay.py

# Run tests
python -m pytest tests/ -v
```

---

## 10. Testing

**Directory:** `tests/`

Tests use `pytest`. The test suite covers:
- `debezium_parser.py`: parse all op codes, BSON coercion, malformed input → ValueError
- `transformer.py`: field mapping, None handling, ts_ms conversion
- `router.py`: routing rules, delete sentinels, tombstone → []
- `schema_guard.py`: required field validation

**Run:**
```bash
python -m pytest tests/ -v
```

**No mocked database in tests** — parser, transformer, router, and schema_guard are pure Python functions with no I/O dependencies. They can be tested with plain dict inputs.

For integration tests (pg_writer, kafka_consumer), a real PostgreSQL instance is required.

---

## 11. Extending the Pipeline

### Adding a New MongoDB Collection

1. Create a new Debezium connector JSON in `connectors/` targeting the new collection
2. Add required fields to `schema_guard.py`'s `REQUIRED_FIELDS` dict
3. Add transformer function(s) in `transformer.py`
4. Add routing rules in `router.py`
5. Add writer function(s) in `pg_writer.py`
6. Add handler in `kafka_consumer.py`'s `_process_message()` write block
7. Add DDL to `init/postgres/`
8. Register connector: `python src/connector/manager.py register connectors/new-connector.json`

### Adding a New Destination Field

If a new field is added to MongoDB order documents:

1. Add column to `init/postgres/01_analytics.sql` DDL (`ALTER TABLE ... ADD COLUMN`)
2. Add field mapping in `transformer.py`'s `to_orders_flat()`
3. Add column to the `INSERT` statement in `pg_writer.py`'s `upsert_orders_flat()`
4. Update `ON CONFLICT DO UPDATE SET` clause in the same function

### Migrating from JSON to Avro (Schema Registry)

1. Deploy Confluent Schema Registry
2. Change connector config: `key.converter` and `value.converter` to `io.confluent.kafka.connect.avro.AvroConverter`
3. Add `schema.registry.url` to connector config
4. Update consumer: use `confluent_kafka.schema_registry.avro.AvroDeserializer` instead of `json.loads()`
5. `debezium_parser.py`'s `_coerce_bson()` still applies — Avro carries the deserialized Python dict
