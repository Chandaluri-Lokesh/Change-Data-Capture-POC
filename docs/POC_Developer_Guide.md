# Change Data Capture (CDC) Pipeline — Developer Guide

**Document Type:** Technical Reference for Developers and Engineering Team
**Project:** Real-Time P2P CDC Pipeline — MongoDB → PostgreSQL + Neo4j via Debezium and Kafka
**Date:** October 2026

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Tech Stack](#2-tech-stack)
3. [Repository Structure](#3-repository-structure)
4. [Prerequisites and Environment Setup](#4-prerequisites-and-environment-setup)
5. [Module-by-Module Reference](#5-module-by-module-reference)
   - 5.1 P2P Data Simulator
   - 5.2 Debezium Connector
   - 5.3 Debezium Parser
   - 5.4 Schema Guard
   - 5.5 Mapping Engine
   - 5.6 PostgreSQL Writer
   - 5.7 Neo4j Writer
   - 5.8 Dead Letter Queue
   - 5.9 Kafka Consumer (Orchestrator)
   - 5.10 FastAPI Backend
   - 5.11 React Dashboard
   - 5.12 Connector Manager
   - 5.13 Offset Manager
   - 5.14 Scripts
6. [Database Schemas](#6-database-schemas)
7. [Latency Calculations](#7-latency-calculations)
8. [Configuration Reference](#8-configuration-reference)
9. [How to Run](#9-how-to-run)
10. [Testing](#10-testing)
11. [Extending the Pipeline](#11-extending-the-pipeline)

---

## 1. System Overview

The pipeline implements the **Change Data Capture (CDC)** pattern across a full Procure-to-Pay (P2P) document lifecycle. Instead of the application writing to multiple databases simultaneously, a separate process reads MongoDB's internal changelog and fans out changes to two downstream targets — PostgreSQL (relational) and Neo4j (graph).

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA FLOW                                        │
│                                                                               │
│  MongoDB (rs0)  ──  5 collections: rfqs, purchase_orders, asns, grns,        │
│    └─ Change Stream (oplog)           invoices                                │
│         └─ Debezium Connector (Kafka Connect :8083)                          │
│              └─ Apache Kafka broker (:9092)                                  │
│                   └─ 5 topics: poc.mydb.{rfqs|purchase_orders|asns|grns|     │
│                                         invoices}                             │
│                        └─ Python Consumer (kafka_consumer.py)                │
│                             ├─ parse    (debezium_parser.py)                 │
│                             ├─ validate (schema_guard.py)                    │
│                             ├─ route    (mapping_engine.get_pg_routes())     │
│                             ├─ write PG (pg_writer.py → 10 tables)           │
│                             ├─ write Neo4j (neo4j_writer.py)                 │
│                             ├─ metrics  (cdc_pipeline_metrics table)         │
│                             └─ DLQ      (dlq.py)                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Design Decisions

| Decision | Choice | Reason |
|---|---|---|
| Serialisation format | JSON (not Avro) | Simpler for POC; no schema registry needed |
| Capture mode | `change_streams_update_full` | Delivers full document on every update |
| Offset commit strategy | Manual, post-batch | At-least-once delivery; safe with idempotent upserts |
| Routing / field mapping | YAML-driven mapping engine | Adding a new document type = new YAML only, zero code |
| Async I/O | asyncio + asyncpg | High-throughput, non-blocking PostgreSQL writes |
| Graph target | Neo4j (async driver) | Enables cross-document relationship queries |
| Idempotency | ON CONFLICT DO UPDATE (PG) / MERGE (Neo4j) | Re-delivered events produce same result |
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
| **Python** | 3.10–3.12 | Consumer, mapping engine, writers, API |
| **PostgreSQL** | 14+ | Relational target — 10 P2P tables + metrics |
| **Neo4j** | 5.x | Graph target — 7 node types, 12 relationship types |

### Python Libraries

| Library | Used In | Purpose |
|---|---|---|
| `confluent-kafka` | consumer, dlq, metrics | Kafka client (wraps librdkafka) |
| `asyncpg` | pg_writer, api | Async PostgreSQL driver; connection pools |
| `neo4j` | neo4j_writer, api | Official Neo4j async Python driver |
| `pymongo` | p2p_simulator | MongoDB client |
| `fastapi` | api | REST API + WebSocket backend |
| `uvicorn` | api | ASGI server |
| `python-dotenv` | all modules | `.env` file loading |
| `requests` | connector/manager | Kafka Connect REST API |
| `PyYAML` | mapping_engine | Load `*_mapping.yaml` files |
| `Faker` | p2p_simulator | Realistic synthetic test data |

### Frontend

| Technology | Purpose |
|---|---|
| React 18 + TypeScript | UI framework |
| Vite 5 | Build tool and dev server |
| Tailwind CSS | Styling |
| Recharts | Latency bar chart, radar chart |
| react-force-graph-2d | Neo4j graph visualisation |
| Axios | HTTP client for API calls |
| React Router v6 | Client-side routing |

### Infrastructure

| Tool | Role |
|---|---|
| WSL2 (Ubuntu) | Kafka and Kafka Connect run in Linux environment on Windows |
| Windows 11 | Host OS; MongoDB, PostgreSQL, Neo4j, Python, Node.js run natively |
| `.env` file | Runtime configuration (ports, passwords, URLs) |

---

## 3. Repository Structure

```
Change-Data-Capture-POC/
├── src/
│   ├── consumer/
│   │   ├── kafka_consumer.py      # Main pipeline orchestrator
│   │   └── dlq.py                 # Dead Letter Queue producer
│   ├── transformer/
│   │   ├── debezium_parser.py     # Debezium envelope → ParsedEvent
│   │   └── schema_guard.py        # Field presence validation
│   ├── engine/
│   │   └── mapping_engine.py      # YAML-driven routing, PG rows, Neo4j Cypher
│   ├── writer/
│   │   ├── pg_writer.py           # asyncpg upsert/delete + metrics
│   │   └── neo4j_writer.py        # Async Neo4j MERGE/DELETE
│   ├── generator/
│   │   └── p2p_simulator.py       # P2P chain simulator (continuous loop)
│   ├── connector/
│   │   └── manager.py             # Register/delete Debezium connectors via REST
│   ├── ops/
│   │   └── offset_manager.py      # Consumer lag and offset CLI tool
│   └── api/
│       ├── main.py                # FastAPI app entrypoint, lifespan
│       └── routers/
│           ├── documents.py       # POST/DELETE/GET /api/documents/{type}
│           ├── pipeline.py        # GET /api/pipeline/status
│           ├── metrics.py         # GET /api/metrics/* + WS /ws/metrics
│           ├── graph.py           # GET /api/graph/*
│           ├── schema.py          # GET /api/schema/mongodb|postgresql|neo4j
│           └── simulator.py       # POST/GET /api/simulate/*
├── connectors/
│   └── p2p-connector.json         # Debezium connector for all 5 P2P collections
├── init/
│   ├── postgres/
│   │   ├── 01_analytics.sql       # Analytics DB DDL
│   │   ├── 02_finance.sql         # Finance DB DDL
│   │   ├── 03_metrics.sql         # cdc_pipeline_metrics table
│   │   └── 04_p2p.sql             # P2P tables (10 tables across both DBs)
│   └── neo4j/
│       └── constraints.cypher     # Uniqueness constraints for all 7 node types
├── SAP-Files-Mappings/
│   ├── sample_documents/          # Sample MongoDB objects (rfq, po, asn, grn, invoice)
│   └── mapping_rules/             # YAML mapping rules per document type
├── scripts/
│   ├── verify.py                  # Data reconciliation: MongoDB vs PG counts
│   ├── dlq_replay.py              # Re-publish DLQ events to source topic
│   └── init_replica_set.py        # Bootstrap MongoDB replica set
├── tests/                         # pytest test suite
├── cdc-dashboard-ui/              # React + Vite frontend (separate project)
├── setup.bat                      # First-time Windows setup script
├── launch.bat                     # Start all components
├── requirements.txt
└── .env
```

---

## 4. Prerequisites and Environment Setup

### 4.1 Required Software

| Software | Version | Notes |
|---|---|---|
| Python | 3.10–3.12 | 3.14 NOT supported — C-extension wheels unavailable |
| MongoDB Community | 6.x+ | Replica Set mode required |
| PostgreSQL | 14+ | |
| Neo4j Community | 5.x | |
| Node.js | 18+ | For React dashboard |
| WSL2 (Ubuntu 22.04+) | — | For Kafka / Kafka Connect |
| Java | 11 or 17 | Inside WSL: `sudo apt install openjdk-17-jdk` |
| Kafka | 2.13-4.2.0 | KRaft mode (no ZooKeeper) |
| Debezium MongoDB Plugin | 2.x | Installed by `setup_ubuntu_debezium.sh` |

### 4.2 First-Time Setup (Windows)

```bat
setup.bat
```

Verifies Python, creates `.venv`, installs `requirements.txt`, creates `mongodb-data/`, copies `.env.example` to `.env`, applies PostgreSQL DDL.

### 4.3 WSL Kafka Setup (one-time)

```bash
bash setup_ubuntu_debezium.sh
# Installs Java 17, Kafka 2.13-4.2.0 (KRaft), Debezium MongoDB plugin
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

# PostgreSQL
POSTGRES_ANALYTICS_HOST=localhost
POSTGRES_ANALYTICS_PORT=5432
POSTGRES_ANALYTICS_USER=postgres
POSTGRES_ANALYTICS_PASSWORD=password
POSTGRES_ANALYTICS_DB=analytics

POSTGRES_FINANCE_HOST=localhost
POSTGRES_FINANCE_PORT=5432
POSTGRES_FINANCE_USER=postgres
POSTGRES_FINANCE_PASSWORD=password
POSTGRES_FINANCE_DB=finance

# Neo4j
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=<password>
```

### 4.5 librdkafka IPv6 Note

On Windows + WSL2, the librdkafka client tries IPv4 first (~2s timeout) then falls back to IPv6. All Kafka client configs in this project set `'broker.address.family': 'v6'` to skip the IPv4 attempt.

---

## 5. Module-by-Module Reference

---

### 5.1 P2P Data Simulator

**File:** `src/generator/p2p_simulator.py`

Generates a realistic Procure-to-Pay document chain in MongoDB and drives continuous CDC traffic.

**Chain sequence:**

```
RFQ → Purchase Order → ASN → GRN → Invoice
```

Each step references the previous document's number (e.g., `po.rfq_number = rfq.rfq_number`). This creates the foreign-key and graph-relationship structure that the mapping engine propagates to PostgreSQL and Neo4j.

**Operation mix (continuous loop):**

```python
weights = [50, 30, 10, 10]  # chain_insert, update, delete, status_change
```

**How to run:**

```bash
python src/generator/p2p_simulator.py
```

---

### 5.2 Debezium Connector

**File:** `connectors/p2p-connector.json`

```json
{
  "name": "p2p-connector",
  "config": {
    "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
    "mongodb.connection.string": "mongodb://<WSL_IP>:27018/?replicaSet=rs0",
    "topic.prefix": "poc",
    "database.include.list": "mydb",
    "collection.include.list": "mydb.rfqs,mydb.purchase_orders,mydb.asns,mydb.grns,mydb.invoices",
    "snapshot.mode": "initial",
    "capture.mode": "change_streams_update_full",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
```

Produces 5 Kafka topics: `poc.mydb.rfqs`, `poc.mydb.purchase_orders`, `poc.mydb.asns`, `poc.mydb.grns`, `poc.mydb.invoices`.

**Debezium Envelope Structure:**

```json
{
  "op": "c",
  "ts_ms": 1711900000000,
  "after": "{\"_id\": {\"$oid\": \"abc123\"}, \"rfq_number\": \"RFQ-2026-00841\", ...}",
  "source": { ... }
}
```

- `op`: `c` (insert), `u` (update), `d` (delete), `r` (snapshot read)
- `ts_ms`: MongoDB oplog commit time — Timestamp 1 in latency tracking
- `after`: Full document post-change (null for deletes)

---

### 5.3 Debezium Parser

**File:** `src/transformer/debezium_parser.py`

Converts raw Kafka message bytes into a typed `ParsedEvent` dataclass. Unchanged from the original pipeline.

```python
@dataclass
class ParsedEvent:
    op: str            # 'c', 'u', 'd', 'r', 'tombstone', 'unknown'
    topic: str
    collection: str    # e.g. 'rfqs', 'purchase_orders'
    doc_id: str
    document: dict
    ts_ms: int
    extra_fields: dict
```

**BSON coercion** handles `$oid`, `$date`, `$numberDecimal`, `$numberLong`, `$numberInt`.

---

### 5.4 Schema Guard

**File:** `src/transformer/schema_guard.py`

Validates required fields per collection before routing. Updated for all 5 P2P collections:

```python
REQUIRED_FIELDS = {
    'rfqs':            {'rfq_number', 'status'},
    'purchase_orders': {'po_number', 'rfq_number', 'vendor_id'},
    'asns':            {'asn_number', 'po_number'},
    'grns':            {'grn_number', 'po_number'},
    'invoices':        {'invoice_number', 'po_number', 'vendor_id'},
}
```

Events with missing required fields are sent to DLQ without blocking the batch.

---

### 5.5 Mapping Engine

**File:** `src/engine/mapping_engine.py`

The central component of the P2P pipeline extension. Loads all `*_mapping.yaml` files at startup and drives all routing and field mapping decisions.

#### YAML Structure

Each mapping file has two sections:

```yaml
postgresql:
  parent_table: rfq
  upsert_key: rfq_number
  fields:
    - source: rfq_number
      column: rfq_number
      type: TEXT
    ...
  child_tables:
    - table: rfq_line_items
      source_array_field: line_items
      parent_fk: rfq_number
      fields: [...]

neo4j:
  node:
    label: RFQ
    key_property: rfq_number
    merge_cypher: "MERGE (r:RFQ {rfq_number: $rfq_number}) SET r += $props"
  relationships:
    - type: INVITED
      direction: outgoing
      target_label: Vendor
      target_key: vendor_id
      source_array_field: invited_vendors
      ...
```

#### `get_pg_routes(collection, document, op, doc_id)`

Returns a list of `(table, row_dict, upsert_key)` tuples. For inserts/updates:
- One tuple for the parent table
- One tuple per row in each child array (line items, invited vendors)

For deletes: returns delete sentinels with `_delete: True`.

#### `get_neo4j_ops(collection, document, op, doc_id)`

Returns a list of `(cypher, params)` pairs. For inserts/updates:
- One node MERGE from `neo4j.node.merge_cypher`
- One relationship MERGE per entry in `neo4j.relationships`

For deletes: returns a `MATCH (n:Label {key: $val}) DETACH DELETE n` pair.

---

### 5.6 PostgreSQL Writer

**File:** `src/writer/pg_writer.py`

All PostgreSQL I/O using `asyncpg` connection pools.

#### Generic `upsert_table(pool, table, rows, upsert_key)`

```sql
INSERT INTO {table} ({columns})
VALUES ({placeholders})
ON CONFLICT ({upsert_key}) DO UPDATE SET {col} = EXCLUDED.{col}, ...
```

Used for all P2P parent tables. The `upsert_key` and column list are derived at runtime from the mapping engine output.

#### `delete_cascade(pool, table, pk_col, pk_val)`

```sql
DELETE FROM {table} WHERE {pk_col} = $1
```

PostgreSQL foreign key CASCADE deletes handle child table cleanup automatically when the parent row is deleted.

#### `write_metric(pool, metric: dict)`

Inserts one row into `cdc_pipeline_metrics`. Exceptions are logged and swallowed — metrics failures never kill the pipeline.

---

### 5.7 Neo4j Writer

**File:** `src/writer/neo4j_writer.py`

Async Neo4j I/O using the official `neo4j` Python driver.

#### `merge_node(session, cypher, params)`

Runs the node `MERGE` Cypher verbatim from the mapping YAML. Idempotent by design — repeated events produce the same graph state.

#### `merge_relationships(session, rel_configs, document, doc_id)`

Iterates the `relationships` list from the YAML. For array-based relationships (e.g., one `ORDERS` edge per line item), expands the source array and runs one MERGE per element.

#### `delete_node(session, label, key_property, key_value)`

```cypher
MATCH (n:Label {key_property: $key_value}) DETACH DELETE n
```

Removes the node and all its relationships.

#### Retry Logic

Wraps each operation in a retry block that catches `TransientError` (deadlock, leader switch) and retries up to 3 times with exponential backoff.

---

### 5.8 Dead Letter Queue

**File:** `src/consumer/dlq.py`

Publishes failed events to a Kafka DLQ topic for inspection and replay.

DLQ topic naming: `{original_topic}.dlq` (e.g., `poc.mydb.rfqs.dlq`)

Failed events are published with headers: `error`, `source_topic`, `source_partition`, `source_offset`.

Events that go to DLQ:
- Parse errors (malformed JSON)
- Schema validation failures
- PostgreSQL write errors
- Neo4j write errors

---

### 5.9 Kafka Consumer (Orchestrator)

**File:** `src/consumer/kafka_consumer.py`

Main pipeline process. Subscribes to all 5 P2P Kafka topics and orchestrates all pipeline stages.

**Subscribed topics:**

```python
TOPICS = [
    'poc.mydb.rfqs',
    'poc.mydb.purchase_orders',
    'poc.mydb.asns',
    'poc.mydb.grns',
    'poc.mydb.invoices',
]
```

#### `_process_message()` Stages

```
1. debezium_parser.parse()       →  ParsedEvent  (or ValueError → DLQ)
2. tombstone check               →  skip if op='tombstone'
3. schema_guard.validate()       →  False → DLQ
4. mapping_engine.get_pg_routes()   →  list of (table, row, upsert_key)
5. pg_writer.upsert_table() / delete_cascade() for each PG route
6. mapping_engine.get_neo4j_ops()  →  list of (cypher, params)
7. neo4j_writer.merge_node() / merge_relationships() / delete_node()
8. write_metric() — records 4-point latency
```

#### Offset Commit Strategy

`enable.auto.commit=False`. Commits synchronously after each batch. At-least-once delivery; safe because all writes are idempotent.

---

### 5.10 FastAPI Backend

**Directory:** `src/api/`

**`main.py`** — creates asyncpg pools and Neo4j driver in the FastAPI `lifespan` context manager, shared across all request handlers via `app.state`.

#### Routers

| Router | Endpoints |
|---|---|
| `documents.py` | `POST /api/documents/{type}` — insert; `DELETE /api/documents/{type}/{id}` — delete; `GET /api/documents/{type}` — list (100 most recent, sorted by `updated_at DESC`) |
| `pipeline.py` | `GET /api/pipeline/status` — connector health + Kafka consumer lag |
| `metrics.py` | `GET /api/metrics/summary` — aggregated latency stats + table row counts; `GET /api/metrics/recent` — last N rows; `GET /api/metrics/collections` — events per collection (last hour); `GET /api/metrics/benchmarks` — throughput + full percentile breakdown; `WS /ws/metrics` — push new rows every 2s |
| `graph.py` | `GET /api/graph/{collection}/{id}` — subgraph (nodes + links); `GET /api/graph/stats/overview` — node/edge counts per label/type |
| `schema.py` | `GET /api/schema/mongodb` — collections with inferred field types; `GET /api/schema/postgresql` — tables with columns, PKs, FKs, row counts; `GET /api/schema/neo4j` — labels + properties, relationship types, constraints |
| `simulator.py` | `POST /api/simulate/chain` — single RFQ→INV chain; `POST /api/simulate/start` / `stop` — continuous background simulator; `GET /api/simulate/status` |

---

### 5.11 React Dashboard

**Directory:** `cdc-dashboard-ui/`

Seven pages, all connecting to the FastAPI backend at `http://localhost:8000`:

| Page | Route | Key Features |
|---|---|---|
| Home | `/` | Pipeline overview, 6-step P2P walkthrough, tech stack cards |
| Dashboard | `/dashboard` | Live KPI cards (WebSocket), E2E latency time-series, collection table |
| Documents | `/documents` | Insert form per doc type, right-panel document list with status badges |
| Graph | `/graph` | Force-directed Neo4j subgraph, node inspector, responsive document list grid |
| Pipeline | `/pipeline` | Connector health badges, consumer lag table, simulator start/stop |
| Schema | `/schema` | MongoDB (collections + field types), PostgreSQL (tables + columns), Neo4j (labels, relationships, constraints) — each with CRUD + exploration query blocks |
| Performance | `/performance` | Measured latency stat cards, stage bar chart (Debezium/Consumer/Write), latency radar (min/p50/p95/p99/max), operation breakdown table, scale projection table |

---

### 5.12 Connector Manager

**File:** `src/connector/manager.py`

CLI wrapper for the Kafka Connect REST API.

```bash
python src/connector/manager.py register connectors/p2p-connector.json
python src/connector/manager.py delete p2p-connector
python src/connector/manager.py status p2p-connector
python src/connector/manager.py list
```

---

### 5.13 Offset Manager

**File:** `src/ops/offset_manager.py`

```bash
python src/ops/offset_manager.py lag      # Per-partition lag across all 5 P2P topics
python src/ops/offset_manager.py reset    # Reset offsets to earliest (triggers replay)
```

---

### 5.14 Scripts

| Script | Purpose |
|---|---|
| `verify.py` | Reconciles MongoDB vs PostgreSQL row counts for all 5 collections |
| `dlq_replay.py` | Re-publishes DLQ messages to source topics for reprocessing |
| `init_replica_set.py` | Bootstraps MongoDB replica set (`rs0`) — run once |

---

## 6. Database Schemas

### PostgreSQL — analytics database

```sql
-- P2P tables (10 total)
rfq                  (rfq_number PK, requested_date, requested_by, plant, status, ...)
rfq_line_items       (rfq_number FK, line_no, material_code, quantity, uom, ...)
rfq_invited_vendors  (rfq_number FK, vendor_id, vendor_name, invited_on)
purchase_orders      (po_number PK, rfq_number FK, vendor_id, order_date, status, ...)
po_line_items        (po_number FK, line_no, material_code, quantity, unit_price, ...)
asns                 (asn_number PK, po_number FK, ship_date, carrier, status, ...)
asn_line_items       (asn_number FK, line_no, material_code, quantity_shipped, ...)
grns                 (grn_number PK, po_number FK, asn_number FK, receipt_date, ...)
grn_line_items       (grn_number FK, line_no, material_code, quantity_received, ...)
invoices             (invoice_number PK, po_number FK, grn_number FK, total_amount, ...)
invoice_line_items   (invoice_number FK, line_no, material_code, quantity, amount, ...)

-- Metrics table
cdc_pipeline_metrics (
    id               SERIAL PRIMARY KEY,
    doc_id           TEXT,
    collection       TEXT,
    operation        TEXT,       -- c | u | d | r
    doc_size_bytes   INT,
    debezium_lat_ms  INT,        -- kafka_ts_ms - mongo_ts_ms
    consumer_lat_ms  INT,        -- consumer_recv_ms - kafka_ts_ms
    write_lat_ms     INT,        -- pg_stored_ms - consumer_recv_ms
    e2e_lat_ms       INT,        -- pg_stored_ms - mongo_ts_ms
    recorded_at      TIMESTAMPTZ DEFAULT now()
)
```

### Neo4j — 7 node types, 12 relationship types

```
Node types:  RFQ, PurchaseOrder, ASN, GRN, Invoice, Vendor, Material
Relationships:
  (RFQ)-[:INVITED]→(Vendor)
  (RFQ)-[:REQUESTS]→(Material)
  (PurchaseOrder)-[:ISSUED_AGAINST]→(RFQ)
  (PurchaseOrder)-[:ISSUED_TO]→(Vendor)
  (PurchaseOrder)-[:ORDERS]→(Material)
  (ASN)-[:FULFILLS]→(PurchaseOrder)
  (ASN)-[:SHIPS]→(Material)
  (GRN)-[:RECEIVES]→(ASN)
  (GRN)-[:CONFIRMS]→(PurchaseOrder)
  (GRN)-[:RECEIVED]→(Material)
  (Invoice)-[:BILLS]→(PurchaseOrder)
  (Invoice)-[:REFERENCES]→(GRN)
```

---

## 7. Latency Calculations

Every document records four Unix epoch millisecond timestamps:

```
Timestamp 1 (mongo_ts_ms):      MongoDB oplog commit time  (from Debezium ts_ms)
Timestamp 2 (kafka_ts_ms):      Kafka broker assign time   (from msg.timestamp()[1])
Timestamp 3 (consumer_recv_ms): Python poll time           (time.time()*1000 after dequeue)
Timestamp 4 (pg_stored_ms):     PostgreSQL write complete  (time.time()*1000 after upsert)
```

**Derived latencies:**

```
debezium_lat_ms  = kafka_ts_ms      - mongo_ts_ms    (oplog → Kafka)
consumer_lat_ms  = consumer_recv_ms - kafka_ts_ms    (Kafka queue wait)
write_lat_ms     = pg_stored_ms     - consumer_recv_ms (parse → PG write)
e2e_lat_ms       = pg_stored_ms     - mongo_ts_ms    (full pipeline)
```

These are stored per-event in `cdc_pipeline_metrics` and queried by the Performance page (`/api/metrics/benchmarks`) for p50/p95/p99 breakdown.

> **Note:** All latency figures are **per-record** (time for one document to complete the full pipeline). The total time to ingest a batch of N records depends on throughput: `total_time ≈ N / events_per_second`.

---

## 8. Configuration Reference

### Kafka Consumer Tuning

| Parameter | Value | Effect |
|---|---|---|
| `BATCH_SIZE` | 100 | Max messages per `consumer.consume()` call |
| `POLL_TIMEOUT` | 1.0 s | Max time to block waiting for messages |
| `max.poll.interval.ms` | 300,000 ms | Max between polls before broker marks consumer dead |
| `session.timeout.ms` | 30,000 ms | Heartbeat timeout for group membership |

### PostgreSQL Pool Tuning

| Parameter | Value | Effect |
|---|---|---|
| `min_size` | 2 | Minimum idle connections |
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

See `docs/LOCAL_EXECUTION_GUIDE.md` for detailed step-by-step instructions.

**Quick start:**

```bat
launch.bat
```

**Manual steps:**

```bash
# 1. Start Kafka in WSL
wsl bash -c "cd ~/kafka_2.13-4.2.0 && bin/kafka-server-start.sh config/kraft/server.properties"

# 2. Start Kafka Connect in WSL
wsl bash -c "cd ~/kafka_2.13-4.2.0 && bin/connect-distributed.sh config/connect-distributed.properties"

# 3. Start MongoDB (Windows)
mongod --replSet rs0 --bind_ip localhost --port 27018 --dbpath mongodb-data

# 4. Start Neo4j (Windows service or Desktop)

# 5. Register P2P connector
python src/connector/manager.py register connectors/p2p-connector.json

# 6. Start CDC consumer
PYTHONPATH=src python src/consumer/kafka_consumer.py

# 7. Start P2P simulator
python src/generator/p2p_simulator.py

# 8. Start FastAPI backend
uvicorn src.api.main:app --reload --port 8000

# 9. Start React dashboard
cd cdc-dashboard-ui && npm run dev
```

---

## 10. Testing

```bash
python -m pytest tests/ -v
```

Test coverage:
- `debezium_parser.py`: all op codes, BSON coercion, malformed input → ValueError
- `schema_guard.py`: required field validation per collection
- `mapping_engine.py`: PG route generation, Neo4j op generation, array expansion

All test targets (parser, schema_guard, mapping_engine) are pure Python functions with no I/O dependencies.

---

## 11. Extending the Pipeline

### Adding a New MongoDB Collection

With the mapping engine, adding a new collection requires only:

1. Create `SAP-Files-Mappings/mapping_rules/{collection}_mapping.yaml`
2. Add `mydb.{collection}` to `connectors/p2p-connector.json` `collection.include.list`
3. Add required fields to `schema_guard.py` `REQUIRED_FIELDS`
4. Add PostgreSQL DDL to `init/postgres/04_p2p.sql`
5. Re-register the connector: `python src/connector/manager.py register connectors/p2p-connector.json`

No changes to `kafka_consumer.py`, `pg_writer.py`, `neo4j_writer.py`, or `router.py`.

### Migrating from JSON to Avro (Schema Registry)

1. Deploy Confluent Schema Registry
2. Change connector config: `key.converter` and `value.converter` to `io.confluent.kafka.connect.avro.AvroConverter`
3. Add `schema.registry.url` to connector config
4. Update consumer: use `confluent_kafka.schema_registry.avro.AvroDeserializer` instead of `json.loads()`
5. `debezium_parser.py`'s `_coerce_bson()` still applies to the deserialized Python dict
