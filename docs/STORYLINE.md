# POC Debezium - System Storyline & Architecture

## Overview
This project is an end-to-end Change Data Capture (CDC) streaming POC. It mirrors real-time database changes from a **MongoDB** source, routes them through **Apache Kafka** using **Debezium**, transforms the events on-the-fly using a custom Python application, and reflects those changes in **PostgreSQL** target databases.

The implementation follows a structured 6-phase approach based on the `debezium_poc_full_plan`:

### Phase 1: Infrastructure Setup (Foundation)
- **Components (Windows + WSL, no Docker):** MongoDB (single-node replica set) on Windows, Kafka (**KRaft mode**, no ZooKeeper) + Kafka Connect (with Debezium MongoDB plugin) on WSL, and two Postgres instances (`analytics` and `finance`) installed locally.
- **Initialization:** MongoDB collections and schema validators (`orders`, `customers`, `products`). Postgres DDL wrapped in idempotent `IF NOT EXISTS` statements.
- **Critical Edge Cases:** Ensured MongoDB oplog size is adequate (`oplogSize: 1024`), replica set is initiated on start, and Kafka connects in the right order.

### Phase 2: Data Generator (Data In)
- **Simulator (`src/generator/simulator.py`):** Generates active traffic (orders/line items) simulating `INSERT`, `UPDATE`, `REPLACE`, and `DELETE` operations using Faker.
- **Considerations:** Addresses Python client-side ObjectId generation to guarantee duplicate-safe upserts, and mitigates clock skew by heavily relying on MongoDB server-side times where applicable.

### Phase 3: Debezium Connector (CDC Capture)
- **Connector (`connectors/orders-connector.json`):** Configured to snapshot initially and stream oplog changes. 
- **Connector Manager (`src/connector/manager.py`):** Performs registration, active health monitoring, and handles edge cases like re-registering existing connectors or dealing with Zombie tasks.

### Phase 4: Consumer, Transformer & Writer (Core Pipeline)
- **Kafka Consumer (`src/consumer/kafka_consumer.py`):** Uses manual commits and batch polling (JSON payloads; no Schema Registry required for the native setup).
- **Debezium Envelope Parser (`src/transformer/debezium_parser.py`):** Unwraps operations (`c`, `u`, `d`, `r`, `tombstone`). Maps MongoDB ISODate and `ObjectId` specifically for the target relational databases.
- **Postgres Writer (`src/writer/pg_writer.py`):** Writes data via batched connection pools (using `asyncpg` or similar). Prioritizes idempotency with `ON CONFLICT DO UPDATE` and handles delete propagations gracefully.
- **DLQ (`dlq.py`):** Captures individual or batch failures defensively.

### Phase 5: Offset Management & Recovery (Resilience)
- **Offset Tracking (`src/ops/offset_manager.py`):** Safely tracks offsets and enables resetting when replay pipelines are necessary.
- **Schema Drift (`src/transformer/schema_guard.py`):** Safeguards against missing required fields and logs unmapped metadata automatically.

### Phase 6: Verification & Observability (Validate)
- **Verification (`scripts/verify.py`):** Proof of correctness through record count parity, duplicate safety, and delete propagation tests.
- **Metrics (`src/metrics.py`):** Real-time CLI observability into consumer lag and end-to-end pipeline latency.
