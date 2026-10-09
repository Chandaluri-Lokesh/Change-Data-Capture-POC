# Software Requirements Specification (SRS)

**Project:** Change Data Capture (CDC) Pipeline — Procure-to-Pay (P2P) Edition
**Document Version:** 1.0
**Date:** September 2026
**Branch:** neo4j

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Overall Description](#2-overall-description)
3. [Software and Environment Stack](#3-software-and-environment-stack)
4. [System Architecture](#4-system-architecture)
5. [Functional Requirements](#5-functional-requirements)
6. [Non-Functional Requirements](#6-non-functional-requirements)
7. [Data Requirements](#7-data-requirements)
8. [External Interface Requirements](#8-external-interface-requirements)
9. [Constraints and Assumptions](#9-constraints-and-assumptions)
10. [Glossary](#10-glossary)

---

## 1. Introduction

### 1.1 Purpose

This document specifies the software requirements for a Change Data Capture (CDC) proof-of-concept system that streams Procure-to-Pay (P2P) business documents from a MongoDB source database to two downstream analytical stores — a relational PostgreSQL database and a property graph database (Neo4j) — in real time.

### 1.2 Scope

The system, referred to as **CDC-P2P-POC**, demonstrates the full end-to-end CDC pattern for a five-document P2P procurement chain:

```
RFQ → Purchase Order → Advance Shipping Notice (ASN) → Goods Receipt Note (GRN) → Invoice
```

Changes committed to MongoDB are automatically captured by Debezium, streamed through Apache Kafka, consumed by a Python pipeline, and written to both PostgreSQL (for relational analytics) and Neo4j (for relationship/graph traversal). A React/FastAPI web application provides a live observability dashboard, document management interface, and graph visualization.

### 1.3 Intended Audience

- Academic evaluators / project examiners
- Developers maintaining or extending the system
- Engineers evaluating CDC architecture patterns

### 1.4 Document Conventions

- **SHALL** denotes a mandatory requirement.
- **SHOULD** denotes a recommended but non-mandatory behavior.
- Version strings follow the format used in official release notes (e.g., `2.13-4.2.0` is the Kafka Scala-version/release-version format).

---

## 2. Overall Description

### 2.1 Product Perspective

CDC-P2P-POC is a standalone pipeline POC, not a component of a production ERP. It is designed to validate the following architectural hypotheses:

1. MongoDB change streams can be reliably captured by Debezium without application-level dual-writes.
2. A single Python consumer can fan out to both relational (PostgreSQL) and graph (Neo4j) stores transactionally within the same processing loop.
3. A YAML-driven mapping engine can eliminate per-collection hardcoded transformer functions.
4. Sub-second end-to-end latency (MongoDB commit → PostgreSQL storage) is achievable on local hardware.

### 2.2 Product Functions (Summary)

| Function | Description |
|---|---|
| CDC Capture | Capture insert, update, replace, and delete events from MongoDB via Debezium |
| Event Streaming | Buffer and deliver events over Apache Kafka topics |
| Event Processing | Parse, validate, transform, and route events in a Python consumer |
| Relational Persistence | Upsert/delete P2P documents into 11 PostgreSQL tables |
| Graph Persistence | Merge P2P documents and relationships into Neo4j |
| Dead Letter Queue | Isolate and preserve failed events for later replay |
| Observability API | Expose pipeline metrics, connector status, and Kafka lag via REST and WebSocket |
| Web Dashboard | Live KPI cards, latency charts, document insertion UI, and graph visualization |
| P2P Simulation | Generate realistic P2P document chains into MongoDB for testing |

### 2.3 User Classes

| User | Interaction |
|---|---|
| Developer | Runs pipeline, inspects logs, extends mapping YAML, replays DLQ |
| Examiner | Views web dashboard, reviews architecture, reads documentation |
| Simulator | Automated test-data generator (`p2p_simulator.py`), no human interaction |

### 2.4 Operating Environment

The system runs on a single developer workstation using a split-host strategy:

| Layer | Host |
|---|---|
| MongoDB, PostgreSQL, Neo4j, Python, FastAPI, Node.js | Windows 11 (native) |
| Apache Kafka (KRaft), Kafka Connect, Debezium | WSL2 Ubuntu (Linux subsystem on Windows) |

---

## 3. Software and Environment Stack

### 3.1 Host Operating System

| Component | Value |
|---|---|
| OS Name | Windows 11 Pro |
| OS Build | 10.0.26200 |
| Architecture | x86-64 |
| WSL Version | WSL2 |
| WSL Distribution | Ubuntu 22.04 LTS |
| WSL Kernel | Linux (Microsoft WSL2 kernel) |

### 3.2 Runtime Environments

| Software | Version | Host | Purpose |
|---|---|---|---|
| **Python** | 3.12 (3.10–3.12 supported) | Windows | Consumer, API, generator, scripts |
| **Node.js** | 18+ LTS | Windows | React/Vite frontend build toolchain |
| **npm** | 9+ | Windows | Frontend package management |
| **Java (JDK)** | 17 (OpenJDK) | WSL2 Ubuntu | Kafka and Kafka Connect runtime |

> Note: Python 3.14 is NOT supported — C-extension wheels for `confluent-kafka` and `asyncpg` are unavailable for that version.

### 3.3 Databases

| Database | Version | Host | Port | Role |
|---|---|---|---|---|
| **MongoDB Community** | 6.x+ | Windows | 27018 | CDC source; replica set `rs0` |
| **PostgreSQL** | 14+ | Windows | 5432 | Analytics destination (11 P2P tables + metrics) |
| **Neo4j Desktop** | 5.x | Windows | 7687 (Bolt) | Property graph destination |

### 3.4 Messaging Infrastructure

| Software | Version | Host | Port | Role |
|---|---|---|---|---|
| **Apache Kafka** | 2.13-4.2.0 (KRaft mode) | WSL2 Ubuntu | 9092 | Message broker (no ZooKeeper) |
| **Kafka Connect** | Bundled with Kafka 4.2.0 | WSL2 Ubuntu | 8083 | Debezium plugin host |
| **Debezium MongoDB Connector** | 2.x | WSL2 Ubuntu (plugin) | — | CDC source connector |

### 3.5 Python Libraries

| Library | Version | Purpose |
|---|---|---|
| `confluent-kafka` | 2.3.0 | Kafka consumer and producer client (wraps librdkafka) |
| `pymongo` | 4.6.1 | MongoDB client for simulator and API |
| `asyncpg` | 0.29.0 | Async PostgreSQL driver for the write path |
| `psycopg2-binary` | >=2.9.0 | Sync PostgreSQL driver (used by FastAPI routes) |
| `neo4j` | >=5.20.0 | Official Neo4j async Python driver |
| `fastapi` | >=0.110.0 | REST API and WebSocket server framework |
| `uvicorn[standard]` | >=0.29.0 | ASGI server for FastAPI |
| `websockets` | >=12.0 | WebSocket support |
| `pyyaml` | >=6.0 | YAML mapping rule loader |
| `python-dotenv` | 1.0.1 | `.env` file loading at startup |
| `Faker` | 22.5.0 | Realistic synthetic P2P data generation |
| `requests` | 2.31.0 | Kafka Connect REST API calls |
| `httpx` | >=0.27.0 | Async HTTP client for FastAPI routes |

### 3.6 Frontend Libraries

| Library | Version | Purpose |
|---|---|---|
| **React** | 18+ | UI component framework |
| **Vite** | 5+ | Frontend build tool and dev server |
| **TypeScript** | 5+ | Typed JavaScript for all frontend code |
| **Recharts** | 2+ | Latency and throughput charts on Dashboard |
| **react-force-graph-2d** | Latest | Force-directed graph visualization (Neo4j view) |
| **Axios** | 1+ | HTTP client for API calls |

### 3.7 Development and Tooling

| Tool | Version | Purpose |
|---|---|---|
| **Git** | 2.x | Version control |
| **pytest** | 9+ | Python unit and integration test runner |
| **pyproject.toml** | — | Python project metadata |

---

## 4. System Architecture

### 4.1 Data Flow Diagram

```
MongoDB (rs0) :27018 — Windows
    |
    |  oplog / change stream
    v
Debezium MongoDB Connector (Kafka Connect :8083) — WSL2
    |
    |  Debezium envelope (JSON)
    v
Apache Kafka :9092 — WSL2
    |  Topics: poc.mydb.[rfqs | purchase_orders | asns | grns | invoices]
    v
Python Kafka Consumer (kafka_consumer.py) — Windows
    |
    |-- debezium_parser.py     (parse envelope, coerce BSON)
    |-- schema_guard.py        (validate required fields)
    |-- mapping_engine.py      (load YAML rules)
    |-- router.py              (route to PG + Neo4j ops)
    |
    |---> PostgreSQL :5432 (analytics DB)
    |       11 P2P tables + cdc_pipeline_metrics
    |
    |---> Neo4j :7687 (bolt)
    |       7 node types + 13 relationship types
    |
    \---> poc.dlq (Kafka DLQ topic — failed events)

FastAPI :8000 — Windows
    |-- REST API  (/api/*)
    |-- WebSocket (/ws/metrics — polls every 2s)
    \-- Static files (web/dist — production build)

React/Vite :5173 (dev) — Windows
    \-- Dashboard, Documents, Graph, Pipeline pages
```

### 4.2 Deployment Topology

```
+--------------------Windows 11 (Host)--------------------+
|                                                          |
|  MongoDB :27018        PostgreSQL :5432                  |
|  Neo4j Desktop :7687   FastAPI :8000                     |
|  Python consumer       Node.js / Vite :5173              |
|                                                          |
|  +------ WSL2 (Ubuntu 22.04) --------+                  |
|  |  Kafka KRaft :9092                |                   |
|  |  Kafka Connect :8083              |                   |
|  |  Debezium MongoDB Plugin          |                   |
|  +------------------------------------+                  |
+----------------------------------------------------------+
```

### 4.3 Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| CDC mechanism | Debezium (oplog-based) | No application changes; captures all op types |
| Serialization | JSON (not Avro) | Simpler for POC; no Schema Registry needed |
| Capture mode | `change_streams_update_full` | Delivers full document on updates, not a diff |
| Offset strategy | Manual commit, post-batch | At-least-once delivery; safe with idempotent upserts |
| Async I/O | asyncio + asyncpg | Non-blocking PostgreSQL writes |
| Idempotency | `ON CONFLICT DO UPDATE` | Re-delivered events produce same result |
| Mapping rules | YAML-driven (`mapping_engine.py`) | No hardcoded per-collection transformer functions |
| Neo4j failure mode | Graceful degradation | Consumer continues if Neo4j is unavailable at startup |
| WebSocket | FastAPI polls `cdc_pipeline_metrics` every 2s | No Redis dependency |

---

## 5. Functional Requirements

### FR-1: CDC Capture

| ID | Requirement |
|---|---|
| FR-1.1 | The system SHALL capture insert (`c`), update (`u`), replace, and delete (`d`) events from MongoDB using Debezium change streams. |
| FR-1.2 | The system SHALL capture events from five MongoDB collections: `rfqs`, `purchase_orders`, `asns`, `grns`, `invoices`. |
| FR-1.3 | On first startup, Debezium SHALL perform an initial snapshot of existing documents before switching to live stream mode. |
| FR-1.4 | Debezium SHALL publish captured events to five Kafka topics following the naming convention `poc.mydb.<collection>`. |

### FR-2: Event Consumption

| ID | Requirement |
|---|---|
| FR-2.1 | The Python consumer SHALL poll all five Kafka topics in a single consumer group (`poc-pipeline-consumer`). |
| FR-2.2 | The consumer SHALL process messages in batches of up to 100 and commit offsets only after the entire batch is processed. |
| FR-2.3 | The consumer SHALL parse Debezium envelopes including BSON Extended JSON coercion for ObjectId, Date, Decimal128, Int64, and Int32 types. |
| FR-2.4 | The consumer SHALL detect tombstone messages (null Kafka value) and skip them without error. |

### FR-3: Validation and Routing

| ID | Requirement |
|---|---|
| FR-3.1 | The system SHALL validate each parsed event against per-collection required field lists defined in `schema_guard.py`. |
| FR-3.2 | Events failing validation SHALL be published to the Dead Letter Queue topic (`poc.dlq`) with error metadata headers. |
| FR-3.3 | The system SHALL route events to PostgreSQL write operations and Neo4j Cypher operations based on YAML mapping rules loaded at startup. |

### FR-4: PostgreSQL Persistence

| ID | Requirement |
|---|---|
| FR-4.1 | The system SHALL upsert P2P documents into 11 PostgreSQL tables using `ON CONFLICT DO UPDATE` semantics. |
| FR-4.2 | The system SHALL cascade-delete dependent rows when a delete event is received. |
| FR-4.3 | The system SHALL record one pipeline metrics row per processed event in `cdc_pipeline_metrics`. |
| FR-4.4 | All PostgreSQL write operations SHALL be idempotent — replaying any event SHALL produce the same final state. |

### FR-5: Neo4j Persistence

| ID | Requirement |
|---|---|
| FR-5.1 | The system SHALL merge P2P document nodes (RFQ, PurchaseOrder, ASN, GRN, Invoice) and entity nodes (Vendor, Material) into Neo4j. |
| FR-5.2 | The system SHALL create or update 13 relationship types between nodes based on document cross-references. |
| FR-5.3 | The system SHALL apply uniqueness constraints to all 7 node labels on startup via `init/neo4j/constraints.cypher`. |
| FR-5.4 | If Neo4j is unavailable, the consumer SHALL log a warning and continue processing into PostgreSQL only. |

### FR-6: Dead Letter Queue

| ID | Requirement |
|---|---|
| FR-6.1 | The DLQ topic SHALL be named `{source_topic}.dlq`. |
| FR-6.2 | Every DLQ message SHALL include Kafka headers: `error`, `source_topic`, `source_partition`, `source_offset`. |
| FR-6.3 | A replay script (`scripts/dlq_replay.py`) SHALL republish DLQ messages to the original source topic. |

### FR-7: REST API

| ID | Requirement |
|---|---|
| FR-7.1 | The FastAPI server SHALL expose endpoints to insert and delete any of the five P2P document types via `POST /api/documents/{type}` and `DELETE /api/documents/{type}/{id}`. |
| FR-7.2 | The API SHALL expose pipeline status (connector state + Kafka lag) via `GET /api/pipeline/status`. |
| FR-7.3 | The API SHALL expose metrics summaries and recent events via `GET /api/metrics/summary` and `GET /api/metrics/recent`. |
| FR-7.4 | The API SHALL expose graph neighborhood and stats via `GET /api/graph/{collection}/{id}` and `GET /api/graph/stats/overview`. |
| FR-7.5 | The API SHALL provide a WebSocket endpoint at `/ws/metrics` that broadcasts pipeline metrics every 2 seconds. |

### FR-8: Web Dashboard

| ID | Requirement |
|---|---|
| FR-8.1 | The Dashboard page SHALL display live KPI cards (total events, avg/P50/P95/P99 latency, throughput) updated via WebSocket. |
| FR-8.2 | The Documents page SHALL allow manual insertion and deletion of P2P documents and trigger full chain simulation. |
| FR-8.3 | The Graph page SHALL render a force-directed graph of Neo4j nodes and relationships with a node inspector panel. |
| FR-8.4 | The Pipeline page SHALL display Debezium connector state and per-topic Kafka lag. |

### FR-9: P2P Simulator

| ID | Requirement |
|---|---|
| FR-9.1 | The simulator SHALL generate complete P2P document chains (RFQ → PO → ASN → GRN → Invoice) into MongoDB. |
| FR-9.2 | The simulator SHALL support insert, update, and delete operations with configurable frequency. |

---

## 6. Non-Functional Requirements

### NFR-1: Performance

| ID | Requirement |
|---|---|
| NFR-1.1 | End-to-end latency (MongoDB oplog commit → PostgreSQL write complete) SHALL be under 1 second for 95% of events under normal load on local hardware. |
| NFR-1.2 | The consumer SHALL sustain a throughput of at least 100 events per minute under simulated load. |
| NFR-1.3 | PostgreSQL connection pools SHALL maintain 2 idle connections and scale up to 10 concurrent connections. |

### NFR-2: Reliability

| ID | Requirement |
|---|---|
| NFR-2.1 | The system SHALL provide at-least-once delivery guarantees. Events may be re-delivered on consumer restart; all writes SHALL be idempotent. |
| NFR-2.2 | A single malformed or unwritable event SHALL not block processing of subsequent events in the same batch. |
| NFR-2.3 | Failed events SHALL be preserved in the DLQ and be replayable without data loss. |

### NFR-3: Observability

| ID | Requirement |
|---|---|
| NFR-3.1 | The system SHALL record four timestamps per event: MongoDB oplog commit time, Kafka assign time, consumer poll time, and PostgreSQL write completion time. |
| NFR-3.2 | The system SHALL derive and store three latency segments per event: Debezium latency, consumer queue latency, and write latency. |
| NFR-3.3 | The dashboard SHALL display P50, P95, and P99 latency percentiles calculated from `cdc_pipeline_metrics`. |

### NFR-4: Maintainability

| ID | Requirement |
|---|---|
| NFR-4.1 | Adding a new MongoDB collection to the pipeline SHALL not require changes to the consumer core — only new YAML mapping files. |
| NFR-4.2 | All configuration (ports, credentials, hosts) SHALL be managed via a single `.env` file; no hardcoded values in source code. |

### NFR-5: Portability

| ID | Requirement |
|---|---|
| NFR-5.1 | The WSL2 IP used by Debezium to reach Windows MongoDB SHALL be configurable in `.env` (`MONGO_RS_HOST`) and `connectors/p2p-connector.json`. |
| NFR-5.2 | The Kafka client SHALL explicitly use `broker.address.family: v6` to avoid IPv4/IPv6 fallback delays on WSL2. |

---

## 7. Data Requirements

### 7.1 MongoDB Source Collections (database: `mydb`, port 27018)

| Collection | Description | Key Fields |
|---|---|---|
| `rfqs` | Request for Quotation | `rfq_number`, `vendor_id`, `material_id`, `status` |
| `purchase_orders` | Purchase Order | `po_number`, `rfq_id`, `vendor_id`, `status` |
| `asns` | Advance Shipping Notice | `asn_number`, `po_id`, `vendor_id`, `status` |
| `grns` | Goods Receipt Note | `grn_number`, `po_id`, `asn_id`, `status` |
| `invoices` | Invoice | `invoice_number`, `po_id`, `vendor_id`, `total_amount`, `status` |

### 7.2 PostgreSQL Tables (database: `analytics`, port 5432)

| Table | Description |
|---|---|
| `rfq` | RFQ header records |
| `rfq_line_items` | RFQ line items |
| `rfq_invited_vendors` | Vendors invited per RFQ |
| `purchase_orders` | PO header records |
| `po_line_items` | PO line items |
| `asns` | ASN header records |
| `asn_line_items` | ASN line items |
| `grns` | GRN header records |
| `grn_line_items` | GRN line items |
| `invoices` | Invoice header records |
| `invoice_line_items` | Invoice line items |
| `cdc_pipeline_metrics` | Per-event pipeline timing telemetry |

### 7.3 Neo4j Graph Model (bolt://localhost:7687)

**Node Labels:**

| Label | Unique Key | Description |
|---|---|---|
| `RFQ` | `rfq_id` | Request for Quotation node |
| `PurchaseOrder` | `po_id` | Purchase Order node |
| `ASN` | `asn_id` | Advance Shipping Notice node |
| `GRN` | `grn_id` | Goods Receipt Note node |
| `Invoice` | `invoice_id` | Invoice node |
| `Vendor` | `vendor_id` | Vendor entity node |
| `Material` | `material_id` | Material/product entity node |

**Relationship Types:**

| Relationship | From → To | Description |
|---|---|---|
| `INVITED` | RFQ → Vendor | Vendor invited on RFQ |
| `REQUESTS` | RFQ → Material | Material requested in RFQ |
| `ISSUED_AGAINST` | PurchaseOrder → RFQ | PO raised against RFQ |
| `ISSUED_TO` | PurchaseOrder → Vendor | PO issued to vendor |
| `ORDERS` | PurchaseOrder → Material | Material ordered in PO |
| `FULFILLS` | ASN → PurchaseOrder | ASN fulfills a PO |
| `SHIPS` | ASN → Vendor | Vendor ships ASN |
| `RECEIVES` | GRN → PurchaseOrder | GRN receives goods for PO |
| `CONFIRMS` | GRN → ASN | GRN confirms ASN |
| `RECEIVED` | GRN → Material | Material received in GRN |
| `BILLS` | Invoice → PurchaseOrder | Invoice bills against PO |
| `REFERENCES` | Invoice → GRN | Invoice references GRN |
| `INVOICES` | Invoice → Vendor | Invoice issued by vendor |

### 7.4 Kafka Topics

| Topic | Source Collection | Partitions |
|---|---|---|
| `poc.mydb.rfqs` | rfqs | 1 (default) |
| `poc.mydb.purchase_orders` | purchase_orders | 1 (default) |
| `poc.mydb.asns` | asns | 1 (default) |
| `poc.mydb.grns` | grns | 1 (default) |
| `poc.mydb.invoices` | invoices | 1 (default) |
| `poc.dlq` | Dead Letter Queue | 1 (default) |

---

## 8. External Interface Requirements

### 8.1 Kafka Connect REST API

The connector manager communicates with Kafka Connect at `http://localhost:8083` using the standard Connect REST API:

| Method | Endpoint | Purpose |
|---|---|---|
| POST | `/connectors` | Register Debezium connector |
| GET | `/connectors/{name}/status` | Check connector/task health |
| DELETE | `/connectors/{name}` | Remove connector |

### 8.2 FastAPI REST Endpoints

| Method | Path | Description |
|---|---|---|
| POST | `/api/documents/{type}` | Insert a document into MongoDB |
| DELETE | `/api/documents/{type}/{id}` | Delete a document from MongoDB |
| GET | `/api/pipeline/status` | Connector state + Kafka lag |
| GET | `/api/metrics/summary` | Aggregated KPIs |
| GET | `/api/metrics/recent` | Recent event feed |
| GET | `/api/graph/{collection}/{id}` | Node neighborhood from Neo4j |
| GET | `/api/graph/stats/overview` | Graph node/edge counts |
| POST | `/api/simulate/chain` | Trigger full P2P chain simulation |
| POST | `/api/simulate/update` | Trigger document status updates |
| WS | `/ws/metrics` | Live metrics stream (2s interval) |

### 8.3 Web Frontend

- Development server: `http://localhost:5173` (Vite proxy forwards `/api` and `/ws` to FastAPI `:8000`)
- Production: served as static files from `web/dist/` by FastAPI

### 8.4 MongoDB Replica Set

MongoDB MUST run as a replica set (`rs0`) to enable change streams, which Debezium requires. The replica set can be single-node (for local POC use). Port: **27018** (non-default to avoid conflicts with any existing MongoDB installation on 27017).

---

## 9. Constraints and Assumptions

### 9.1 Constraints

| ID | Constraint |
|---|---|
| C-1 | Kafka and Kafka Connect MUST run in WSL2 (Linux) due to the Debezium MongoDB plugin requiring a Linux JVM environment. |
| C-2 | MongoDB MUST be run as a replica set (`rs0`); standalone mode does not support change streams. |
| C-3 | Python version MUST be 3.10–3.12; Python 3.14 is not supported due to missing C-extension wheels for `confluent-kafka` and `asyncpg`. |
| C-4 | The WSL2 host IP (used by Debezium to reach Windows MongoDB) can change on reboot and MUST be verified and updated in `.env` and `connectors/p2p-connector.json`. |
| C-5 | Neo4j Desktop MUST be started manually before the consumer is launched; the consumer will degrade gracefully if Neo4j is unreachable but will not retry Neo4j connection automatically. |
| C-6 | No Confluent Schema Registry is used; all Kafka messages are plain JSON. |

### 9.2 Assumptions

| ID | Assumption |
|---|---|
| A-1 | The system runs on a single developer machine; no distributed or multi-node deployment is required for the POC. |
| A-2 | Network latency between WSL2 and Windows host processes is negligible for latency measurement purposes. |
| A-3 | PostgreSQL and Neo4j are pre-initialized with schemas before the consumer starts (via DDL scripts and constraint Cypher). |
| A-4 | Clock skew between Windows and WSL2 system clocks is minimal (< 100 ms). |
| A-5 | MongoDB documents in all five P2P collections conform to the field schemas defined in `SAP-Files-Mappings/mapping_rules/*.yaml`. |

---

## 10. Glossary

| Term | Definition |
|---|---|
| **CDC** | Change Data Capture — a pattern for capturing database mutations without modifying the application |
| **Debezium** | Open-source CDC framework that reads database oplogs/WALs and publishes changes to Kafka |
| **Kafka Connect** | Kafka-native framework for hosting connector plugins such as Debezium |
| **KRaft** | Kafka Raft — Kafka's built-in consensus mechanism that replaces ZooKeeper (available since Kafka 3.x, mandatory in 4.x) |
| **oplog** | MongoDB's internal operation log; the source of truth for change streams |
| **DLQ** | Dead Letter Queue — a secondary Kafka topic that receives events that failed to process |
| **P2P** | Procure-to-Pay — the end-to-end procurement cycle from RFQ to invoice payment |
| **RFQ** | Request for Quotation |
| **PO** | Purchase Order |
| **ASN** | Advance Shipping Notice |
| **GRN** | Goods Receipt Note |
| **Idempotent** | A write operation that can be applied multiple times with the same outcome |
| **BSON** | Binary JSON — MongoDB's internal document format; Extended JSON is its text representation |
| **WSL2** | Windows Subsystem for Linux version 2 — a full Linux kernel running inside Windows 11 |
| **Bolt** | Neo4j's binary transport protocol used by the official drivers |
| **YAML mapping engine** | The `mapping_engine.py` + `SAP-Files-Mappings/*.yaml` system that declaratively defines field-to-column and field-to-node mappings |
