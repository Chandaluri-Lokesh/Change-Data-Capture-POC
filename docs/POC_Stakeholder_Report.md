# Change Data Capture (CDC) Pipeline — Proof of Concept
## Stakeholder Report

**Document Type:** Executive & Business Stakeholder Submission
**Project:** Real-Time P2P CDC Pipeline — MongoDB to PostgreSQL + Neo4j via Debezium and Kafka
**POC Status:** Completed
**Date:** October 2026

---

## 1. Executive Summary

This Proof of Concept demonstrates that real-time data replication across a full Procure-to-Pay (P2P) document lifecycle — from an operational MongoDB database to both a relational PostgreSQL store and a Neo4j graph database — is achievable using industry-standard open-source tools with sub-second end-to-end latency under typical load.

The system captures every insert, update, and delete that occurs across five MongoDB collections (RFQ, Purchase Order, ASN, GRN, Invoice) and propagates each change to PostgreSQL (10 relational tables) and Neo4j (7 node types, 12 relationship types) within milliseconds, without modifying the source application or requiring any application-level changes.

A live React web dashboard provides full visibility into pipeline health, document state, graph relationships, database schemas, and measured performance metrics.

**Key result:** Documents complete the full pipeline (MongoDB → Kafka → Consumer → PostgreSQL + Neo4j) in under 1,000 ms on a local developer machine. P95 latency remained below 2,000 ms during sustained simulation load.

---

## 2. Problem Statement

Modern procurement organisations maintain operational databases (MongoDB) for high-speed transactional workloads while simultaneously needing those same records in relational databases for reporting and in graph databases for supply chain relationship analysis. Traditional approaches:

| Approach | Problem |
|---|---|
| Periodic batch ETL jobs | Data is stale by minutes to hours; high DB load during batch windows |
| Dual-writes from application | Requires application code changes; partial failure risk |
| Database triggers | Tightly couples source schema to destination; fragile and hard to maintain |
| Manual data exports | Not real-time; error-prone; operationally expensive |

Change Data Capture solves all of these by reading MongoDB's internal change log (oplog) and streaming every change event to downstream consumers in real time, with no application-level coupling.

---

## 3. Aim and Objectives

**Primary Aim:** Validate that a CDC pipeline can reliably replicate a full Procure-to-Pay document lifecycle from MongoDB to both PostgreSQL and Neo4j in real time with measurable, sub-second latency.

**Objectives validated in this POC:**

1. Capture all DML operations (insert, update, delete) across five P2P collections without touching application code
2. Deliver events to downstream consumers via Apache Kafka with at-least-once delivery guarantees
3. Write deduplicated, idempotent rows to PostgreSQL across 10 relational tables using a YAML-driven mapping engine
4. Simultaneously write graph nodes and relationships to Neo4j, enabling cross-document supply chain queries
5. Maintain a Dead Letter Queue (DLQ) for failed events without blocking the pipeline
6. Measure and display four-point latency (MongoDB → Kafka → Consumer → PostgreSQL) for every document
7. Provide a live web dashboard showing pipeline health, document state, graph explorer, schema browser, and performance metrics

---

## 4. Solution Architecture Overview

```
MongoDB (rs0)  —  5 collections: rfqs, purchase_orders, asns, grns, invoices
    │  Change Stream (oplog)
    ▼
Debezium Connector (Kafka Connect)
    │  JSON events → 5 Kafka topics (poc.mydb.{collection})
    ▼
Apache Kafka (KRaft, single broker)
    │  Consumer group: poc-pipeline-consumer
    ▼
Python Consumer + YAML Mapping Engine
    ├──► PostgreSQL (analytics DB)  →  10 P2P tables + cdc_pipeline_metrics
    ├──► Neo4j                      →  7 node types, 12 relationship types
    └──► Dead Letter Queue          →  Failed events, replayable
         │
         ▼
FastAPI Backend + React Dashboard
    →  Live metrics, document management, graph explorer, schema browser, performance analysis
```

**Data flow per document:**

1. Application inserts/updates/deletes a document in MongoDB (e.g., a new Purchase Order)
2. Debezium reads the change from MongoDB's change stream (oplog) and publishes a JSON event to Kafka within milliseconds
3. The Python consumer polls Kafka in batches, processes each message through parse → validate → route stages
4. The YAML mapping engine determines which PostgreSQL tables and Neo4j nodes/relationships to write
5. Both PostgreSQL (relational) and Neo4j (graph) are updated in the same pipeline pass using idempotent operations
6. Four timestamps are recorded per document enabling precise latency measurement at every stage

---

## 5. P2P Document Lifecycle

The POC models the standard Procure-to-Pay chain:

```
RFQ → Purchase Order → ASN → GRN → Invoice
```

| Document | MongoDB Collection | PostgreSQL Tables | Neo4j Node |
|---|---|---|---|
| Request for Quotation | `rfqs` | `rfq`, `rfq_line_items`, `rfq_invited_vendors` | `RFQ` |
| Purchase Order | `purchase_orders` | `purchase_orders`, `po_line_items` | `PurchaseOrder` |
| Advance Shipment Notice | `asns` | `asns`, `asn_line_items` | `ASN` |
| Goods Receipt Note | `grns` | `grns`, `grn_line_items` | `GRN` |
| Invoice | `invoices` | `invoices`, `invoice_line_items` | `Invoice` |

Cross-document relationships (e.g., `PurchaseOrder ISSUED_AGAINST RFQ`, `Invoice BILLS PurchaseOrder`) are written to Neo4j as typed relationships with properties, enabling supply chain traversal queries that are impractical in SQL.

---

## 6. Technical Requirements

### 6.1 Infrastructure (POC / Development)

| Component | Requirement | Reason |
|---|---|---|
| MongoDB | 6.x+, Replica Set mode | Change streams require replica set |
| Apache Kafka | 3.x+ (KRaft mode) | Message broker; KRaft eliminates ZooKeeper dependency |
| Kafka Connect | Bundled with Kafka | Plugin host for Debezium connectors |
| Debezium MongoDB Connector | 2.x+ | CDC capture from MongoDB |
| Python | 3.10–3.12 | Consumer runtime |
| PostgreSQL | 14+ | Relational target |
| Neo4j | 5.x | Graph target |
| Node.js | 18+ | React dashboard build |

### 6.2 Infrastructure (Production — Recommended)

| Component | Recommended Service | Notes |
|---|---|---|
| MongoDB | MongoDB Atlas M10+ | Atlas supports change streams natively |
| Kafka | Confluent Cloud or AWS MSK | Managed broker; auto-scaling |
| Kafka Connect | Confluent Cloud or self-hosted | Connector lifecycle management |
| Python Consumer | AWS ECS / Kubernetes pod | Containerised, horizontally scalable |
| PostgreSQL | AWS RDS PostgreSQL | Managed, HA, automated backups |
| Neo4j | Neo4j AuraDB | Managed graph database |
| Dashboard | Deploy React build to any static host | FastAPI serves API; frontend on CDN |
| Monitoring | Grafana + Prometheus | Production observability |

### 6.3 Software / Library Requirements

| Library | Purpose |
|---|---|
| `confluent-kafka` | Kafka producer and consumer client |
| `asyncpg` | Async PostgreSQL driver for high-throughput writes |
| `neo4j` | Official Neo4j async Python driver |
| `pymongo` | MongoDB client (simulator) |
| `fastapi` + `uvicorn` | REST API + WebSocket backend |
| `PyYAML` | YAML mapping rule loading |
| `python-dotenv` | Environment variable management |
| `Faker` | Realistic synthetic data generation |

### 6.4 Non-Technical Requirements

- All components are open-source with permissive licences (Apache 2.0, MIT)
- No vendor lock-in at the POC stage; production can migrate to managed services
- Pipeline is idempotent — safe to restart without data duplication
- Failed events do not block the pipeline (DLQ pattern)
- Adding a new document type requires only a new YAML mapping file — zero code changes

---

## 7. Estimated Timeline for Practical Implementation

The following is an estimate for a production-grade implementation based on POC findings.

### Phase 1 — Infrastructure Setup (Weeks 1–2)
- Provision managed Kafka cluster (Confluent Cloud / AWS MSK)
- Provision MongoDB with replica set enabled
- Provision PostgreSQL and Neo4j instances
- Configure networking, VPC peering, security groups
- Set up CI/CD pipeline for consumer application

### Phase 2 — Connector & Consumer Hardening (Weeks 3–4)
- Deploy Debezium connector with production configuration (Avro + Schema Registry)
- Containerise Python consumer (Docker / Kubernetes)
- Implement horizontal scaling (multiple consumer instances, multiple partitions)
- Add DLQ monitoring and alerting

### Phase 3 — Testing & Validation (Weeks 5–6)
- Load testing: simulate 10,000+ events/minute
- Failure testing: kill consumer mid-batch, verify no data loss on restart
- Latency benchmarking under sustained load
- Data reconciliation checks (MongoDB vs PostgreSQL vs Neo4j)

### Phase 4 — Monitoring & Operations (Week 7)
- Deploy Grafana + Prometheus dashboards
- Configure alerting on consumer lag threshold
- Runbook documentation for on-call team
- Disaster recovery drill

### Phase 5 — Go-Live (Week 8)
- Shadow mode: run pipeline alongside existing ETL, compare results
- Cutover to CDC as primary replication method
- Decommission legacy ETL

**Total estimated timeline: 8–10 weeks** (2 engineers)

---

## 8. Resource Requirements

### 8.1 Team Composition

| Role | Responsibility | Estimated Hours |
|---|---|---|
| Backend Engineer (Python) | Consumer, mapping engine, DLQ, testing | 120–160 hours |
| Data Engineer / Platform Engineer | Kafka, Debezium, connector configuration | 80–120 hours |
| DevOps / Infrastructure Engineer | Cloud provisioning, CI/CD, Kubernetes | 80–100 hours |
| Frontend Engineer | React dashboard, API integration | 40–60 hours |
| QA Engineer | Load testing, failure testing, data validation | 40–60 hours |
| Technical Lead / Architect | Design review, decisions, stakeholder communication | 20–30 hours |

**Total labour estimate: 380–530 person-hours** across 8–10 weeks

### 8.2 Skills Required

- Python 3.10+ (asyncio, asyncpg, confluent-kafka, neo4j driver)
- Kafka fundamentals (topics, partitions, consumer groups, offsets)
- MongoDB (replica sets, change streams, BSON)
- PostgreSQL (DDL, upserts, connection pooling)
- Neo4j (Cypher, constraints, async driver)
- React / TypeScript (frontend)
- Docker / Kubernetes (containerisation and deployment)

---

## 9. Cost of Computation and Maintenance

### 9.1 Development / POC Environment (Current)

All components run locally or in WSL2. Cost: **$0** (developer hardware only).

### 9.2 Production Environment (Monthly Estimates — AWS + Neo4j AuraDB)

| Service | Specification | Estimated Monthly Cost (USD) |
|---|---|---|
| AWS MSK (Kafka) | 3-broker kafka.m5.large cluster | $300–$500 |
| MongoDB Atlas | M10 replica set (3 nodes) | $200–$350 |
| AWS RDS PostgreSQL | db.t3.medium, Multi-AZ | $150–$250 |
| Neo4j AuraDB | Professional tier | $200–$400 |
| ECS Fargate (Consumer) | 0.5 vCPU / 1 GB, always-on | $30–$60 |
| Data Transfer | Inter-service traffic | $20–$50 |
| Monitoring (Grafana Cloud) | Up to 10k series free tier | $0–$50 |
| **Total** | | **$900–$1,660 / month** |

### 9.3 Maintenance Cost Estimates (Ongoing)

| Activity | Frequency | Estimated Hours/Month |
|---|---|---|
| Connector version upgrades | Quarterly | 4 hours |
| Schema changes (new document type = new YAML) | As needed | 2–4 hours per change |
| DLQ monitoring and replay | Weekly | 1–2 hours |
| Capacity review and scaling | Monthly | 2 hours |
| Incident response | As needed | Variable |

**Estimated ongoing engineering cost: 8–12 person-hours/month** at steady state.

---

## 10. Evaluation Criteria

### 10.1 Latency

| Metric | POC Result | Production Target |
|---|---|---|
| Median (P50) end-to-end latency | < 500 ms | < 1,000 ms |
| 95th percentile (P95) latency | < 2,000 ms | < 3,000 ms |
| 99th percentile (P99) latency | < 5,000 ms | < 10,000 ms |
| Debezium capture delay | < 200 ms | < 500 ms |
| PostgreSQL write time | < 100 ms | < 300 ms |

> Latency figures are **per-record** (time for one document to traverse the full pipeline). Total batch ingestion time scales with throughput: N records ÷ events/second.

### 10.2 Throughput

| Metric | POC Validated | Production Target |
|---|---|---|
| Sustained events/minute | ~60–300 (simulated) | 10,000+ |
| Burst capacity | Up to batch size (100) | 50,000+ |
| Max consumer lag recovery | < 2 minutes | < 5 minutes |

### 10.3 Reliability

| Criterion | Requirement |
|---|---|
| Data loss on consumer restart | Zero (at-least-once delivery + idempotent upserts) |
| Data duplication on restart | Zero (ON CONFLICT DO UPDATE / MERGE absorbs re-delivered events) |
| Failed event handling | DLQ capture without blocking pipeline |
| Consumer availability | 99.9% uptime (Kubernetes restarts on failure) |

### 10.4 Data Integrity

| Criterion | Verification Method |
|---|---|
| MongoDB row count = PostgreSQL row count | `scripts/verify.py` reconciliation script |
| Neo4j graph reflects all documents | Cypher count queries post-chain insert |
| No phantom deletes | Verify DELETE propagates to PG and Neo4j DETACH DELETE |
| DLQ events are replayable | `scripts/dlq_replay.py` |

---

## 11. Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| MongoDB oplog window too small | Medium | High | Increase oplog size; monitor connector offset lag |
| Schema drift (new fields in MongoDB documents) | High | Medium | Schema guard in consumer; DLQ routes unknown-schema events; YAML is additive |
| Kafka broker outage | Low | High | Multi-broker cluster; replication factor 3 |
| Consumer falling behind under spike load | Medium | Medium | Increase partition count; scale consumer horizontally |
| Neo4j connection pool exhaustion | Low | Medium | Tuned pool sizes; retry on transient errors |
| Debezium connector failure | Low | High | Kafka Connect auto-restarts tasks; alerting on FAILED state |
| PostgreSQL connection pool exhaustion | Low | Medium | Tuned pool sizes; retry logic |

---

## 12. Conclusion and Recommendation

The POC successfully validates all primary objectives. The pipeline:

- Captures every MongoDB change operation with zero data loss across all five P2P collections
- Delivers events to both PostgreSQL (10 tables) and Neo4j (graph) in under 1 second (median) on a local machine
- Handles all failure modes: bad events to DLQ, restarts are safe, deletes propagate with DETACH DELETE
- Uses a YAML-driven mapping engine that makes adding a new document type a configuration change, not a code change
- Is monitored end-to-end through a live React dashboard with pipeline health, graph explorer, schema browser, and performance analysis

**Recommendation:** Proceed to production implementation. The architecture is sound, the technology stack is mature and widely adopted in industry, and the YAML-driven design significantly reduces the cost of extending the pipeline to new document types. Estimated production readiness in 8–10 weeks with a team of 2–3 engineers.
