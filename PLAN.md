# CDC POC — Extension Plan: P2P Pipeline + Neo4j + Web Application

> **Status: Fully Implemented.** All phases below have been completed. This document is preserved as a record of the original design decisions and the issues identified before implementation began.

---

## Original Assessment

The initial POC was clean and well-structured. The layered pipeline
(parser → schema_guard → router → transformer → writer) was the right abstraction.
The problem was that every layer was hardcoded for a single collection (`orders`).

**The solution:** The SAP-Files-Mappings YAML files are declarative mapping rules.
A **mapping-engine** (`src/engine/mapping_engine.py`) loads those YAMLs at startup
and drives routing, SQL upserts, and Neo4j Cypher merges automatically.
Adding a new document type requires only a new YAML — zero code changes.

---

## Issues Found Before Starting — All Resolved

| # | Issue | Fix Applied |
|---|-------|-------------|
| 1 | `.env` had no Neo4j vars | Added `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD` |
| 2 | Default ports (5433/5434) vs `.env` (5432) | Standardised to single PostgreSQL instance |
| 3 | Connector watched only `mydb.orders` | `p2p-connector.json` watches all 5 collections |
| 4 | Consumer subscribed only to `poc.mydb.orders` | Subscribes to all 5 P2P topics |
| 5 | `router.py` only routed `orders` | Delegates to mapping engine |
| 6 | `transformer.py` only mapped 2 flat tables | Replaced with YAML-driven engine |
| 7 | `pg_writer.py` had hardcoded SQL | Generic `upsert_table()` / `delete_cascade()` |
| 8 | No Neo4j writer | `src/writer/neo4j_writer.py` added |
| 9 | No PostgreSQL DDL for P2P tables | `init/postgres/04_p2p.sql` created (10 tables) |
| 10 | Streamlit dashboard was orders-only | Replaced with FastAPI + React web application |

---

## Phase 0 — Verify Current Implementation ✓

Confirmed the existing pipeline ran end-to-end. `.env` values aligned with running services.

---

## Phase 1 — Extend Pipeline for P2P Schema (MongoDB → PostgreSQL) ✓

### What Was Built

- **`connectors/p2p-connector.json`** — watches `mydb.rfqs`, `mydb.purchase_orders`, `mydb.asns`, `mydb.grns`, `mydb.invoices`. Produces 5 Kafka topics under the `poc.` prefix.
- **`init/postgres/04_p2p.sql`** — DDL for all 10 P2P tables (parent + line-item child tables for each document type).
- **`src/engine/mapping_engine.py`** — loads all `*_mapping.yaml` files at startup; exposes `get_pg_routes()` (returns `(table, row_dict, upsert_key)` tuples per CDC event) and `get_neo4j_ops()`.
- **`src/generator/p2p_simulator.py`** — generates realistic P2P chains (RFQ → PO → ASN → GRN → Invoice) with continuous weighted update/delete traffic.
- Updated `kafka_consumer.py`, `router.py`, `pg_writer.py` to use the engine.

---

## Phase 2 — Neo4j Mapping ✓

### What Was Built

- **`src/writer/neo4j_writer.py`** — async `merge_node()`, `merge_relationships()`, `delete_node()` using the official neo4j Python driver. Retries on transient errors.
- **`init/neo4j/constraints.cypher`** — uniqueness constraints for RFQ, PurchaseOrder, ASN, GRN, Invoice, Vendor, Material (idempotent `IF NOT EXISTS`).
- Mapping engine extended: `get_neo4j_ops()` returns Cypher + params pairs from YAML.
- Consumer integration: Neo4j writes run after Postgres writes succeed; failures go to DLQ.

---

## Phase 3 — Web Application ✓

### Architecture: FastAPI backend + React (Vite) frontend

**Backend — `src/api/`**

```
src/api/
  main.py            FastAPI app, lifespan (pool setup/teardown)
  routers/
    documents.py     POST/DELETE/GET /api/documents/{type}
    pipeline.py      GET /api/pipeline/status
    metrics.py       GET /api/metrics/summary|recent|collections|benchmarks
                     WS  /ws/metrics
    graph.py         GET /api/graph/{collection}/{id}
                     GET /api/graph/stats/overview
    schema.py        GET /api/schema/mongodb|postgresql|neo4j
    simulator.py     POST /api/simulate/chain|start|stop
                     GET  /api/simulate/status
```

**Frontend — `cdc-dashboard-ui/`**

```
src/pages/
  Home.tsx           Pipeline overview, P2P walkthrough, tech stack
  Dashboard.tsx      Live KPI cards, latency chart (WebSocket)
  Documents.tsx      Insert form + document list panel
  Graph.tsx          Force-directed Neo4j graph + document list grid
  Pipeline.tsx       Connector status, consumer lag, simulator controls
  Schema.tsx         Three-tab schema browser (MongoDB / PostgreSQL / Neo4j)
  Performance.tsx    Latency stats, stage breakdown, scale projections
```

---

## Final File Change Summary

### New files (vs original orders-only POC)
```
connectors/p2p-connector.json
init/postgres/04_p2p.sql
init/neo4j/constraints.cypher
src/engine/mapping_engine.py
src/generator/p2p_simulator.py
src/writer/neo4j_writer.py
src/api/main.py
src/api/routers/documents.py
src/api/routers/pipeline.py
src/api/routers/metrics.py
src/api/routers/graph.py
src/api/routers/schema.py
src/api/routers/simulator.py
cdc-dashboard-ui/    (React/Vite project — full SPA)
```

### Modified files
```
.env                              (Neo4j vars added)
src/consumer/kafka_consumer.py   (5 topics, Neo4j driver, engine-driven routing)
src/transformer/router.py        (delegates to mapping_engine)
src/writer/pg_writer.py          (generic upsert_table / delete_cascade)
src/connector/manager.py         (registers p2p-connector)
```
