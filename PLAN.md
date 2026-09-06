# CDC POC — Extension Plan: P2P Pipeline + Neo4j + Web Application

## My Assessment of the Current State

The existing POC is clean and well-structured. The layered pipeline
(parser → schema_guard → router → transformer → writer) is the right abstraction.
The problem is that every layer is **hardcoded for a single collection** (`orders`).
Extending naively — copy-pasting a route + transformer + writer function per P2P
document — would work but produce ~500 lines of repetitive code that is fragile to
maintain.

**The better move:** The SAP-Files-Mappings YAML files are already declarative
mapping rules. We should build a **mapping-engine** that loads those YAMLs at
startup and drives routing, SQL upserts, and Neo4j Cypher merges automatically.
Adding a 6th document type then requires only a new YAML — zero code changes.

---

## Issues Found Before Starting

| # | Issue | Location | Fix |
|---|-------|----------|-----|
| 1 | `.env` has no Neo4j vars | `.env` | Add `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD` |
| 2 | `kafka_consumer.py` default ports (5433/5434) differ from `.env` (5432/5432) | `kafka_consumer.py:92-103` | `.env` already overrides — fine, but document it |
| 3 | Connector watches only `mydb.orders` | `connectors/orders-connector.json` | Add `collection.include.list` for all 5 P2P collections |
| 4 | Consumer subscribes only to `poc.mydb.orders` | `kafka_consumer.py:79` | Subscribe to all 5 P2P topics |
| 5 | `router.py` only routes `orders` collection | `router.py:51` | Replace with mapping-engine driven routing |
| 6 | `transformer.py` only maps to 2 flat tables | `transformer.py` | Replace with YAML-driven field mapping |
| 7 | `pg_writer.py` has hardcoded SQL for 2 tables | `pg_writer.py` | Replace with generic upsert/delete driven by mapping rules |
| 8 | No Neo4j writer exists at all | — | New `src/writer/neo4j_writer.py` |
| 9 | No PostgreSQL DDL for P2P tables | `init/postgres/` | New SQL files for all 10 P2P tables |
| 10 | Streamlit dashboard is orders-only | `src/ui/dashboard.py` | Replace with full web application |

---

## Phase 0 — Verify Current Implementation

**Goal:** Confirm the existing pipeline runs end-to-end before touching anything.

Steps:
1. Check `.env` values match actual running services (MongoDB port, Kafka broker,
   Postgres credentials/ports).
2. Verify MongoDB replica set `rs0` is active on port `27018`.
3. Verify Kafka broker is reachable on `localhost:9092` (WSL or native).
4. Verify Kafka Connect REST is reachable on `localhost:8083`.
5. Verify both Postgres databases exist (`analytics`, `finance`) with correct schema.
6. Run `python scripts/verify.py` — all checks must pass.
7. Fix any env mismatches found before proceeding.

Deliverables: confirmed green `verify.py` run, updated `.env` if needed.

---

## Phase 1 — Extend Pipeline for P2P Schema (MongoDB → PostgreSQL)

**Goal:** Stream all 5 P2P collections into PostgreSQL with the table shapes defined
in the mapping YAMLs.

### 1a. MongoDB Collections

The 5 P2P collections (`rfqs`, `purchase_orders`, `asns`, `grns`, `invoices`) need
to exist in MongoDB as replica-set-watched collections. A new simulator
(`src/generator/p2p_simulator.py`) will generate realistic P2P document flows
following the chain: RFQ → PO → ASN → GRN → Invoice.

### 1b. Debezium Connector

Update `connectors/orders-connector.json` (or create `connectors/p2p-connector.json`)
to watch all 5 collections:

```json
"collection.include.list": "mydb.rfqs,mydb.purchase_orders,mydb.asns,mydb.grns,mydb.invoices"
```

This produces 5 Kafka topics:
- `poc.mydb.rfqs`
- `poc.mydb.purchase_orders`
- `poc.mydb.asns`
- `poc.mydb.grns`
- `poc.mydb.invoices`

### 1c. PostgreSQL DDL

New file `init/postgres/04_p2p.sql` creates all P2P tables:

```
rfq                  (rfq_number PK, requested_date, requested_by, plant, status, response_due_date, source_updated_at)
rfq_line_items       (rfq_number FK, line_no, material_code, description, quantity, uom, target_delivery_date)
rfq_invited_vendors  (rfq_number FK, vendor_id, vendor_name, invited_on)

purchase_orders      (po_number PK, rfq_number FK, vendor_id, vendor_name, order_date, delivery_date, plant, currency, status, payment_terms, order_total, source_updated_at)
po_line_items        (po_number FK, line_no, material_code, description, quantity, uom, unit_price, line_total)

asns                 (asn_number PK, po_number FK, vendor_id, ship_date, carrier, tracking_number, expected_arrival, status, source_updated_at)
asn_line_items       (asn_number FK, line_no, material_code, quantity_shipped, uom)

grns                 (grn_number PK, po_number FK, asn_number FK, receipt_date, received_by, plant, status, source_updated_at)
grn_line_items       (grn_number FK, line_no, material_code, quantity_received, uom, condition, remarks)

invoices             (invoice_number PK, po_number FK, grn_number FK, vendor_id, invoice_date, due_date, currency, status, subtotal, tax_rate, tax_amount, total_amount, source_updated_at)
invoice_line_items   (invoice_number FK, line_no, material_code, quantity, unit_price, amount)
```

### 1d. Mapping Engine

New module `src/engine/mapping_engine.py`:
- Loads all `*_mapping.yaml` files at startup into a registry keyed by collection name
- Exposes `get_pg_routes(collection, document, op, doc_id)` → list of
  `(table, row_dict, upsert_key)` tuples (one per parent + each child table row)
- Handles the `source_array_field` expansion for line items and invited vendors
- Handles `nullable` fields and type coercion (dates, numerics, timestamps)

### 1e. Updated Pipeline Modules

| Module | Change |
|--------|--------|
| `kafka_consumer.py` | Subscribe to all 5 P2P topics |
| `router.py` | Delegate to `mapping_engine.get_pg_routes()` for any recognised collection |
| `transformer.py` | Remove hardcoded mappings; engine handles field mapping |
| `pg_writer.py` | Add generic `upsert_table(pool, table, rows, upsert_key)` and `delete_cascade(pool, table, pk_col, pk_val)` |
| `connector/manager.py` | Register the new P2P connector |

### 1f. Verification

New checks in `scripts/verify.py`:
- Insert one complete RFQ→PO→ASN→GRN→Invoice chain via simulator
- Assert all parent + child rows land in Postgres within 5 seconds
- Assert update propagation (change PO status → Postgres row updates)
- Assert delete propagation (delete GRN → CASCADE removes grn_line_items)

---

## Phase 2 — Neo4j Mapping

**Goal:** Every CDC event also writes/updates the graph in Neo4j, using the
`neo4j.merge_cypher` statements from the mapping YAMLs verbatim.

### 2a. Environment

Add to `.env`:
```
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=<password>
```

### 2b. Neo4j Writer

New `src/writer/neo4j_writer.py`:
- Uses the official `neo4j` Python driver (async sessions via `AsyncGraphDatabase`)
- `merge_node(session, cypher, params)` — runs the node `merge_cypher` from the YAML
- `merge_relationships(session, rel_configs, document, doc_id)` — iterates the
  `relationships` list from the YAML; expands `source_array_field` for array-based
  relationships (e.g., ORDERS per line item)
- `delete_node(session, label, key_property, key_value)` — `MATCH (n:Label {key: $val}) DETACH DELETE n`
- Retries on transient Neo4j errors (deadlock, leader switch)

### 2c. Mapping Engine Extension

`mapping_engine.py` gets a second method:
- `get_neo4j_ops(collection, document, op, doc_id)` → list of Cypher+params pairs
  (node merge + all relationship merges, or a DETACH DELETE on op=d)

### 2d. Consumer Integration

`kafka_consumer.py` acquires a Neo4j driver at startup alongside the Postgres pools.
Each message: after Postgres writes succeed, run Neo4j ops in the same try/except
block — Neo4j failure goes to DLQ, does not silently swallow.

### 2e. Constraints & Indexes (DDL)

Run once on Neo4j startup:
```cypher
CREATE CONSTRAINT rfq_unique        IF NOT EXISTS FOR (r:RFQ)           REQUIRE r.rfq_number     IS UNIQUE;
CREATE CONSTRAINT po_unique         IF NOT EXISTS FOR (p:PurchaseOrder)  REQUIRE p.po_number      IS UNIQUE;
CREATE CONSTRAINT asn_unique        IF NOT EXISTS FOR (a:ASN)            REQUIRE a.asn_number     IS UNIQUE;
CREATE CONSTRAINT grn_unique        IF NOT EXISTS FOR (g:GRN)            REQUIRE g.grn_number     IS UNIQUE;
CREATE CONSTRAINT invoice_unique    IF NOT EXISTS FOR (i:Invoice)         REQUIRE i.invoice_number IS UNIQUE;
CREATE CONSTRAINT vendor_unique     IF NOT EXISTS FOR (v:Vendor)         REQUIRE v.vendor_id      IS UNIQUE;
CREATE CONSTRAINT material_unique   IF NOT EXISTS FOR (m:Material)       REQUIRE m.material_code  IS UNIQUE;
```

### 2f. Graph Verification

Add Cypher-based checks to `scripts/verify.py`:
- After inserting the P2P chain, assert the full path exists:
  `MATCH p=(r:RFQ)-[:ISSUED_AGAINST|FULFILLS|RECEIVES|BILLS*..4]-(i:Invoice) RETURN count(p)`
- Assert relationship properties (quantity, unit_price on ORDERS edge)
- Assert DETACH DELETE removes node and all its relationships

---

## Phase 3 — Web Application

**Goal:** A single web application that serves two purposes:
1. **Control panel** — trigger P2P document generation, manage connectors
2. **Live dashboard** — real-time metrics, graph visualization, table row counts

### Architecture Decision

**FastAPI (Python) backend + React (Vite) frontend.**

Rationale:
- FastAPI integrates naturally with the existing Python pipeline code (shares `.env`,
  asyncpg pools, neo4j driver)
- WebSocket support is first-class in FastAPI — needed for real-time metric push
- React gives proper component isolation for the graph visualization (use
  `react-force-graph` or `neovis.js` for Neo4j graph rendering)
- Both run on `localhost` — no deployment complexity for a POC

Alternative (simpler but less capable): Streamlit with `st.empty()` auto-refresh —
acceptable if time is constrained, but lacks WebSocket real-time and graph viz.

### 3a. Backend — `src/api/`

```
src/api/
  main.py          FastAPI app entrypoint, lifespan (pool setup/teardown)
  routers/
    documents.py   POST /api/documents/{type}  — insert a document into MongoDB
    pipeline.py    GET  /api/pipeline/status   — connector health, Kafka lag
    metrics.py     GET  /api/metrics/summary   — latency stats from cdc_pipeline_metrics
                   WS   /ws/metrics            — push metric row every 2 s
    graph.py       GET  /api/graph/{doc_type}/{id} — return Neo4j subgraph as JSON
    simulator.py   POST /api/simulate/chain    — trigger one full RFQ→INV chain
```

Key design choices:
- Postgres pool and Neo4j driver created once in FastAPI `lifespan`, shared via `app.state`
- WebSocket endpoint streams new rows from `cdc_pipeline_metrics` using
  `LISTEN/NOTIFY` (Postgres) so it is push-based, not polling
- `/api/graph/` returns `{nodes: [], links: []}` JSON consumed by the frontend graph
  component — no Cypher exposed to the browser

### 3b. Frontend — `web/`

```
web/
  src/
    App.tsx
    pages/
      Dashboard.tsx     Live metric cards + latency time-series chart (Recharts)
      Documents.tsx     Form UI to trigger document insertion per type
      Graph.tsx         Force-directed Neo4j graph (react-force-graph-2d)
      Pipeline.tsx      Connector status, Kafka lag, DLQ count
    components/
      MetricCard.tsx
      LatencyChart.tsx
      TopologyGraph.tsx
      DocumentForm.tsx
  vite.config.ts
  package.json
```

Key pages:
- **Dashboard** — live cards (events/sec, avg e2e latency, DLQ count, PG row counts
  per table); latency time-series chart auto-updating via WebSocket
- **Documents** — form to insert a single RFQ/PO/ASN/GRN/Invoice, or trigger a
  full synthetic chain; shows the resulting Kafka event and DB write in real time
- **Graph** — search by document ID, renders the Neo4j subgraph with node labels
  and relationship types; click a node to expand neighbors
- **Pipeline** — connector status (green/red), Kafka consumer lag per partition,
  DLQ message count, last 10 DLQ entries

### 3c. Development & Run

```
# Backend
pip install fastapi uvicorn[standard] websockets
uvicorn src.api.main:app --reload --port 8000

# Frontend
cd web && npm install && npm run dev   # Vite dev server on :5173 with proxy to :8000
```

Production build: `npm run build` → FastAPI serves `web/dist` as static files on
`/` — single process, no separate servers needed for demo.

---

## Execution Order & Dependencies

```
Phase 0 (Verify)
    │
    ├─► Fix .env if needed
    └─► Green verify.py
         │
Phase 1 (P2P → PostgreSQL)
    ├─► 04_p2p.sql DDL
    ├─► mapping_engine.py
    ├─► p2p-connector.json
    ├─► p2p_simulator.py
    ├─► Update router, consumer, pg_writer
    └─► Green verify.py (P2P checks)
         │
Phase 2 (Neo4j)
    ├─► Neo4j constraints script
    ├─► neo4j_writer.py
    ├─► mapping_engine neo4j ops
    ├─► Consumer integration
    └─► Green verify.py (graph checks)
         │
Phase 3 (Web App)
    ├─► FastAPI backend (api/)
    ├─► React frontend (web/)
    └─► End-to-end demo: insert chain → watch graph + metrics update live
```

---

## File Change Summary

### New files
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
src/api/routers/simulator.py
web/  (React/Vite project)
```

### Modified files
```
.env                              (add NEO4J_* vars, remove old orders-only vars)
connectors/orders-connector.json  (retire or repurpose for P2P)
src/consumer/kafka_consumer.py    (5 topics, Neo4j driver, engine-driven routing)
src/transformer/router.py         (delegate to mapping_engine)
src/transformer/transformer.py    (delegate to mapping_engine or remove)
src/writer/pg_writer.py           (add generic upsert_table / delete_cascade)
src/connector/manager.py          (register p2p-connector)
scripts/verify.py                 (add P2P + Neo4j checks)
```

### Retired (no longer needed once engine is in place)
```
src/transformer/transformer.py    (logic absorbed into mapping_engine)
```

---

## Open Questions (need your input before starting)

1. **Keep the old `orders` pipeline running in parallel, or replace it entirely
   with the P2P collections?**
   Recommendation: replace — the P2P chain subsumes the old `orders` test schema.

2. **Neo4j instance**: local Community Edition (desktop app or bare bolt) or AuraDB
   free tier? This affects the connection string in `.env`.

3. **Web app frontend**: React/Vite (full SPA, better graph viz) or Streamlit
   (faster to build, limited graph support)? Recommended: React for the final demo.

4. **Authentication on the web app?** For a POC/academic project, no auth is fine —
   just localhost.

5. **Should the web app's "insert document" UI do direct MongoDB writes, or go
   through the same simulator logic?** Recommendation: direct MongoDB write via
   PyMongo from FastAPI — this is what triggers real CDC events.
