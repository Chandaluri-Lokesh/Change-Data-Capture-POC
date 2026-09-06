# Change Data Capture POC — P2P Procure-to-Pay Pipeline

End-to-end CDC pipeline using Debezium. Captures real-time changes from MongoDB, streams through Kafka, transforms via Python, and writes to PostgreSQL (analytics) and Neo4j (graph).

**No Docker** — runs natively on Windows (MongoDB, Python, Neo4j) + WSL (Kafka KRaft, Kafka Connect + Debezium).

---

## Architecture

```
MongoDB (rs0 :27018)
    └── Debezium MongoDB Connector (Kafka Connect :8083)
            └── Kafka (:9092)  [5 topics: rfqs, purchase_orders, asns, grns, invoices]
                    └── Python Consumer
                            ├── PostgreSQL analytics (:5432)  [11 P2P tables]
                            └── Neo4j DBMD-Minor (:7687)      [7 node types, 13 relationship types]

FastAPI (:8000)  ──  React/Vite (:5173)
```

**Collections tracked**: `rfqs`, `purchase_orders`, `asns`, `grns`, `invoices`

---

## Prerequisites

| Component | Where | Notes |
|---|---|---|
| Python 3.11+ | Windows | Add to PATH |
| MongoDB 8.x | Windows | `mongod.exe` |
| PostgreSQL 14–17 | Windows | `analytics` database |
| Neo4j Desktop | Windows | Create a local DBMS named `DBMD-Minor`, password `password` |
| Node.js LTS | Windows | For React frontend |
| Kafka (KRaft) | WSL | `/home/lokesh/kafka/` |
| Debezium MongoDB plugin | WSL | `/home/lokesh/kafka-plugins/debezium/` |
| WSL mirrored networking | WSL | `~/.wslconfig`: `[wsl2]` / `networkingMode=mirrored` |

---

## First-Time Setup

```bat
setup.bat
```

This creates `.venv`, installs Python deps, applies PostgreSQL DDL, installs React deps, and prints WSL/Neo4j reminders.

---

## Startup Sequence

### WSL (two terminals)

```bash
# Terminal 1 — Kafka broker
/home/lokesh/kafka/bin/kafka-server-start.sh /home/lokesh/kafka/config/server.properties

# Terminal 2 — Kafka Connect + Debezium
/home/lokesh/kafka/bin/connect-distributed.sh /home/lokesh/kafka/config/connect-distributed.properties
```

### Windows

1. Start **Neo4j Desktop** → start the `DBMD-Minor` DBMS
2. Run `launch.bat` (starts MongoDB, registers connector, consumer, simulator, FastAPI, React)

Or manually:

```bat
:: MongoDB
"C:\Program Files\MongoDB\Server\8.2\bin\mongod.exe" --replSet rs0 --bind_ip_all --port 27018 --dbpath mongodb-data --logpath mongodb-data\mongod.log --logappend

:: Register connector
set PYTHONPATH=src && .venv\Scripts\python src\connector\manager.py

:: Consumer
set PYTHONPATH=src && .venv\Scripts\python src\consumer\kafka_consumer.py

:: Simulator
set PYTHONPATH=src && .venv\Scripts\python src\generator\p2p_simulator.py

:: FastAPI
set PYTHONPATH=src && .venv\Scripts\python -m uvicorn api.main:app --port 8000

:: React
cd web && npm run dev
```

---

## Services

| Service | URL |
|---|---|
| FastAPI (Swagger) | http://localhost:8000/docs |
| React Dashboard | http://localhost:5173 |
| Kafka Connect | http://localhost:8083 |
| MongoDB | mongodb://localhost:27018/?replicaSet=rs0 |
| Neo4j Browser | bolt://localhost:7687 |

---

## Key Design Decisions

- **No Schema Registry / Avro** — raw `JsonConverter` for bare-metal simplicity
- **No FK constraints in PostgreSQL** — CDC events arrive out-of-order across Kafka topics; referential integrity guaranteed by MongoDB source
- **YAML-driven mapping engine** — `SAP-Files-Mappings/mapping_rules/*.yaml` defines PostgreSQL columns and Neo4j Cypher for each collection
- **Neo4j MERGE pattern** — relationship Cyphers use `MATCH` for both source and target nodes before `MERGE`-ing the relationship, avoiding uniqueness constraint violations
- **At-least-once delivery** — manual Kafka offset commits after batch; idempotent upserts absorb duplicates
- **DLQ per-row** — bad rows go to `poc.dlq` topic without blocking the rest of the batch
- **`broker.address.family: v4`** — required for rdkafka to connect to Kafka in WSL from Windows

---

## Project Structure

```
src/
  api/            FastAPI app + routers (metrics, pipeline, graph, documents, simulator)
  consumer/       Kafka consumer + DLQ producer
  connector/      Debezium connector registration + watchdog
  engine/         YAML-driven mapping engine
  generator/      P2P chain simulator (RFQ→PO→ASN→GRN→Invoice)
  transformer/    Debezium parser, schema guard, router
  writer/         PostgreSQL (asyncpg) + Neo4j (async driver) writers

SAP-Files-Mappings/mapping_rules/   YAML mapping rules (one per collection)
connectors/       Debezium connector JSON configs
init/postgres/    DDL scripts (04_p2p.sql — 11 tables, no FK constraints)
init/neo4j/       Cypher constraint scripts
web/              React/Vite/Tailwind/TypeScript frontend
```

---

## Environment Variables (`.env`)

```ini
MONGO_URI=mongodb://localhost:27018/?replicaSet=rs0
MONGO_DIRECT_URI=mongodb://localhost:27018/?directConnection=true
MONGO_RS_HOST=localhost:27018
KAFKA_BROKER_URL=localhost:9092
KAFKA_CONNECT_REST_URL=http://localhost:8083
POSTGRES_ANALYTICS_HOST=localhost
POSTGRES_ANALYTICS_PORT=5432
POSTGRES_ANALYTICS_USER=postgres
POSTGRES_ANALYTICS_PASSWORD=postgres
POSTGRES_ANALYTICS_DB=analytics
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password
NEO4J_DATABASE=DBMD-Minor
```
