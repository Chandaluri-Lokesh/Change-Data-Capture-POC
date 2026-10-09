# Local Execution Guide

Step-by-step instructions to run the full P2P CDC pipeline natively on Windows + WSL (no Docker).

---

## What This Runs

| Component | Runs On | Port |
|---|---|---|
| MongoDB (Replica Set rs0) | Windows | 27018 |
| Apache Kafka (KRaft) | WSL | 9092 |
| Kafka Connect + Debezium | WSL | 8083 |
| PostgreSQL (analytics + finance) | Windows | 5432 |
| Neo4j | Windows | 7687 (Bolt), 7474 (Browser) |
| Python CDC Consumer | Windows | — |
| FastAPI backend | Windows | 8000 |
| React dashboard (Vite) | Windows | 5173 |

---

## Prerequisites

### On WSL (Ubuntu)
- Java 11 or 17: `sudo apt install openjdk-17-jdk`
- Apache Kafka 2.13-4.2.0 (KRaft mode — no ZooKeeper)
- Debezium MongoDB plugin installed via `setup_ubuntu_debezium.sh`

### On Windows
- Python 3.10–3.12 (3.14 not supported — C-extension wheels unavailable)
- MongoDB Community 6.x+
- PostgreSQL 14+
- Neo4j Community 5.x
- Node.js 18+ (for the React dashboard)
- All Python packages: `pip install -r requirements.txt`

---

## First-Time Setup

### 1. Run the Windows setup script

```bat
setup.bat
```

This creates `.venv`, installs Python dependencies, creates `mongodb-data/`, applies PostgreSQL DDL, and copies `.env.example` to `.env` if needed.

### 2. Run the WSL Kafka setup script (once)

```bash
# Inside WSL terminal
bash setup_ubuntu_debezium.sh
```

This installs Java 17, Kafka 2.13-4.2.0 in KRaft mode, and the Debezium MongoDB Connector plugin.

### 3. Configure `.env`

Edit `.env` to match your environment:

```ini
# MongoDB
MONGO_URI=mongodb://localhost:27018/?replicaSet=rs0
MONGO_DIRECT_URI=mongodb://localhost:27018/?directConnection=true
MONGO_RS_HOST=<WSL_IP>:27018

# Kafka
KAFKA_BROKER_URL=localhost:9092
KAFKA_CONNECT_REST_URL=http://localhost:8083

# PostgreSQL (analytics and finance share one instance in this POC)
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
NEO4J_PASSWORD=<your-neo4j-password>
```

> **WSL IP note:** The `MONGO_RS_HOST` must be the WSL2 IP for Kafka Connect (running inside WSL) to reach MongoDB (running on Windows). Find it with `cat /etc/resolv.conf` inside WSL and look for `nameserver`.

---

## Step-by-Step Startup

### Step 1: Start Kafka on WSL (KRaft mode)

*Open a WSL terminal:*

```bash
# Format storage once (first run only):
bin/kafka-storage.sh random-uuid      # generates <CLUSTER_ID>
bin/kafka-storage.sh format -t <CLUSTER_ID> -c config/kraft/server.properties

# Start Kafka:
bin/kafka-server-start.sh config/kraft/server.properties
```

### Step 2: Start Kafka Connect on WSL

*Open a second WSL terminal:*

```bash
bin/connect-distributed.sh config/connect-distributed.properties
```

Wait until you see `[KafkaConnect] Kafka Connect started` in the logs (typically 15–30 seconds).

### Step 3: Start MongoDB as a Replica Set

*Open a Windows terminal:*

```powershell
mongod --replSet rs0 --bind_ip localhost --port 27018 --dbpath mongodb-data
```

> If this is a first run, the replica set needs to be initiated. The P2P simulator (Step 5) handles this automatically.

### Step 4: Start Neo4j

Launch Neo4j Desktop or start the service from the Windows Services panel. Confirm it is accessible at `bolt://localhost:7687`.

### Step 5: Start the P2P Simulator

*Open a Windows terminal with the virtualenv active:*

```powershell
.venv\Scripts\activate
python src/generator/p2p_simulator.py
```

On first run, this:
- Initialises the MongoDB replica set (`rs0`)
- Applies collection validators for all 5 P2P collections
- Seeds an initial RFQ → PO → ASN → GRN → Invoice chain
- Starts a continuous loop of inserts, updates, and deletes

### Step 6: Register the Debezium Connector

*Open a new Windows terminal with the virtualenv active:*

```powershell
python src/connector/manager.py register connectors/p2p-connector.json
```

This registers the connector and watches `mydb.rfqs`, `mydb.purchase_orders`, `mydb.asns`, `mydb.grns`, `mydb.invoices`. It produces 5 Kafka topics:

```
poc.mydb.rfqs
poc.mydb.purchase_orders
poc.mydb.asns
poc.mydb.grns
poc.mydb.invoices
```

### Step 7: Start the Python CDC Consumer

```powershell
PYTHONPATH=src python src/consumer/kafka_consumer.py
```

The consumer subscribes to all 5 P2P topics, processes events through the mapping engine, and writes to PostgreSQL (10 tables) and Neo4j simultaneously.

### Step 8: Start the FastAPI Backend

```powershell
uvicorn src.api.main:app --reload --port 8000
```

The backend starts on `http://localhost:8000`. API docs are available at `http://localhost:8000/docs`.

### Step 9: Start the React Dashboard

*Open a new terminal in the `cdc-dashboard-ui/` directory:*

```bash
npm install    # first run only
npm run dev
```

The dashboard opens at `http://localhost:5173`.

---

## Quick-Start (All at Once)

```bat
launch.bat
```

The launch script starts all components in order, polling ports for readiness between steps.

---

## Useful Commands

```bash
# Check connector status
python src/connector/manager.py status p2p-connector

# Check consumer lag
python src/ops/offset_manager.py lag

# Verify MongoDB vs PostgreSQL row counts
python scripts/verify.py

# Replay DLQ events
python scripts/dlq_replay.py

# Run unit tests
python -m pytest tests/ -v
```

---

## Troubleshooting

### Kafka Connect cannot reach MongoDB

Kafka Connect runs in WSL and cannot reach `localhost` on Windows. Set `MONGO_RS_HOST` to the Windows host IP as seen from WSL. Find it:

```bash
# Inside WSL:
cat /etc/resolv.conf | grep nameserver
```

### librdkafka IPv6 delay

On Windows + WSL2, the Python Kafka client may log a ~2s IPv4 timeout before connecting over IPv6. All client configs in this project set `'broker.address.family': 'v6'` to skip the IPv4 attempt.

### Neo4j constraint already exists warnings

On startup, `init/neo4j/constraints.cypher` creates uniqueness constraints with `IF NOT EXISTS`. Neo4j 5.x logs an INFO message when the constraint already exists — this is normal and not an error.

### PostgreSQL DDL not applied

Run manually:

```powershell
psql -U postgres -d analytics -f init/postgres/01_analytics.sql
psql -U postgres -d analytics -f init/postgres/03_metrics.sql
psql -U postgres -d analytics -f init/postgres/04_p2p.sql
psql -U postgres -d finance  -f init/postgres/02_finance.sql
```
