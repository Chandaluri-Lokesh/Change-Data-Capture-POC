# POC Debezium - Implementation Master Checklist

This checklist tracks the precise 6-Phase plan from the core HTML design, updated exclusively for a Native Local (Windows + WSL) architecture (no Docker).

## Phase 1: Infrastructure Setup (Foundation) - [COMPLETED]
- [x] **M1 — Infrastructure Stack**
  - [x] Configure Apache Kafka (KRaft mode; no ZooKeeper) (Local/WSL)
  - [x] Configure Kafka Connect with Debezium MongoDB plugin script (`setup_ubuntu_debezium.sh`)
  - [x] Strip environment configurations (`.env`) uniquely targeting local bare-metal ports natively.
- [x] **M2 — Schema Initialization**
  - [x] Create idempotent Postgres DDL initialization (`init/postgres/*.sql`).
  - [x] Transition MongoDB Replica Set instantiation out of Docker deeply into the Python Simulator.
  - [x] Develop MongoDB collection strict-validators natively for orders, customers, and products on boot.

## Phase 2: Data Generator (Data In) - [COMPLETED]
- [x] **M3 — Event Simulator (`src/generator/`)**
  - [x] Write `order_gen.py` using `Faker` to generate authentic transaction schemas.
  - [x] Write `simulator.py` to authentically seed 10 initial core snapshots effortlessly.
  - [x] Build an infinite looping workflow continuously dispatching `INSERT`, `UPDATE`, `REPLACE`, and `DELETE` operations securely.

## Phase 3: Debezium Connector (CDC Capture) - [COMPLETED]
- [x] **M4 — Connector Registration (`connectors/`)**
  - [x] Write `orders-connector.json` explicitly bypassing Schema Registry configurations for pure raw `JsonConverters`.
  - [x] Write `products-connector.json` guaranteeing core initial snapshot capturing mode.
- [x] **M5 — Connector Lifecycle Manager (`src/connector/`)**
  - [x] Write `manager.py` that waits flawlessly for Kafka Connect REST API on Port `8083`.
  - [x] Implement logic verifying idempotency (Updates existing configs over POST/PUT APIs naturally without overlapping connectors).
  - [x] Build the `health_check()` watchdog to forcefully restart any `FAILED` Debezium Task gracefully.

## Phase 4: Core Pipeline (Consumer, Transformer & Writer) - [COMPLETED]
- [x] **M6 — Kafka Consumer (`src/consumer/kafka_consumer.py`)**
  - [x] Manual offset commits after full batch (success + DLQ'd) — exactly-once semantics.
  - [x] Batch polling via `consumer.consume(num_messages=100, timeout=1.0)`.
  - [x] Row-by-row error handling — single bad row → DLQ, rest of batch unaffected.
- [x] **M7 — Debezium Envelope Parser (`src/transformer/debezium_parser.py`)**
  - [x] Handles op=c/u/d/r/tombstone/unknown.
  - [x] BSON coercion: `$oid`, `$date`, `$numberDecimal`, `$numberLong`, `$numberInt`.
  - [x] Tombstone (null msg_value) detected and returned as op='tombstone'.
  - [x] Delete doc_id extracted from Kafka message key.
- [x] **M8 — Postgres Writer (`src/writer/pg_writer.py`)**
  - [x] asyncpg connection pools with single-retry on stale connection.
  - [x] `ON CONFLICT DO UPDATE` upserts for orders_flat and transactions.
  - [x] line_items: delete-then-insert for idempotency (no unique constraint in DDL).
  - [x] Concurrent analytics + finance deletes via `asyncio.gather`.
- [x] **M9 — Dead Letter Queue (`src/consumer/dlq.py`)**
  - [x] Failed events → `poc.dlq` with metadata (source, error, retry_count, failed_at).
  - [x] retry_count >= MAX_RETRIES (3) → written to `dlq.dead.log` to prevent infinite loops.

## Phase 5: Resilience (Offset Management & Recovery) - [COMPLETED]
- [x] **M10 — Offset & Resume Management (`src/ops/offset_manager.py`)**
  - [x] `show_lag()` — per-partition committed offset, HWM, lag table.
  - [x] `reset_to_earliest()` / `reset_to_latest()` with confirmation prompt.
  - [x] SQLite audit log (`offsets.db`) for committed offset history.
- [x] **M11 — Schema Drift Handler (`src/transformer/schema_guard.py`)**
  - [x] Required field validation per collection; missing/None fields → DLQ.
  - [x] Unknown fields logged and stored in `ParsedEvent.extra_fields` for audit.

## Phase 6: Validate (Verification & Observability) - [COMPLETED]
- [x] **M12 — Verification Suite (`scripts/verify.py`)**
  - [x] Count parity: MongoDB orders ≈ Postgres orders_flat (within tolerance).
  - [x] Insert latency probe: measures ms until row appears in both PG targets.
  - [x] Update propagation: status change reflected within MAX_WAIT_S.
  - [x] Delete propagation: order disappears from both targets.
  - [x] Duplicate safety: same order re-delivered → only 1 row (upsert idempotency).
- [x] **M13 — Observability (`src/metrics.py`)**
  - [x] CLI dashboard refreshing every 10 s: consumer lag, PG row counts, connector status.
  - [x] `CDCJsonFormatter` for structured JSON log lines (op_type, collection, latency_ms …).
  - [x] `scripts/dlq_replay.py` — replay poc.dlq back to source topics with retry guard.

## Package Structure
- [x] `__init__.py` files added to all src subpackages (consumer, transformer, writer, ops, connector, generator).
- [x] Removed: `deprecated/`, `init/mongo_init.js`, `src/connector/register.py` (superseded or blank).
- [x] Unit tests: `tests/test_parser.py`, `tests/test_transformer.py`, `tests/test_writer.py`.
- [x] `tests/conftest.py` sets `PYTHONPATH=src` for all tests.
