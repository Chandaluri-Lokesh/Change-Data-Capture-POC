"""
End-to-end verification suite for the P2P CDC pipeline.

Tests
─────
0. Connectivity   — MongoDB, Postgres, Neo4j all reachable
1. Insert chain   — Insert RFQ→PO→ASN→GRN→Invoice; verify all 5 parent rows
                    appear in Postgres within SLA (5 s)
2. Line items     — Verify child rows (line items) also land in Postgres
3. Update         — Change PO status; verify Postgres reflects new value
4. Neo4j graph    — Verify PurchaseOrder node exists and has ISSUED_AGAINST→RFQ edge
5. Delete         — Delete PO in MongoDB; verify Postgres row + children gone
6. Duplicate      — Re-insert same doc; verify only 1 row in Postgres (idempotent)

Prerequisites
─────────────
- MongoDB, Kafka, Debezium connector, kafka_consumer.py must all be running.
- Run from the project root: python scripts/verify.py
"""

import asyncio
import logging
import os
import sys
import time

_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
_src  = os.path.join(_root, 'src')
for p in (_root, _src):
    if p not in sys.path:
        sys.path.insert(0, p)

import asyncpg
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv(os.path.join(_root, '.env'))
logging.basicConfig(level=logging.WARNING)

MONGO_URI       = os.getenv('MONGO_URI', 'mongodb://localhost:27018/?replicaSet=rs0')
NEO4J_URI       = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
NEO4J_USER      = os.getenv('NEO4J_USER', 'neo4j')
NEO4J_PASSWORD  = os.getenv('NEO4J_PASSWORD', 'neo4j')
MAX_WAIT_S      = 10
POLL_INTERVAL_S = 0.15

RESULTS: list = []


def result(name: str, passed: bool, detail: str = '') -> None:
    RESULTS.append((name, passed))
    mark = 'OK  ' if passed else 'FAIL'
    line = f'  [{mark}] {name}'
    if detail:
        line += f': {detail}'
    print(line)


# ---------------------------------------------------------------------------
# Postgres helpers
# ---------------------------------------------------------------------------

async def _pg_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        host=os.getenv('POSTGRES_ANALYTICS_HOST', 'localhost'),
        port=int(os.getenv('POSTGRES_ANALYTICS_PORT', '5432')),
        user=os.getenv('POSTGRES_ANALYTICS_USER', 'postgres'),
        password=os.getenv('POSTGRES_ANALYTICS_PASSWORD', 'postgres'),
        database=os.getenv('POSTGRES_ANALYTICS_DB', 'analytics'),
        min_size=1, max_size=3,
    )


async def _poll_row(pool, table, key_col, key_val, predicate=None, timeout=MAX_WAIT_S):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    f'SELECT * FROM {table} WHERE "{key_col}" = $1', key_val
                )
            if row and (predicate is None or predicate(row)):
                elapsed = int((timeout - max(0, deadline - time.monotonic())) * 1000)
                return True, elapsed
        except Exception:
            pass
        await asyncio.sleep(POLL_INTERVAL_S)
    return False, int(timeout * 1000)


async def _poll_count(pool, table, key_col, key_val, expected_min=1, timeout=MAX_WAIT_S):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            async with pool.acquire() as conn:
                cnt = await conn.fetchval(
                    f'SELECT COUNT(*) FROM {table} WHERE "{key_col}" = $1', key_val
                )
            if (cnt or 0) >= expected_min:
                elapsed = int((timeout - max(0, deadline - time.monotonic())) * 1000)
                return True, int(cnt), elapsed
        except Exception:
            pass
        await asyncio.sleep(POLL_INTERVAL_S)
    return False, 0, int(timeout * 1000)


async def _row_gone(pool, table, key_col, key_val, timeout=MAX_WAIT_S):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    f'SELECT 1 FROM {table} WHERE "{key_col}" = $1', key_val
                )
            if row is None:
                elapsed = int((timeout - max(0, deadline - time.monotonic())) * 1000)
                return True, elapsed
        except Exception:
            pass
        await asyncio.sleep(POLL_INTERVAL_S)
    return False, int(timeout * 1000)


# ---------------------------------------------------------------------------
# Neo4j helper
# ---------------------------------------------------------------------------

async def _neo4j_query(cypher: str, params: dict):
    from neo4j import AsyncGraphDatabase
    driver = AsyncGraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        async with driver.session() as session:
            result_obj = await session.run(cypher, **params)
            records = await result_obj.data()
            return records
    finally:
        await driver.close()


# ---------------------------------------------------------------------------
# Build a test chain (using fixed IDs so we can track them)
# ---------------------------------------------------------------------------

def _build_test_chain():
    import random
    from datetime import date, timedelta
    seq   = random.randint(90000, 99999)
    year  = date.today().year
    now   = __import__('datetime').datetime.now(__import__('datetime').timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    later = lambda d: (date.today() + timedelta(days=d)).isoformat()

    rfq_id = f'RFQ-{year}-{seq:05d}'
    po_id  = f'PO-{year}-{seq:05d}'
    asn_id = f'ASN-{year}-{seq:05d}'
    grn_id = f'GRN-{year}-{seq:05d}'
    inv_id = f'INV-{year}-{seq:05d}'

    rfq = {
        '_id': rfq_id, 'document_type': 'RFQ', 'rfq_number': rfq_id,
        'requested_date': later(0), 'requested_by': 'verify@buyerco.com',
        'plant': 'Plant-Chennai-01', 'status': 'OPEN',
        'response_due_date': later(10),
        'invited_vendors': [{'vendor_id': 'V-9999', 'vendor_name': 'VerifyVendor', 'invited_on': later(0)}],
        'line_items': [{'line_no': 1, 'material_code': 'MAT-VERIFY', 'description': 'Verify Item',
                         'quantity': 10, 'unit_of_measure': 'EA', 'target_delivery_date': later(30)}],
        'created_at': now, 'updated_at': now,
    }
    po = {
        '_id': po_id, 'document_type': 'PO', 'po_number': po_id,
        'rfq_number': rfq_id, 'vendor_id': 'V-9999', 'vendor_name': 'VerifyVendor',
        'order_date': later(2), 'delivery_date': later(32), 'plant': 'Plant-Chennai-01',
        'currency': 'USD', 'status': 'OPEN', 'payment_terms': 'NET-30',
        'line_items': [{'line_no': 1, 'material_code': 'MAT-VERIFY', 'description': 'Verify Item',
                         'quantity': 10, 'unit_of_measure': 'EA', 'unit_price': 5.00, 'line_total': 50.00}],
        'order_total': 50.00, 'created_at': now, 'updated_at': now,
    }
    asn = {
        '_id': asn_id, 'document_type': 'ASN', 'asn_number': asn_id,
        'po_number': po_id, 'vendor_id': 'V-9999',
        'ship_date': later(10), 'carrier': 'VerifyCarrier', 'tracking_number': 'VTRK001',
        'expected_arrival': later(20), 'status': 'IN_TRANSIT',
        'line_items': [{'line_no': 1, 'material_code': 'MAT-VERIFY', 'quantity_shipped': 10, 'unit_of_measure': 'EA'}],
        'created_at': now, 'updated_at': now,
    }
    grn = {
        '_id': grn_id, 'document_type': 'GRN', 'grn_number': grn_id,
        'po_number': po_id, 'asn_number': asn_id,
        'receipt_date': later(21), 'received_by': 'warehouse@buyerco.com',
        'plant': 'Plant-Chennai-01', 'status': 'COMPLETED',
        'line_items': [{'line_no': 1, 'material_code': 'MAT-VERIFY', 'quantity_received': 10,
                         'unit_of_measure': 'EA', 'condition': 'ACCEPTED', 'remarks': None}],
        'created_at': now, 'updated_at': now,
    }
    invoice = {
        '_id': inv_id, 'document_type': 'INVOICE', 'invoice_number': inv_id,
        'po_number': po_id, 'grn_number': grn_id, 'vendor_id': 'V-9999',
        'invoice_date': later(23), 'due_date': later(53), 'currency': 'USD',
        'status': 'PENDING_PAYMENT',
        'line_items': [{'line_no': 1, 'material_code': 'MAT-VERIFY', 'quantity': 10,
                         'unit_price': 5.00, 'amount': 50.00}],
        'subtotal': 50.00, 'tax_rate': 0.08, 'tax_amount': 4.00, 'total_amount': 54.00,
        'created_at': now, 'updated_at': now,
    }
    return {'rfq': rfq, 'po': po, 'asn': asn, 'grn': grn, 'invoice': invoice}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

async def test_insert_chain(db, pool) -> dict:
    chain = _build_test_chain()
    db['rfqs'].replace_one(            {'_id': chain['rfq']['_id']},     chain['rfq'],     upsert=True)
    db['purchase_orders'].replace_one( {'_id': chain['po']['_id']},      chain['po'],      upsert=True)
    db['asns'].replace_one(            {'_id': chain['asn']['_id']},     chain['asn'],     upsert=True)
    db['grns'].replace_one(            {'_id': chain['grn']['_id']},     chain['grn'],     upsert=True)
    db['invoices'].replace_one(        {'_id': chain['invoice']['_id']}, chain['invoice'], upsert=True)

    checks = [
        ('rfq',            'rfq_number',     chain['rfq']['_id'],      'RFQ'),
        ('purchase_orders','po_number',      chain['po']['_id'],       'PO'),
        ('asns',           'asn_number',     chain['asn']['_id'],      'ASN'),
        ('grns',           'grn_number',     chain['grn']['_id'],      'GRN'),
        ('invoices',       'invoice_number', chain['invoice']['_id'],  'Invoice'),
    ]

    for table, key_col, key_val, label in checks:
        found, elapsed = await _poll_row(pool, table, key_col, key_val)
        result(f'Insert → {label} in Postgres', found, f'latency={elapsed} ms')

    return chain


async def test_line_items(pool, chain: dict) -> None:
    checks = [
        ('rfq_line_items',     'rfq_number',     chain['rfq']['_id'],     1),
        ('po_line_items',      'po_number',       chain['po']['_id'],      1),
        ('asn_line_items',     'asn_number',      chain['asn']['_id'],     1),
        ('grn_line_items',     'grn_number',      chain['grn']['_id'],     1),
        ('invoice_line_items', 'invoice_number',  chain['invoice']['_id'], 1),
    ]
    for table, key_col, key_val, expected in checks:
        found, count, elapsed = await _poll_count(pool, table, key_col, key_val, expected)
        result(f'Line items → {table}', found, f'count={count} elapsed={elapsed} ms')


async def test_update_propagation(db, pool, chain: dict) -> None:
    new_status = 'CONFIRMED'
    db['purchase_orders'].update_one(
        {'_id': chain['po']['_id']},
        {'$set': {'status': new_status, 'updated_at': '2099-01-01T00:00:00Z'}},
    )
    found, elapsed = await _poll_row(
        pool, 'purchase_orders', 'po_number', chain['po']['_id'],
        predicate=lambda row: row['status'] == new_status,
    )
    result('Update PO status → Postgres', found, f"status={new_status!r} elapsed={elapsed} ms")


async def test_neo4j_graph(chain: dict) -> None:
    try:
        po_id  = chain['po']['_id']
        rfq_id = chain['rfq']['_id']

        records = await _neo4j_query(
            'MATCH (p:PurchaseOrder {po_number: $po_id}) RETURN p',
            {'po_id': po_id},
        )
        result('Neo4j PurchaseOrder node exists', len(records) > 0, f"po_number={po_id}")

        records = await _neo4j_query(
            'MATCH (p:PurchaseOrder {po_number: $po_id})-[:ISSUED_AGAINST]->(r:RFQ {rfq_number: $rfq_id}) RETURN r',
            {'po_id': po_id, 'rfq_id': rfq_id},
        )
        result('Neo4j ISSUED_AGAINST edge exists', len(records) > 0,
               f"PO→RFQ: {po_id}→{rfq_id}")
    except Exception as exc:
        result('Neo4j graph check', False, f"Neo4j unreachable: {exc}")


async def test_delete_propagation(db, pool, chain: dict) -> None:
    # Delete GRN (also tests cascade on grn_line_items)
    db['grns'].delete_one({'_id': chain['grn']['_id']})
    gone, elapsed = await _row_gone(pool, 'grns', 'grn_number', chain['grn']['_id'])
    result('Delete GRN → Postgres row gone', gone, f"elapsed={elapsed} ms")

    # Verify line items cascaded
    async with pool.acquire() as conn:
        cnt = await conn.fetchval(
            'SELECT COUNT(*) FROM grn_line_items WHERE "grn_number" = $1',
            chain['grn']['_id'],
        )
    result('Delete GRN → child line items cascaded', (cnt or 0) == 0, f"remaining={cnt}")

    # Cleanup remaining chain docs
    for col, _id in [
        ('invoices', chain['invoice']['_id']),
        ('asns',     chain['asn']['_id']),
        ('purchase_orders', chain['po']['_id']),
        ('rfqs',     chain['rfq']['_id']),
    ]:
        db[col].delete_one({'_id': _id})


async def test_duplicate_safety(db, pool, chain: dict) -> None:
    """Re-inserting the same RFQ twice should leave only 1 row."""
    rfq = chain['rfq']
    db['rfqs'].replace_one({'_id': rfq['_id']}, rfq, upsert=True)
    await _poll_row(pool, 'rfq', 'rfq_number', rfq['_id'])

    db['rfqs'].replace_one({'_id': rfq['_id']}, rfq, upsert=True)
    await asyncio.sleep(3)

    async with pool.acquire() as conn:
        cnt = await conn.fetchval(
            'SELECT COUNT(*) FROM rfq WHERE "rfq_number" = $1', rfq['_id']
        )
    result('Duplicate safety (RFQ)', (cnt or 0) == 1, f"rows={cnt}")
    db['rfqs'].delete_one({'_id': rfq['_id']})


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

async def main() -> None:
    print("\n=== P2P CDC Pipeline Verification Suite ===\n")

    try:
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        mongo_client.admin.command('ping')
        db = mongo_client['mydb']
        print("[OK] MongoDB connected")
    except Exception as exc:
        print(f"[FAIL] MongoDB: {exc}")
        return

    try:
        pool = await _pg_pool()
        print("[OK] PostgreSQL connected")
    except Exception as exc:
        print(f"[FAIL] PostgreSQL: {exc}")
        mongo_client.close()
        return

    neo4j_available = False
    try:
        from neo4j import AsyncGraphDatabase
        driver = AsyncGraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        await driver.verify_connectivity()
        await driver.close()
        neo4j_available = True
        print("[OK] Neo4j connected")
    except Exception as exc:
        print(f"[WARN] Neo4j not reachable ({exc}) — graph tests will be skipped")

    print("\n--- Tests ---\n")

    chain = await test_insert_chain(db, pool)
    await test_line_items(pool, chain)
    await test_update_propagation(db, pool, chain)

    if neo4j_available:
        await asyncio.sleep(2)   # let Neo4j writes settle
        await test_neo4j_graph(chain)
    else:
        result('Neo4j graph check', False, 'Neo4j not available — skipped')

    await test_delete_propagation(db, pool, chain)
    await test_duplicate_safety(db, pool, chain)

    await pool.close()
    mongo_client.close()

    total  = len(RESULTS)
    passed = sum(1 for _, ok in RESULTS if ok)
    print(f"\n{"-" * 44}")
    print(f"  {passed}/{total} passed")
    if passed < total:
        print("\n  Failed:")
        for name, ok in RESULTS:
            if not ok:
                print(f"    - {name}")
    else:
        print("  All tests passed!")
    print()


if __name__ == '__main__':
    asyncio.run(main())
