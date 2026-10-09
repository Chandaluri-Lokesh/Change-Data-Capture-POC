"""
P2P document simulator — generates realistic Procure-to-Pay traffic into MongoDB.

Each iteration inserts a complete linked chain:
  RFQ → PurchaseOrder → ASN → GRN → Invoice

After chains are seeded, the loop randomly applies status updates to existing
documents to simulate real procurement lifecycle progression.

Collections used (in mydb):
  rfqs, purchase_orders, asns, grns, invoices

Run
───
  python src/generator/p2p_simulator.py
"""

import logging
import os
import random
import sys
import time
from datetime import date, datetime, timedelta, timezone

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import OperationFailure

_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
load_dotenv(os.path.join(_root, '.env'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s')
logger = logging.getLogger(__name__)

DIRECT_URI = os.getenv('MONGO_DIRECT_URI', 'mongodb://localhost:27018/?directConnection=true')
MONGO_URI  = os.getenv('MONGO_URI',        'mongodb://localhost:27018/?replicaSet=rs0')
RS_HOST    = os.getenv('MONGO_RS_HOST',    'localhost:27018')
DB_NAME    = 'mydb'

PLANTS    = ['Plant-Chennai-01', 'Plant-Mumbai-02', 'Plant-Delhi-03', 'Plant-Pune-04']
CARRIERS  = ['BlueDart Logistics', 'FedEx India', 'DHL Express', 'DTDC Courier']
MATERIALS = [
    {'code': 'MAT-7734', 'desc': 'Stainless Steel Pipe Fittings, DN50', 'unit_price': 18.75},
    {'code': 'MAT-8821', 'desc': 'Industrial Gasket Set',                'unit_price': 6.40},
    {'code': 'MAT-9002', 'desc': 'Hex Bolt M16x80 Grade 8.8',            'unit_price': 0.85},
    {'code': 'MAT-6611', 'desc': 'Pressure Relief Valve, 16 bar',        'unit_price': 142.00},
    {'code': 'MAT-5523', 'desc': 'PTFE Thread Seal Tape Roll',            'unit_price': 1.20},
]
VENDORS = [
    {'id': 'V-1042', 'name': 'Ashford Industrial Supplies'},
    {'id': 'V-1087', 'name': 'Meridian Components Ltd.'},
    {'id': 'V-2031', 'name': 'Pinnacle Engineering Works'},
    {'id': 'V-3007', 'name': 'Apex Fasteners Pvt. Ltd.'},
]

_seq = random.randint(10000, 50000)


def _next_seq() -> int:
    global _seq
    _seq += 1
    return _seq


def _today_plus(days: int) -> str:
    return (date.today() + timedelta(days=days)).isoformat()


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def _pick_line_items(n: int = 2) -> list:
    chosen = random.sample(MATERIALS, min(n, len(MATERIALS)))
    return [
        {
            'line_no':         i + 1,
            'material_code':   m['code'],
            'description':     m['desc'],
            'quantity':        random.randint(50, 1000),
            'unit_of_measure': 'EA',
            'unit_price':      m['unit_price'],
        }
        for i, m in enumerate(chosen)
    ]


def generate_chain(seq: int) -> dict:
    """Generate one complete RFQ→PO→ASN→GRN→Invoice chain as a dict of 5 documents."""
    year = date.today().year
    rfq_id  = f'RFQ-{year}-{seq:05d}'
    po_id   = f'PO-{year}-{seq:05d}'
    asn_id  = f'ASN-{year}-{seq:05d}'
    grn_id  = f'GRN-{year}-{seq:05d}'
    inv_id  = f'INV-{year}-{seq:05d}'

    plant    = random.choice(PLANTS)
    vendor   = random.choice(VENDORS)
    invited  = random.sample(VENDORS, 2)
    line_items = _pick_line_items(2)
    order_total = sum(li['quantity'] * li['unit_price'] for li in line_items)

    rfq = {
        '_id':              rfq_id,
        'document_type':    'RFQ',
        'rfq_number':       rfq_id,
        'requested_date':   _today_plus(0),
        'requested_by':     f'procurement.{plant.lower().replace("-",".")}@buyerco.com',
        'plant':            plant,
        'status':           'OPEN',
        'response_due_date': _today_plus(10),
        'invited_vendors': [
            {'vendor_id': v['id'], 'vendor_name': v['name'], 'invited_on': _today_plus(0)}
            for v in invited
        ],
        'line_items': [
            {
                'line_no':              li['line_no'],
                'material_code':        li['material_code'],
                'description':          li['description'],
                'quantity':             li['quantity'],
                'unit_of_measure':      li['unit_of_measure'],
                'target_delivery_date': _today_plus(30),
            }
            for li in line_items
        ],
        'created_at': _now_iso(),
        'updated_at': _now_iso(),
    }

    po = {
        '_id':            po_id,
        'document_type':  'PO',
        'po_number':      po_id,
        'rfq_number':     rfq_id,
        'vendor_id':      vendor['id'],
        'vendor_name':    vendor['name'],
        'order_date':     _today_plus(2),
        'delivery_date':  _today_plus(32),
        'plant':          plant,
        'currency':       'USD',
        'status':         'OPEN',
        'payment_terms':  'NET-30',
        'line_items': [
            {
                'line_no':         li['line_no'],
                'material_code':   li['material_code'],
                'description':     li['description'],
                'quantity':        li['quantity'],
                'unit_of_measure': li['unit_of_measure'],
                'unit_price':      li['unit_price'],
                'line_total':      round(li['quantity'] * li['unit_price'], 2),
            }
            for li in line_items
        ],
        'order_total':  round(order_total, 2),
        'created_at':   _now_iso(),
        'updated_at':   _now_iso(),
    }

    asn = {
        '_id':              asn_id,
        'document_type':    'ASN',
        'asn_number':       asn_id,
        'po_number':        po_id,
        'vendor_id':        vendor['id'],
        'ship_date':        _today_plus(10),
        'carrier':          random.choice(CARRIERS),
        'tracking_number':  f'TRK{random.randint(1000000000, 9999999999)}',
        'expected_arrival': _today_plus(20),
        'status':           'IN_TRANSIT',
        'line_items': [
            {
                'line_no':         li['line_no'],
                'material_code':   li['material_code'],
                'quantity_shipped': li['quantity'],
                'unit_of_measure': li['unit_of_measure'],
            }
            for li in line_items
        ],
        'created_at': _now_iso(),
        'updated_at': _now_iso(),
    }

    # Simulate a small shortage on one line
    grn_lines = []
    for li in line_items:
        shortfall = random.randint(0, max(1, li['quantity'] // 20))
        received  = li['quantity'] - shortfall
        grn_lines.append({
            'line_no':         li['line_no'],
            'material_code':   li['material_code'],
            'quantity_received': received,
            'unit_of_measure': li['unit_of_measure'],
            'condition':       'ACCEPTED' if shortfall == 0 else 'ACCEPTED_SHORT',
            'remarks':         (
                f'{shortfall} units short; discrepancy logged against {asn_id}'
                if shortfall else None
            ),
        })

    grn = {
        '_id':           grn_id,
        'document_type': 'GRN',
        'grn_number':    grn_id,
        'po_number':     po_id,
        'asn_number':    asn_id,
        'receipt_date':  _today_plus(21),
        'received_by':   f'warehouse.{plant.lower().replace("-",".")}@buyerco.com',
        'plant':         plant,
        'status':        'COMPLETED',
        'line_items':    grn_lines,
        'created_at':    _now_iso(),
        'updated_at':    _now_iso(),
    }

    tax_rate = 0.08
    subtotal = sum(
        li['unit_price'] * gl['quantity_received']
        for li, gl in zip(line_items, grn_lines)
    )
    tax_amount   = round(subtotal * tax_rate, 2)
    total_amount = round(subtotal + tax_amount, 2)

    invoice = {
        '_id':            inv_id,
        'document_type':  'INVOICE',
        'invoice_number': inv_id,
        'po_number':      po_id,
        'grn_number':     grn_id,
        'vendor_id':      vendor['id'],
        'invoice_date':   _today_plus(23),
        'due_date':       _today_plus(53),
        'currency':       'USD',
        'status':         'PENDING_PAYMENT',
        'line_items': [
            {
                'line_no':       li['line_no'],
                'material_code': li['material_code'],
                'quantity':      gl['quantity_received'],
                'unit_price':    li['unit_price'],
                'amount':        round(li['unit_price'] * gl['quantity_received'], 2),
            }
            for li, gl in zip(line_items, grn_lines)
        ],
        'subtotal':      round(subtotal, 2),
        'tax_rate':      tax_rate,
        'tax_amount':    tax_amount,
        'total_amount':  total_amount,
        'created_at':    _now_iso(),
        'updated_at':    _now_iso(),
    }

    return {'rfq': rfq, 'po': po, 'asn': asn, 'grn': grn, 'invoice': invoice}


def _setup_mongodb(client: MongoClient) -> None:
    """Initialise replica set (if not already) and create P2P collections."""
    try:
        client.admin.command('replSetGetStatus')
        logger.info('Replica set already initialised.')
    except OperationFailure:
        logger.info(f'Initialising replica set to {RS_HOST}...')
        client.admin.command('replSetInitiate', {
            '_id': 'rs0',
            'members': [{'_id': 0, 'host': RS_HOST}],
        })
        time.sleep(5)

    db = client[DB_NAME]
    existing = db.list_collection_names()
    for col in ('rfqs', 'purchase_orders', 'asns', 'grns', 'invoices'):
        if col not in existing:
            db.create_collection(col)
            logger.info(f'Created collection: {col}')


def run_simulation() -> None:
    logger.info('Connecting to MongoDB...')
    for _ in range(30):
        try:
            client = MongoClient(DIRECT_URI, serverSelectionTimeoutMS=2000)
            client.admin.command('ping')
            break
        except Exception:
            logger.info('Waiting for MongoDB...')
            time.sleep(2)
    else:
        logger.error('MongoDB did not become available — aborting.')
        sys.exit(1)

    _setup_mongodb(client)
    db = client[DB_NAME]

    # Seed a few initial chains
    logger.info('Seeding initial P2P chains...')
    active_chains = []
    for _ in range(5):
        chain = generate_chain(_next_seq())
        db['rfqs'].replace_one({'_id': chain['rfq']['_id']}, chain['rfq'], upsert=True)
        db['purchase_orders'].replace_one({'_id': chain['po']['_id']}, chain['po'], upsert=True)
        db['asns'].replace_one({'_id': chain['asn']['_id']}, chain['asn'], upsert=True)
        db['grns'].replace_one({'_id': chain['grn']['_id']}, chain['grn'], upsert=True)
        db['invoices'].replace_one({'_id': chain['invoice']['_id']}, chain['invoice'], upsert=True)
        active_chains.append(chain)
        logger.info(f"Seeded chain: {chain['rfq']['_id']} → {chain['invoice']['_id']}")

    logger.info('Starting live simulation loop...')
    po_statuses  = ['OPEN', 'CONFIRMED', 'PARTIAL_DELIVERY', 'CLOSED']
    asn_statuses = ['IN_TRANSIT', 'DELIVERED', 'PARTIALLY_DELIVERED']
    inv_statuses = ['PENDING_PAYMENT', 'APPROVED', 'PAID', 'DISPUTED']

    while True:
        action = random.choices(
            ['insert_chain', 'update_po', 'update_asn', 'update_invoice'],
            weights=[30, 25, 20, 25],
        )[0]

        try:
            if action == 'insert_chain':
                chain = generate_chain(_next_seq())
                db['rfqs'].replace_one({'_id': chain['rfq']['_id']}, chain['rfq'], upsert=True)
                db['purchase_orders'].replace_one({'_id': chain['po']['_id']}, chain['po'], upsert=True)
                db['asns'].replace_one({'_id': chain['asn']['_id']}, chain['asn'], upsert=True)
                db['grns'].replace_one({'_id': chain['grn']['_id']}, chain['grn'], upsert=True)
                db['invoices'].replace_one({'_id': chain['invoice']['_id']}, chain['invoice'], upsert=True)
                active_chains.append(chain)
                logger.info(f"[INSERT] Chain {chain['rfq']['_id']}")

            elif action == 'update_po' and active_chains:
                chain = random.choice(active_chains)
                new_status = random.choice(po_statuses)
                db['purchase_orders'].update_one(
                    {'_id': chain['po']['_id']},
                    {'$set': {'status': new_status, 'updated_at': _now_iso()}},
                )
                logger.info(f"[UPDATE] PO {chain['po']['_id']} → status={new_status}")

            elif action == 'update_asn' and active_chains:
                chain = random.choice(active_chains)
                new_status = random.choice(asn_statuses)
                db['asns'].update_one(
                    {'_id': chain['asn']['_id']},
                    {'$set': {'status': new_status, 'updated_at': _now_iso()}},
                )
                logger.info(f"[UPDATE] ASN {chain['asn']['_id']} → status={new_status}")

            elif action == 'update_invoice' and active_chains:
                chain = random.choice(active_chains)
                new_status = random.choice(inv_statuses)
                db['invoices'].update_one(
                    {'_id': chain['invoice']['_id']},
                    {'$set': {'status': new_status, 'updated_at': _now_iso()}},
                )
                logger.info(f"[UPDATE] Invoice {chain['invoice']['_id']} → status={new_status}")

        except Exception as exc:
            logger.error(f'Simulation error during {action}: {exc}')

        time.sleep(random.uniform(1.0, 3.0))


if __name__ == '__main__':
    run_simulation()
