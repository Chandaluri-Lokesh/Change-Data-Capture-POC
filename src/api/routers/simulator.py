"""
POST /api/simulate/chain   — insert one complete RFQ→PO→ASN→GRN→Invoice chain
POST /api/simulate/update  — update a random document status
POST /api/simulate/start   — start continuous background simulation loop
POST /api/simulate/stop    — stop the background simulation loop
GET  /api/simulate/status  — returns { running: bool }
"""

import logging
import random
import threading
from datetime import datetime, timezone

from fastapi import APIRouter, Request, HTTPException

logger = logging.getLogger(__name__)
router = APIRouter()

# ── Background simulation state ───────────────────────────────────────────────
_sim_thread: threading.Thread | None = None
_stop_event: threading.Event | None  = None


def _simulation_loop(db, stop_event: threading.Event) -> None:
    """Continuous P2P simulation loop — runs in a daemon thread."""
    from generator.p2p_simulator import generate_chain, _next_seq

    po_statuses  = ['OPEN', 'CONFIRMED', 'PARTIAL_DELIVERY', 'CLOSED']
    asn_statuses = ['IN_TRANSIT', 'DELIVERED', 'PARTIALLY_DELIVERED']
    inv_statuses = ['PENDING_PAYMENT', 'APPROVED', 'PAID', 'DISPUTED']
    active_chains: list = []

    logger.info('[simulator] Background loop started')
    while not stop_event.is_set():
        action = random.choices(
            ['insert_chain', 'update_po', 'update_asn', 'update_invoice'],
            weights=[30, 25, 20, 25],
        )[0]
        try:
            if action == 'insert_chain':
                chain = generate_chain(_next_seq())
                db['rfqs'].replace_one(            {'_id': chain['rfq']['_id']},     chain['rfq'],     upsert=True)
                db['purchase_orders'].replace_one( {'_id': chain['po']['_id']},      chain['po'],      upsert=True)
                db['asns'].replace_one(            {'_id': chain['asn']['_id']},     chain['asn'],     upsert=True)
                db['grns'].replace_one(            {'_id': chain['grn']['_id']},     chain['grn'],     upsert=True)
                db['invoices'].replace_one(        {'_id': chain['invoice']['_id']}, chain['invoice'], upsert=True)
                active_chains.append(chain)
                logger.info(f"[simulator] [INSERT] Chain {chain['rfq']['_id']}")

            elif action == 'update_po' and active_chains:
                chain = random.choice(active_chains)
                new_status = random.choice(po_statuses)
                db['purchase_orders'].update_one(
                    {'_id': chain['po']['_id']},
                    {'$set': {'status': new_status, 'updated_at': _now_iso()}},
                )
                logger.info(f"[simulator] [UPDATE] PO {chain['po']['_id']} → {new_status}")

            elif action == 'update_asn' and active_chains:
                chain = random.choice(active_chains)
                new_status = random.choice(asn_statuses)
                db['asns'].update_one(
                    {'_id': chain['asn']['_id']},
                    {'$set': {'status': new_status, 'updated_at': _now_iso()}},
                )
                logger.info(f"[simulator] [UPDATE] ASN {chain['asn']['_id']} → {new_status}")

            elif action == 'update_invoice' and active_chains:
                chain = random.choice(active_chains)
                new_status = random.choice(inv_statuses)
                db['invoices'].update_one(
                    {'_id': chain['invoice']['_id']},
                    {'$set': {'status': new_status, 'updated_at': _now_iso()}},
                )
                logger.info(f"[simulator] [UPDATE] Invoice {chain['invoice']['_id']} → {new_status}")

        except Exception as exc:
            logger.error(f'[simulator] Error during {action}: {exc}')

        stop_event.wait(random.uniform(1.0, 3.0))

    logger.info('[simulator] Background loop stopped')


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


@router.post('/chain')
async def simulate_chain(request: Request):
    """
    Insert a complete, referentially consistent RFQ→PO→ASN→GRN→Invoice chain
    into MongoDB.  Debezium picks up the inserts and the CDC pipeline
    propagates them to Postgres and Neo4j.
    """
    db = request.app.state.mongo_db

    from generator.p2p_simulator import generate_chain, _next_seq
    chain = generate_chain(_next_seq())

    try:
        db['rfqs'].replace_one(            {'_id': chain['rfq']['_id']},     chain['rfq'],     upsert=True)
        db['purchase_orders'].replace_one( {'_id': chain['po']['_id']},      chain['po'],      upsert=True)
        db['asns'].replace_one(            {'_id': chain['asn']['_id']},     chain['asn'],     upsert=True)
        db['grns'].replace_one(            {'_id': chain['grn']['_id']},     chain['grn'],     upsert=True)
        db['invoices'].replace_one(        {'_id': chain['invoice']['_id']}, chain['invoice'], upsert=True)
    except Exception as exc:
        logger.error(f"[simulator] MongoDB write error: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))

    logger.info(f"[simulator] Chain inserted: {chain['rfq']['_id']} → {chain['invoice']['_id']}")
    return {
        'status':       'ok',
        'rfq_id':       chain['rfq']['_id'],
        'po_id':        chain['po']['_id'],
        'asn_id':       chain['asn']['_id'],
        'grn_id':       chain['grn']['_id'],
        'invoice_id':   chain['invoice']['_id'],
        'inserted_at':  _now_iso(),
    }


@router.post('/update')
async def simulate_update(request: Request):
    """
    Pick a random existing PO and update its status.
    Useful for demonstrating CDC update propagation in the demo.
    """
    db = request.app.state.mongo_db
    statuses = ['OPEN', 'CONFIRMED', 'PARTIAL_DELIVERY', 'CLOSED']

    try:
        docs = list(db['purchase_orders'].find({}, {'_id': 1}).limit(20))
        if not docs:
            raise HTTPException(status_code=404, detail='No purchase orders found — run /simulate/chain first')
        target = random.choice(docs)
        new_status = random.choice(statuses)
        db['purchase_orders'].update_one(
            {'_id': target['_id']},
            {'$set': {'status': new_status, 'updated_at': _now_iso()}},
        )
        return {'status': 'ok', 'po_id': target['_id'], 'new_status': new_status}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post('/start')
async def simulator_start(request: Request):
    """Start the continuous background simulation loop."""
    global _sim_thread, _stop_event
    if _sim_thread and _sim_thread.is_alive():
        return {'status': 'already_running'}
    _stop_event = threading.Event()
    db = request.app.state.mongo_db
    _sim_thread = threading.Thread(
        target=_simulation_loop,
        args=(db, _stop_event),
        daemon=True,
        name='p2p-simulator',
    )
    _sim_thread.start()
    return {'status': 'started'}


@router.post('/stop')
async def simulator_stop():
    """Stop the background simulation loop."""
    global _sim_thread, _stop_event
    if not _sim_thread or not _sim_thread.is_alive():
        return {'status': 'not_running'}
    _stop_event.set()
    _sim_thread.join(timeout=5)
    return {'status': 'stopped'}


@router.get('/status')
async def simulator_status():
    """Return whether the background simulation loop is running."""
    running = bool(_sim_thread and _sim_thread.is_alive())
    return {'running': running}
