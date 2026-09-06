"""
POST /api/simulate/chain   — insert one complete RFQ→PO→ASN→GRN→Invoice chain
POST /api/simulate/update  — update a random document status
"""

import logging
import random
from datetime import datetime, timezone

from fastapi import APIRouter, Request, HTTPException

logger = logging.getLogger(__name__)
router = APIRouter()


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

    # Import the simulator's generator so we share the same logic
    import sys, os
    _src = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    if _src not in sys.path:
        sys.path.insert(0, _src)

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
