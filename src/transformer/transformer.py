"""
Field mapping: ParsedEvent → Postgres table row dicts.

Mapping rules
─────────────
orders (op=c/u/r) → analytics.orders_flat
  order_id    = _id  (coerced to str by parser)
  customer_id = customer_id
  status      = status
  city        = city
  total       = total  (float or None)
  created_at  = ts_ms converted to UTC datetime
  updated_at  = ts_ms converted to UTC datetime

orders (op=c/u/r) → finance.transactions
  txn_id      = "txn-{order_id}"  (deterministic → idempotent upsert)
  order_id    = _id
  amount      = total
  status      = status
  recorded_at = ts_ms converted to UTC datetime

Edge cases
──────────
- Missing fields: .get() with None default — never raises KeyError.
- total=None or items=[]: handled gracefully (None in row, empty list).
- _id missing from document: falls back to ParsedEvent.doc_id.
- Invalid item entries in items list: skipped silently (non-dict entries).
- ts_ms=0: falls back to datetime.now(utc) to avoid epoch timestamps in PG.
"""

from datetime import datetime, timezone
from typing import Optional

from transformer.debezium_parser import ParsedEvent


def _ts_to_utc(ts_ms: int) -> datetime:
    """Convert Debezium ts_ms (epoch ms) to a timezone-aware UTC datetime."""
    if ts_ms:
        return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    return datetime.now(tz=timezone.utc)


def to_orders_flat(event: ParsedEvent) -> Optional[dict]:
    """
    Map an orders ParsedEvent to an analytics.orders_flat row.
    Returns None when the event has no document body (e.g. op=d).
    """
    if not event.document:
        return None

    doc = event.document
    ts  = _ts_to_utc(event.ts_ms)

    return {
        'order_id':    str(doc.get('_id') or event.doc_id),
        'customer_id': doc.get('customer_id'),
        'status':      doc.get('status'),
        'city':        doc.get('city'),
        'total':       float(doc['total']) if doc.get('total') is not None else None,
        'created_at':  ts,
        'updated_at':  ts,
    }


def to_transaction(event: ParsedEvent) -> Optional[dict]:
    """
    Map an orders ParsedEvent to a finance.transactions row.

    txn_id is derived deterministically as "txn-{order_id}" so that
    re-delivered events produce the same txn_id — safe for ON CONFLICT upsert.
    Returns None when the event has no document body.
    """
    if not event.document:
        return None

    doc      = event.document
    order_id = str(doc.get('_id') or event.doc_id)
    ts       = _ts_to_utc(event.ts_ms)

    return {
        'txn_id':      f"txn-{order_id}",
        'order_id':    order_id,
        'amount':      float(doc['total']) if doc.get('total') is not None else None,
        'status':      doc.get('status'),
        'recorded_at': ts,
    }
