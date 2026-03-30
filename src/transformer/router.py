"""
Event routing: maps each ParsedEvent to a list of write targets.

Returns a list of (db_target, table, row_dict) tuples.

Routing rules
─────────────
orders op=c/u/r → ('analytics', 'orders_flat', row)
                   ('finance',   'transactions', row)
orders op=d     → ('analytics', 'orders_flat', {'order_id': id, '_delete': True})
                   ('finance',   'transactions', {'order_id': id, '_delete': True})
tombstone/unknown → []  (caller skips)

Edge cases
──────────
- op='r' (snapshot): treated identically to insert — upsert handles duplicates.
- op='tombstone': returns [] so caller skips without error.
- Unknown collection: logged at DEBUG; returns [].
- to_orders_flat / to_transaction returning None: those targets are skipped.
"""

import logging
from typing import List, Tuple

from transformer.debezium_parser import ParsedEvent
from transformer.transformer import to_orders_flat, to_transaction

logger = logging.getLogger(__name__)

# Type alias for a single routing result
RouteResult = Tuple[str, str, dict]


def route(event: ParsedEvent) -> List[RouteResult]:
    """
    Determine write targets for a ParsedEvent.

    Args:
        event: A ParsedEvent produced by debezium_parser.parse().

    Returns:
        List of (db_target, table, row_dict) tuples.
        Delete rows include '_delete': True so pg_writer issues a DELETE.
        Empty list for tombstone, unknown op, or unrecognised collection.
    """
    if event.op in ('tombstone', 'unknown'):
        return []

    results: List[RouteResult] = []

    if event.collection == 'orders':

        if event.op in ('c', 'u', 'r'):
            flat = to_orders_flat(event)
            if flat:
                results.append(('analytics', 'orders_flat', flat))

            txn = to_transaction(event)
            if txn:
                results.append(('finance', 'transactions', txn))

        elif event.op == 'd':
            results.append((
                'analytics', 'orders_flat',
                {'order_id': event.doc_id, '_delete': True},
            ))
            results.append((
                'finance', 'transactions',
                {'order_id': event.doc_id, '_delete': True},
            ))

    else:
        logger.debug(
            f"[router] No routing rule for collection={event.collection!r} "
            f"op={event.op!r} — skipping"
        )

    return results
