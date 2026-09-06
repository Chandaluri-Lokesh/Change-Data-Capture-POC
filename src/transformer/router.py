"""
Event routing — thin delegation layer over MappingEngine.

Returns the list of (table_name, rows, upsert_key_cols) tuples that
pg_writer needs.  Neo4j ops are handled separately by the consumer
calling mapping_engine.get_neo4j_ops() directly.

The engine is a module-level singleton loaded once at import time.
Unknown collections (not in any YAML) return [] and are logged at DEBUG.
"""

import logging
from typing import List, Tuple, Optional

from engine.mapping_engine import MappingEngine
from transformer.debezium_parser import ParsedEvent

logger = logging.getLogger(__name__)

_engine = MappingEngine()

RouteResult = Tuple[str, list, Optional[list]]


def route(event: ParsedEvent) -> List[RouteResult]:
    """
    Map a ParsedEvent to a list of Postgres write targets.

    Returns:
        List of (table_name, rows, upsert_key_cols).
        Rows with '_delete': True signal a cascading delete.
        Empty list for tombstone, unknown op, or unrecognised collection.
    """
    if event.op in ('tombstone', 'unknown'):
        return []

    if event.collection not in _engine.collections:
        logger.debug(
            f"[router] No mapping for collection={event.collection!r} "
            f"op={event.op!r} — skipping"
        )
        return []

    return _engine.get_pg_routes(
        event.collection,
        event.document,
        event.op,
        event.doc_id,
    )


def get_neo4j_ops(event: ParsedEvent) -> List[Tuple[str, dict]]:
    """Convenience wrapper — returns Neo4j (cypher, params) list for an event."""
    if event.op in ('tombstone', 'unknown'):
        return []
    return _engine.get_neo4j_ops(
        event.collection,
        event.document,
        event.op,
        event.doc_id,
    )


def engine() -> MappingEngine:
    """Return the shared engine instance (used by the API layer)."""
    return _engine
