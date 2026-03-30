"""
Debezium envelope parser for the MongoDB CDC pipeline.

Handles all op codes:
  c  — insert (create)
  u  — update (full document via change_streams_update_full)
  d  — delete (after=null; doc_id extracted from Kafka message key)
  r  — read/snapshot (treated identically to insert)
  tombstone — null msg_value emitted after every delete for log compaction

MongoDB extended JSON coercion:
  {"$oid": "x"}                     → "x"  (str)
  {"$date": ms}                     → ms   (int, epoch milliseconds)
  {"$date": {"$numberLong": "ms"}}  → ms   (int)
  {"$numberDecimal": "x"}           → x    (float)
  {"$numberLong": "x"}              → x    (int)
  {"$numberInt": "x"}               → x    (int)

Debezium MongoDB connector key format (JSON converter, schemas.enable=false):
  {"id": "{\"$oid\": \"abc123\"}"}
  The outer `id` value is itself a stringified JSON containing the $oid.

Edge cases handled:
  - `after` may be a JSON string (standard) or an embedded object (some versions).
  - Delete events have after=null; doc_id is parsed from the message key.
  - Tombstone: msg_value is None (Kafka null); skip silently in caller.
  - Malformed envelope or malformed `after` → raises ValueError for DLQ routing.
  - Unknown op codes → op='unknown'; caller routes to DLQ.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class ParsedEvent:
    op: str                       # 'c', 'u', 'd', 'r', 'tombstone', 'unknown'
    topic: str                    # full Kafka topic name
    collection: str               # last segment of topic (e.g. 'orders')
    doc_id: str                   # string form of _id (empty string if unavailable)
    document: Optional[dict]      # None for delete and tombstone
    ts_ms: int                    # event timestamp in epoch milliseconds
    extra_fields: dict = field(default_factory=dict)  # unknown fields (schema drift)


# ---------------------------------------------------------------------------
# BSON Extended JSON coercion
# ---------------------------------------------------------------------------

def _coerce_bson(obj):
    """
    Recursively coerce MongoDB Extended JSON types to plain Python types.
    Only recognises single-key extended type dicts to avoid false positives.
    """
    if isinstance(obj, dict):
        keys = set(obj.keys())

        if keys == {'$oid'}:
            return str(obj['$oid'])

        if keys == {'$date'}:
            d = obj['$date']
            if isinstance(d, (int, float)):
                return int(d)
            if isinstance(d, dict) and '$numberLong' in d:
                return int(d['$numberLong'])
            return d  # unexpected shape — return as-is

        if keys == {'$numberDecimal'}:
            return float(obj['$numberDecimal'])

        if keys == {'$numberLong'}:
            return int(obj['$numberLong'])

        if keys == {'$numberInt'}:
            return int(obj['$numberInt'])

        # Recurse for regular dicts
        return {k: _coerce_bson(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [_coerce_bson(v) for v in obj]

    return obj


# ---------------------------------------------------------------------------
# Key parsing for delete events
# ---------------------------------------------------------------------------

def _parse_key_for_id(msg_key: Optional[bytes]) -> str:
    """
    Extract the document _id from a Debezium Kafka message key.

    Key format: {"id": "{\"$oid\": \"abc123\"}"}
    The `id` value is a stringified JSON string that may contain a $oid dict.

    Returns:
        String form of the document _id, or '' if parsing fails.
    """
    if not msg_key:
        return ''
    try:
        key_obj = json.loads(msg_key.decode('utf-8'))
        id_raw = key_obj.get('id', '')
        if isinstance(id_raw, str) and id_raw:
            try:
                id_parsed = json.loads(id_raw)
                return str(_coerce_bson(id_parsed))
            except (json.JSONDecodeError, TypeError):
                return str(id_raw)
        return str(id_raw)
    except Exception as e:
        logger.warning(f"[parser] Could not parse message key: {e!r}")
        return ''


# ---------------------------------------------------------------------------
# Main parse function
# ---------------------------------------------------------------------------

def parse(topic: str, msg_value: Optional[bytes],
          msg_key: Optional[bytes] = None) -> ParsedEvent:
    """
    Parse a raw Debezium Kafka message into a ParsedEvent.

    Args:
        topic:     Kafka topic name (e.g. 'poc.mydb.orders')
        msg_value: Raw message value bytes.  None signals a tombstone.
        msg_key:   Raw message key bytes.  Required for correct doc_id on deletes.

    Returns:
        A ParsedEvent with all fields populated.

    Raises:
        ValueError: If msg_value is non-None but contains malformed JSON,
                    or if the 'after' field is a malformed JSON string.
                    Callers should catch this and route to DLQ.
    """
    # Collection is the last dot-separated segment of the topic
    collection = topic.split('.')[-1] if '.' in topic else topic

    # ── Tombstone ────────────────────────────────────────────────────────────
    # After every delete Debezium emits a second message with null value.
    # Used by Kafka for log compaction.  Skip silently in the consumer.
    if msg_value is None:
        doc_id = _parse_key_for_id(msg_key)
        return ParsedEvent(
            op='tombstone',
            topic=topic,
            collection=collection,
            doc_id=doc_id,
            document=None,
            ts_ms=0,
        )

    # ── Parse envelope ───────────────────────────────────────────────────────
    try:
        envelope = json.loads(msg_value.decode('utf-8'))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"Malformed Debezium envelope: {exc}") from exc

    # Normalise unknown op codes to 'unknown' so the router can handle them uniformly
    _VALID_OPS = {'c', 'u', 'd', 'r'}
    op_raw = envelope.get('op', '')
    if op_raw in _VALID_OPS:
        op = op_raw
    else:
        if op_raw:
            logger.warning(
                f"[parser] Unrecognised op={op_raw!r} on {topic} — treating as 'unknown'"
            )
        op = 'unknown'
    ts_ms = envelope.get('ts_ms', 0) or 0

    # ── Parse 'after' ────────────────────────────────────────────────────────
    # MongoDB connector serialises 'after' as a *stringified* JSON document.
    # Some connector builds may embed it as a plain object — handle both.
    after_raw = envelope.get('after')
    document: Optional[dict] = None
    doc_id:   str             = ''

    if after_raw is not None:
        if isinstance(after_raw, str):
            try:
                doc = json.loads(after_raw)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Malformed 'after' JSON string: {exc}") from exc
        else:
            # after is already an object (non-standard but handle gracefully)
            doc = after_raw

        doc       = _coerce_bson(doc)
        doc_id    = str(doc.get('_id', ''))
        document  = doc

    elif op == 'd':
        # Delete event: after is null.
        # The document _id is only available in the Kafka message key.
        doc_id = _parse_key_for_id(msg_key)
        if not doc_id:
            logger.warning(
                f"[parser] Delete on {topic} has no message key — doc_id will be empty. "
                "The delete will be a no-op in Postgres."
            )

    return ParsedEvent(
        op=op,
        topic=topic,
        collection=collection,
        doc_id=doc_id,
        document=document,
        ts_ms=ts_ms,
    )
