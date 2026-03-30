"""
Unit tests for transformer/debezium_parser.py.

Covers:
  - op=c (insert), u (update), d (delete), r (snapshot), tombstone
  - BSON extended JSON coercion: $oid, $date, $numberDecimal, $numberLong, $numberInt
  - doc_id extraction from Kafka message key on delete
  - Tombstone detection (msg_value=None)
  - Error handling: malformed envelope, malformed 'after' string
  - after as object (non-standard but handled)
  - Unknown topic format

No Kafka or database connection required.
"""

import json
import pytest

from transformer.debezium_parser import parse, _coerce_bson, ParsedEvent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _envelope(op: str, after_doc=None, ts_ms: int = 1_706_000_000_000) -> bytes:
    """Build a Debezium envelope value as raw bytes (after is stringified)."""
    after_str = json.dumps(after_doc) if after_doc is not None else None
    return json.dumps({
        'op':          op,
        'ts_ms':       ts_ms,
        'after':       after_str,
        'source':      {'connector': 'mongodb', 'collection': 'orders', 'db': 'mydb'},
        'transaction': None,
    }).encode()


def _key(oid: str) -> bytes:
    """Build a Debezium message key containing a MongoDB ObjectId."""
    return json.dumps({'id': json.dumps({'$oid': oid})}).encode()


ORDER_DOC = {
    '_id':         {'$oid': 'aaaaaaaaaaaaaaaaaaaaaaaa'},
    'customer_id': 'cust-001',
    'status':      'PENDING',
    'city':        'Boston',
    'total':       99.99,
}

TOPIC = 'poc.mydb.orders'


# ---------------------------------------------------------------------------
# op=c  insert
# ---------------------------------------------------------------------------

class TestParseInsert:
    def test_op(self):
        assert parse(TOPIC, _envelope('c', ORDER_DOC)).op == 'c'

    def test_collection_from_topic(self):
        assert parse(TOPIC, _envelope('c', ORDER_DOC)).collection == 'orders'

    def test_doc_id_is_oid_string(self):
        event = parse(TOPIC, _envelope('c', ORDER_DOC))
        assert event.doc_id == 'aaaaaaaaaaaaaaaaaaaaaaaa'

    def test_document_customer_id(self):
        event = parse(TOPIC, _envelope('c', ORDER_DOC))
        assert event.document['customer_id'] == 'cust-001'

    def test_document_total(self):
        event = parse(TOPIC, _envelope('c', ORDER_DOC))
        assert event.document['total'] == pytest.approx(99.99)

    def test_ts_ms_captured(self):
        event = parse(TOPIC, _envelope('c', ORDER_DOC, ts_ms=1_234_567_890_000))
        assert event.ts_ms == 1_234_567_890_000


# ---------------------------------------------------------------------------
# op=u  update
# ---------------------------------------------------------------------------

class TestParseUpdate:
    def test_op(self):
        doc = dict(ORDER_DOC, status='SHIPPED')
        assert parse(TOPIC, _envelope('u', doc)).op == 'u'

    def test_updated_field(self):
        doc = dict(ORDER_DOC, status='SHIPPED')
        event = parse(TOPIC, _envelope('u', doc))
        assert event.document['status'] == 'SHIPPED'


# ---------------------------------------------------------------------------
# op=r  snapshot / read
# ---------------------------------------------------------------------------

class TestParseSnapshot:
    def test_op(self):
        assert parse(TOPIC, _envelope('r', ORDER_DOC)).op == 'r'

    def test_document_present(self):
        event = parse(TOPIC, _envelope('r', ORDER_DOC))
        assert event.document is not None


# ---------------------------------------------------------------------------
# op=d  delete
# ---------------------------------------------------------------------------

class TestParseDelete:
    def _delete_envelope(self) -> bytes:
        return json.dumps({
            'op': 'd', 'ts_ms': 1_706_000_000_000,
            'after': None,
            'source': {'connector': 'mongodb', 'collection': 'orders', 'db': 'mydb'},
            'transaction': None,
        }).encode()

    def test_op(self):
        key   = _key('aaaaaaaaaaaaaaaaaaaaaaaa')
        event = parse(TOPIC, self._delete_envelope(), key)
        assert event.op == 'd'

    def test_document_is_none(self):
        key   = _key('aaaaaaaaaaaaaaaaaaaaaaaa')
        event = parse(TOPIC, self._delete_envelope(), key)
        assert event.document is None

    def test_doc_id_from_key(self):
        key   = _key('bbbbbbbbbbbbbbbbbbbbbbbb')
        event = parse(TOPIC, self._delete_envelope(), key)
        assert event.doc_id == 'bbbbbbbbbbbbbbbbbbbbbbbb'

    def test_doc_id_empty_when_no_key(self):
        event = parse(TOPIC, self._delete_envelope(), None)
        assert event.doc_id == ''


# ---------------------------------------------------------------------------
# Tombstone
# ---------------------------------------------------------------------------

class TestTombstone:
    def test_op_tombstone(self):
        event = parse(TOPIC, None)
        assert event.op == 'tombstone'

    def test_document_is_none(self):
        assert parse(TOPIC, None).document is None

    def test_ts_ms_zero(self):
        assert parse(TOPIC, None).ts_ms == 0

    def test_doc_id_from_key(self):
        event = parse(TOPIC, None, _key('cccccccccccccccccccccccc'))
        assert event.doc_id == 'cccccccccccccccccccccccc'

    def test_doc_id_empty_without_key(self):
        assert parse(TOPIC, None).doc_id == ''


# ---------------------------------------------------------------------------
# BSON coercion
# ---------------------------------------------------------------------------

class TestCoerceBson:
    def test_oid_to_string(self):
        assert _coerce_bson({'$oid': 'abc123'}) == 'abc123'

    def test_date_int_preserved(self):
        assert _coerce_bson({'$date': 1_706_000_000_000}) == 1_706_000_000_000

    def test_date_number_long(self):
        assert _coerce_bson({'$date': {'$numberLong': '1706000000000'}}) == 1_706_000_000_000

    def test_number_decimal_to_float(self):
        assert _coerce_bson({'$numberDecimal': '9.99'}) == pytest.approx(9.99)

    def test_number_long_to_int(self):
        assert _coerce_bson({'$numberLong': '42'}) == 42

    def test_number_int_to_int(self):
        assert _coerce_bson({'$numberInt': '7'}) == 7

    def test_nested_oid_in_document(self):
        doc = {'_id': {'$oid': 'deadbeef'}, 'count': 1}
        assert _coerce_bson(doc)['_id'] == 'deadbeef'
        assert _coerce_bson(doc)['count'] == 1

    def test_list_of_oids(self):
        lst = [{'$oid': 'aaa'}, {'$oid': 'bbb'}]
        assert _coerce_bson(lst) == ['aaa', 'bbb']

    def test_plain_string_unchanged(self):
        assert _coerce_bson('hello') == 'hello'

    def test_plain_int_unchanged(self):
        assert _coerce_bson(42) == 42

    def test_plain_dict_not_mutated(self):
        d = {'a': 1, 'b': 'x'}
        assert _coerce_bson(d) == d

    def test_multi_key_dict_not_coerced_as_type(self):
        # A dict with two keys should NOT be treated as an extended JSON type
        d = {'$oid': 'abc', 'extra': 1}
        result = _coerce_bson(d)
        # Recursed normally — both keys present
        assert result == {'$oid': 'abc', 'extra': 1}

    def test_oid_embedded_in_after(self):
        """End-to-end: $oid inside a full order document is coerced correctly."""
        event = parse(TOPIC, _envelope('c', ORDER_DOC))
        assert event.doc_id == 'aaaaaaaaaaaaaaaaaaaaaaaa'
        assert event.document['_id'] == 'aaaaaaaaaaaaaaaaaaaaaaaa'


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestParseErrors:
    def test_malformed_envelope_raises_value_error(self):
        with pytest.raises(ValueError, match='Malformed Debezium envelope'):
            parse(TOPIC, b'not-valid-json')

    def test_malformed_after_string_raises_value_error(self):
        envelope = json.dumps({
            'op': 'c', 'ts_ms': 0,
            'after': '{broken json',
            'source': {}, 'transaction': None,
        }).encode()
        with pytest.raises(ValueError, match="Malformed 'after' JSON"):
            parse(TOPIC, envelope)

    def test_after_as_object_handled(self):
        """after embedded as dict (not stringified) should still parse."""
        envelope = json.dumps({
            'op': 'c', 'ts_ms': 0,
            'after': ORDER_DOC,   # dict, not string
            'source': {}, 'transaction': None,
        }).encode()
        event = parse(TOPIC, envelope)
        assert event.op == 'c'
        assert event.document is not None

    def test_topic_without_dots(self):
        """Single-segment topic uses the whole name as collection."""
        event = parse('orders', _envelope('c', ORDER_DOC))
        assert event.collection == 'orders'

    def test_unknown_op_code(self):
        envelope = json.dumps({
            'op': 'x', 'ts_ms': 0,
            'after': None, 'source': {}, 'transaction': None,
        }).encode()
        event = parse(TOPIC, envelope)
        assert event.op == 'unknown'
