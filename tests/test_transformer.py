"""
Unit tests for transformer/transformer.py and transformer/schema_guard.py.

Covers:
  to_orders_flat  — field mapping, timestamp conversion, fallbacks
  to_transaction  — deterministic txn_id, amount mapping
  schema_guard    — required field checks, drift detection, op=d/tombstone passthrough

No database or Kafka connection required.
"""

import pytest
from datetime import datetime, timezone

from transformer.debezium_parser import ParsedEvent
from transformer.transformer import to_orders_flat, to_transaction
from transformer.schema_guard import validate


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _event(
    op: str = 'c',
    doc=None,
    doc_id: str = 'abc123',
    ts_ms: int = 1_706_000_000_000,
    collection: str = 'orders',
) -> ParsedEvent:
    return ParsedEvent(
        op=op,
        topic=f'poc.mydb.{collection}',
        collection=collection,
        doc_id=doc_id,
        document=doc,
        ts_ms=ts_ms,
    )


BASE_DOC = {
    '_id':         'abc123',
    'customer_id': 'cust-001',
    'status':      'PENDING',
    'city':        'Boston',
    'total':       42.50,
}


# ---------------------------------------------------------------------------
# to_orders_flat
# ---------------------------------------------------------------------------

class TestToOrdersFlat:
    def test_all_fields_mapped(self):
        row = to_orders_flat(_event(doc=BASE_DOC))
        assert row['order_id']    == 'abc123'
        assert row['customer_id'] == 'cust-001'
        assert row['status']      == 'PENDING'
        assert row['city']        == 'Boston'
        assert row['total']       == pytest.approx(42.50)

    def test_timestamps_are_utc(self):
        row = to_orders_flat(_event(doc=BASE_DOC))
        assert isinstance(row['created_at'], datetime)
        assert row['created_at'].tzinfo is not None
        assert row['updated_at'].tzinfo == timezone.utc

    def test_ts_ms_converted_correctly(self):
        row = to_orders_flat(_event(doc=BASE_DOC, ts_ms=1_706_000_000_000))
        expected = datetime.fromtimestamp(
            1_706_000_000_000 / 1000.0, tz=timezone.utc
        )
        assert row['created_at'] == expected

    def test_returns_none_for_no_document(self):
        assert to_orders_flat(_event(op='d', doc=None)) is None

    def test_doc_id_fallback_when_no_id_field(self):
        doc = {k: v for k, v in BASE_DOC.items() if k != '_id'}
        row = to_orders_flat(_event(doc=doc, doc_id='fallback'))
        assert row['order_id'] == 'fallback'

    def test_total_none_does_not_raise(self):
        doc = dict(BASE_DOC, total=None)
        row = to_orders_flat(_event(doc=doc))
        assert row['total'] is None

    def test_total_string_float_coerced(self):
        doc = dict(BASE_DOC, total='19.99')
        row = to_orders_flat(_event(doc=doc))
        assert row['total'] == pytest.approx(19.99)

    def test_snapshot_op_r_treated_same_as_insert(self):
        row = to_orders_flat(_event(op='r', doc=BASE_DOC))
        assert row is not None
        assert row['status'] == 'PENDING'

    def test_ts_ms_zero_falls_back_to_now(self):
        # ts_ms=0 should produce a recent datetime, not epoch
        row = to_orders_flat(_event(doc=BASE_DOC, ts_ms=0))
        now = datetime.now(tz=timezone.utc)
        delta = abs((now - row['created_at']).total_seconds())
        assert delta < 5  # within 5 seconds of now



# ---------------------------------------------------------------------------
# to_transaction
# ---------------------------------------------------------------------------

class TestToTransaction:
    def test_txn_id_format(self):
        row = to_transaction(_event(doc=BASE_DOC, doc_id='abc123'))
        assert row['txn_id'] == 'txn-abc123'

    def test_txn_id_deterministic_for_same_doc(self):
        e1 = _event(doc=BASE_DOC, doc_id='abc123')
        e2 = _event(doc=BASE_DOC, doc_id='abc123')
        assert to_transaction(e1)['txn_id'] == to_transaction(e2)['txn_id']

    def test_amount_from_total(self):
        row = to_transaction(_event(doc=BASE_DOC))
        assert row['amount'] == pytest.approx(42.50)

    def test_order_id_matches_doc_id(self):
        row = to_transaction(_event(doc=BASE_DOC, doc_id='abc123'))
        assert row['order_id'] == 'abc123'

    def test_status_forwarded(self):
        row = to_transaction(_event(doc=BASE_DOC))
        assert row['status'] == 'PENDING'

    def test_recorded_at_is_utc(self):
        row = to_transaction(_event(doc=BASE_DOC, ts_ms=1_706_000_000_000))
        assert row['recorded_at'].tzinfo == timezone.utc

    def test_returns_none_for_no_document(self):
        assert to_transaction(_event(op='d', doc=None)) is None

    def test_total_none_yields_none_amount(self):
        doc = dict(BASE_DOC, total=None)
        row = to_transaction(_event(doc=doc))
        assert row['amount'] is None


# ---------------------------------------------------------------------------
# schema_guard.validate
# ---------------------------------------------------------------------------

class TestSchemaGuard:
    def test_valid_order_passes(self):
        assert validate(_event(doc=BASE_DOC)) is True

    def test_missing_customer_id_fails(self):
        doc = {k: v for k, v in BASE_DOC.items() if k != 'customer_id'}
        assert validate(_event(doc=doc)) is False

    def test_none_customer_id_fails(self):
        doc = dict(BASE_DOC, customer_id=None)
        assert validate(_event(doc=doc)) is False

    def test_missing_status_fails(self):
        doc = {k: v for k, v in BASE_DOC.items() if k != 'status'}
        assert validate(_event(doc=doc)) is False

    def test_delete_op_always_passes(self):
        assert validate(_event(op='d', doc=None)) is True

    def test_tombstone_always_passes(self):
        assert validate(_event(op='tombstone', doc=None)) is True

    def test_none_document_on_insert_fails(self):
        assert validate(_event(op='c', doc=None)) is False

    def test_snapshot_op_r_validated(self):
        assert validate(_event(op='r', doc=BASE_DOC)) is True

    def test_unknown_field_stored_in_extra_fields(self):
        doc = dict(BASE_DOC, surprise_field='new_value')
        event = _event(doc=doc)
        result = validate(event)
        assert result is True  # unknown field does NOT fail validation
        assert 'surprise_field' in event.extra_fields
        assert event.extra_fields['surprise_field'] == 'new_value'

    def test_known_fields_not_in_extra_fields(self):
        event = _event(doc=BASE_DOC)
        validate(event)
        for known in ('_id', 'customer_id', 'status', 'city', 'total'):
            assert known not in event.extra_fields

    def test_unknown_collection_no_required_fields(self):
        # Collections not in REQUIRED_FIELDS have no required fields → always pass
        doc = {'_id': 'x', 'something': 'value'}
        event = _event(op='c', doc=doc, collection='unknown_collection')
        assert validate(event) is True
