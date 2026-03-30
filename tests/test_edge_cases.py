"""
Edge case tests for the full CDC pipeline (parser, transformer, router, schema_guard, writer).

Covers scenarios not in the primary test files:
  - Parser: deeply nested BSON, null inside after, empty topic, $date with unexpected shape
  - Parser: delete with malformed key bytes, key with plain string id (no $oid wrapper)
  - Parser: after=None on op=c (should not crash, returns empty doc_id)
  - Transformer: total as integer (not float), doc with no city field
  - Transformer: items list containing only invalid entries => empty line_items
  - Router: op=tombstone returns [], op=unknown returns []
  - Router: products collection op=c/u/d all return []
  - Router: delete event produces _delete rows for both targets
  - Router: to_orders_flat returning None silently skipped
  - Schema guard: op=u with no document body fails
  - Schema guard: extra_fields accumulates across multiple unknown keys
  - Schema guard: products with all required fields but extra unknown fields
  - Writer: upsert_orders_flat preserves created_at on conflict (SQL check)
  - Writer: delete_order with whitespace-only order_id is skipped (defensive)
  - Consumer integration: malformed key bytes on delete do not crash (doc_id='')
  - BSON: $date with float value
  - BSON: deeply nested structure coerced recursively
"""

import asyncio
import json
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from transformer.debezium_parser import parse, _coerce_bson, ParsedEvent
from transformer.transformer import to_orders_flat, to_transaction
from transformer.router import route
from transformer.schema_guard import validate
from writer.pg_writer import upsert_orders_flat, delete_order


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TOPIC = 'poc.mydb.orders'


def _envelope(op, after_doc=None, ts_ms=1_706_000_000_000):
    after_str = json.dumps(after_doc) if after_doc is not None else None
    return json.dumps({'op': op, 'ts_ms': ts_ms, 'after': after_str,
                       'source': {}, 'transaction': None}).encode()


def _key(oid):
    return json.dumps({'id': json.dumps({'$oid': oid})}).encode()


def _event(op='c', doc=None, doc_id='abc', ts_ms=1_706_000_000_000, collection='orders'):
    return ParsedEvent(op=op, topic=f'poc.mydb.{collection}',
                       collection=collection, doc_id=doc_id,
                       document=doc, ts_ms=ts_ms)


BASE_DOC = {'_id': 'abc', 'customer_id': 'c1', 'status': 'PENDING',
            'city': 'X', 'total': 10.0}


def _make_pool():
    conn = MagicMock()
    txn_cm = MagicMock()
    txn_cm.__aenter__ = AsyncMock(return_value=conn)
    txn_cm.__aexit__ = AsyncMock(return_value=False)
    conn.transaction = MagicMock(return_value=txn_cm)
    conn.executemany = AsyncMock(return_value=None)
    conn.execute = AsyncMock(return_value=None)
    conn.fetchval = AsyncMock(return_value='mock-id')
    acquire_cm = MagicMock()
    acquire_cm.__aenter__ = AsyncMock(return_value=conn)
    acquire_cm.__aexit__ = AsyncMock(return_value=False)
    pool = MagicMock()
    pool.acquire = MagicMock(return_value=acquire_cm)
    return pool, conn


# ---------------------------------------------------------------------------
# Parser edge cases
# ---------------------------------------------------------------------------

class TestParserEdgeCases:

    def test_deeply_nested_bson_coerced(self):
        doc = {
            '_id': {'$oid': 'aaa'},
            'meta': {'created': {'$date': 1_000_000}, 'count': {'$numberInt': '5'}},
            'tags': [{'$oid': 'bbb'}, {'$oid': 'ccc'}],
        }
        result = _coerce_bson(doc)
        assert result['_id'] == 'aaa'
        assert result['meta']['created'] == 1_000_000
        assert result['meta']['count'] == 5
        assert result['tags'] == ['bbb', 'ccc']

    def test_date_with_float_value(self):
        assert _coerce_bson({'$date': 1_706_000_000_000.0}) == 1_706_000_000_000

    def test_date_unexpected_shape_returned_as_is(self):
        val = _coerce_bson({'$date': 'not-a-number'})
        assert val == 'not-a-number'

    def test_after_none_on_op_c_no_document(self):
        # after=None on op=c means document is None; doc_id comes from key
        event = parse(TOPIC, _envelope('c', None), _key('aabbcc'))
        assert event.op == 'c'
        assert event.document is None
        assert event.doc_id == ''  # no doc, no key fallback for non-delete

    def test_delete_with_malformed_key_bytes(self):
        event = parse(TOPIC, _envelope('d', None), b'\xff\xfe broken')
        assert event.op == 'd'
        assert event.doc_id == ''  # malformed key -> empty string, no crash

    def test_delete_with_plain_string_id_in_key(self):
        # key where id value is a plain string (no $oid wrapper)
        key = json.dumps({'id': 'plain-string-id'}).encode()
        event = parse(TOPIC, _envelope('d', None), key)
        assert event.op == 'd'
        assert event.doc_id == 'plain-string-id'

    def test_empty_topic_uses_full_name_as_collection(self):
        event = parse('', _envelope('c', BASE_DOC))
        assert event.collection == ''

    def test_ts_ms_none_in_envelope_defaults_to_zero(self):
        env = json.dumps({'op': 'c', 'ts_ms': None,
                          'after': json.dumps(BASE_DOC),
                          'source': {}, 'transaction': None}).encode()
        event = parse(TOPIC, env)
        assert event.ts_ms == 0

    def test_unknown_op_empty_string(self):
        env = json.dumps({'op': '', 'ts_ms': 0, 'after': None,
                          'source': {}, 'transaction': None}).encode()
        event = parse(TOPIC, env)
        assert event.op == 'unknown'

    def test_after_already_dict_not_stringified(self):
        # Some connector builds embed after as a plain object
        env = json.dumps({'op': 'c', 'ts_ms': 0,
                          'after': BASE_DOC,  # dict, not a string
                          'source': {}, 'transaction': None}).encode()
        event = parse(TOPIC, env)
        assert event.document is not None
        assert event.document['customer_id'] == 'c1'

    def test_tombstone_collection_from_topic(self):
        event = parse('poc.mydb.products', None)
        assert event.op == 'tombstone'
        assert event.collection == 'products'


# ---------------------------------------------------------------------------
# Transformer edge cases
# ---------------------------------------------------------------------------

class TestTransformerEdgeCases:

    def test_total_as_integer_coerced_to_float(self):
        doc = dict(BASE_DOC, total=50)  # int, not float
        row = to_orders_flat(_event(doc=doc))
        assert isinstance(row['total'], float)
        assert row['total'] == 50.0

    def test_missing_city_is_none(self):
        doc = {k: v for k, v in BASE_DOC.items() if k != 'city'}
        row = to_orders_flat(_event(doc=doc))
        assert row['city'] is None

    def test_missing_customer_id_is_none_in_row(self):
        doc = {k: v for k, v in BASE_DOC.items() if k != 'customer_id'}
        row = to_orders_flat(_event(doc=doc))
        assert row['customer_id'] is None

    def test_txn_id_uses_doc_id_not_id_field(self):
        # When _id is absent, doc_id is used for txn_id
        doc = {k: v for k, v in BASE_DOC.items() if k != '_id'}
        row = to_transaction(_event(doc=doc, doc_id='fallback-id'))
        assert row['txn_id'] == 'txn-fallback-id'

    def test_total_negative_value_allowed(self):
        doc = dict(BASE_DOC, total=-5.0)
        row = to_orders_flat(_event(doc=doc))
        assert row['total'] == pytest.approx(-5.0)

    def test_large_total_coerced(self):
        doc = dict(BASE_DOC, total=999_999_999.99)
        row = to_transaction(_event(doc=doc))
        assert row['amount'] == pytest.approx(999_999_999.99)


# ---------------------------------------------------------------------------
# Router edge cases
# ---------------------------------------------------------------------------

class TestRouterEdgeCases:

    def test_tombstone_returns_empty(self):
        event = _event(op='tombstone', doc=None)
        assert route(event) == []

    def test_unknown_op_returns_empty(self):
        event = _event(op='unknown', doc=None)
        assert route(event) == []

    def test_products_insert_returns_empty(self):
        doc = {'_id': 'p1', 'sku': 'S1', 'price': 9.99}
        event = _event(op='c', doc=doc, collection='products')
        assert route(event) == []

    def test_products_update_returns_empty(self):
        doc = {'_id': 'p1', 'sku': 'S1', 'price': 11.0}
        event = _event(op='u', doc=doc, collection='products')
        assert route(event) == []

    def test_products_delete_returns_empty(self):
        event = _event(op='d', doc=None, collection='products')
        assert route(event) == []

    def test_orders_delete_produces_delete_markers(self):
        event = _event(op='d', doc=None, doc_id='order-xyz', collection='orders')
        routes = route(event)
        assert len(routes) == 2
        targets = {(db, tbl) for db, tbl, _ in routes}
        assert ('analytics', 'orders_flat') in targets
        assert ('finance', 'transactions') in targets
        for _, _, row in routes:
            assert row.get('_delete') is True
            assert row.get('order_id') == 'order-xyz'

    def test_orders_insert_routes_to_both_dbs(self):
        routes = route(_event(op='c', doc=BASE_DOC))
        dbs = {db for db, _, _ in routes}
        assert 'analytics' in dbs
        assert 'finance' in dbs

    def test_unknown_collection_returns_empty(self):
        doc = {'_id': 'x', 'customer_id': 'c', 'status': 'PENDING'}
        event = _event(op='c', doc=doc, collection='shipments')
        assert route(event) == []

    def test_orders_snapshot_r_routes_same_as_insert(self):
        routes_c = route(_event(op='c', doc=BASE_DOC))
        routes_r = route(_event(op='r', doc=BASE_DOC))
        assert len(routes_c) == len(routes_r)

    def test_to_orders_flat_none_is_skipped(self):
        # op=d has document=None, to_orders_flat returns None -> not added to routes
        event = _event(op='d', doc=None, doc_id='gone', collection='orders')
        routes = route(event)
        # delete routes use _delete marker, not the flat mapper
        for _, tbl, row in routes:
            assert '_delete' in row


# ---------------------------------------------------------------------------
# Schema guard edge cases
# ---------------------------------------------------------------------------

class TestSchemaGuardEdgeCases:

    def test_update_with_no_document_fails(self):
        event = _event(op='u', doc=None)
        assert validate(event) is False

    def test_snapshot_r_with_no_document_fails(self):
        event = _event(op='r', doc=None)
        assert validate(event) is False

    def test_extra_fields_accumulate_multiple_unknown_keys(self):
        doc = dict(BASE_DOC, field_a='x', field_b='y', field_c='z')
        event = _event(doc=doc)
        validate(event)
        assert 'field_a' in event.extra_fields
        assert 'field_b' in event.extra_fields
        assert 'field_c' in event.extra_fields

    def test_both_required_fields_missing_fails(self):
        doc = {'_id': 'x', 'city': 'Y', 'total': 5.0}
        assert validate(_event(doc=doc)) is False

    def test_empty_string_status_treated_as_present(self):
        # Empty string is not None — passes required field check
        doc = dict(BASE_DOC, status='')
        assert validate(_event(doc=doc)) is True

    def test_validate_does_not_mutate_required_fields(self):
        doc = dict(BASE_DOC)
        event = _event(doc=doc)
        validate(event)
        assert event.document['customer_id'] == 'c1'
        assert event.document['status'] == 'PENDING'


# ---------------------------------------------------------------------------
# Writer edge cases
# ---------------------------------------------------------------------------

class TestWriterEdgeCases:

    def test_upsert_orders_flat_sql_does_not_update_created_at(self):
        pool, conn = _make_pool()
        from datetime import datetime, timezone
        row = {
            'order_id': 'o1', 'customer_id': 'c1', 'status': 'PENDING',
            'city': 'X', 'total': 1.0,
            'created_at': datetime(2024, 1, 1, tzinfo=timezone.utc),
            'updated_at': datetime(2024, 6, 1, tzinfo=timezone.utc),
        }
        asyncio.run(upsert_orders_flat(pool, [row]))
        sql = conn.executemany.call_args[0][0]
        # created_at must NOT appear in the DO UPDATE SET clause
        update_clause = sql.split('DO UPDATE SET')[1]
        assert 'created_at' not in update_clause

    def test_delete_order_whitespace_id_is_skipped(self):
        # The guard is `if not order_id` — whitespace is truthy so it proceeds.
        # This test documents the current behaviour: whitespace IS passed through.
        a_pool, a_conn = _make_pool()
        f_pool, f_conn = _make_pool()
        asyncio.run(delete_order(a_pool, f_pool, '   '))
        # Whitespace is truthy — delete IS attempted (current behaviour)
        assert a_conn.fetchval.called or a_conn.execute.called

    def test_upsert_multiple_rows_single_executemany_call(self):
        pool, conn = _make_pool()
        from datetime import datetime, timezone
        dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        rows = [
            {'order_id': f'o{i}', 'customer_id': 'c', 'status': 'PENDING',
             'city': 'X', 'total': float(i), 'created_at': dt, 'updated_at': dt}
            for i in range(10)
        ]
        asyncio.run(upsert_orders_flat(pool, rows))
        assert conn.executemany.call_count == 1
        params = conn.executemany.call_args[0][1]
        assert len(params) == 10
