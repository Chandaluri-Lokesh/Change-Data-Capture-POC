# -*- coding: utf-8 -*-
"""
Streamlit dashboard for the CDC pipeline.

Shows:
  - Pipeline topology (MongoDB -> Debezium -> Kafka -> Consumer -> Postgres)
  - Live component health and status
  - Consumer lag per partition
  - Connector states
  - PostgreSQL row counts

Run:
    streamlit run src/ui/dashboard.py
"""

import os
import sys
import time

import pandas as pd
import psycopg2
import requests
import streamlit as st
from confluent_kafka import Consumer, KafkaException, TopicPartition
from dotenv import load_dotenv
from pymongo import MongoClient

# ── path + env ────────────────────────────────────────────────────────────────
_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
_src  = os.path.join(_root, 'src')
for p in (_root, _src):
    if p not in sys.path:
        sys.path.insert(0, p)

load_dotenv(dotenv_path=os.path.join(_root, '.env'))

MONGO_URI   = os.getenv('MONGO_URI',             'mongodb://localhost:27018/?replicaSet=rs0')
BROKER      = os.getenv('KAFKA_BROKER_URL',       'localhost:9092')
CONNECT_URL = os.getenv('KAFKA_CONNECT_REST_URL', 'http://localhost:8083')
PG_A = dict(
    host=os.getenv('POSTGRES_ANALYTICS_HOST', 'localhost'),
    port=int(os.getenv('POSTGRES_ANALYTICS_PORT', '5432')),
    user=os.getenv('POSTGRES_ANALYTICS_USER', 'postgres'),
    password=os.getenv('POSTGRES_ANALYTICS_PASSWORD', 'postgres'),
    dbname=os.getenv('POSTGRES_ANALYTICS_DB', 'analytics'),
)
PG_F = dict(
    host=os.getenv('POSTGRES_FINANCE_HOST', 'localhost'),
    port=int(os.getenv('POSTGRES_FINANCE_PORT', '5432')),
    user=os.getenv('POSTGRES_FINANCE_USER', 'postgres'),
    password=os.getenv('POSTGRES_FINANCE_PASSWORD', 'postgres'),
    dbname=os.getenv('POSTGRES_FINANCE_DB', 'finance'),
)
TOPICS   = ['poc.mydb.orders']
GROUP_ID = 'poc-pipeline-consumer'


# ── status helpers (cached 5 s) ───────────────────────────────────────────────

@st.cache_data(ttl=5)
def check_mongo():
    try:
        c    = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        info = c.server_info()
        rs   = c.admin.command('replSetGetStatus')
        members = [
            {'host': m['name'], 'state': m['stateStr']}
            for m in rs.get('members', [])
        ]
        primary = next((m['host'] for m in members if m['state'] == 'PRIMARY'), '—')
        return {
            'ok': True, 'version': info.get('version', '?'),
            'rs': rs.get('set', 'rs0'), 'primary': primary,
            'members': members,
        }
    except Exception as e:
        return {'ok': False, 'error': str(e)}


@st.cache_data(ttl=5)
def check_kafka():
    try:
        c    = Consumer({'bootstrap.servers': BROKER, 'group.id': 'poc-ui-probe',
                         'enable.auto.commit': False, 'broker.address.family': 'v6'})
        meta = c.list_topics(timeout=5)
        cdc  = sorted(t for t in meta.topics if t.startswith('poc.'))
        c.close()
        return {'ok': True, 'cdc_topics': cdc}
    except Exception as e:
        return {'ok': False, 'error': str(e)}


@st.cache_data(ttl=5)
def check_connect():
    try:
        resp = requests.get(f'{CONNECT_URL}/connectors?expand=status', timeout=3)
        resp.raise_for_status()
        out  = {}
        for name, info in resp.json().items():
            st_info = info['status']
            tasks   = [{'id': t['id'], 'state': t['state']} for t in st_info['tasks']]
            all_ok  = (st_info['connector']['state'] == 'RUNNING'
                       and all(t['state'] == 'RUNNING' for t in tasks))
            out[name] = {'state': st_info['connector']['state'],
                         'tasks': tasks, 'all_running': all_ok}
        return {'ok': True, 'connectors': out}
    except Exception as e:
        return {'ok': False, 'error': str(e), 'connectors': {}}


@st.cache_data(ttl=5)
def check_postgres():
    res = {'ok': False, 'analytics': {}, 'finance': {}}
    try:
        with psycopg2.connect(**PG_A, connect_timeout=3) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT COUNT(*) FROM orders_flat'); res['analytics']['orders_flat'] = cur.fetchone()[0]
        res['analytics']['ok'] = True
    except Exception as e:
        res['analytics']['error'] = str(e)
    try:
        with psycopg2.connect(**PG_F, connect_timeout=3) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT COUNT(*) FROM transactions'); res['finance']['transactions'] = cur.fetchone()[0]
        res['finance']['ok'] = True
        res['ok'] = True
    except Exception as e:
        res['finance']['error'] = str(e)
    return res


@st.cache_data(ttl=5)
def check_lag():
    lag, total = {}, 0
    try:
        c = Consumer({'bootstrap.servers': BROKER, 'group.id': GROUP_ID,
                      'enable.auto.commit': False, 'broker.address.family': 'v6'})
        for topic in TOPICS:
            lag[topic] = {}
            try:
                meta = c.list_topics(topic=topic, timeout=3)
                if topic not in meta.topics:
                    continue
                for pid in meta.topics[topic].partitions:
                    tp  = TopicPartition(topic, pid)
                    com = c.committed([tp], timeout=3)[0]
                    com_off = com.offset if com and com.offset >= 0 else 0
                    _, high = c.get_watermark_offsets(tp, timeout=3, cached=False)
                    l = max(0, high - com_off)
                    lag[topic][pid] = {'lag': l, 'committed': com_off, 'hwm': high}
                    total += l
            except Exception:
                pass
        c.close()
    except Exception:
        pass
    return {'by_topic': lag, 'total': total}


@st.cache_data(ttl=5)
def check_metrics():
    try:
        with psycopg2.connect(**PG_A, connect_timeout=3) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*)                                                             AS total_docs,
                        COALESCE(ROUND(AVG(e2e_lat_ms)),0)                                  AS avg_e2e_ms,
                        COALESCE(ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP
                            (ORDER BY e2e_lat_ms)),0)                                        AS p50_ms,
                        COALESCE(ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP
                            (ORDER BY e2e_lat_ms)),0)                                        AS p95_ms,
                        COALESCE(ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP
                            (ORDER BY e2e_lat_ms)),0)                                        AS p99_ms,
                        COALESCE(ROUND(AVG(doc_size_bytes)),0)                               AS avg_bytes,
                        COALESCE(ROUND(AVG(debezium_lat_ms)),0)                              AS avg_deb_ms,
                        COALESCE(ROUND(AVG(consumer_lat_ms)),0)                              AS avg_con_ms,
                        COALESCE(ROUND(AVG(write_lat_ms)),0)                                 AS avg_wrt_ms,
                        COUNT(*) FILTER (WHERE recorded_at > now() - interval '5 minutes')  AS tput_5m,
                        COUNT(*) FILTER (WHERE recorded_at > now() - interval '1 minute')   AS tput_1m
                    FROM cdc_pipeline_metrics
                """)
                row  = cur.fetchone()
                kpi  = dict(zip([d[0] for d in cur.description], row)) if row else {}

                cur.execute("""
                    SELECT recorded_at, e2e_lat_ms, debezium_lat_ms, consumer_lat_ms, write_lat_ms
                    FROM   cdc_pipeline_metrics
                    ORDER  BY recorded_at DESC
                    LIMIT  100
                """)
                ts_rows = cur.fetchall()
                ts_cols = [d[0] for d in cur.description]

                cur.execute("""
                    SELECT
                        doc_id,
                        collection,
                        operation                                              AS op,
                        doc_size_bytes                                         AS bytes,
                        to_char(to_timestamp(mongo_ts_ms /1000.0)
                            AT TIME ZONE 'UTC', 'HH24:MI:SS.MS')              AS mongo_time,
                        to_char(to_timestamp(kafka_ts_ms /1000.0)
                            AT TIME ZONE 'UTC', 'HH24:MI:SS.MS')              AS kafka_time,
                        to_char(to_timestamp(consumer_recv_ms/1000.0)
                            AT TIME ZONE 'UTC', 'HH24:MI:SS.MS')              AS consumer_time,
                        to_char(to_timestamp(pg_stored_ms /1000.0)
                            AT TIME ZONE 'UTC', 'HH24:MI:SS.MS')              AS pg_time,
                        debezium_lat_ms   AS deb_ms,
                        consumer_lat_ms   AS con_ms,
                        write_lat_ms      AS wrt_ms,
                        e2e_lat_ms        AS e2e_ms
                    FROM   cdc_pipeline_metrics
                    ORDER  BY recorded_at DESC
                    LIMIT  20
                """)
                recent_rows = cur.fetchall()
                recent_cols = [d[0] for d in cur.description]

        return {
            'ok':         True,
            'kpi':        kpi,
            'timeseries': [dict(zip(ts_cols, r)) for r in ts_rows],
            'recent':     [dict(zip(recent_cols, r)) for r in recent_rows],
        }
    except Exception as e:
        return {'ok': False, 'error': str(e), 'kpi': {}, 'timeseries': [], 'recent': []}


# ── helpers ───────────────────────────────────────────────────────────────────

def dot(ok):
    return '🟢' if ok else '🔴'

def badge(state):
    state = (state or '').upper()
    if state == 'RUNNING': return '🟢 RUNNING'
    if state == 'FAILED':  return '🔴 FAILED'
    if state == 'PAUSED':  return '🟡 PAUSED'
    return f'⚪ {state}'

def fmt(n):
    return f'{n:,}' if n is not None else '—'


# ── page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title='CDC Pipeline Dashboard',
    page_icon='⚡',
    layout='wide',
    initial_sidebar_state='expanded',
)

# Custom CSS
st.markdown("""
<style>
  .block-container { padding-top: 1.5rem; padding-bottom: 1rem; }
  .stMetric { background: #1a1d27; border: 1px solid #2a2d3e; border-radius: 10px; padding: 12px 16px; }
</style>
""", unsafe_allow_html=True)


# ── sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.title('⚡ CDC Pipeline')
    st.markdown('---')
    auto_refresh = st.toggle('Auto-refresh', value=True)
    interval     = st.slider('Interval (s)', 3, 30, 5)
    if st.button('🔄 Refresh Now', use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.markdown('---')
    st.caption(f'Group: `{GROUP_ID}`')
    st.caption(f'Broker: `{BROKER}`')
    st.caption(f'Connect: `{CONNECT_URL}`')


# ── fetch all data ────────────────────────────────────────────────────────────

mongo   = check_mongo()
kafka   = check_kafka()
connect = check_connect()
pg      = check_postgres()
lag     = check_lag()
metrics = check_metrics()

connectors   = connect.get('connectors', {})
running_conn = sum(1 for c in connectors.values() if c.get('all_running'))
total_lag    = lag['total']
all_ok       = mongo['ok'] and kafka['ok'] and connect['ok'] and pg['ok']


# ── header ────────────────────────────────────────────────────────────────────

h_col1, h_col2 = st.columns([3, 1])
with h_col1:
    st.title('CDC Pipeline Dashboard')
with h_col2:
    st.markdown(f"<div style='text-align:right;padding-top:20px;color:#64748b;font-size:13px'>Updated: {time.strftime('%H:%M:%S')}</div>", unsafe_allow_html=True)

if not all_ok:
    down = [n for n, ok in [('MongoDB', mongo['ok']), ('Kafka', kafka['ok']),
                              ('Kafka Connect', connect['ok']), ('PostgreSQL', pg['ok'])] if not ok]
    st.error(f"Components unreachable: {', '.join(down)}")


# ── pipeline topology ─────────────────────────────────────────────────────────

st.markdown('#### Pipeline Topology')

def _node(col, icon, name, sub, ok):
    with col:
        with st.container(border=True):
            s = '🟢' if ok else '🔴'
            st.markdown(f"<div style='text-align:center'>"
                        f"<div style='font-size:24px'>{icon}</div>"
                        f"<div style='font-weight:700;font-size:13px'>{s} {name}</div>"
                        f"<div style='font-size:11px;color:gray'>{sub}</div>"
                        f"</div>", unsafe_allow_html=True)

def _arrow(col, label):
    with col:
        st.markdown(
            f"<div style='text-align:center;padding-top:28px;"
            f"font-size:20px;color:#06b6d4'>→<br>"
            f"<span style='font-size:10px;color:gray'>{label}</span></div>",
            unsafe_allow_html=True,
        )

n1, a1, n2, a2, n3, a3, n4, a4, n5 = st.columns([3, 1, 3, 1, 3, 1, 3, 1, 3])

_node(n1, '🍃', 'MongoDB',  ':27018 / rs0',        mongo['ok'])
_arrow(a1, 'Change Stream')
_node(n2, '⚡', 'Debezium', 'Connect :8083',        connect['ok'])
_arrow(a2, 'Kafka Events')
_node(n3, '📨', 'Kafka',    ':9092 / poc.mydb.*',   kafka['ok'])
_arrow(a3, 'Consumer Group')
_node(n4, '⚙️', 'Consumer', f'lag: {total_lag:,}',  kafka['ok'])
_arrow(a4, 'Write')

with n5:
    with st.container(border=True):
        s_a = '🟢' if pg['analytics'].get('ok') else '🔴'
        s_f = '🟢' if pg['finance'].get('ok')   else '🔴'
        st.markdown(
            f"<div style='text-align:center'>"
            f"<div style='font-size:18px'>📊</div>"
            f"<div style='font-weight:700;font-size:13px'>{s_a} Analytics</div>"
            f"<div style='font-size:11px;color:gray'>orders_flat</div>"
            f"<hr style='margin:6px 0'>"
            f"<div style='font-size:18px'>💰</div>"
            f"<div style='font-weight:700;font-size:13px'>{s_f} Finance</div>"
            f"<div style='font-size:11px;color:gray'>transactions</div>"
            f"</div>", unsafe_allow_html=True
        )


# ── summary metrics ───────────────────────────────────────────────────────────

st.markdown('#### Live Metrics')
m1, m2, m3, m4 = st.columns(4)

orders_val = pg['analytics'].get('orders_flat')
txns_val   = pg['finance'].get('transactions')

m1.metric('orders_flat',   fmt(orders_val),          help='Analytics DB — total order rows')
m2.metric('transactions',  fmt(txns_val),             help='Finance DB — total transaction rows')
m3.metric('Consumer Lag',  f'{total_lag:,} events',  help='Total events behind high-water mark')
m4.metric('Connectors',    f'{running_conn}/{len(connectors)} running', help='Debezium connector task health')


st.markdown('---')


# ── component health + connector status (two columns) ─────────────────────────

left, right = st.columns(2)

# Component health
with left:
    st.markdown('#### Component Health')

    with st.container(border=True):
        r1, r2, r3, r4 = st.columns(4)
        r1.metric('MongoDB',       dot(mongo['ok']))
        r2.metric('Kafka',         dot(kafka['ok']))
        r3.metric('Kafka Connect', dot(connect['ok']))
        r4.metric('PostgreSQL',    dot(pg['ok']))

    with st.expander('MongoDB details'):
        if mongo['ok']:
            st.write(f"**Version:** {mongo.get('version')}")
            st.write(f"**Replica Set:** {mongo.get('rs')}")
            st.write(f"**Primary:** {mongo.get('primary')}")
            members = mongo.get('members', [])
            if members:
                st.table([{'Host': m['host'], 'State': m['state']} for m in members])
        else:
            st.error(mongo.get('error'))

    with st.expander('Kafka details'):
        if kafka['ok']:
            st.write(f"**Broker:** `{BROKER}`")
            st.write('**CDC Topics:**')
            for t in kafka.get('cdc_topics', []):
                st.code(t)
        else:
            st.error(kafka.get('error'))

    with st.expander('PostgreSQL details'):
        st.write('**Analytics DB**')
        if pg['analytics'].get('ok'):
            st.write(f"- orders_flat: `{fmt(pg['analytics'].get('orders_flat'))}`")
        else:
            st.error(pg['analytics'].get('error', 'unreachable'))
        st.write('**Finance DB**')
        if pg['finance'].get('ok'):
            st.write(f"- transactions: `{fmt(pg['finance'].get('transactions'))}`")
        else:
            st.error(pg['finance'].get('error', 'unreachable'))

# Connector status
with right:
    st.markdown('#### Connector Status')

    if not connect['ok']:
        st.error(f"Kafka Connect unreachable: {connect.get('error')}")
    elif not connectors:
        st.warning('No connectors registered. Run `python src/connector/manager.py`')
    else:
        for name, info in connectors.items():
            state  = info['state']
            tasks  = info['tasks']
            all_ok_conn = info['all_running']

            with st.container(border=True):
                c1, c2 = st.columns([3, 1])
                c1.markdown(f'**{name}**')
                c2.markdown(badge(state))
                for t in tasks:
                    st.markdown(f"&nbsp;&nbsp;task[{t['id']}]: {badge(t['state'])}", unsafe_allow_html=True)
                if not tasks:
                    st.caption('No tasks spawned yet')


st.markdown('---')


# ── pipeline queue status ─────────────────────────────────────────────────────

st.markdown('#### Pipeline Queue Status')

by_topic     = lag['by_topic']
has_any      = any(by_topic.values())
total_lag    = lag['total']
tput_1m      = int((metrics.get('kpi') or {}).get('tput_1m', 0) or 0)
total_done   = int((metrics.get('kpi') or {}).get('total_docs', 0) or 0)

if not has_any:
    st.info('No partition data. Kafka may be unreachable or topics not yet created.')
else:
    # ── top summary ───────────────────────────────────────────────────────────
    total_produced = sum(
        info['hwm']
        for parts in by_topic.values()
        for info in parts.values()
    )

    total_committed = sum(
        info['committed']
        for parts in by_topic.values()
        for info in parts.values()
    )

    q1, q2, q3, q4, q5 = st.columns(5)
    q1.metric('Kafka Messages',    f"{total_produced:,}",
              help='Total messages Debezium has published to Kafka — includes tombstones and product events')
    q2.metric('Consumer Committed', f"{total_committed:,}",
              help='Messages the consumer has committed offsets for (consumed from Kafka)')
    q3.metric('Written to PG',    f"{total_done:,}",
              help='Documents actually written to PostgreSQL — excludes tombstones, products (no PG target), and DLQ events')
    q4.metric('Pending (lag)',
              f"{total_lag:,}",
              delta=f"-{total_lag}" if total_lag > 0 else "0",
              delta_color='inverse',
              help='Kafka Messages − Consumer Committed = messages not yet consumed')
    q5.metric('Throughput (1 min)', f"{tput_1m} docs/min",
              help='Documents written to PG in the last minute')

    # second row: skipped breakdown + catch-up estimate
    skipped = max(0, total_committed - total_done)
    s1, s2, s3 = st.columns([2, 2, 6])
    s1.caption(f'Skipped (tombstones / no-target): **{skipped:,}**')
    if tput_1m > 0 and total_lag > 0:
        eta_sec = (total_lag / tput_1m) * 60
        eta_str = f"{int(eta_sec)}s" if eta_sec < 120 else f"{eta_sec/60:.1f} min"
        s2.caption(f'Est. catch-up at current rate: **{eta_str}**')
    elif total_lag == 0:
        s2.caption('Pipeline is **caught up** ✓')

    st.markdown('')

    # ── per-topic breakdown ───────────────────────────────────────────────────
    for topic, parts in by_topic.items():
        if not parts:
            st.caption(f'{topic}: no partitions found')
            continue

        topic_lag       = sum(i['lag'] for i in parts.values())
        topic_hwm       = sum(i['hwm'] for i in parts.values())
        topic_committed = sum(i['committed'] for i in parts.values())
        topic_pct       = topic_committed / topic_hwm if topic_hwm > 0 else 1.0
        lag_color       = '#22c55e' if topic_lag == 0 else '#f59e0b' if topic_lag < 50 else '#ef4444'
        lag_label       = 'caught up' if topic_lag == 0 else f'{topic_lag:,} pending'

        with st.container(border=True):
            th1, th2, th3, th4 = st.columns([4, 2, 2, 2])
            th1.markdown(f'**`{topic}`**')
            th2.markdown(f"<div style='text-align:center;font-size:12px;color:gray'>Produced<br><b>{topic_hwm:,}</b></div>",
                         unsafe_allow_html=True)
            th3.markdown(f"<div style='text-align:center;font-size:12px;color:gray'>Processed<br><b>{topic_committed:,}</b></div>",
                         unsafe_allow_html=True)
            th4.markdown(f"<div style='text-align:center;font-size:12px'>Pending<br>"
                         f"<b style='color:{lag_color}'>{lag_label}</b></div>",
                         unsafe_allow_html=True)

            st.progress(min(topic_pct, 1.0),
                        text=f'{topic_pct*100:.1f}% consumed')

            # per-partition detail (collapsed if only 1 partition)
            if len(parts) > 1:
                with st.expander(f'Per-partition detail ({len(parts)} partitions)'):
                    for pid, info in sorted(parts.items()):
                        l   = info['lag']
                        hwm = info['hwm']
                        com = info['committed']
                        pct = com / hwm if hwm > 0 else 1.0
                        lc  = '#22c55e' if l == 0 else '#f59e0b' if l < 50 else '#ef4444'
                        r1, r2, r3, r4 = st.columns([3, 1, 1, 1])
                        r1.progress(min(pct, 1.0), text=f'partition {pid}')
                        r2.markdown(f"<div style='text-align:right;font-size:12px;color:gray'>produced<br>{hwm:,}</div>",
                                    unsafe_allow_html=True)
                        r3.markdown(f"<div style='text-align:right;font-size:12px;color:gray'>processed<br>{com:,}</div>",
                                    unsafe_allow_html=True)
                        r4.markdown(f"<div style='text-align:right;font-size:13px'>pending<br>"
                                    f"<b style='color:{lc}'>{l:,}</b></div>",
                                    unsafe_allow_html=True)
            else:
                for pid, info in parts.items():
                    l   = info['lag']
                    hwm = info['hwm']
                    com = info['committed']
                    lc  = '#22c55e' if l == 0 else '#f59e0b' if l < 50 else '#ef4444'
                    st.caption(
                        f'partition {pid} — '
                        f'produced: {hwm:,} | '
                        f'processed: {com:,} | '
                        f'pending: **{l:,}**'
                    )


# ── performance metrics ───────────────────────────────────────────────────────

st.markdown('---')
st.markdown('#### CDC Performance Metrics')

if not metrics['ok']:
    st.warning(f"Metrics unavailable: {metrics.get('error')} — consumer may not have started yet.")
else:
    kpi   = metrics['kpi']
    total = int(kpi.get('total_docs', 0) or 0)

    if total == 0:
        st.info('No metrics yet — waiting for the first document to flow through the pipeline.')
    else:
        # ── KPI row ───────────────────────────────────────────────────────────
        k1, k2, k3, k4, k5, k6, k7 = st.columns(7)
        k1.metric('Total Docs', f"{total:,}",
                  help='Total documents that have completed the full CDC pipeline '
                       '(MongoDB → Kafka → Consumer → PostgreSQL). '
                       'Does not include tombstones or events with no PG target (e.g. products).')
        k2.metric('Avg E2E Latency', f"{int(kpi.get('avg_e2e_ms', 0))} ms",
                  help='Average end-to-end latency across all processed documents. '
                       'Measured from when MongoDB recorded the change (ts_ms in the '
                       'Debezium envelope) to when the row was written to PostgreSQL. '
                       'Lower is better — target is typically < 1000 ms for a local pipeline.')
        k3.metric('P50 E2E', f"{int(kpi.get('p50_ms', 0))} ms",
                  help='50th percentile (median) end-to-end latency. '
                       'Half of all documents completed faster than this value. '
                       'A healthy baseline — compare against P95/P99 to spot outliers.')
        k4.metric('P95 E2E', f"{int(kpi.get('p95_ms', 0))} ms",
                  help='95th percentile end-to-end latency. '
                       '95% of documents completed within this time. '
                       'Spikes here usually indicate write contention or batch buildup.')
        k5.metric('P99 E2E', f"{int(kpi.get('p99_ms', 0))} ms",
                  help='99th percentile end-to-end latency — the worst 1% of documents. '
                       'A large gap between P95 and P99 signals occasional slow outliers '
                       '(e.g. PostgreSQL connection pool exhaustion, GC pauses).')
        k6.metric('Throughput (1 min)', f"{int(kpi.get('tput_1m', 0))} docs",
                  help='Number of documents written to PostgreSQL in the last 60 seconds. '
                       'Reflects the current live processing rate of the consumer. '
                       'Goes to 0 if the consumer is idle or stopped.')
        k7.metric('Avg Doc Size', f"{int(kpi.get('avg_bytes', 0))} B",
                  help='Average size of the raw Kafka message value in bytes. '
                       'This is the size of the full Debezium envelope (JSON) as it '
                       'travels through Kafka — not just the MongoDB document fields.')

        st.markdown('')

        # ── charts ────────────────────────────────────────────────────────────
        col_l, col_r = st.columns(2)

        with col_l:
            st.markdown('**Avg Latency by Stage (ms)**',
                        help='Breaks the end-to-end latency into three pipeline stages:\n\n'
                             '**Mongo → Kafka (Debezium):** Time from when MongoDB wrote the '
                             'change to when Debezium published it to Kafka. '
                             'High values here mean Debezium or Kafka Connect is slow.\n\n'
                             '**Kafka → Consumer:** Time the message sat in Kafka waiting to '
                             'be picked up by the Python consumer (queue wait time). '
                             'High values here mean the consumer is falling behind.\n\n'
                             '**Consumer → PG (Write):** Time to parse, route, and write the '
                             'row to PostgreSQL. High values here mean slow DB writes.')
            df_breakdown = pd.DataFrame({
                'Stage':    ['Mongo → Kafka\n(Debezium)', 'Kafka → Consumer', 'Consumer → PG\n(Write)'],
                'Avg (ms)': [
                    int(kpi.get('avg_deb_ms', 0) or 0),
                    int(kpi.get('avg_con_ms', 0) or 0),
                    int(kpi.get('avg_wrt_ms', 0) or 0),
                ],
            }).set_index('Stage')
            st.bar_chart(df_breakdown)

        with col_r:
            ts = metrics['timeseries']
            if ts:
                st.markdown('**E2E Latency Over Time (ms) — last 100 events**',
                            help='End-to-end latency for each of the last 100 documents, '
                                 'ordered oldest → newest (left → right). '
                                 'Flat line = stable pipeline. '
                                 'Spikes = individual slow documents. '
                                 'Upward trend = pipeline falling behind.')
                df_ts = pd.DataFrame(ts[::-1])
                df_ts['recorded_at'] = pd.to_datetime(df_ts['recorded_at'], utc=True)
                df_ts = df_ts.set_index('recorded_at')[['e2e_lat_ms']]
                df_ts.index = df_ts.index.strftime('%H:%M:%S')
                df_ts.columns = ['E2E (ms)']
                st.line_chart(df_ts)

        # ── stacked latency breakdown over time ───────────────────────────────
        if metrics['timeseries']:
            with st.expander('Latency breakdown over time (stacked area)'):
                st.caption('Same last 100 events split into three pipeline stages stacked on top '
                           'of each other. Shows which stage dominates the latency at any point in time.')
                df_s = pd.DataFrame(metrics['timeseries'][::-1])
                df_s['recorded_at'] = pd.to_datetime(df_s['recorded_at'], utc=True)
                df_s = df_s.set_index('recorded_at')[
                    ['debezium_lat_ms', 'consumer_lat_ms', 'write_lat_ms']
                ]
                df_s.index = df_s.index.strftime('%H:%M:%S')
                df_s.columns = ['Debezium (ms)', 'Consumer (ms)', 'Write (ms)']
                st.bar_chart(df_s)

        # ── recent events table ────────────────────────────────────────────────
        st.markdown('**Recent Events — timestamp trail & per-stage latency**',
                    help='Last 20 documents that completed the pipeline, newest first.\n\n'
                         '**Mongo (UTC):** Timestamp from the Debezium `ts_ms` field — '
                         'when MongoDB committed the change to its oplog.\n\n'
                         '**Kafka (UTC):** When Debezium published the message to Kafka '
                         '(Kafka broker-assigned timestamp).\n\n'
                         '**Consumer (UTC):** When the Python consumer received the message '
                         'from the Kafka broker.\n\n'
                         '**PG Stored (UTC):** When the row was successfully written to '
                         'PostgreSQL.\n\n'
                         '**Deb (ms):** Kafka(UTC) − Mongo(UTC) — Debezium capture delay.\n\n'
                         '**Con (ms):** Consumer(UTC) − Kafka(UTC) — queue wait time.\n\n'
                         '**Wrt (ms):** PG(UTC) − Consumer(UTC) — database write time.\n\n'
                         '**E2E (ms):** PG(UTC) − Mongo(UTC) — total pipeline latency.')
        recent = metrics['recent']
        if recent:
            df_r = pd.DataFrame(recent)
            df_r.columns = [
                'Doc ID', 'Collection', 'Op', 'Size (B)',
                'Mongo (UTC)', 'Kafka (UTC)', 'Consumer (UTC)', 'PG Stored (UTC)',
                'Deb (ms)', 'Con (ms)', 'Wrt (ms)', 'E2E (ms)',
            ]
            st.dataframe(df_r, use_container_width=True, hide_index=True)


# ── auto-refresh ──────────────────────────────────────────────────────────────

if auto_refresh:
    time.sleep(interval)
    st.cache_data.clear()
    st.rerun()
