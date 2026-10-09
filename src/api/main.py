"""
FastAPI application — serves the P2P CDC web application.

Startup
───────
- Opens asyncpg pool and Neo4j async driver (stored in app.state).
- Mounts web/dist as static files at "/" once React is built.

Run (development — serves API only)
────────────────────────────────────
  PYTHONPATH=src uvicorn api.main:app --reload --port 8000

Run (production — API + React build)
─────────────────────────────────────
  cd web && npm run build
  PYTHONPATH=src uvicorn api.main:app --port 8000
"""

import logging
import os
import sys
from contextlib import asynccontextmanager

_src = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if _src not in sys.path:
    sys.path.insert(0, _src)

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from writer.pg_writer import create_pool, ensure_metrics_table
from writer.neo4j_writer import create_driver, apply_constraints

load_dotenv(os.path.join(_src, '..', '.env'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s [%(name)s] %(message)s',
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ──────────────────────────────────────────────────────────────
    pg_pool = await create_pool(
        host=os.getenv('POSTGRES_ANALYTICS_HOST', 'localhost'),
        port=os.getenv('POSTGRES_ANALYTICS_PORT', '5432'),
        user=os.getenv('POSTGRES_ANALYTICS_USER', 'postgres'),
        password=os.getenv('POSTGRES_ANALYTICS_PASSWORD', 'postgres'),
        db=os.getenv('POSTGRES_ANALYTICS_DB', 'analytics'),
    )
    await ensure_metrics_table(pg_pool)
    app.state.pg_pool = pg_pool
    logger.info("PostgreSQL pool ready")

    neo4j_conn = None
    try:
        neo4j_conn = await create_driver(
            uri=os.getenv('NEO4J_URI',           'bolt://localhost:7687'),
            user=os.getenv('NEO4J_USER',         'neo4j'),
            password=os.getenv('NEO4J_PASSWORD', 'password'),
            database=os.getenv('NEO4J_DATABASE', 'DBMD-Minor'),
        )
        await apply_constraints(neo4j_conn)
    except Exception as exc:
        logger.warning(f"Neo4j unavailable at startup ({exc!r}) — graph endpoints will return 503")
    app.state.neo4j_conn = neo4j_conn

    from pymongo import MongoClient
    mongo = MongoClient(
        os.getenv('MONGO_URI', 'mongodb://localhost:27018/?replicaSet=rs0'),
        serverSelectionTimeoutMS=5000,
    )
    app.state.mongo = mongo
    app.state.mongo_db = mongo['mydb']
    logger.info("MongoDB client ready")

    yield

    # ── Shutdown ─────────────────────────────────────────────────────────────
    await pg_pool.close()
    if neo4j_conn:
        await neo4j_conn.close()
    mongo.close()
    logger.info("Application shutdown complete")


app = FastAPI(
    title='P2P CDC Dashboard API',
    version='2.0.0',
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=['http://localhost:5173', 'http://localhost:3000'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

# ── Routers ──────────────────────────────────────────────────────────────────
from api.routers import documents, pipeline, metrics, graph, simulator

app.include_router(documents.router,  prefix='/api/documents',  tags=['Documents'])
app.include_router(pipeline.router,   prefix='/api/pipeline',   tags=['Pipeline'])
app.include_router(metrics.router,    prefix='/api/metrics',    tags=['Metrics'])
app.include_router(graph.router,      prefix='/api/graph',      tags=['Graph'])
app.include_router(simulator.router,  prefix='/api/simulate',   tags=['Simulator'])

# Frontend is served from the separate cdc-dashboard-ui repo (http://localhost:5173 in dev)
