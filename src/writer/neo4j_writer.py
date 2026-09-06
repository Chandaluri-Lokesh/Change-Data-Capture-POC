"""
Neo4j async writer for the CDC P2P pipeline.

Uses the official neo4j Python async driver (neo4j>=5.20).
Each Cypher+params pair from the mapping engine is executed as an
auto-committed transaction (write_transaction / execute_write).

Retries
───────
Transient Neo4j errors (deadlock, leader election) are retried automatically
by the driver's managed transactions (session.execute_write).  We wrap each
op in execute_write so the driver handles this transparently.

Connection lifecycle
────────────────────
Call `create_driver(uri, user, password)` once at consumer startup.
Pass the driver to `run_cypher()` for every event.
Call `driver.close()` on shutdown.
"""

import logging
from typing import List, Tuple

from neo4j import AsyncGraphDatabase, AsyncDriver

logger = logging.getLogger(__name__)


async def create_driver(uri: str, user: str, password: str, database: str = None) -> AsyncDriver:
    """Create and verify an async Neo4j driver connection."""
    driver = AsyncGraphDatabase.driver(uri, auth=(user, password))
    await driver.verify_connectivity()
    logger.info(f"[neo4j] Connected to {uri} (database={database or 'default'})")
    driver._cdc_database = database  # stash for session calls
    return driver


async def run_cypher(driver: AsyncDriver, cypher: str, params: dict) -> None:
    """
    Execute a single Cypher statement with params as a write transaction.
    Transient errors are retried by the driver automatically.
    Permanent errors are logged and re-raised for the consumer's DLQ handler.
    """
    db = getattr(driver, '_cdc_database', None)
    async with driver.session(database=db) as session:
        await session.execute_write(_tx_run, cypher, params)


async def _tx_run(tx, cypher: str, params: dict):
    await tx.run(cypher, **params)


async def run_ops(
    driver: AsyncDriver,
    ops: List[Tuple[str, dict]],
) -> None:
    """
    Execute a list of (cypher, params) tuples produced by the mapping engine.
    Each op runs in its own write transaction.
    Raises on the first permanent error.
    """
    for cypher, params in ops:
        await run_cypher(driver, cypher, params)


async def apply_constraints(driver: AsyncDriver) -> None:
    """
    Create Neo4j uniqueness constraints for all P2P node types.
    Idempotent — uses IF NOT EXISTS.
    Call once at consumer startup (or run constraints.cypher manually).
    """
    constraints = [
        "CREATE CONSTRAINT rfq_unique IF NOT EXISTS FOR (r:RFQ) REQUIRE r.rfq_number IS UNIQUE",
        "CREATE CONSTRAINT po_unique IF NOT EXISTS FOR (p:PurchaseOrder) REQUIRE p.po_number IS UNIQUE",
        "CREATE CONSTRAINT asn_unique IF NOT EXISTS FOR (a:ASN) REQUIRE a.asn_number IS UNIQUE",
        "CREATE CONSTRAINT grn_unique IF NOT EXISTS FOR (g:GRN) REQUIRE g.grn_number IS UNIQUE",
        "CREATE CONSTRAINT invoice_unique IF NOT EXISTS FOR (i:Invoice) REQUIRE i.invoice_number IS UNIQUE",
        "CREATE CONSTRAINT vendor_unique IF NOT EXISTS FOR (v:Vendor) REQUIRE v.vendor_id IS UNIQUE",
        "CREATE CONSTRAINT material_unique IF NOT EXISTS FOR (m:Material) REQUIRE m.material_code IS UNIQUE",
    ]
    db = getattr(driver, '_cdc_database', None)
    async with driver.session(database=db) as session:
        for cypher in constraints:
            try:
                await session.run(cypher)
            except Exception as exc:
                # Constraint may already exist under a different name on older Neo4j
                logger.warning(f"[neo4j] Constraint statement skipped ({exc!r}): {cypher[:60]}")
    logger.info("[neo4j] Constraints applied.")
