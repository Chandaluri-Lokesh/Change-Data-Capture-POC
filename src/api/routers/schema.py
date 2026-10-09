"""
GET /api/schema/mongodb     — collections with doc counts and inferred field types
GET /api/schema/postgresql  — tables with columns, types, PK/FK info and row counts
GET /api/schema/neo4j       — labels, relationship types, property keys, constraints
"""

import logging
from fastapi import APIRouter, HTTPException, Request

logger = logging.getLogger(__name__)
router = APIRouter()


# ---------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------

def _infer_type(v) -> str:
    if isinstance(v, bool):   return 'Boolean'
    if isinstance(v, int):    return 'Integer'
    if isinstance(v, float):  return 'Float'
    if isinstance(v, str):    return 'String'
    if isinstance(v, list):   return 'Array'
    if isinstance(v, dict):   return 'Object'
    return type(v).__name__


@router.get('/mongodb')
async def mongodb_schema(request: Request):
    db = request.app.state.mongo_db
    try:
        result = []
        for name in db.list_collection_names():
            count = db[name].count_documents({})
            sample = db[name].find_one({}) or {}
            fields = [
                {'name': k, 'type': _infer_type(v)}
                for k, v in sample.items()
                if k != '_id'
            ]
            result.append({'name': name, 'count': count, 'fields': fields})
        result.sort(key=lambda c: c['name'])
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------

@router.get('/postgresql')
async def postgresql_schema(request: Request):
    pool = request.app.state.pg_pool
    try:
        async with pool.acquire() as conn:
            # All user tables
            tables = await conn.fetch("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)

            result = []
            for t in tables:
                tname = t['table_name']

                # Row count
                row_count = await conn.fetchval(
                    f'SELECT COUNT(*) FROM "{tname}"'
                )

                # Columns
                cols = await conn.fetch("""
                    SELECT
                        c.column_name,
                        c.data_type,
                        c.is_nullable,
                        c.column_default,
                        COALESCE(pk.constraint_type, '') AS constraint_type
                    FROM information_schema.columns c
                    LEFT JOIN (
                        SELECT kcu.column_name, tc.constraint_type
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                          ON tc.constraint_name = kcu.constraint_name
                         AND tc.table_schema    = kcu.table_schema
                        WHERE tc.table_name   = $1
                          AND tc.table_schema = 'public'
                          AND tc.constraint_type = 'PRIMARY KEY'
                    ) pk ON pk.column_name = c.column_name
                    WHERE c.table_name   = $1
                      AND c.table_schema = 'public'
                    ORDER BY c.ordinal_position
                """, tname)

                # Foreign keys
                fks = await conn.fetch("""
                    SELECT
                        kcu.column_name,
                        ccu.table_name  AS foreign_table,
                        ccu.column_name AS foreign_column
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                     AND tc.table_schema    = kcu.table_schema
                    JOIN information_schema.constraint_column_usage ccu
                      ON ccu.constraint_name = tc.constraint_name
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                      AND tc.table_name      = $1
                      AND tc.table_schema    = 'public'
                """, tname)
                fk_map = {r['column_name']: f"{r['foreign_table']}.{r['foreign_column']}" for r in fks}

                result.append({
                    'name':      tname,
                    'row_count': row_count,
                    'columns': [
                        {
                            'name':       c['column_name'],
                            'type':       c['data_type'],
                            'nullable':   c['is_nullable'] == 'YES',
                            'primary_key': c['constraint_type'] == 'PRIMARY KEY',
                            'foreign_key': fk_map.get(c['column_name']),
                        }
                        for c in cols
                    ],
                })
        return result
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Neo4j
# ---------------------------------------------------------------------------

@router.get('/neo4j')
async def neo4j_schema(request: Request):
    conn = request.app.state.neo4j_conn
    if not conn:
        raise HTTPException(status_code=503, detail='Neo4j not connected')
    try:
        async with conn.driver.session(database=conn.database) as session:
            # Labels + counts + property keys
            label_result = await session.run(
                "CALL db.labels() YIELD label RETURN label ORDER BY label"
            )
            labels_raw = [r['label'] async for r in label_result]

            labels = []
            for label in labels_raw:
                r = await session.run(
                    f"MATCH (n:`{label}`) RETURN COUNT(n) AS count LIMIT 1"
                )
                rec = await r.single()
                count = rec['count'] if rec else 0

                pk = await session.run(
                    f"MATCH (n:`{label}`) UNWIND keys(n) AS k RETURN DISTINCT k ORDER BY k LIMIT 20"
                )
                props = [r2['k'] async for r2 in pk]
                labels.append({'label': label, 'count': count, 'properties': props})

            # Relationship types + counts
            rel_result = await session.run(
                "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType ORDER BY relationshipType"
            )
            rel_types_raw = [r['relationshipType'] async for r in rel_result]

            relationships = []
            for rtype in rel_types_raw:
                r = await session.run(
                    f"MATCH ()-[r:`{rtype}`]->() RETURN COUNT(r) AS count LIMIT 1"
                )
                rec = await r.single()
                count = rec['count'] if rec else 0

                ends = await session.run(
                    f"""
                    MATCH (a)-[r:`{rtype}`]->(b)
                    RETURN DISTINCT labels(a)[0] AS from_label, labels(b)[0] AS to_label
                    LIMIT 1
                    """
                )
                end_rec = await ends.single()
                relationships.append({
                    'type':       rtype,
                    'count':      count,
                    'from_label': end_rec['from_label'] if end_rec else None,
                    'to_label':   end_rec['to_label']   if end_rec else None,
                })

            # Constraints
            try:
                c_result = await session.run("SHOW CONSTRAINTS")
                constraints = [
                    {'name': r['name'], 'type': r['type'], 'entity': r.get('labelsOrTypes', []), 'properties': r.get('properties', [])}
                    async for r in c_result
                ]
            except Exception:
                constraints = []

        return {'labels': labels, 'relationships': relationships, 'constraints': constraints}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
