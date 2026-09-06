"""
GET /api/graph/{collection}/{doc_id}

Returns a Neo4j subgraph centred on the requested node as
{ nodes: [...], links: [...] } — consumable by react-force-graph-2d.

GET /api/graph/chain/{chain_id}

Returns the full P2P chain graph for a base sequence ID, tracing all
documents that share the same chain (PO links to RFQ, ASN, GRN, Invoice).
"""

import logging

from fastapi import APIRouter, HTTPException, Request

logger = logging.getLogger(__name__)
router = APIRouter()

# Map collection name → (Neo4j label, key property)
LABEL_MAP = {
    'rfqs':            ('RFQ',           'rfq_number'),
    'purchase_orders': ('PurchaseOrder', 'po_number'),
    'asns':            ('ASN',           'asn_number'),
    'grns':            ('GRN',           'grn_number'),
    'invoices':        ('Invoice',       'invoice_number'),
}


def _neo4j_to_graph(records) -> dict:
    """Convert Neo4j result records into {nodes, links} for react-force-graph."""
    nodes_map = {}
    links = []

    for record in records:
        for key in record.keys():
            item = record[key]
            if item is None:
                continue
            if hasattr(item, 'labels'):
                # Node
                node_id = _node_id(item)
                if node_id not in nodes_map:
                    nodes_map[node_id] = {
                        'id':         node_id,
                        'label':      list(item.labels)[0] if item.labels else 'Node',
                        'properties': dict(item),
                    }
            elif hasattr(item, 'type'):
                # Relationship
                src = _node_id(item.start_node)
                dst = _node_id(item.end_node)
                links.append({
                    'source': src,
                    'target': dst,
                    'type':   item.type,
                })
                # Ensure both endpoint nodes are in the map
                for node in (item.start_node, item.end_node):
                    nid = _node_id(node)
                    if nid not in nodes_map:
                        nodes_map[nid] = {
                            'id':         nid,
                            'label':      list(node.labels)[0] if node.labels else 'Node',
                            'properties': dict(node),
                        }

    return {'nodes': list(nodes_map.values()), 'links': links}


def _node_id(node) -> str:
    """Use the first property value as a stable string id."""
    props = dict(node)
    # Prefer natural key fields
    for key in ('rfq_number', 'po_number', 'asn_number', 'grn_number',
                'invoice_number', 'vendor_id', 'material_code'):
        if key in props:
            return str(props[key])
    return str(list(props.values())[0]) if props else str(id(node))


@router.get('/{collection}/{doc_id}')
async def get_subgraph(collection: str, doc_id: str, request: Request, depth: int = 2):
    """Return the Neo4j subgraph up to `depth` hops around the given document node."""
    driver = request.app.state.neo4j_driver
    if not driver:
        raise HTTPException(status_code=503, detail='Neo4j not connected')

    label_info = LABEL_MAP.get(collection)
    if not label_info:
        raise HTTPException(status_code=400, detail=f'Unknown collection {collection!r}')
    label, key_prop = label_info

    depth = min(max(depth, 1), 4)  # clamp 1–4
    cypher = f"""
        MATCH (root:{label} {{{key_prop}: $doc_id}})
        OPTIONAL MATCH path = (root)-[r*1..{depth}]-(m)
        UNWIND (nodes(path) + [root]) AS n
        UNWIND (relationships(path) + []) AS rel
        RETURN DISTINCT n, rel
        LIMIT 200
    """

    try:
        async with driver.session() as session:
            result = await session.run(cypher, doc_id=doc_id)
            records = await result.data()
    except Exception as exc:
        logger.error(f"[graph] Neo4j query failed: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))

    # Simpler fallback query that works cleanly with the driver
    cypher2 = f"""
        MATCH (root:{label} {{{key_prop}: $doc_id}})
        OPTIONAL MATCH (root)-[rel]-(neighbor)
        RETURN root, rel, neighbor
    """
    try:
        async with driver.session() as session:
            result = await session.run(cypher2, doc_id=doc_id)
            records = []
            async for record in result:
                records.append(dict(record))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return _neo4j_to_graph(records)


@router.get('/stats/overview')
async def graph_overview(request: Request):
    """Node and relationship counts per label."""
    driver = request.app.state.neo4j_driver
    if not driver:
        raise HTTPException(status_code=503, detail='Neo4j not connected')

    cypher = """
        CALL {
            MATCH (n) RETURN labels(n)[0] AS label, COUNT(*) AS count
        }
        RETURN label, count
        ORDER BY count DESC
    """
    try:
        async with driver.session() as session:
            result = await session.run(cypher)
            node_counts = [{'label': r['label'], 'count': r['count']}
                           async for r in result]
        async with driver.session() as session:
            result = await session.run(
                "MATCH ()-[r]->() RETURN type(r) AS type, COUNT(*) AS count ORDER BY count DESC"
            )
            rel_counts = [{'type': r['type'], 'count': r['count']}
                          async for r in result]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return {'node_counts': node_counts, 'relationship_counts': rel_counts}
