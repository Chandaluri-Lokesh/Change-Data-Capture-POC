"""
Mapping Engine — drives CDC routing from declarative YAML rules.

Loads all *_mapping.yaml files from SAP-Files-Mappings/mapping_rules/ at
startup and indexes them by source_collection.

Public API
──────────
engine = MappingEngine()          # loads YAMLs once
engine.collections                # set of known collection names
engine.get_pg_routes(collection, document, op, doc_id)
    → list of (table_name, rows, upsert_key_cols)
engine.get_neo4j_ops(collection, document, op, doc_id)
    → list of (cypher_str, params_dict)
engine.get_primary_key(collection)
    → str  (column name of the parent table PK)

PostgreSQL route format
───────────────────────
Each tuple in the returned list is:
  table_name    : str   — target Postgres table
  rows          : list  — list of row dicts (column → value)
  upsert_key    : list  — column names that form the ON CONFLICT target
                          None for delete rows (which carry _delete=True)

For op='d', exactly ONE tuple is returned:
  (parent_table, [{'_delete': True, pk_col: doc_id}], None)

Neo4j op format
───────────────
Each tuple is:
  cypher : str  — Cypher statement (uses $param syntax)
  params : dict — parameter values for all $params in the statement

For op='d', a DETACH DELETE is returned.

Type coercion
─────────────
Mapping YAML types → Python:
  DATE        → datetime.date  (from ISO string "YYYY-MM-DD")
  TIMESTAMPTZ → datetime with UTC tz (from ISO string or epoch-ms int)
  NUMERIC/DECIMAL → float
  INTEGER/INT → int
  VARCHAR/CHAR/TEXT → str (unchanged)
  None input  → None (nullable fields pass through)
"""

import logging
import os
from datetime import date, datetime, timezone
from glob import glob
from typing import List, Optional, Tuple

import yaml

logger = logging.getLogger(__name__)

_RULES_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), '..', '..', 'SAP-Files-Mappings', 'mapping_rules')
)


# ---------------------------------------------------------------------------
# Type coercion
# ---------------------------------------------------------------------------

def _coerce(value, pg_type: str):
    """Coerce a raw Python value from the Debezium document to a Postgres-typed value."""
    if value is None:
        return None
    t = pg_type.upper().split('(')[0].strip()   # strip length/precision
    try:
        if t == 'DATE':
            if isinstance(value, date):
                return value
            return date.fromisoformat(str(value))
        if t in ('TIMESTAMPTZ', 'TIMESTAMP'):
            if isinstance(value, datetime):
                return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
            if isinstance(value, (int, float)):
                return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
            s = str(value).replace('Z', '+00:00')
            return datetime.fromisoformat(s)
        if t in ('NUMERIC', 'DECIMAL', 'FLOAT', 'REAL', 'DOUBLE'):
            return float(value)
        if t in ('INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'SERIAL'):
            return int(value)
        return str(value)
    except (ValueError, TypeError) as exc:
        logger.warning(f"[engine] coerce({value!r}, {pg_type!r}) failed: {exc} — using raw value")
        return value


# ---------------------------------------------------------------------------
# MappingEngine
# ---------------------------------------------------------------------------

class MappingEngine:
    """
    Loads all YAML mapping rules and exposes route-generation methods.
    Thread-safe for reads after __init__ completes.
    """

    def __init__(self, rules_dir: str = _RULES_DIR):
        self._rules_dir = rules_dir
        # collection_name → full YAML dict
        self._registry: dict = {}
        self._load()

    def _load(self) -> None:
        pattern = os.path.join(self._rules_dir, '*_mapping.yaml')
        files = glob(pattern)
        if not files:
            logger.warning(f"[engine] No mapping YAMLs found in {self._rules_dir!r}")
        for path in files:
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    mapping = yaml.safe_load(f)
                collection = mapping.get('source_collection')
                if not collection:
                    logger.warning(f"[engine] {path} has no source_collection — skipped")
                    continue
                self._registry[collection] = mapping
                logger.info(f"[engine] Loaded mapping for collection={collection!r} from {os.path.basename(path)}")
            except Exception as exc:
                logger.error(f"[engine] Failed to load {path}: {exc}")

    # ── Public helpers ────────────────────────────────────────────────────────

    @property
    def collections(self):
        return set(self._registry.keys())

    def get_primary_key(self, collection: str) -> Optional[str]:
        mapping = self._registry.get(collection)
        if not mapping:
            return None
        return mapping['postgresql']['parent_table']['primary_key']

    # ── PostgreSQL routes ─────────────────────────────────────────────────────

    def get_pg_routes(
        self,
        collection: str,
        document: Optional[dict],
        op: str,
        doc_id: str,
    ) -> List[Tuple[str, list, Optional[list]]]:
        """
        Returns list of (table_name, rows, upsert_key_cols) for Postgres writes.

        For op='d': [(parent_table, [{'_delete': True, pk: doc_id}], None)]
        For op in ('c','u','r'): parent row + all child rows expanded from arrays
        """
        mapping = self._registry.get(collection)
        if not mapping:
            return []

        pg = mapping['postgresql']
        parent_def = pg['parent_table']
        pk_col = parent_def['primary_key']
        results = []

        if op == 'd':
            results.append((
                parent_def['name'],
                [{'_delete': True, pk_col: doc_id}],
                None,
            ))
            return results

        if op not in ('c', 'u', 'r') or not document:
            return []

        # ── Parent row ───────────────────────────────────────────────────────
        parent_row = {}
        for col_def in parent_def['columns']:
            raw = document.get(col_def['source_field'])
            parent_row[col_def['column']] = _coerce(raw, col_def.get('type', 'TEXT'))
        results.append((parent_def['name'], [parent_row], [pk_col]))

        # ── Child / line-item rows ───────────────────────────────────────────
        source_key_val = document.get(mapping['source_key_field']) or doc_id

        for child_def in pg.get('child_tables', []):
            fk_col = child_def['foreign_key']['column']
            arr = document.get(child_def['source_array_field']) or []
            child_rows = []
            for item in arr:
                if not isinstance(item, dict):
                    continue
                row = {fk_col: source_key_val}
                for col_def in child_def['columns']:
                    raw = item.get(col_def['source_field'])
                    row[col_def['column']] = _coerce(raw, col_def.get('type', 'TEXT'))
                child_rows.append(row)
            if child_rows:
                results.append((child_def['name'], child_rows, child_def['upsert_key']))

        return results

    # ── Neo4j operations ──────────────────────────────────────────────────────

    def get_neo4j_ops(
        self,
        collection: str,
        document: Optional[dict],
        op: str,
        doc_id: str,
    ) -> List[Tuple[str, dict]]:
        """
        Returns list of (cypher, params) tuples for Neo4j writes.

        For op='d': single DETACH DELETE.
        For op in ('c','u','r'): node merge + all relationship merges.
        """
        mapping = self._registry.get(collection)
        if not mapping or 'neo4j' not in mapping:
            return []

        neo = mapping['neo4j']
        node_def = neo['node']
        ops = []

        if op == 'd':
            kp = node_def['key_property']
            cypher = (
                f"MATCH (n:{node_def['label']} {{{kp}: ${kp}}}) DETACH DELETE n"
            )
            return [(cypher, {kp: doc_id})]

        if op not in ('c', 'u', 'r') or not document:
            return []

        source_key_val = document.get(mapping['source_key_field']) or doc_id

        # ── Node merge ───────────────────────────────────────────────────────
        params = {}
        for prop in node_def['properties']:
            params[prop['property']] = document.get(prop['source_field'])
        ops.append((node_def['merge_cypher'].strip(), params))

        # ── Relationships ────────────────────────────────────────────────────
        for rel in neo.get('relationships', []):
            # Optional condition (e.g. "rfq_number IS NOT NULL")
            condition = rel.get('condition', '')
            if condition:
                cond_field = condition.split()[0]
                if document.get(cond_field) is None:
                    continue

            cypher = rel['merge_cypher'].strip()

            if 'source_array_field' in rel:
                # One Cypher execution per array element
                arr = document.get(rel['source_array_field']) or []
                for item in arr:
                    if not isinstance(item, dict):
                        continue
                    p = {mapping['source_key_field']: source_key_val}
                    p.update(document)   # parent fields (for SET clauses)
                    p.update(item)       # item fields (override with item values)
                    ops.append((cypher, p))
            else:
                # Single relationship from a scalar field on the document
                p = dict(document)
                p[mapping['source_key_field']] = source_key_val
                ops.append((cypher, p))

        return ops
