# Object Files & Mapping Rules — RFQ, PO, ASN, GRN, Invoice

This bundle contains, for each of the five procure-to-pay document types used in
the proof-of-concept, (1) a sample MongoDB object (the source document as it
would exist in the source collection) and (2) a declarative mapping rule file
that specifies exactly how that document is transformed into PostgreSQL rows
and Neo4j nodes/relationships.

The five sample documents are referentially linked to each other, matching the
real procure-to-pay flow:

```
RFQ-2026-00841  →  PO-2026-05512  →  ASN-2026-01223  →  GRN-2026-00976  →  INV-2026-03390
   (RFQ)              (PO)              (ASN)              (GRN)             (Invoice)
```

## Folder structure

```
sample_documents/
  rfq.json       Sample RFQ object (MongoDB "rfqs" collection)
  po.json        Sample PO object (MongoDB "purchase_orders" collection), references rfq_number
  asn.json       Sample ASN object (MongoDB "asns" collection), references po_number
  grn.json       Sample GRN object (MongoDB "grns" collection), references po_number + asn_number
  invoice.json   Sample Invoice object (MongoDB "invoices" collection), references po_number + grn_number

mapping_rules/
  rfq_mapping.yaml
  po_mapping.yaml
  asn_mapping.yaml
  grn_mapping.yaml
  invoice_mapping.yaml
```

## How to read a mapping file

Each `*_mapping.yaml` has two top-level sections:

- **`postgresql`** — a `parent_table` (one row per document) plus one or more
  `child_tables` (one row per array element, e.g. `line_items`), each with an
  explicit `source_field → column` list, data types, foreign keys, and an
  `upsert_key` so replayed CDC events are applied idempotently
  (`INSERT ... ON CONFLICT ... DO UPDATE`).
- **`neo4j`** — a `node` definition (label, key property, properties) plus a
  list of `relationships`, each naming the relationship type, direction,
  target node label/key, any relationship properties, and the exact Cypher
  `MERGE` statement used to apply it idempotently.

These files are meant to be consumed directly by the CDC mapping engine
(Python) — they are the "declarative, reusable mapping rules" referenced in
the SRS, rather than hand-written per-event transformation code.

## Cross-document graph shape (Neo4j)

```
(RFQ)-[:INVITED]->(Vendor)
(RFQ)-[:REQUESTS]->(Material)
(PurchaseOrder)-[:ISSUED_AGAINST]->(RFQ)
(PurchaseOrder)-[:ISSUED_TO]->(Vendor)
(PurchaseOrder)-[:ORDERS]->(Material)
(ASN)-[:FULFILLS]->(PurchaseOrder)
(ASN)-[:SHIPS]->(Material)
(GRN)-[:RECEIVES]->(ASN)
(GRN)-[:CONFIRMS]->(PurchaseOrder)
(GRN)-[:RECEIVED]->(Material)
(Invoice)-[:BILLS]->(PurchaseOrder)
(Invoice)-[:REFERENCES]->(GRN)
(Invoice)-[:INVOICES]->(Material)
```

This lets a single Cypher query trace the full lifecycle of a material or a
vendor across all five document types — the relationship-heavy query pattern
that motivates the Neo4j target in the first place.

## PostgreSQL table shape (relational)

Each document type produces one parent table plus one child (line-item)
table, linked by the document's primary key:

```
rfq  ──< rfq_line_items
rfq  ──< rfq_invited_vendors
purchase_orders ──< po_line_items
asns ──< asn_line_items
grns ──< grn_line_items
invoices ──< invoice_line_items
```

Cross-document references (`rfq_number` on `purchase_orders`, `po_number` /
`asn_number` on `grns`, etc.) are modeled as foreign keys, enabling standard
SQL joins for reconciliation and reporting.
