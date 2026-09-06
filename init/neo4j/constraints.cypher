// ============================================================
// Neo4j uniqueness constraints and indexes for the P2P graph.
// Run this once after starting Neo4j (Desktop Community Ed.).
//
// Execute via Neo4j Browser or:
//   cypher-shell -u neo4j -p <password> < init/neo4j/constraints.cypher
// ============================================================

// --- Uniqueness constraints (also create backing indexes) ---

CREATE CONSTRAINT rfq_unique IF NOT EXISTS
  FOR (r:RFQ) REQUIRE r.rfq_number IS UNIQUE;

CREATE CONSTRAINT po_unique IF NOT EXISTS
  FOR (p:PurchaseOrder) REQUIRE p.po_number IS UNIQUE;

CREATE CONSTRAINT asn_unique IF NOT EXISTS
  FOR (a:ASN) REQUIRE a.asn_number IS UNIQUE;

CREATE CONSTRAINT grn_unique IF NOT EXISTS
  FOR (g:GRN) REQUIRE g.grn_number IS UNIQUE;

CREATE CONSTRAINT invoice_unique IF NOT EXISTS
  FOR (i:Invoice) REQUIRE i.invoice_number IS UNIQUE;

CREATE CONSTRAINT vendor_unique IF NOT EXISTS
  FOR (v:Vendor) REQUIRE v.vendor_id IS UNIQUE;

CREATE CONSTRAINT material_unique IF NOT EXISTS
  FOR (m:Material) REQUIRE m.material_code IS UNIQUE;
