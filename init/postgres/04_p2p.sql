-- ============================================================
-- P2P Procure-to-Pay DDL
-- FK columns are stored for joins but not enforced as DB
-- constraints, because CDC events arrive out-of-order across
-- Kafka topics and referential integrity is guaranteed by the
-- source MongoDB documents.
-- ============================================================

-- ------------------------------------------------------------
-- RFQ
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rfq (
    rfq_number          VARCHAR(20)  PRIMARY KEY,
    requested_date      DATE,
    requested_by        VARCHAR(100),
    plant               VARCHAR(30),
    status              VARCHAR(20),
    response_due_date   DATE,
    source_updated_at   TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS rfq_line_items (
    rfq_number          VARCHAR(20)  NOT NULL,
    line_no             INTEGER      NOT NULL,
    material_code       VARCHAR(20),
    description         VARCHAR(200),
    quantity            NUMERIC(12,2),
    uom                 VARCHAR(10),
    target_delivery_date DATE,
    PRIMARY KEY (rfq_number, line_no)
);

CREATE TABLE IF NOT EXISTS rfq_invited_vendors (
    rfq_number          VARCHAR(20)  NOT NULL,
    vendor_id           VARCHAR(20)  NOT NULL,
    vendor_name         VARCHAR(100),
    invited_on          DATE,
    PRIMARY KEY (rfq_number, vendor_id)
);

-- ------------------------------------------------------------
-- Purchase Order
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS purchase_orders (
    po_number           VARCHAR(20)  PRIMARY KEY,
    rfq_number          VARCHAR(20),
    vendor_id           VARCHAR(20),
    vendor_name         VARCHAR(100),
    order_date          DATE,
    delivery_date       DATE,
    plant               VARCHAR(30),
    currency            CHAR(3),
    status              VARCHAR(20),
    payment_terms       VARCHAR(20),
    order_total         NUMERIC(14,2),
    source_updated_at   TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS po_line_items (
    po_number           VARCHAR(20)  NOT NULL,
    line_no             INTEGER      NOT NULL,
    material_code       VARCHAR(20),
    description         VARCHAR(200),
    quantity            NUMERIC(12,2),
    uom                 VARCHAR(10),
    unit_price          NUMERIC(12,2),
    line_total          NUMERIC(14,2),
    PRIMARY KEY (po_number, line_no)
);

-- ------------------------------------------------------------
-- ASN (Advance Shipment Notice)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS asns (
    asn_number          VARCHAR(20)  PRIMARY KEY,
    po_number           VARCHAR(20),
    vendor_id           VARCHAR(20),
    ship_date           DATE,
    carrier             VARCHAR(60),
    tracking_number     VARCHAR(40),
    expected_arrival    DATE,
    status              VARCHAR(20),
    source_updated_at   TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS asn_line_items (
    asn_number          VARCHAR(20)  NOT NULL,
    line_no             INTEGER      NOT NULL,
    material_code       VARCHAR(20),
    quantity_shipped    NUMERIC(12,2),
    uom                 VARCHAR(10),
    PRIMARY KEY (asn_number, line_no)
);

-- ------------------------------------------------------------
-- GRN (Goods Receipt Note)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS grns (
    grn_number          VARCHAR(20)  PRIMARY KEY,
    po_number           VARCHAR(20),
    asn_number          VARCHAR(20),
    receipt_date        DATE,
    received_by         VARCHAR(100),
    plant               VARCHAR(30),
    status              VARCHAR(20),
    source_updated_at   TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS grn_line_items (
    grn_number          VARCHAR(20)  NOT NULL,
    line_no             INTEGER      NOT NULL,
    material_code       VARCHAR(20),
    quantity_received   NUMERIC(12,2),
    uom                 VARCHAR(10),
    condition           VARCHAR(30),
    remarks             VARCHAR(200),
    PRIMARY KEY (grn_number, line_no)
);

-- ------------------------------------------------------------
-- Invoice
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS invoices (
    invoice_number      VARCHAR(20)  PRIMARY KEY,
    po_number           VARCHAR(20),
    grn_number          VARCHAR(20),
    vendor_id           VARCHAR(20),
    invoice_date        DATE,
    due_date            DATE,
    currency            CHAR(3),
    status              VARCHAR(20),
    subtotal            NUMERIC(14,2),
    tax_rate            NUMERIC(5,4),
    tax_amount          NUMERIC(14,2),
    total_amount        NUMERIC(14,2),
    source_updated_at   TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS invoice_line_items (
    invoice_number      VARCHAR(20)  NOT NULL,
    line_no             INTEGER      NOT NULL,
    material_code       VARCHAR(20),
    quantity            NUMERIC(12,2),
    unit_price          NUMERIC(12,2),
    amount              NUMERIC(14,2),
    PRIMARY KEY (invoice_number, line_no)
);

-- ------------------------------------------------------------
-- Indexes for common join / lookup patterns
-- ------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_po_rfq        ON purchase_orders(rfq_number);
CREATE INDEX IF NOT EXISTS idx_asn_po        ON asns(po_number);
CREATE INDEX IF NOT EXISTS idx_grn_po        ON grns(po_number);
CREATE INDEX IF NOT EXISTS idx_grn_asn       ON grns(asn_number);
CREATE INDEX IF NOT EXISTS idx_inv_po        ON invoices(po_number);
CREATE INDEX IF NOT EXISTS idx_inv_grn       ON invoices(grn_number);
CREATE INDEX IF NOT EXISTS idx_po_vendor     ON purchase_orders(vendor_id);
CREATE INDEX IF NOT EXISTS idx_asn_vendor    ON asns(vendor_id);
CREATE INDEX IF NOT EXISTS idx_inv_vendor    ON invoices(vendor_id);
CREATE INDEX IF NOT EXISTS idx_poli_mat      ON po_line_items(material_code);
CREATE INDEX IF NOT EXISTS idx_asn_li_mat    ON asn_line_items(material_code);
CREATE INDEX IF NOT EXISTS idx_grn_li_mat    ON grn_line_items(material_code);
CREATE INDEX IF NOT EXISTS idx_inv_li_mat    ON invoice_line_items(material_code);
