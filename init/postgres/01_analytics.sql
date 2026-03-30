-- Postgres Analytics DDL
-- Wrap CREATE TABLE in IF NOT EXISTS to ensure restart idempotency
CREATE TABLE IF NOT EXISTS orders_flat (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    status TEXT,
    city TEXT,
    total NUMERIC,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

