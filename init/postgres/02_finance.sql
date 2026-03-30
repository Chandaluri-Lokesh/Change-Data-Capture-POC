-- Postgres Finance DDL
-- Wrap CREATE TABLE in IF NOT EXISTS to ensure restart idempotency
CREATE TABLE IF NOT EXISTS transactions (
    txn_id TEXT PRIMARY KEY,
    order_id TEXT UNIQUE,
    amount NUMERIC,
    status TEXT,
    recorded_at TIMESTAMPTZ
);
