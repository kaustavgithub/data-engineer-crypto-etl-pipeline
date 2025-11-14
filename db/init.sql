-- ---------------------------------------------------------
-- File: init.sql
-- Purpose: Create schema objects for the Crypto ETL project.
-- This script initializes the database used by the ETL pipeline.
-- ---------------------------------------------------------

CREATE TABLE IF NOT EXISTS crypto_prices (
    coin_id TEXT NOT NULL,
    symbol TEXT,
    name TEXT,
    current_price NUMERIC(20, 6),
    market_cap NUMERIC(30, 2),
    total_volume NUMERIC(30, 2),
    high_24h NUMERIC(20, 6),
    low_24h NUMERIC(20, 6),
    pct_change_24h NUMERIC(10, 6),
    last_updated TIMESTAMPTZ,
    load_timestamp TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (coin_id, load_timestamp)
);

