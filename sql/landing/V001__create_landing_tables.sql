-- V001__create_landing_tables.sql
-- Landing schema (PostgreSQL) for raw ingestion from Yahoo Finance and similar sources.
-- Deduplication strategy:
--   1) Hard dedup via PRIMARY KEY / UNIQUE constraints by natural business grain.
--   2) Soft dedup via ingested_at, keeping newest records in downstream curated layer.
--   3) Upserts should use ON CONFLICT ... DO UPDATE setting latest payload + audit columns.

CREATE SCHEMA IF NOT EXISTS landing;

-- 1) bank_basic_info
CREATE TABLE IF NOT EXISTS landing.bank_basic_info (
    symbol              VARCHAR(20) NOT NULL,
    company_name        TEXT,
    industry            TEXT,
    sector              TEXT,
    employee_count      BIGINT,
    city                TEXT,
    phone               TEXT,
    state               TEXT,
    country             TEXT,
    website             TEXT,
    address             TEXT,
    market_cap          NUMERIC(20,2),
    currency            VARCHAR(10),
    exchange            VARCHAR(20),
    snapshot_date       DATE NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_system       VARCHAR(100) NOT NULL,
    batch_id            VARCHAR(100) NOT NULL,
    CONSTRAINT pk_bank_basic_info PRIMARY KEY (symbol, snapshot_date),
    CONSTRAINT chk_bank_basic_info_employee_count CHECK (employee_count IS NULL OR employee_count >= 0),
    CONSTRAINT chk_bank_basic_info_market_cap CHECK (market_cap IS NULL OR market_cap >= 0)
);

CREATE INDEX IF NOT EXISTS idx_bank_basic_info_snapshot_date
    ON landing.bank_basic_info (snapshot_date);
CREATE INDEX IF NOT EXISTS idx_bank_basic_info_ingested_at
    ON landing.bank_basic_info (ingested_at);
CREATE INDEX IF NOT EXISTS idx_bank_basic_info_source_batch
    ON landing.bank_basic_info (source_system, batch_id);

-- 2) stock_daily_price
CREATE TABLE IF NOT EXISTS landing.stock_daily_price (
    symbol              VARCHAR(20) NOT NULL,
    price_date          DATE NOT NULL,
    open_price          NUMERIC(18,6) NOT NULL,
    high_price          NUMERIC(18,6) NOT NULL,
    low_price           NUMERIC(18,6) NOT NULL,
    close_price         NUMERIC(18,6) NOT NULL,
    adjusted_close      NUMERIC(18,6),
    volume              BIGINT NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_system       VARCHAR(100) NOT NULL,
    batch_id            VARCHAR(100) NOT NULL,
    CONSTRAINT pk_stock_daily_price PRIMARY KEY (symbol, price_date),
    CONSTRAINT chk_stock_daily_price_prices_non_negative CHECK (
        open_price >= 0 AND high_price >= 0 AND low_price >= 0 AND close_price >= 0
        AND (adjusted_close IS NULL OR adjusted_close >= 0)
    ),
    CONSTRAINT chk_stock_daily_price_ohlc_consistency CHECK (
        high_price >= low_price
        AND high_price >= GREATEST(open_price, close_price)
        AND low_price <= LEAST(open_price, close_price)
    ),
    CONSTRAINT chk_stock_daily_price_volume CHECK (volume >= 0)
);

CREATE INDEX IF NOT EXISTS idx_stock_daily_price_date_symbol
    ON landing.stock_daily_price (price_date, symbol);
CREATE INDEX IF NOT EXISTS idx_stock_daily_price_ingested_at
    ON landing.stock_daily_price (ingested_at);
CREATE INDEX IF NOT EXISTS idx_stock_daily_price_source_batch
    ON landing.stock_daily_price (source_system, batch_id);

-- 3) bank_fundamentals
CREATE TABLE IF NOT EXISTS landing.bank_fundamentals (
    symbol              VARCHAR(20) NOT NULL,
    statement_date      DATE NOT NULL,
    period_type         VARCHAR(20) NOT NULL, -- annual / quarterly / ttm
    total_assets        NUMERIC(22,2),
    total_debt          NUMERIC(22,2),
    invested_capital    NUMERIC(22,2),
    shares_issued       NUMERIC(22,2),
    currency            VARCHAR(10),
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_system       VARCHAR(100) NOT NULL,
    batch_id            VARCHAR(100) NOT NULL,
    CONSTRAINT pk_bank_fundamentals PRIMARY KEY (symbol, statement_date, period_type),
    CONSTRAINT chk_bank_fundamentals_positive_values CHECK (
        (total_assets IS NULL OR total_assets >= 0)
        AND (total_debt IS NULL OR total_debt >= 0)
        AND (invested_capital IS NULL OR invested_capital >= 0)
        AND (shares_issued IS NULL OR shares_issued >= 0)
    )
);

CREATE INDEX IF NOT EXISTS idx_bank_fundamentals_statement_date
    ON landing.bank_fundamentals (statement_date, symbol);
CREATE INDEX IF NOT EXISTS idx_bank_fundamentals_ingested_at
    ON landing.bank_fundamentals (ingested_at);
CREATE INDEX IF NOT EXISTS idx_bank_fundamentals_source_batch
    ON landing.bank_fundamentals (source_system, batch_id);

-- 4) holders
CREATE TABLE IF NOT EXISTS landing.holders (
    symbol              VARCHAR(20) NOT NULL,
    holder_type         VARCHAR(30) NOT NULL, -- institution / insider / mutual_fund / major
    holder_name         TEXT NOT NULL,
    holdings_date       DATE NOT NULL,
    shares              NUMERIC(22,2),
    market_value        NUMERIC(22,2),
    pct_outstanding     NUMERIC(9,6),
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_system       VARCHAR(100) NOT NULL,
    batch_id            VARCHAR(100) NOT NULL,
    CONSTRAINT pk_holders PRIMARY KEY (symbol, holder_type, holder_name, holdings_date),
    CONSTRAINT chk_holders_non_negative CHECK (
        (shares IS NULL OR shares >= 0)
        AND (market_value IS NULL OR market_value >= 0)
        AND (pct_outstanding IS NULL OR (pct_outstanding >= 0 AND pct_outstanding <= 1))
    )
);

CREATE INDEX IF NOT EXISTS idx_holders_date_symbol
    ON landing.holders (holdings_date, symbol);
CREATE INDEX IF NOT EXISTS idx_holders_holder_name
    ON landing.holders (holder_name);
CREATE INDEX IF NOT EXISTS idx_holders_ingested_at
    ON landing.holders (ingested_at);
CREATE INDEX IF NOT EXISTS idx_holders_source_batch
    ON landing.holders (source_system, batch_id);

-- 5) ratings
CREATE TABLE IF NOT EXISTS landing.ratings (
    symbol              VARCHAR(20) NOT NULL,
    rating_date         DATE NOT NULL,
    firm_name           TEXT NOT NULL,
    to_grade            VARCHAR(50),
    from_grade          VARCHAR(50),
    rating_action       VARCHAR(30),
    recommendation_score NUMERIC(8,4),
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_system       VARCHAR(100) NOT NULL,
    batch_id            VARCHAR(100) NOT NULL,
    CONSTRAINT pk_ratings PRIMARY KEY (symbol, rating_date, firm_name),
    CONSTRAINT chk_ratings_score CHECK (
        recommendation_score IS NULL OR recommendation_score >= 0
    )
);

CREATE INDEX IF NOT EXISTS idx_ratings_date_symbol
    ON landing.ratings (rating_date, symbol);
CREATE INDEX IF NOT EXISTS idx_ratings_firm
    ON landing.ratings (firm_name);
CREATE INDEX IF NOT EXISTS idx_ratings_ingested_at
    ON landing.ratings (ingested_at);
CREATE INDEX IF NOT EXISTS idx_ratings_source_batch
    ON landing.ratings (source_system, batch_id);

-- Operational note for ingestion jobs:
-- Example pattern for dedup/upsert:
-- INSERT INTO landing.stock_daily_price (...)
-- VALUES (...)
-- ON CONFLICT (symbol, price_date)
-- DO UPDATE SET
--   open_price = EXCLUDED.open_price,
--   high_price = EXCLUDED.high_price,
--   low_price = EXCLUDED.low_price,
--   close_price = EXCLUDED.close_price,
--   adjusted_close = EXCLUDED.adjusted_close,
--   volume = EXCLUDED.volume,
--   ingested_at = EXCLUDED.ingested_at,
--   source_system = EXCLUDED.source_system,
--   batch_id = EXCLUDED.batch_id;