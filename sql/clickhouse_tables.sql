-- Run this file manually against the `laevitas` database.
-- It creates only Hydromancer project tables and contains no credentials.

CREATE TABLE IF NOT EXISTS laevitas.hydromancer_snapshot_perp
(
    snapshot_date Date,
    snapshot_block UInt64,
    snapshot_timestamp DateTime64(3, 'UTC'),
    updated_at DateTime64(6, 'UTC'),
    user String,
    market LowCardinality(String),
    size Float64,
    notional Float64,
    entry_price Float64,
    liquidation_price Nullable(Float64),
    leverage_type LowCardinality(String),
    leverage UInt32,
    funding_pnl Float64,
    account_value Float64,
    account_mode Nullable(String)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY snapshot_date
ORDER BY (snapshot_date, market, user);

CREATE TABLE IF NOT EXISTS laevitas.hydromancer_snapshot_spot
(
    snapshot_date Date,
    snapshot_block UInt64,
    snapshot_timestamp DateTime64(3, 'UTC'),
    updated_at DateTime64(6, 'UTC'),
    user String,
    token LowCardinality(String),
    balance Float64,
    entry_value Float64
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY snapshot_date
ORDER BY (snapshot_date, token, user);

CREATE TABLE IF NOT EXISTS laevitas.hydromancer_snapshot_account_values
(
    snapshot_date Date,
    snapshot_block UInt64,
    snapshot_timestamp DateTime64(3, 'UTC'),
    updated_at DateTime64(6, 'UTC'),
    user String,
    dex LowCardinality(String),
    collateral_token LowCardinality(String),
    account_value Float64,
    total_long_notional Float64,
    total_short_notional Float64,
    account_mode Nullable(String),
    is_unified UInt8,
    pm_ratio Nullable(Float64),
    pm_avail_by_branch_json Nullable(String),
    pm_total_account_value Nullable(Float64)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY snapshot_date
ORDER BY (snapshot_date, dex, user);

CREATE TABLE IF NOT EXISTS laevitas.hydromancer_sync_tasks
(
    dataset LowCardinality(String),
    snapshot_date Date,
    source_key String,
    source_etag String,
    source_size UInt64,
    total_rows UInt64,
    next_batch UInt64,
    batch_rows UInt64,
    rows_written UInt64,
    status LowCardinality(String),
    last_error String,
    version UInt64,
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (dataset, snapshot_date);

