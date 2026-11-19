-- Run this file manually against the `laevitas` database.
-- It creates the generic S3 ingestion task table and the node fills fact table.
-- It contains no credentials and performs no mutation of existing tables.
-- This is single-node DDL; adapt engines to the production replication topology if needed.

CREATE TABLE IF NOT EXISTS laevitas.hyperliquid_s3_ingestion_tasks
(
    dataset LowCardinality(String)
        COMMENT '数据集或导入管道标识，例如 node_fills_by_block、node_trades',
    source_bucket LowCardinality(String)
        COMMENT '源 S3 bucket 名称',
    source_key String
        COMMENT '源 S3 object key，与 dataset 和 source_bucket 共同构成任务逻辑键',
    source_etag String
        COMMENT '发现对象时记录的 S3 ETag，仅用于源身份校验，不假设为内容 MD5',
    source_size UInt64
        COMMENT '发现对象时记录的压缩对象字节数',
    source_last_modified DateTime64(3, 'UTC')
        COMMENT 'S3 返回的对象最后修改时间',
    source_date Date
        COMMENT '从 S3 key 解析的 UTC 数据日期，用于手动日期范围同步和状态查询',
    status LowCardinality(String)
        COMMENT '任务状态：discovered、downloading、downloaded、ingesting、completed 或 failed',
    checkpoint_kind LowCardinality(String)
        COMMENT 'checkpoint 单位，例如 jsonl_line 或 parquet_batch',
    next_checkpoint UInt64
        COMMENT '下一条待处理源记录或 batch 的零基位置',
    source_records_read UInt64
        COMMENT '已经成功解析的源记录数；node_fills_by_block 中表示 JSONL block 行数',
    output_rows_written UInt64
        COMMENT '已经逻辑写入目标业务表的行数；node_fills_by_block 中表示 fill 行数',
    last_error String
        COMMENT '最近一次失败的截断错误摘要，成功状态使用空字符串',
    version UInt64
        COMMENT '任务状态版本；同一逻辑任务保留数值最大的版本',
    updated_at DateTime64(6, 'UTC') DEFAULT now64(6)
        COMMENT '该任务状态版本的写入时间'
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY (dataset, toYYYYMM(source_date))
ORDER BY (dataset, source_bucket, source_key);

CREATE TABLE IF NOT EXISTS laevitas.hyperliquid_s3_node_fills_by_block_fills
(
    user String
        COMMENT '用户地址，写入时统一转为小写',
    event_time DateTime64(3, 'UTC')
        COMMENT '成交时间，UTC 毫秒精度',
    event_time_ms UInt64
        COMMENT '成交时间的原始 Unix 毫秒值',
    block_number UInt64
        COMMENT '成交所在的 Hyperliquid block number',
    block_time DateTime64(9, 'UTC')
        COMMENT 'block 时间，UTC 纳秒精度',
    event_index UInt32
        COMMENT 'event 在 block events 数组中的零基下标',
    coin LowCardinality(String)
        COMMENT '资产名称或现货交易对',
    px Nullable(Decimal(38, 18))
        COMMENT '成交价格',
    sz Nullable(Decimal(38, 18))
        COMMENT '成交数量',
    side LowCardinality(String)
        COMMENT '成交方向：B=买，A=卖',
    start_position Nullable(Decimal(38, 18))
        COMMENT '成交前仓位',
    direction LowCardinality(String)
        COMMENT '成交类型，例如 Open Long、Close Short、Buy 或 Sell',
    closed_pnl Nullable(Decimal(38, 18))
        COMMENT '本次成交的已实现盈亏',
    tx_hash String
        COMMENT 'Hyperliquid L1 交易 hash；特殊成交可能为全零 hash',
    oid Nullable(UInt64)
        COMMENT '订单 ID',
    crossed Nullable(UInt8)
        COMMENT '是否主动跨价成交：1=是，0=否，NULL=未知',
    fee Nullable(Decimal(38, 18))
        COMMENT '成交手续费或返佣',
    tid Nullable(UInt64)
        COMMENT '成交 ID',
    cloid Nullable(String)
        COMMENT '客户端订单 ID',
    fee_token Nullable(String)
        COMMENT '手续费计价 token',
    builder_fee Nullable(Decimal(38, 18))
        COMMENT 'builder 收取的费用',
    builder Nullable(String)
        COMMENT 'builder 地址',
    twap_id Nullable(UInt64)
        COMMENT '关联的 TWAP ID',
    deployer_fee Nullable(Decimal(38, 18))
        COMMENT 'HIP-3 deployer 费用',
    priority_gas Nullable(Decimal(38, 18))
        COMMENT '优先 gas 费用',
    source_bucket LowCardinality(String)
        COMMENT '源 S3 bucket 名称',
    source_key String
        COMMENT '源 S3 object key，用于回溯原始对象',
    source_line UInt64
        COMMENT '所在 JSONL block 行的零基行号',
    raw_fill_json String
        COMMENT '完整 fill JSON，用于审计和保留未结构化字段',
    ingested_at DateTime64(6, 'UTC') DEFAULT now64(6)
        COMMENT '该 batch 写入 ClickHouse 的 UTC 时间，同时作为去重版本'
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (user, event_time, block_number, event_index);
