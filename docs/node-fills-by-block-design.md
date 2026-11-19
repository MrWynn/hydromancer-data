# Node Fills by Block 存储技术方案

## 1. 目标与范围

本方案用于将 `s3://hl-mainnet-node-data/node_fills_by_block/` 中用户指定的 UTC 日期范围同步到 ClickHouse。首期目标如下：

- 从 Requester Pays S3 列举、下载 `node_fills_by_block` 对象；
- 流式解压 LZ4 frame 并解析 JSON Lines；
- 解析每个 block，将其中的 event 展开为可按用户查询的 fill 明细；
- 支持对象级断点恢复、幂等重试、数据校验和按日期分段回补；
- 复用 Hydromancer 已有的 Python、boto3、SQLite 本地目录和 ClickHouse Native 写入技术栈。

本方案暂不修改现有 snapshot CLI、`sync_snapshots.sh`、配置或业务表，也不包含 `node_fills`、`node_trades`、实时订阅、Web API 和调度平台实现。

## 2. 数据源分析

以下结论来自 `hyperlake/hyperlake-report.md` 中的实际 S3 抽样：

| 项目 | 观测结果 |
|---|---|
| Bucket | `hl-mainnet-node-data` |
| Region | `ap-northeast-1` |
| 付费方式 | Requester Pays |
| Prefix | `node_fills_by_block/` |
| 对象路径 | `node_fills_by_block/hourly/YYYYMMDD/H.lz4` |
| 压缩与记录格式 | LZ4 frame + JSON Lines |
| 单行语义 | 一个 block，包含 `local_time`、`block_time`、`block_number`、`events` |
| event 结构 | `[user_address, fill_object]` |
| 空 block | `events: []`，需要保留以校验 block 完整性 |
| 报告观测区间 | 2025-07-27 至 2026-11-16，不应视为固定边界 |
| 报告观测规模 | 11,448 个对象、322.95 GiB 压缩数据，不是未来容量上限 |

顶层样例：

```json
{
  "local_time": "2025-12-09T00:00:00.060682962",
  "block_time": "2025-12-08T23:59:59.976368411",
  "block_number": 822371339,
  "events": [
    [
      "0x0d58586a54a1b2070677f898acebb8317e24e1c2",
      {
        "coin": "BABY",
        "px": "0.019006",
        "sz": "945.0",
        "side": "B",
        "time": 1765238399976,
        "startPosition": "18549.0",
        "dir": "Open Long",
        "closedPnl": "0.0",
        "hash": "0x96f1d119187a4125986b043104640b02028000feb37d5ff73aba7c6bd77e1b10",
        "oid": 262956345766,
        "crossed": false,
        "fee": "0.0",
        "tid": 44148727150723,
        "cloid": "0x20251209000000000000000000021753",
        "feeToken": "USDC",
        "twapId": null
      }
    ]
  ]
}
```

已观测 fill 字段包括：

```text
coin, px, sz, side, time, startPosition, dir, closedPnl, hash,
oid, crossed, fee, tid, cloid, feeToken, builderFee, builder,
twapId, deployerFee, priorityGas
```

样例表明字段会随时间扩展，例如较新数据增加了 `twapId`、`deployerFee`、`priorityGas`，因此数据模型不能只保留固定强类型列。

## 3. 已确定的设计约束

- 用户通过 `--from` 和 `--to` 手动指定 UTC 日期范围，首尾日期均包含；
- 同步流程沿用 Hydromancer 的 inspect、dry-run、sync、status，不增加定时调度或后台滚动扫描；
- S3 client region 固定为 bucket 所在的 `ap-northeast-1`，每个 LIST/GET 都传递 `RequestPayer=requester`；
- S3 key 中的日期和小时用于对象发现，实际成交时间以记录中的 `block_time` 和 `time` 为准；
- 同一个 `block_number` 中，`events` 数组顺序稳定，`event_index` 作为 block 内 fill 身份；
- S3 是原始事实源，ClickHouse 用于分析查询，本地文件只是可清理缓存；
- 首期使用与当前 Hydromancer 一致的单机 ClickHouse MergeTree，不设计集群表；
- ClickHouse 数据不设置 TTL，默认永久保留；
- DDL 固定使用 `DateTime64(9, 'UTC')` 和 `Decimal(38, 18)`，数据类型已经确定；部署前只执行兼容性校验；
- 不保存空 block，不提供全局 block 连续性审计；需要时重新读取原始 S3 对象；
- 全量历史数据由用户拆分日期范围顺序执行，下载费用、缓存空间和执行窗口在每次 dry-run 后由用户控制。

## 4. 总体方案

推荐使用 Python 流式 ETL：

```text
Requester Pays S3
  -> 按日期 LIST hourly 对象
  -> 下载到本地 .part 文件并原子改名
  -> LZ4 流式解压
  -> JSON Lines 逐行解析
  -> fill batch
  -> ClickHouse Native INSERT
  -> 对象任务表标记 completed
```

选择该方案的原因：

- boto3 可以为每个 LIST/GET 明确传入 `RequestPayer=requester`；
- 流式解压和逐行解析不会将整个解压结果装入内存；
- 可以把嵌套 event 拆为可按用户查询的 fill 行，同时保留原始 fill JSON；
- 对象任务表可以表达下载、解析、写入、失败和完成状态；
- 与 Hydromancer 当前技术栈一致，无需引入新运行平台。

直接使用 ClickHouse S3 table function 的 SQL 更短，但 Requester Pays header、LZ4 frame、嵌套异构 JSON、断点恢复和坏行定位都更难稳定处理，因此只适合临时探查，不作为生产导入路径。

### 4.1 手动日期同步流程

操作方式与当前 Hydromancer 一致：

1. `hyperliquid-etl node-fills inspect --from DATE --to DATE`：列举指定日期范围内的 S3 对象并更新本地目录；
2. `hyperliquid-etl node-fills sync --from DATE --to DATE --dry-run`：展示待下载对象、压缩大小、已有缓存和恢复位置；
3. `hyperliquid-etl node-fills sync --from DATE --to DATE --yes`：确认 Requester Pays 费用后下载并写入 ClickHouse；
4. `hyperliquid-etl node-fills status --from DATE --to DATE`：只查询任务状态，不访问 S3。

命令中的日期均按 UTC 解释且包含首尾日期。同步按日期升序、同一天按小时升序执行；用户可以先同步单日验收，再逐步扩大日期范围。相同日期范围重复执行时，completed 且 source identity 未变化的对象直接跳过，failed 或未完成对象从 checkpoint 恢复。

### 4.2 与现有 Hydromancer 同步方案的对应关系

本方案保持现有同步架构和安全边界，只替换数据源适配与记录展开逻辑，不另起一套框架：

| 现有 Hydromancer 方案 | Node fills 方案 | 关系说明 |
|---|---|---|
| boto3 + `RequestPayer=requester` | 相同 | 仅 bucket、prefix 不同 |
| SQLite 缓存 S3 扫描和下载状态 | 相同 | 任务粒度由 dataset/date 调整为 S3 object |
| `.part` 下载、大小校验、原子改名 | 相同 | 文件类型由 Parquet 变为 LZ4 |
| PyArrow 分批读取 Parquet | LZ4 流式解压 + JSONL 逐行解析 | 数据格式要求的必要差异 |
| ClickHouse Native 列式 batch INSERT | 相同 | 新增 fill 字段映射 |
| `ReplacingMergeTree(version)` 任务表 | 相同原则 | 使用所有 S3 数据集共享的通用对象任务表 |
| `ReplacingMergeTree(updated_at)` 数据表 | 相同 | 成交表使用 `ingested_at` 作为去重版本和审计时间 |
| `next_batch` 恢复 Parquet batch | `checkpoint_kind + next_checkpoint` | 同一任务表支持 JSONL 行和 Parquet batch 等恢复单位 |
| completed 后在 S3 请求前跳过 | 相同 | 同时校验 key、ETag、Size source identity |
| DDL 手动执行、程序不建表 | 相同 | 保持现有数据库安全边界 |
| mock S3 / mock ClickHouse 测试 | 相同 | 真实数据库写入仍需单独批准 |

建议实现时复用或小幅扩展现有 `Catalog`、`S3Store`、`ClickHouseStore` 的公共能力，并为 node fills 增加独立 service/CLI 入口。不要把小时级流数据伪装成现有的日快照 dataset，否则会破坏当前“一天一个 Parquet、一个 snapshot identity”的假设。现有 `hydromancer_sync_tasks` 暂不修改；新的 `hyperliquid_s3_ingestion_tasks` 供 node fills、trades 等对象型 Hyperliquid S3 数据集共享，避免每增加一个数据集就新建一张任务表。

## 5. ClickHouse 数据模型

采用两张表：

| 表 | 用途 | 逻辑唯一键 |
|---|---|---|
| `hyperliquid_s3_ingestion_tasks` | 所有对象型 Hyperliquid S3 数据集共享的任务、断点、错误和审计表 | `(dataset, source_bucket, source_key)` |
| `hyperliquid_s3_node_fills_by_block_fills` | 展开后的用户成交明细 | `(user, event_time, block_number, event_index)` |

### 5.1 通用 S3 导入任务表

- `dataset` 区分 `node_fills_by_block`、`node_trades` 等导入管道；
- `ReplacingMergeTree(version)` 保存同一个 dataset/bucket/object 的最新任务状态；
- `checkpoint_kind` 描述 checkpoint 单位，例如 `jsonl_line` 或 `parquet_batch`；
- `next_checkpoint` 表示下一条待处理记录或 batch 的零基位置；
- `source_date` 从 S3 key 解析，用于 `--from/--to` 范围过滤和按月分区；
- `source_records_read` 记录已解析的源记录数，`output_rows_written` 记录展开后写入业务表的行数；
- `status` 约定为 `discovered`、`downloading`、`downloaded`、`ingesting`、`completed`、`failed`；
- 查询最新状态应使用 `argMax(..., version)`，不能假设后台 merge 已完成。

该表不包含 node fills 专属字段，因此未来接入 trades 时只需要增加解析逻辑和对应业务表，不需要再建 `node_trades_objects`。现有快照任务表保持不变，避免影响已经运行的 Hydromancer 日快照同步。

### 5.2 Fill 明细表

- event 在源数组中的零基下标写入 `event_index`；
- 用户地址在写入前统一转为小写，避免相同地址因大小写产生多个查询键；
- 地址、hash、cloid 使用 String，避免十六进制特殊值造成解析失败；
- 源十进制定点字符串使用 `Nullable(Decimal(38, 18))`，避免 `Float64` 精度损失；
- 可选字段使用 Nullable；
- `raw_fill_json` 始终保存完整 fill object，未知新增字段不会丢失；
- `event_time` 从源 `time`（Unix 毫秒）转换，`block_time` 保留纳秒精度。

表按 `event_time` 月份分区，并以 `(user, event_time, block_number, event_index)` 排序，使主要查询“某个用户在时间范围内的成交明细”能够利用主键顺序。

典型查询如下；调用方也应先将地址转为小写：

```sql
SELECT *
FROM laevitas.hyperliquid_s3_node_fills_by_block_fills FINAL
WHERE user = lower('0x用户地址')
  AND event_time >= '2026-01-01 00:00:00'
  AND event_time < '2026-02-01 00:00:00'
ORDER BY event_time, block_number, event_index;
```

### 5.3 为什么不保存 Block 表

目标是查询用户成交明细，不需要持久化没有成交的空 block。导入器仍逐行解析所有 block，空 block 计入任务表的 `source_records_read`，但不会产生业务行。

删除 block 表后的取舍：

- 优点：减少表数量、写入次数和 ClickHouse 存储，不重复保存整块 JSON；
- 保留能力：可验证 LZ4 是否完整、每个 JSONL 行是否可解析、每个 event 是否成功生成 fill；
- 放弃能力：缓存清理后不能仅依赖 ClickHouse 重建所有空 block，也不能做完整的 block 连续性审计；
- 原始 S3 对象仍是事实源，需要严格 block 审计时可重新读取源对象。

## 6. ClickHouse 建表语句

DDL 独立保存在 [`../sql/node_fills_by_block_tables.sql`](../sql/node_fills_by_block_tables.sql)，包含：

- 通用任务表 `laevitas.hyperliquid_s3_ingestion_tasks`；
- 成交明细表 `laevitas.hyperliquid_s3_node_fills_by_block_fills`；
- 每个字段的 ClickHouse `COMMENT`。

字段 COMMENT 只描述业务含义、单位、枚举或特殊值；字段与源 `fill` key 的映射由本文第 2 节统一说明，避免在每个 COMMENT 中重复“源 fill.xxx”。

DDL 由管理员手动执行，同步程序不自动建表。当前文件使用与现有 Hydromancer 一致的单机 MergeTree。`event_time` 对应源 `time`，`crossed` 使用兼容性更广的 `Nullable(UInt8)` 表示 false/true。部署前需要在目标 ClickHouse 执行只读版本检查和 DDL 语法检查，但不改变本文已经确定的数据类型。

## 7. 对象发现与下载

按 UTC 日期列举：

```text
Bucket=hl-mainnet-node-data
Prefix=node_fills_by_block/hourly/YYYYMMDD/
RequestPayer=requester
```

不要假设每天严格存在 24 个对象，以 S3 LIST 结果为准。每个对象记录 `Key`、`ETag`、`Size`、`LastModified`，并从 key 解析 `object_date`、`object_hour`。

每次只列举用户通过 `--from`、`--to` 指定的日期范围，并按日期、小时升序处理。默认复用 SQLite 中已有的扫描结果；与当前 Hydromancer 一样，只有显式使用 `--refresh` 才重新 LIST S3。若对象已 completed 且 source identity 未变化，则不再 GET。

建议缓存路径：

```text
data/cache/hl-mainnet-node-data/node_fills_by_block/hourly/YYYYMMDD/H.lz4
```

下载规则：

1. GET 必须传 `RequestPayer=requester`；
2. 先写 `H.lz4.part`；
3. 校验本地大小等于 S3 `ContentLength`；
4. `fsync` 后原子改名；
5. ETag 只作为 source identity，不假设它一定是内容 MD5；
6. 对象标记为 `completed` 后立即清理本地缓存和 SQLite download 记录，S3 仍是原始事实源；失败对象保留缓存用于恢复。本次发现的日期对象全部处理成功后，只用非递归 `rmdir` 尝试删除日期目录，目录非空时保留。

## 8. 解析和写入流程

每个 JSONL 行执行：

1. 解析并校验 `block_number`、`block_time`、`local_time`、`events`；
2. 将该 JSONL 行计入 `source_records_read`，空 `events` 不产生业务行；
3. 按数组顺序展开非空 `events`，生成 `event_index`；
4. 用户地址统一转为小写，已知字段写强类型列；
5. 完整 fill object 写入 `raw_fill_json`，未知字段不阻塞导入；
6. 已知字段类型不合法时令整个对象失败，不静默写默认值。

fill 使用 ClickHouse Native 列式批量 INSERT，不逐行写入。每批最大行数复用现有 `clickhouse.batch_rows` 配置，默认 100,000；一个 block 的 events 不会拆到两个 batch，从而保证 checkpoint 只落在完整 block 之后。

对象状态流转：

```text
discovered -> downloading -> downloaded -> ingesting -> completed
                                      \-> failed <-/
```

每个 batch 成功后向 `hyperliquid_s3_ingestion_tasks` 追加一个任务版本，更新 `next_checkpoint`、`source_records_read`、`output_rows_written`。该数据集的 `checkpoint_kind` 固定为 `jsonl_line`。异常时记录 `failed` 和截断后的错误摘要。

恢复规则：

- completed 且 source identity 未变化：直接跳过；
- 未完成：校验 `source_key + ETag + Size` 与任务首次记录一致；
- identity 变化：停止并报警，禁止拼接新旧对象内容；
- 从 `next_checkpoint` 指向的 JSONL 行继续解析；
- checkpoint 写入前已成功 INSERT 的重复 batch，由 ReplacingMergeTree 最终去重。

## 9. 幂等性与一致性

ClickHouse 普通 MergeTree INSERT 不提供成交表和任务表之间的跨表事务。本方案采用“可重试写入，最后标记完成”：

1. 写 fill batch；
2. 成功后写任务 checkpoint；
3. 全部源记录完成并校验通过后，将对象标记为 `completed`。
4. `completed` 成功写入后，删除该对象的本地 LZ4 cache 和 SQLite download 记录；失败对象保留 cache 用于断点续传。重跑已完成对象时也会清理历史残留 cache，不重新 GET S3。日期目录只在本次发现的对象全部处理成功后通过非递归 `rmdir` 清理，且不作为同步完成标记。

进程可能在 fill INSERT 成功、checkpoint 写入前中断，因此重试可能产生物理重复。成交表使用 `ReplacingMergeTree(ingested_at)`：

- 后台 merge 后，每个逻辑键保留最大 `ingested_at`；
- merge 前要求严格去重的查询使用 `FINAL`；
- 不使用 `OPTIMIZE TABLE ... FINAL` 作为日常同步步骤，避免高额 merge 开销；
- 如果未来要求查询立即无重复，可再评估 staging table + 分区替换，首期不增加该复杂度。

导入程序应为每次 batch 尝试显式生成一个 UTC 微秒时间，并让该 batch 的所有 fill 使用同一个 `ingested_at`。重试会生成更晚的时间，因此最终保留最新写入版本。DDL 中的 `DEFAULT now64(6)` 只作为兜底，不替代程序显式传值。任务表仍保留独立的 `version UInt64`，用于 `argMax(..., version)` 确定任务状态顺序。

## 10. Schema 演进策略

- 新的未知 fill 字段：导入不中断，保存在 `raw_fill_json`；
- 新字段产生稳定查询需求后：人工 `ALTER TABLE ADD COLUMN`，再按需从原始 JSON 回填；
- 已知字段类型发生变化：对象失败并报警，人工确认映射后恢复；
- 顶层 block 结构变化：对象失败，禁止静默丢失 block；
- schema 变更必须抽样早期、中期和最新对象，不能只验证最新数据。

## 11. 数据校验与监控

### 11.1 对象完成条件

对象同时满足以下条件后才能标记 completed：

- LZ4 流正常读到 EOF；
- 每个非空 JSONL 行解析成功；
- `source_records_read` 等于成功解析的 JSONL block 行数，空 block 也计数；
- 解析期间累计的 event 数等于 `output_rows_written`；
- 按 `source_bucket + source_key` 查询成交表的 `FINAL` 行数等于 `output_rows_written`；
- 最终 checkpoint 已成功写入任务表。

这里的 written 是逻辑写入数；物理行数在 merge 前可能包含重试版本。

### 11.2 对账查询

对比 completed 任务记录的逻辑输出行数与成交表的 `FINAL` 行数：

```sql
SELECT
    t.source_bucket,
    t.source_key,
    t.expected_fills,
    coalesce(f.actual_fills, 0) AS actual_fills
FROM
(
    SELECT
        source_bucket,
        source_key,
        argMax(output_rows_written, version) AS expected_fills,
        argMax(status, version) AS latest_status
    FROM laevitas.hyperliquid_s3_ingestion_tasks
    WHERE dataset = 'node_fills_by_block'
    GROUP BY source_bucket, source_key
    HAVING latest_status = 'completed'
) AS t
LEFT JOIN
(
    SELECT source_bucket, source_key, count() AS actual_fills
    FROM laevitas.hyperliquid_s3_node_fills_by_block_fills FINAL
    GROUP BY source_bucket, source_key
) AS f USING (source_bucket, source_key)
WHERE t.expected_fills != actual_fills
ORDER BY t.source_bucket, t.source_key;
```

该查询不验证空 block 或全局 block 连续性。严格 block 审计不属于本方案的持久化目标，需要重新读取原始 S3 对象执行。

建议监控：

- 本次指定日期范围内 completed、failed、未完成对象数量；
- failed 对象数和最新错误；
- S3 LIST/GET 次数、下载字节与 Requester Pays 费用；
- 单对象耗时、解析行数、fill/s；
- completed 任务与成交表的对象级行数差异；
- 本地缓存水位、ClickHouse 分区大小和 part 数量。

## 12. 权限与安全边界

同步身份只需要以下 S3 权限：

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::hl-mainnet-node-data",
      "Condition": {
        "StringLike": {
          "s3:prefix": ["node_fills_by_block/*"]
        }
      }
    },
    {
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::hl-mainnet-node-data/node_fills_by_block/*"
    }
  ]
}
```

ClickHouse 同步用户只授予 `hyperliquid_s3_ingestion_tasks` 和 `hyperliquid_s3_node_fills_by_block_fills` 的 `SELECT`、`INSERT`。DDL 由管理员手动执行；同步程序不持有 `CREATE`、`ALTER`、`DROP`、`TRUNCATE` 或分区删除权限。AWS 凭证和 ClickHouse 密码不得进入代码、SQL、日志或任务表。

## 13. 实施与验收计划

### 阶段 1：只读探查

1. 对早期、中期、最新日期各抽取至少一个完整对象；
2. 统计顶层字段、fill 字段、类型、最大字符串长度和 Decimal 精度；
3. 验证 LZ4 frame 完整解压；
4. 确认生产 ClickHouse 版本和 DDL 类型兼容。

验收标准：样本所有 JSONL 行可解析，未知字段清单已输出，不执行数据库写入。

### 阶段 2：最小导入器

1. 增加独立 `node-fills` CLI，不改变现有 snapshot 命令语义；
2. 实现对象发现、缓存、流式解析、通用任务表与成交表写入和恢复；
3. 使用 mock S3、临时文件和 mock ClickHouse 测试，不连接真实数据库。

验收标准：同一对象在指定失败点重跑后，成交表 `FINAL` 的逻辑行数和内容不变，任务表 checkpoint 能正确恢复。

### 阶段 3：受控真实验收

真实数据库写入需要另行明确批准。批准后只选择一个历史小时对象：

1. dry-run 输出对象 key、大小和预估行数；
2. 导入该对象；
3. 执行完成条件和对账 SQL；
4. 重跑同一对象，验证跳过或 `FINAL` 结果不变；
5. 人工抽查若干 fill 与源 JSON 一致。

### 阶段 4：手动历史回补与后续更新

1. 按日分段回补，限制并发和本地磁盘水位；
2. 每段完成后对账，再推进下一段；
3. 历史追平后，仍由用户手动指定新日期范围执行 inspect、dry-run、sync、status；
4. 不启用定时器或自动滚动扫描；
5. 记录费用、吞吐和 ClickHouse 压缩比，据此调整后续手动批次大小。

最终成功标准：

- 所有已发现对象都有唯一的最新任务状态；
- completed 对象的 `output_rows_written` 全部与成交表 `FINAL` 行数一致；
- 重跑不会改变 `FINAL` 逻辑结果；
- 新增源字段不会丢失，至少存在于 `raw_fill_json`；
- 每次手动日期任务的完成状态、失败数、费用和磁盘使用可观测。

## 14. 真实数据验收记录

已使用 `2026-11-01` 的 Requester Pays S3 真实数据完成全日验收：

| 项目 | 结果 |
|---|---:|
| S3 小时对象 | 24 |
| 压缩大小 | 622,625,455 bytes（593.78 MiB） |
| 解析 JSONL block 记录 | 1,210,890 |
| 写入 fill 逻辑行数 | 6,766,485 |
| 不同用户数 | 52,655 |
| completed 对象 | 24 / 24 |
| 对象级任务行数与 `FINAL` 行数不一致 | 0 |

验收同时验证：

- `inspect --refresh` 能发现全部 24 个对象；
- dry-run 能输出对象大小、缓存状态和恢复行号；
- LZ4 流能够完整读到 EOF；
- 空 block 计入 `source_records_read` 且不产生 fill；
- 成交表最早事件为 `2026-10-31 23:59:59.926 UTC`，说明对象日期边界附近允许出现上一 UTC 秒的数据，查询完整源日期应优先按 `source_key` 或 `source_date` 任务范围校验；
- 同日期重跑 dry-run 时 24 个 completed 对象全部跳过，没有重复下载或写入。
