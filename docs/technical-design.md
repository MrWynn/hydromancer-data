# Hydromancer Snapshot CLI 技术方案

## 1. 目标与范围

工具从 `s3://hydromancer-reservoir` 读取以下 UTC 日分区：

- `global/snapshots/perp/all/date=YYYY-MM-DD/`
- `global/snapshots/spot/date=YYYY-MM-DD/`
- `global/snapshots/account_values/date=YYYY-MM-DD/`

它提供数据可用性和压缩大小查询、Parquet 下载、本地缓存复用及远程 ClickHouse 同步。首版为手动 CLI，不包含 Web、调度、fills、candles 或 orderbook。

Hydromancer snapshot 文件名采用 `{block}_{timestamp_ms}.parquet`。部分日期可能缺失，因此缺失数据记录为 `missing`，而不是任务失败。

## 2. 技术选型

- Python 3.9+
- Typer：CLI
- boto3：S3 List/Get 与 Requester Pays
- SQLite：本地目录和下载状态
- PyArrow：分批读取 Parquet
- clickhouse-driver：通过 Native TCP 端口列式写入 ClickHouse

真实配置保存在被 Git 忽略的 `config.toml`，仓库只提交 `config.example.toml`。密码不进入 SQL、命令参数或日志。

## 3. CLI

### inspect

```bash
hyperliquid-etl inspect --from DATE --to DATE \
  --dataset perp,spot,account_values [--refresh] [--format table|json]
```

日期首尾均包含。输出每个 dataset/date 的 available/missing、对象数、压缩字节数、block、timestamp 和本地下载状态。默认使用 SQLite 中的扫描结果；`--refresh` 才重新 List S3。它不读取 Parquet footer，也不计算行数。

### download

```bash
hyperliquid-etl download --from DATE --to DATE [--dataset ...] [--yes]
```

下载前展示新增对象数和字节数。使用 `.part` 文件写入、校验 `ContentLength` 后原子改名。有效本地文件不会重复下载。

### sync

```bash
hyperliquid-etl sync --from DATE --to DATE [--dataset ...] [--dry-run] [--yes]
```

同步首先查询 ClickHouse 任务表。状态为 `completed` 的 dataset/date 在任何 S3 请求前跳过，并清理可能残留的本地 cache；未完成任务从记录的 batch checkpoint 继续。未同步数据优先使用本地 Parquet；本地没有时只下载一次。任务成功标记为 `completed` 后，删除对应的 Parquet cache 和 SQLite download 记录；失败时保留它们用于断点续传。

### status

```bash
hyperliquid-etl status --from DATE --to DATE [--dataset ...]
```

只查询项目任务表，不访问 S3；显示最新的 running/failed/completed 状态、逻辑写入行数、总行数、下一个 batch 和错误摘要。

## 4. 本地目录与费用控制

SQLite 包含三类记录：

- `scans`：已扫描的 dataset/date。
- `objects`：key、ETag、压缩大小、block、timestamp。
- `downloads`：本地路径、大小和完成时间。

每个 S3 请求都传递 `RequestPayer=requester`，region 固定为 `ap-northeast-1`。列表结果被缓存；下载文件按原始 S3 key 保存。下载和同步复用同一文件。

按已确认策略，成功同步的 dataset/date 永久视为不可变。即使源对象后来改变 ETag，也不会自动刷新 ClickHouse。这能避免重复费用，但可能保留 Hydromancer 修订前的数据。

## 5. ClickHouse 表

DDL 位于 `sql/clickhouse_tables.sql`，必须由用户手动执行。程序不会自动建表。

正式表：

- `laevitas.hydromancer_snapshot_perp`
- `laevitas.hydromancer_snapshot_spot`
- `laevitas.hydromancer_snapshot_account_values`

任务表：

- `laevitas.hydromancer_sync_tasks`

三类数据都增加 `snapshot_date`、`snapshot_block`、`snapshot_timestamp` 和 `updated_at`。`snapshot_timestamp` 是 Hydromancer 源快照时间，`updated_at` 是工具为每个写入 batch 生成的 UTC 微秒版本时间。正式表使用 `ReplacingMergeTree(updated_at)` 并按日期分区，排序键同时作为快照行唯一键；相同排序键最终保留 `updated_at` 最大的记录。`updated_at` 不属于排序键。`pm_avail_by_branch` 序列化为 Nullable JSON String，以避免依赖特定 ClickHouse JSON 类型版本。

任务表使用 `ReplacingMergeTree(version)`，按 dataset/date 保存最新状态，包括 source identity、总行数、下一个 batch、batch 大小、逻辑已写行数、状态和错误。读取状态时按 version 取最新记录，不依赖后台 merge 是否已经完成。

## 6. 同步一致性

每个 dataset/date 按以下顺序处理：

1. 查询任务表；最新状态为 `completed` 时清理可能残留的 cache 并跳过。
2. 从缓存目录取得 Parquet，必要时下载。
3. 校验未完成任务记录的 key、ETag、大小和总行数仍与源文件一致。
4. 使用任务首次运行时记录的固定 batch 大小分批读取 Parquet。
5. 从 `next_batch` 继续，直接列式 INSERT 正式表。
6. 每个 batch 成功后追加一条 `running` checkpoint。
7. 全部逻辑行写完后追加 `completed` 状态；异常时追加 `failed` 状态。
8. `completed` 成功写入后删除对应的 Parquet cache 和 SQLite download 记录。

如果 ClickHouse 已接收一个 batch、但客户端没有收到成功响应，该 batch 可能被重试。重试会生成更大的 `updated_at`，正式表的 `ReplacingMergeTree(updated_at)` 会按排序键在后台 merge 时保留最新版本；merge 完成前普通查询可能短暂看到重复，要求即时准确时必须使用 `FINAL`。

## 7. 数据库安全边界

代码将数据库固定校验为 `laevitas`，表名来自代码内 allowlist，不接受用户提供的任意表名或 SQL。

程序仅允许：

- 查询和 INSERT 三张正式表及一张任务表。

程序不执行 `CREATE TABLE`、`DROP TABLE`、`TRUNCATE TABLE`、分区删除或分区替换，也不访问其他业务表。

## 8. 测试与验收

自动化测试使用 mock S3、mock ClickHouse、临时 SQLite 和临时目录，不连接真实数据库。

真实验收必须等待用户手动建表确认，且限制为：

- database：`laevitas`
- 日期：`2026-11-09`
- dataset：perp、spot、account values
- 表：上述三张快照表和一张任务表

验收依次执行 inspect、download、dry-run、sync、`FINAL` 行数检查和重复 sync。第二次同步必须在 S3 请求前跳过，且 `FINAL` 查询行数保持不变。

