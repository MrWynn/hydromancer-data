# Hyperliquid ETL

用于将 Hyperliquid 生态数据导入 ClickHouse。目前支持 Hydromancer Reservoir 日快照和 Hyperliquid 官方 S3 node fills。

## Hydromancer 日快照

支持以下 dataset：

- `perp`：永续合约持仓
- `spot`：现货余额
- `account_values`：账户价值

## 数据同步

日期使用 `YYYY-MM-DD` 格式，`--from` 和 `--to` 均包含当天，并按 UTC 日期解释。默认同步全部三个 dataset；使用 `--dataset` 可以只选择其中一部分。

### 估算下载量和费用

同步较长日期范围前，可以抽样查询 S3 对象大小并估算总下载量：

```bash
hyperliquid-etl cost \
  --dataset perp \
  --start 2026-11-01 \
  --end 2026-11-30
```

命令抽样起始日、中间日和结束日，再外推整个日期范围。它只执行 Requester Pays S3 LIST，不下载 Parquet，也不访问 ClickHouse。当前 S3 bucket 与运行工具的 EC2 位于同一 AWS Region，因此默认数据传输费率为 `$0/GB`。如果以后改为跨 Region 或下载到 AWS 之外，可通过 `--usd-per-gb` 传入对应费率：

```bash
hyperliquid-etl cost \
  --dataset perp \
  --start 2026-11-01 \
  --end 2026-11-30 \
  --usd-per-gb 0.09
```

如需逐日列举 S3 并精确统计当前已有对象的压缩大小，可使用 `--exact`。该模式不会将未来日期或可能尚未发布的日期按零数据计入平均值，也不会下载 Parquet：

```bash
hyperliquid-etl cost \
  --dataset perp \
  --start 2026-11-01 \
  --end 2026-11-30 \
  --exact
```

费用输出不包含少量 S3 LIST/GET 请求费。下载后的实际账单金额以 AWS Cost Explorer 或 CUR 为准。

### 1. 检查源数据

同步前可以检查指定日期的数据是否存在、对象大小、快照 block 以及本地是否已有缓存：

```bash
hyperliquid-etl inspect \
  --from 2026-11-10 \
  --to 2026-11-10 \
  --dataset perp,spot,account_values
```

加入 `--refresh` 会忽略本地 S3 列举缓存并重新查询源数据：

```bash
hyperliquid-etl inspect \
  --from 2026-11-10 \
  --to 2026-11-10 \
  --refresh
```

首次检查或使用 `--refresh` 会访问 Requester Pays S3。

### 2. 预览同步计划

正式写入前先执行 dry-run：

```bash
hyperliquid-etl sync \
  --from 2026-11-10 \
  --to 2026-11-10 \
  --dataset perp,spot,account_values \
  --dry-run
```

输出中的 `plan` 表示需要同步，并显示源对象大小、是否已有本地缓存以及从哪个 batch 继续。已经完成的 dataset/date 会显示 `skip ... already ingested`。

### 3. 执行同步

确认计划后执行：

```bash
hyperliquid-etl sync \
  --from 2026-11-10 \
  --to 2026-11-10 \
  --dataset perp,spot,account_values \
  --yes
```

`--yes` 表示确认 Requester Pays 下载费用和 ClickHouse 写入。未提供 `--yes` 时，命令会在实际下载和写入前要求交互确认。

同步流程会依次：

1. 查询 ClickHouse 任务状态；
2. 跳过已经完成的 dataset/date；
3. 复用本地已下载且校验有效的 Parquet 文件，否则从 S3 下载；
4. 按 batch 写入对应的 ClickHouse 快照表；
5. 将进度写入 `hydromancer_sync_tasks`。

如果同步中断，重新执行相同命令会从任务表记录的 batch 继续。成功完成后再次执行同一 dataset/date，会在访问 S3 前直接跳过。

### 4. 查询同步状态

```bash
hyperliquid-etl status \
  --from 2026-11-10 \
  --to 2026-11-10 \
  --dataset perp,spot,account_values
```

状态输出包含：

- `status`：`running`、`failed` 或 `completed`
- `rows`：已写入行数与总行数
- `next_batch`：下次恢复的 batch
- `error`：最近一次错误

### 同步日期范围或单个 dataset

同步连续日期范围：

```bash
hyperliquid-etl sync \
  --from 2026-11-01 \
  --to 2026-11-10 \
  --dry-run
```

只同步一个 dataset：

```bash
hyperliquid-etl sync \
  --from 2026-11-10 \
  --to 2026-11-10 \
  --dataset perp \
  --yes
```

如需使用其他配置文件，在任意命令后添加：

```bash
--config /path/to/config.toml
```

正式表使用 `ReplacingMergeTree(updated_at)` 处理 batch 重试产生的相同排序键记录。后台 merge 完成前，如果查询必须严格去重，请使用 `FINAL`。

完整实现与数据表设计见 [docs/technical-design.md](docs/technical-design.md)。

## Hyperliquid node fills

`node_fills_by_block` 使用独立子命令，日期按 UTC 解释且包含首尾日期。先手动执行 [sql/node_fills_by_block_tables.sql](sql/node_fills_by_block_tables.sql) 创建项目表。

检查真实 S3 对象并刷新本地清单：

```bash
hyperliquid-etl node-fills inspect \
  --from 2026-11-01 \
  --to 2026-11-01 \
  --refresh
```

预览下载和恢复位置，不下载、不写 ClickHouse：

```bash
hyperliquid-etl node-fills sync \
  --from 2026-11-01 \
  --to 2026-11-01 \
  --dry-run
```

确认 Requester Pays 费用并同步：

```bash
hyperliquid-etl node-fills sync \
  --from 2026-11-01 \
  --to 2026-11-01 \
  --yes
```

查看对象级 checkpoint 和状态：

```bash
hyperliquid-etl node-fills status \
  --from 2026-11-01 \
  --to 2026-11-01
```

同步按日期、小时顺序处理。完成对象再次执行时会直接跳过；失败对象从最近一次成功写入的 JSONL 行恢复。LZ4 文件缓存在 `storage.cache_dir` 下，S3 扫描和下载状态记录在现有 SQLite catalog 中。

完整数据模型、幂等边界和验收方式见 [docs/node-fills-by-block-design.md](docs/node-fills-by-block-design.md)。

## 后台同步脚本

同步单日的全部 dataset：

```bash
./sync_snapshots.sh start 2026-11-10
```

同步日期范围，并指定 dataset：

```bash
./sync_snapshots.sh start 2026-11-01 2026-11-10 perp,spot
```

查看、停止或重启后台任务：

```bash
./sync_snapshots.sh status
./sync_snapshots.sh stop
./sync_snapshots.sh restart
```

脚本会阻止重复启动。任务正常完成、手动停止或异常退出时，会通过 `config.toml` 中配置的 `[lark].webhook_url` 发送通知。`restart` 默认复用上一次启动参数，也可以在命令后提供新的日期和 dataset。

脚本会在后台依次执行源数据检查、dry-run、下载与同步、最终状态查询。启动命令会立即返回，退出当前 shell 后任务仍会继续运行。

每次运行的完整输出保存在 `logs/` 下的独立日志文件中。启动时会显示具体日志路径，可以实时查看：

```bash
tail -f logs/sync_2026-11-10_2026-11-10_YYYYMMDDTHHMMSSZ.log
```

同一时间只允许一个后台同步任务运行，避免并发写入同一批数据。

