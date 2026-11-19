import calendar
import json
import os
import re
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Iterator, List, Optional, Sequence, Tuple

from .clickhouse_store import ClickHouseStore, IngestionTask
from .config import AppConfig, AwsConfig
from .utils import date_range


DATASET = "node_fills_by_block"
BUCKET = "hl-mainnet-node-data"
PREFIX = "node_fills_by_block/hourly"
OBJECT_RE = re.compile(r"^(?P<hour>\d{1,2})\.lz4$")

FILL_COLUMNS = (
    "user",
    "event_time",
    "event_time_ms",
    "block_number",
    "block_time",
    "event_index",
    "coin",
    "px",
    "sz",
    "side",
    "start_position",
    "direction",
    "closed_pnl",
    "tx_hash",
    "oid",
    "crossed",
    "fee",
    "tid",
    "cloid",
    "fee_token",
    "builder_fee",
    "builder",
    "twap_id",
    "deployer_fee",
    "priority_gas",
    "source_bucket",
    "source_key",
    "source_line",
    "raw_fill_json",
    "ingested_at",
)


@dataclass(frozen=True)
class IngestionObject:
    dataset: str
    bucket: str
    source_date: date
    hour: int
    key: str
    etag: str
    size: int
    last_modified: datetime


@dataclass(frozen=True)
class NodeFillsInspection:
    source_date: date
    object_count: int
    compressed_bytes: int
    downloaded_count: int


class IngestionCatalog:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(str(self.path))
        connection.row_factory = sqlite3.Row
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS hyperliquid_s3_scans (
                    dataset TEXT NOT NULL,
                    source_date TEXT NOT NULL,
                    scanned_at TEXT NOT NULL,
                    PRIMARY KEY (dataset, source_date)
                );
                CREATE TABLE IF NOT EXISTS hyperliquid_s3_objects (
                    dataset TEXT NOT NULL,
                    source_bucket TEXT NOT NULL,
                    source_date TEXT NOT NULL,
                    source_hour INTEGER NOT NULL,
                    source_key TEXT NOT NULL,
                    etag TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    last_modified TEXT NOT NULL,
                    PRIMARY KEY (dataset, source_bucket, source_key)
                );
                CREATE INDEX IF NOT EXISTS hyperliquid_s3_objects_by_date
                    ON hyperliquid_s3_objects(dataset, source_date);
                CREATE TABLE IF NOT EXISTS hyperliquid_s3_downloads (
                    dataset TEXT NOT NULL,
                    source_bucket TEXT NOT NULL,
                    source_key TEXT NOT NULL,
                    etag TEXT NOT NULL,
                    local_path TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    downloaded_at TEXT NOT NULL,
                    PRIMARY KEY (dataset, source_bucket, source_key)
                );
                """
            )

    def has_scan(self, dataset: str, source_date: date) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM hyperliquid_s3_scans "
                "WHERE dataset = ? AND source_date = ?",
                (dataset, source_date.isoformat()),
            ).fetchone()
        return row is not None

    def replace_scan(
        self,
        dataset: str,
        source_date: date,
        objects: Iterable[IngestionObject],
    ) -> None:
        items = list(objects)
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM hyperliquid_s3_objects "
                "WHERE dataset = ? AND source_date = ?",
                (dataset, source_date.isoformat()),
            )
            connection.executemany(
                "INSERT INTO hyperliquid_s3_objects "
                "(dataset, source_bucket, source_date, source_hour, source_key, "
                "etag, size, last_modified) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        item.dataset,
                        item.bucket,
                        item.source_date.isoformat(),
                        item.hour,
                        item.key,
                        item.etag,
                        item.size,
                        item.last_modified.isoformat(),
                    )
                    for item in items
                ],
            )
            connection.execute(
                "INSERT INTO hyperliquid_s3_scans "
                "(dataset, source_date, scanned_at) VALUES (?, ?, ?) "
                "ON CONFLICT(dataset, source_date) DO UPDATE SET "
                "scanned_at = excluded.scanned_at",
                (
                    dataset,
                    source_date.isoformat(),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

    def objects_for(
        self, dataset: str, source_date: date
    ) -> List[IngestionObject]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT dataset, source_bucket, source_date, source_hour, "
                "source_key, etag, size, last_modified "
                "FROM hyperliquid_s3_objects "
                "WHERE dataset = ? AND source_date = ? "
                "ORDER BY source_hour, source_key",
                (dataset, source_date.isoformat()),
            ).fetchall()
        return [
            IngestionObject(
                dataset=row["dataset"],
                bucket=row["source_bucket"],
                source_date=date.fromisoformat(row["source_date"]),
                hour=int(row["source_hour"]),
                key=row["source_key"],
                etag=row["etag"],
                size=int(row["size"]),
                last_modified=datetime.fromisoformat(row["last_modified"]),
            )
            for row in rows
        ]

    def record_download(self, item: IngestionObject, local_path: Path) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO hyperliquid_s3_downloads "
                "(dataset, source_bucket, source_key, etag, local_path, size, "
                "downloaded_at) VALUES (?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(dataset, source_bucket, source_key) DO UPDATE SET "
                "etag = excluded.etag, local_path = excluded.local_path, "
                "size = excluded.size, downloaded_at = excluded.downloaded_at",
                (
                    item.dataset,
                    item.bucket,
                    item.key,
                    item.etag,
                    str(local_path),
                    item.size,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

    def downloaded_path(self, item: IngestionObject) -> Optional[Path]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT etag, local_path, size FROM hyperliquid_s3_downloads "
                "WHERE dataset = ? AND source_bucket = ? AND source_key = ?",
                (item.dataset, item.bucket, item.key),
            ).fetchone()
        if row is None or row["etag"] != item.etag:
            return None
        path = Path(row["local_path"])
        if not path.is_file() or path.stat().st_size != int(row["size"]):
            return None
        return path

    def delete_download(
        self, dataset: str, source_bucket: str, source_key: str, etag: str
    ) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT local_path FROM hyperliquid_s3_downloads "
                "WHERE dataset = ? AND source_bucket = ? AND source_key = ? "
                "AND etag = ?",
                (dataset, source_bucket, source_key, etag),
            ).fetchone()
            if row is None:
                return False
            Path(row["local_path"]).unlink(missing_ok=True)
            connection.execute(
                "DELETE FROM hyperliquid_s3_downloads "
                "WHERE dataset = ? AND source_bucket = ? AND source_key = ? "
                "AND etag = ?",
                (dataset, source_bucket, source_key, etag),
            )
        return True


class NodeFillsS3:
    def __init__(self, config: AwsConfig, client: Any = None):
        self.config = config
        if client is None:
            try:
                import boto3
            except ModuleNotFoundError as exc:
                raise RuntimeError("Install project dependencies to use S3") from exc
            session_args = {"region_name": config.region}
            if config.profile:
                session_args["profile_name"] = config.profile
            client = boto3.Session(**session_args).client("s3")
        self.client = client

    def list_objects(self, source_date: date) -> List[IngestionObject]:
        prefix = "%s/%s/" % (PREFIX, source_date.strftime("%Y%m%d"))
        token: Optional[str] = None
        result: List[IngestionObject] = []
        while True:
            request = {
                "Bucket": BUCKET,
                "Prefix": prefix,
                "RequestPayer": "requester",
            }
            if token:
                request["ContinuationToken"] = token
            response = self.client.list_objects_v2(**request)
            for raw in response.get("Contents", []):
                name = raw["Key"].rsplit("/", 1)[-1]
                match = OBJECT_RE.match(name)
                if not match:
                    continue
                hour = int(match.group("hour"))
                if hour > 23:
                    continue
                result.append(
                    IngestionObject(
                        dataset=DATASET,
                        bucket=BUCKET,
                        source_date=source_date,
                        hour=hour,
                        key=raw["Key"],
                        etag=str(raw.get("ETag", "")).strip('"'),
                        size=int(raw["Size"]),
                        last_modified=raw["LastModified"],
                    )
                )
            if not response.get("IsTruncated"):
                break
            token = response["NextContinuationToken"]
        return sorted(result, key=lambda item: (item.hour, item.key))

    def download(self, item: IngestionObject, destination: Path) -> Path:
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(destination.name + ".part")
        try:
            response = self.client.get_object(
                Bucket=item.bucket,
                Key=item.key,
                RequestPayer="requester",
            )
            body = response["Body"]
            try:
                with temporary.open("wb") as handle:
                    while True:
                        chunk = body.read(1024 * 1024)
                        if not chunk:
                            break
                        handle.write(chunk)
                    handle.flush()
                    os.fsync(handle.fileno())
            finally:
                close = getattr(body, "close", None)
                if close is not None:
                    close()
            if temporary.stat().st_size != item.size:
                raise IOError(
                    "Downloaded size mismatch for %s: expected %s, got %s"
                    % (item.key, item.size, temporary.stat().st_size)
                )
            temporary.replace(destination)
            return destination
        except Exception:
            temporary.unlink(missing_ok=True)
            raise


class NodeFillsService:
    def __init__(
        self,
        config: AppConfig,
        catalog: Optional[IngestionCatalog] = None,
        s3: Optional[NodeFillsS3] = None,
        clickhouse: Optional[ClickHouseStore] = None,
    ):
        self.config = config
        self.catalog = catalog or IngestionCatalog(config.storage.catalog_path)
        self._s3 = s3
        self._clickhouse = clickhouse

    @property
    def s3(self) -> NodeFillsS3:
        if self._s3 is None:
            aws = AwsConfig(
                profile=self.config.aws.profile,
                region="ap-northeast-1",
                bucket=BUCKET,
            )
            self._s3 = NodeFillsS3(aws)
        return self._s3

    @property
    def clickhouse(self) -> ClickHouseStore:
        if self._clickhouse is None:
            self._clickhouse = ClickHouseStore(self.config.clickhouse)
        return self._clickhouse

    def _objects_for_date(
        self, source_date: date, refresh: bool = False
    ) -> List[IngestionObject]:
        if refresh or not self.catalog.has_scan(DATASET, source_date):
            objects = self.s3.list_objects(source_date)
            self.catalog.replace_scan(DATASET, source_date, objects)
        return self.catalog.objects_for(DATASET, source_date)

    def inspect(
        self, start: date, end: date, refresh: bool = False
    ) -> List[NodeFillsInspection]:
        result: List[NodeFillsInspection] = []
        for source_date in date_range(start, end):
            objects = self._objects_for_date(source_date, refresh)
            result.append(
                NodeFillsInspection(
                    source_date=source_date,
                    object_count=len(objects),
                    compressed_bytes=sum(item.size for item in objects),
                    downloaded_count=sum(
                        self.catalog.downloaded_path(item) is not None
                        for item in objects
                    ),
                )
            )
        return result

    def sync(self, start: date, end: date, dry_run: bool = False) -> List[str]:
        messages: List[str] = []
        for source_date in date_range(start, end):
            objects = self._objects_for_date(source_date)
            if not objects:
                messages.append("skip %s: source missing" % source_date)
                continue
            for item in objects:
                task = self.clickhouse.get_ingestion_task(
                    item.dataset, item.bucket, item.key
                )
                if task is not None:
                    self._validate_source(item, task)
                    if task.status == "completed":
                        self.catalog.delete_download(
                            item.dataset, item.bucket, item.key, item.etag
                        )
                        messages.append("skip %s: already ingested" % item.key)
                        continue
                if dry_run:
                    messages.append(
                        "plan %s: %s bytes, cached=%s, resume_line=%s"
                        % (
                            item.key,
                            item.size,
                            str(
                                self.catalog.downloaded_path(item) is not None
                            ).lower(),
                            task.next_checkpoint if task is not None else 0,
                        )
                    )
                    continue
                path = self._download(item)
                rows = self._ingest_file(item, path, task)
                self.catalog.delete_download(
                    item.dataset, item.bucket, item.key, item.etag
                )
                messages.append("sync %s: %s fills" % (item.key, rows))
        return messages

    def status(self, start: date, end: date) -> List[IngestionTask]:
        return self.clickhouse.ingestion_tasks_for_range(DATASET, start, end)

    def _download(self, item: IngestionObject) -> Path:
        existing = self.catalog.downloaded_path(item)
        if existing is not None:
            return existing
        destination = self.config.storage.cache_dir / item.bucket / item.key
        path = self.s3.download(item, destination)
        self.catalog.record_download(item, path)
        return path

    @staticmethod
    def _validate_source(item: IngestionObject, task: IngestionTask) -> None:
        if (
            task.source_etag != item.etag
            or task.source_size != item.size
            or task.source_date != item.source_date
        ):
            raise RuntimeError(
                "Source changed for task %s; manual resolution required" % item.key
            )
        if task.checkpoint_kind != "jsonl_line":
            raise RuntimeError(
                "Unsupported checkpoint kind for %s: %s"
                % (item.key, task.checkpoint_kind)
            )

    def _ingest_file(
        self,
        item: IngestionObject,
        path: Path,
        task: Optional[IngestionTask],
    ) -> int:
        try:
            import lz4.frame
        except ModuleNotFoundError as exc:
            raise RuntimeError("Install project dependencies to read LZ4") from exc

        committed_checkpoint = task.next_checkpoint if task is not None else 0
        committed_records = task.source_records_read if task is not None else 0
        output_rows_written = task.output_rows_written if task is not None else 0
        self.clickhouse.record_ingestion_task(
            item,
            "ingesting",
            committed_checkpoint,
            committed_records,
            output_rows_written,
        )
        parsed_checkpoint = committed_checkpoint
        parsed_records = committed_records
        rows: List[Tuple[object, ...]] = []
        try:
            with lz4.frame.open(str(path), mode="rb") as handle:
                for line_index, raw_line in enumerate(handle):
                    if line_index < committed_checkpoint:
                        continue
                    if not raw_line.strip():
                        parsed_checkpoint = line_index + 1
                        continue
                    block_rows = parse_block(raw_line, item, line_index)
                    parsed_checkpoint = line_index + 1
                    parsed_records += 1
                    rows.extend(block_rows)
                    if len(rows) >= self.config.clickhouse.batch_rows:
                        inserted = self._insert_rows(rows)
                        output_rows_written += inserted
                        rows = []
                        committed_checkpoint = parsed_checkpoint
                        committed_records = parsed_records
                        self.clickhouse.record_ingestion_task(
                            item,
                            "ingesting",
                            committed_checkpoint,
                            committed_records,
                            output_rows_written,
                        )
            if rows:
                output_rows_written += self._insert_rows(rows)
            committed_checkpoint = parsed_checkpoint
            committed_records = parsed_records
            self.clickhouse.record_ingestion_task(
                item,
                "completed",
                committed_checkpoint,
                committed_records,
                output_rows_written,
            )
            return output_rows_written
        except Exception as exc:
            try:
                self.clickhouse.record_ingestion_task(
                    item,
                    "failed",
                    committed_checkpoint,
                    committed_records,
                    output_rows_written,
                    str(exc),
                )
            except Exception:
                pass
            raise

    def _insert_rows(self, rows: Sequence[Tuple[object, ...]]) -> int:
        batch_time = datetime.now(timezone.utc)
        versioned_rows = [row + (batch_time,) for row in rows]
        columns = [list(values) for values in zip(*versioned_rows)]
        inserted = self.clickhouse.insert_node_fills(FILL_COLUMNS, columns)
        if inserted != len(rows):
            raise RuntimeError(
                "Inserted fill count mismatch: expected %s, got %s"
                % (len(rows), inserted)
            )
        return inserted


def parse_block(
    raw_line: bytes, item: IngestionObject, source_line: int
) -> List[Tuple[object, ...]]:
    value = json.loads(raw_line)
    if not isinstance(value, dict):
        raise ValueError("JSONL record must be an object")
    block_number = int(value["block_number"])
    block_time = _datetime64_ns(value["block_time"])
    events = value["events"]
    if not isinstance(events, list):
        raise ValueError("events must be an array")
    rows: List[Tuple[object, ...]] = []
    for event_index, event in enumerate(events):
        if not isinstance(event, list) or len(event) != 2:
            raise ValueError("event must be [user, fill]")
        user, fill = event
        if not isinstance(user, str) or not isinstance(fill, dict):
            raise ValueError("event user and fill have invalid types")
        event_time_ms = int(fill["time"])
        crossed = fill.get("crossed")
        if crossed is not None and not isinstance(crossed, bool):
            raise ValueError("crossed must be boolean or null")
        rows.append(
            (
                user.lower(),
                event_time_ms,
                event_time_ms,
                block_number,
                block_time,
                event_index,
                str(fill["coin"]),
                _decimal(fill.get("px")),
                _decimal(fill.get("sz")),
                str(fill["side"]),
                _decimal(fill.get("startPosition")),
                str(fill["dir"]),
                _decimal(fill.get("closedPnl")),
                str(fill["hash"]),
                _integer(fill.get("oid")),
                None if crossed is None else int(crossed),
                _decimal(fill.get("fee")),
                _integer(fill.get("tid")),
                _string(fill.get("cloid")),
                _string(fill.get("feeToken")),
                _decimal(fill.get("builderFee")),
                _string(fill.get("builder")),
                _integer(fill.get("twapId")),
                _decimal(fill.get("deployerFee")),
                _decimal(fill.get("priorityGas")),
                item.bucket,
                item.key,
                source_line,
                json.dumps(
                    fill,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            )
        )
    return rows


def _datetime64_ns(value: Any) -> int:
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if "." in text:
        head, tail = text.split(".", 1)
        fraction = tail
        timezone_suffix = ""
        for marker in ("+", "-"):
            position = fraction.find(marker)
            if position > 0:
                timezone_suffix = fraction[position:]
                fraction = fraction[:position]
                break
        parsed = datetime.fromisoformat(head + timezone_suffix)
    else:
        fraction = ""
        parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    parsed = parsed.astimezone(timezone.utc)
    seconds = calendar.timegm(parsed.utctimetuple())
    nanoseconds = int((fraction + "000000000")[:9]) if fraction else 0
    return seconds * 1_000_000_000 + nanoseconds


def _decimal(value: Any) -> Optional[Decimal]:
    return None if value is None else Decimal(str(value))


def _integer(value: Any) -> Optional[int]:
    return None if value is None else int(value)


def _string(value: Any) -> Optional[str]:
    return None if value is None else str(value)
