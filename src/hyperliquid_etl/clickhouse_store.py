import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence

from .catalog import S3Object
from .config import ClickHouseConfig
from .constants import (
    ALLOWED_TABLES,
    INGESTION_TASK_TABLE,
    NODE_FILLS_TABLE,
    TASK_TABLE,
    DatasetSpec,
)


@dataclass(frozen=True)
class SyncTask:
    dataset: str
    snapshot_date: date
    source_key: str
    source_etag: str
    source_size: int
    total_rows: int
    next_batch: int
    batch_rows: int
    rows_written: int
    status: str
    last_error: str
    version: int


@dataclass(frozen=True)
class IngestionTask:
    dataset: str
    source_bucket: str
    source_key: str
    source_etag: str
    source_size: int
    source_last_modified: datetime
    source_date: date
    status: str
    checkpoint_kind: str
    next_checkpoint: int
    source_records_read: int
    output_rows_written: int
    last_error: str
    version: int


class ClickHouseStore:
    def __init__(self, config: ClickHouseConfig, client=None):
        if config.database != "laevitas":
            raise ValueError("ClickHouse database must be 'laevitas'")
        self.config = config
        if client is None:
            try:
                from clickhouse_driver import Client
            except ModuleNotFoundError as exc:
                raise RuntimeError(
                    "Install project dependencies to use ClickHouse"
                ) from exc
            client = Client(
                host=config.host,
                port=config.port,
                database=config.database,
                user=config.user,
                password=config.password,
                secure=config.secure,
            )
        self.client = client

    def _table(self, name: str) -> str:
        if name not in ALLOWED_TABLES:
            raise ValueError("Refusing to access non-project table: %s" % name)
        return "%s.%s" % (self.config.database, name)

    def get_task(self, dataset: str, snapshot_date: date) -> Optional[SyncTask]:
        rows = self.client.execute(
            "SELECT source_key, source_etag, source_size, total_rows, "
            "next_batch, batch_rows, rows_written, status, last_error, version "
            "FROM %s WHERE dataset = %%(dataset)s "
            "AND snapshot_date = %%(snapshot_date)s "
            "ORDER BY version DESC LIMIT 1"
            % self._table(TASK_TABLE),
            {"dataset": dataset, "snapshot_date": snapshot_date},
        )
        if not rows:
            return None
        row = rows[0]
        return SyncTask(
            dataset=dataset,
            snapshot_date=snapshot_date,
            source_key=row[0],
            source_etag=row[1],
            source_size=int(row[2]),
            total_rows=int(row[3]),
            next_batch=int(row[4]),
            batch_rows=int(row[5]),
            rows_written=int(row[6]),
            status=row[7],
            last_error=row[8],
            version=int(row[9]),
        )

    def has_ingested(self, dataset: str, snapshot_date: date) -> bool:
        task = self.get_task(dataset, snapshot_date)
        return task is not None and task.status == "completed"

    def ingested_dates(
        self, datasets: Sequence[str], start: date, end: date
    ) -> Dict[str, List[date]]:
        if not datasets:
            return {}
        rows = self.client.execute(
            "SELECT dataset, snapshot_date FROM ("
            "SELECT dataset, snapshot_date, argMax(status, version) AS latest_status "
            "FROM %s WHERE dataset IN %%(datasets)s "
            "AND snapshot_date BETWEEN %%(start)s AND %%(end)s "
            "GROUP BY dataset, snapshot_date) WHERE latest_status = 'completed'"
            % self._table(TASK_TABLE),
            {"datasets": tuple(datasets), "start": start, "end": end},
        )
        result: Dict[str, List[date]] = {name: [] for name in datasets}
        for dataset, snapshot_date in rows:
            result.setdefault(dataset, []).append(snapshot_date)
        return result

    def tasks_for_range(
        self, datasets: Sequence[str], start: date, end: date
    ) -> List[SyncTask]:
        if not datasets:
            return []
        rows = self.client.execute(
            "SELECT dataset, snapshot_date, "
            "argMax(source_key, version), argMax(source_etag, version), "
            "argMax(source_size, version), argMax(total_rows, version), "
            "argMax(next_batch, version), argMax(batch_rows, version), "
            "argMax(rows_written, version), argMax(status, version), "
            "argMax(last_error, version), max(version) "
            "FROM %s WHERE dataset IN %%(datasets)s "
            "AND snapshot_date BETWEEN %%(start)s AND %%(end)s "
            "GROUP BY dataset, snapshot_date ORDER BY dataset, snapshot_date"
            % self._table(TASK_TABLE),
            {"datasets": tuple(datasets), "start": start, "end": end},
        )
        return [
            SyncTask(
                dataset=row[0],
                snapshot_date=row[1],
                source_key=row[2],
                source_etag=row[3],
                source_size=int(row[4]),
                total_rows=int(row[5]),
                next_batch=int(row[6]),
                batch_rows=int(row[7]),
                rows_written=int(row[8]),
                status=row[9],
                last_error=row[10],
                version=int(row[11]),
            )
            for row in rows
        ]

    def insert_columns(
        self,
        spec: DatasetSpec,
        column_names: Sequence[str],
        columns: Sequence[Iterable[object]],
    ) -> int:
        identifiers = ", ".join(column_names)
        query = "INSERT INTO %s (%s) VALUES" % (
            self._table(spec.target_table),
            identifiers,
        )
        return int(self.client.execute(query, columns, columnar=True))

    def record_task(
        self,
        item: S3Object,
        total_rows: int,
        next_batch: int,
        batch_rows: int,
        rows_written: int,
        status: str,
        last_error: str = "",
    ) -> SyncTask:
        version = time.time_ns()
        self.client.execute(
            "INSERT INTO %s "
            "(dataset, snapshot_date, source_key, source_etag, source_size, "
            "total_rows, next_batch, batch_rows, rows_written, status, "
            "last_error, version) VALUES"
            % self._table(TASK_TABLE),
            [
                (
                    item.dataset,
                    item.snapshot_date,
                    item.key,
                    item.etag,
                    item.size,
                    total_rows,
                    next_batch,
                    batch_rows,
                    rows_written,
                    status,
                    last_error[:1000],
                    version,
                )
            ],
        )
        return SyncTask(
            dataset=item.dataset,
            snapshot_date=item.snapshot_date,
            source_key=item.key,
            source_etag=item.etag,
            source_size=item.size,
            total_rows=total_rows,
            next_batch=next_batch,
            batch_rows=batch_rows,
            rows_written=rows_written,
            status=status,
            last_error=last_error[:1000],
            version=version,
        )

    def get_ingestion_task(
        self, dataset: str, source_bucket: str, source_key: str
    ) -> Optional[IngestionTask]:
        rows = self.client.execute(
            "SELECT source_etag, source_size, source_last_modified, source_date, "
            "status, checkpoint_kind, next_checkpoint, source_records_read, "
            "output_rows_written, last_error, version FROM %s "
            "WHERE dataset = %%(dataset)s AND source_bucket = %%(source_bucket)s "
            "AND source_key = %%(source_key)s ORDER BY version DESC LIMIT 1"
            % self._table(INGESTION_TASK_TABLE),
            {
                "dataset": dataset,
                "source_bucket": source_bucket,
                "source_key": source_key,
            },
        )
        if not rows:
            return None
        row = rows[0]
        return IngestionTask(
            dataset=dataset,
            source_bucket=source_bucket,
            source_key=source_key,
            source_etag=row[0],
            source_size=int(row[1]),
            source_last_modified=row[2],
            source_date=row[3],
            status=row[4],
            checkpoint_kind=row[5],
            next_checkpoint=int(row[6]),
            source_records_read=int(row[7]),
            output_rows_written=int(row[8]),
            last_error=row[9],
            version=int(row[10]),
        )

    def ingestion_tasks_for_range(
        self, dataset: str, start: date, end: date
    ) -> List[IngestionTask]:
        rows = self.client.execute(
            "SELECT source_bucket, source_key, "
            "argMax(source_etag, version), argMax(source_size, version), "
            "argMax(source_last_modified, version), "
            "argMax(source_date, version) AS latest_source_date, "
            "argMax(status, version), argMax(checkpoint_kind, version), "
            "argMax(next_checkpoint, version), "
            "argMax(source_records_read, version), "
            "argMax(output_rows_written, version), "
            "argMax(last_error, version), max(version) FROM %s "
            "WHERE dataset = %%(dataset)s "
            "AND source_date BETWEEN %%(start)s AND %%(end)s "
            "GROUP BY source_bucket, source_key "
            "ORDER BY latest_source_date, source_key"
            % self._table(INGESTION_TASK_TABLE),
            {"dataset": dataset, "start": start, "end": end},
        )
        return [
            IngestionTask(
                dataset=dataset,
                source_bucket=row[0],
                source_key=row[1],
                source_etag=row[2],
                source_size=int(row[3]),
                source_last_modified=row[4],
                source_date=row[5],
                status=row[6],
                checkpoint_kind=row[7],
                next_checkpoint=int(row[8]),
                source_records_read=int(row[9]),
                output_rows_written=int(row[10]),
                last_error=row[11],
                version=int(row[12]),
            )
            for row in rows
        ]

    def record_ingestion_task(
        self,
        item: Any,
        status: str,
        next_checkpoint: int,
        source_records_read: int,
        output_rows_written: int,
        last_error: str = "",
    ) -> IngestionTask:
        version = time.time_ns()
        self.client.execute(
            "INSERT INTO %s (dataset, source_bucket, source_key, source_etag, "
            "source_size, source_last_modified, source_date, status, "
            "checkpoint_kind, next_checkpoint, source_records_read, "
            "output_rows_written, last_error, version) VALUES"
            % self._table(INGESTION_TASK_TABLE),
            [
                (
                    item.dataset,
                    item.bucket,
                    item.key,
                    item.etag,
                    item.size,
                    item.last_modified,
                    item.source_date,
                    status,
                    "jsonl_line",
                    next_checkpoint,
                    source_records_read,
                    output_rows_written,
                    last_error[:1000],
                    version,
                )
            ],
        )
        return IngestionTask(
            dataset=item.dataset,
            source_bucket=item.bucket,
            source_key=item.key,
            source_etag=item.etag,
            source_size=item.size,
            source_last_modified=item.last_modified,
            source_date=item.source_date,
            status=status,
            checkpoint_kind="jsonl_line",
            next_checkpoint=next_checkpoint,
            source_records_read=source_records_read,
            output_rows_written=output_rows_written,
            last_error=last_error[:1000],
            version=version,
        )

    def insert_node_fills(
        self,
        column_names: Sequence[str],
        columns: Sequence[Iterable[object]],
    ) -> int:
        query = "INSERT INTO %s (%s) VALUES" % (
            self._table(NODE_FILLS_TABLE),
            ", ".join(column_names),
        )
        return int(self.client.execute(query, columns, columnar=True))
