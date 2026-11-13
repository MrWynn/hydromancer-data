import time
from dataclasses import dataclass
from datetime import date
from typing import Dict, Iterable, List, Optional, Sequence

from .catalog import S3Object
from .config import ClickHouseConfig
from .constants import ALLOWED_TABLES, TASK_TABLE, DatasetSpec


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
