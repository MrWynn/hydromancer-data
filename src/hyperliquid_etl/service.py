import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .catalog import Catalog, S3Object
from .clickhouse_store import ClickHouseStore, SyncTask
from .config import AppConfig
from .constants import COMMON_COLUMNS, DATASETS, DatasetSpec
from .s3_store import S3Store
from .utils import date_range, remove_empty_directory


@dataclass(frozen=True)
class InspectionRecord:
    dataset: str
    snapshot_date: date
    available: bool
    object_count: int
    compressed_bytes: int
    block: Optional[int]
    timestamp_ms: Optional[int]
    downloaded: bool


@dataclass(frozen=True)
class CostSample:
    snapshot_date: date
    compressed_bytes: int
    object_count: int


@dataclass(frozen=True)
class CostEstimate:
    dataset: str
    days: int
    samples: Sequence[CostSample]

    @property
    def bytes_per_day(self) -> float:
        return sum(item.compressed_bytes for item in self.samples) / len(self.samples)

    @property
    def objects_per_day(self) -> float:
        return sum(item.object_count for item in self.samples) / len(self.samples)

    @property
    def total_bytes(self) -> float:
        return self.bytes_per_day * self.days

    @property
    def total_objects(self) -> float:
        return self.objects_per_day * self.days


@dataclass(frozen=True)
class ExactCost:
    dataset: str
    samples: Sequence[CostSample]
    as_of: date

    @property
    def available_samples(self) -> Sequence[CostSample]:
        return [item for item in self.samples if item.object_count]

    @property
    def total_bytes(self) -> int:
        return sum(item.compressed_bytes for item in self.available_samples)

    @property
    def total_objects(self) -> int:
        return sum(item.object_count for item in self.available_samples)

    def status(self, sample: CostSample) -> str:
        if sample.object_count:
            return "available"
        if sample.snapshot_date > self.as_of:
            return "future"
        available_dates = [
            item.snapshot_date
            for item in self.available_samples
            if item.snapshot_date <= self.as_of
        ]
        if available_dates and sample.snapshot_date > max(available_dates):
            return "pending"
        return "missing"


OPTIONAL_DEFAULTS = {
    "account_mode": None,
    "is_unified": False,
    "pm_ratio": None,
    "pm_avail_by_branch": None,
    "pm_total_account_value": None,
}


class SnapshotService:
    def __init__(
        self,
        config: AppConfig,
        catalog: Optional[Catalog] = None,
        s3: Optional[S3Store] = None,
        clickhouse: Optional[ClickHouseStore] = None,
    ):
        self.config = config
        self.catalog = catalog or Catalog(config.storage.catalog_path)
        self._s3 = s3
        self._clickhouse = clickhouse

    @property
    def s3(self) -> S3Store:
        if self._s3 is None:
            self._s3 = S3Store(self.config.aws)
        return self._s3

    @property
    def clickhouse(self) -> ClickHouseStore:
        if self._clickhouse is None:
            self._clickhouse = ClickHouseStore(self.config.clickhouse)
        return self._clickhouse

    def _scan_one(
        self, spec: DatasetSpec, snapshot_date: date, refresh: bool = False
    ) -> List[S3Object]:
        if refresh or not self.catalog.has_scan(spec.name, snapshot_date):
            objects = self.s3.list_objects(spec, snapshot_date)
            self.catalog.replace_scan(spec.name, snapshot_date, objects)
        return self.catalog.objects_for(spec.name, snapshot_date)

    def inspect(
        self,
        start: date,
        end: date,
        datasets: Sequence[str],
        refresh: bool = False,
    ) -> List[InspectionRecord]:
        records: List[InspectionRecord] = []
        for snapshot_date in date_range(start, end):
            for name in datasets:
                spec = DATASETS[name]
                objects = self._scan_one(spec, snapshot_date, refresh)
                records.append(
                    InspectionRecord(
                        dataset=name,
                        snapshot_date=snapshot_date,
                        available=bool(objects),
                        object_count=len(objects),
                        compressed_bytes=sum(item.size for item in objects),
                        block=objects[0].block if len(objects) == 1 else None,
                        timestamp_ms=(
                            objects[0].timestamp_ms if len(objects) == 1 else None
                        ),
                        downloaded=bool(objects)
                        and all(
                            self.catalog.downloaded_path(item) is not None
                            for item in objects
                        ),
                    )
                )
        return records

    def estimate_cost(
        self, start: date, end: date, dataset: str
    ) -> CostEstimate:
        days = (end - start).days + 1
        if days <= 0:
            raise ValueError("End date must not be before start date")
        sample_count = min(3, days)
        if sample_count == 1:
            sample_dates = [start]
        else:
            step = (days - 1) / (sample_count - 1)
            sample_dates = sorted(
                {start + timedelta(days=round(index * step))
                 for index in range(sample_count)}
            )
        spec = DATASETS[dataset]
        samples: List[CostSample] = []
        for snapshot_date in sample_dates:
            objects = self._scan_one(spec, snapshot_date, refresh=True)
            samples.append(
                CostSample(
                    snapshot_date=snapshot_date,
                    compressed_bytes=sum(item.size for item in objects),
                    object_count=len(objects),
                )
            )
        if not any(item.object_count for item in samples):
            raise RuntimeError(
                "No source objects found for sampled dates: %s"
                % ", ".join(str(item.snapshot_date) for item in samples)
            )
        return CostEstimate(dataset=dataset, days=days, samples=samples)

    def exact_cost(
        self,
        start: date,
        end: date,
        dataset: str,
        as_of: Optional[date] = None,
    ) -> ExactCost:
        if end < start:
            raise ValueError("End date must not be before start date")
        spec = DATASETS[dataset]
        samples: List[CostSample] = []
        for snapshot_date in date_range(start, end):
            objects = self.s3.list_objects(spec, snapshot_date)
            samples.append(
                CostSample(
                    snapshot_date=snapshot_date,
                    compressed_bytes=sum(item.size for item in objects),
                    object_count=len(objects),
                )
            )
        return ExactCost(
            dataset=dataset,
            samples=samples,
            as_of=as_of or datetime.now(timezone.utc).date(),
        )

    def objects_for_range(
        self, start: date, end: date, datasets: Sequence[str]
    ) -> List[S3Object]:
        objects: List[S3Object] = []
        for snapshot_date in date_range(start, end):
            for name in datasets:
                objects.extend(self._scan_one(DATASETS[name], snapshot_date))
        return objects

    def pending_downloads(self, objects: Iterable[S3Object]) -> List[S3Object]:
        return [
            item for item in objects if self.catalog.downloaded_path(item) is None
        ]

    def download(self, objects: Iterable[S3Object]) -> List[Path]:
        paths: List[Path] = []
        for item in objects:
            existing = self.catalog.downloaded_path(item)
            if existing is not None:
                paths.append(existing)
                continue
            destination = self.config.storage.cache_dir / item.key
            path = self.s3.download(item, destination)
            self.catalog.record_download(item, path)
            paths.append(path)
        return paths

    def sync(
        self,
        start: date,
        end: date,
        datasets: Sequence[str],
        dry_run: bool = False,
    ) -> List[str]:
        messages: List[str] = []
        for snapshot_date in date_range(start, end):
            for name in datasets:
                spec = DATASETS[name]
                task = self.clickhouse.get_task(name, snapshot_date)
                if task is not None and task.status == "completed":
                    if not dry_run:
                        self.catalog.delete_download(
                            task.source_key, task.source_etag
                        )
                        remove_empty_directory(
                            (self.config.storage.cache_dir / task.source_key).parent
                        )
                    messages.append("skip %s %s: already ingested" % (name, snapshot_date))
                    continue
                objects = self._scan_one(spec, snapshot_date)
                if not objects:
                    messages.append("skip %s %s: source missing" % (name, snapshot_date))
                    continue
                if len(objects) != 1:
                    raise RuntimeError(
                        "Expected one snapshot object for %s %s, found %s"
                        % (name, snapshot_date, len(objects))
                    )
                item = objects[0]
                if dry_run:
                    cached = self.catalog.downloaded_path(item) is not None
                    messages.append(
                        "plan %s %s: %s bytes, cached=%s, resume_batch=%s"
                        % (
                            name,
                            snapshot_date,
                            item.size,
                            str(cached).lower(),
                            task.next_batch if task is not None else 0,
                        )
                    )
                    continue
                local_path = self.download([item])[0]
                rows = self._ingest_file(spec, item, local_path, task)
                self.catalog.delete_download(item.key, item.etag)
                remove_empty_directory(local_path.parent)
                messages.append("sync %s %s: %s rows" % (name, snapshot_date, rows))
        return messages

    def _ingest_file(
        self,
        spec: DatasetSpec,
        item: S3Object,
        path: Path,
        task: Optional[SyncTask],
    ) -> int:
        try:
            import pyarrow.parquet as parquet
        except ModuleNotFoundError as exc:
            raise RuntimeError("Install project dependencies to read Parquet") from exc

        parquet_file = parquet.ParquetFile(path)
        total_rows = int(parquet_file.metadata.num_rows)
        if task is not None and (
            task.source_key != item.key
            or task.source_etag != item.etag
            or task.source_size != item.size
            or task.total_rows != total_rows
        ):
            raise RuntimeError(
                "Source changed for incomplete task %s %s; manual resolution required"
                % (item.dataset, item.snapshot_date)
            )
        batch_rows = task.batch_rows if task is not None else self.config.clickhouse.batch_rows
        next_batch = task.next_batch if task is not None else 0
        rows_written = task.rows_written if task is not None else 0
        self.clickhouse.record_task(
            item,
            total_rows,
            next_batch,
            batch_rows,
            rows_written,
            "running",
        )
        try:
            for batch_index, batch in enumerate(
                parquet_file.iter_batches(batch_size=batch_rows)
            ):
                if batch_index < next_batch:
                    continue
                names, columns = self._batch_columns(spec, item, batch)
                inserted = self.clickhouse.insert_columns(spec, names, columns)
                if inserted != batch.num_rows:
                    raise RuntimeError(
                        "Inserted row count mismatch for %s batch %s: expected %s, got %s"
                        % (item.key, batch_index, batch.num_rows, inserted)
                    )
                rows_written += inserted
                next_batch = batch_index + 1
                self.clickhouse.record_task(
                    item,
                    total_rows,
                    next_batch,
                    batch_rows,
                    rows_written,
                    "running",
                )
            if rows_written != total_rows:
                raise RuntimeError(
                    "Logical row count mismatch for %s: expected %s, got %s"
                    % (item.key, total_rows, rows_written)
                )
            self.clickhouse.record_task(
                item,
                total_rows,
                next_batch,
                batch_rows,
                rows_written,
                "completed",
            )
            return rows_written
        except Exception as exc:
            try:
                self.clickhouse.record_task(
                    item,
                    total_rows,
                    next_batch,
                    batch_rows,
                    rows_written,
                    "failed",
                    str(exc),
                )
            except Exception:
                pass
            raise

    def _batch_columns(
        self, spec: DatasetSpec, item: S3Object, batch
    ) -> Tuple[Sequence[str], Sequence[Sequence[object]]]:
        row_count = batch.num_rows
        timestamp = datetime.fromtimestamp(
            item.timestamp_ms / 1000, tz=timezone.utc
        )
        updated_at = datetime.now(timezone.utc)
        columns: List[Sequence[object]] = [
            [item.snapshot_date] * row_count,
            [item.block] * row_count,
            [timestamp] * row_count,
            [updated_at] * row_count,
        ]
        source_names = set(batch.schema.names)
        for output_name in spec.source_columns:
            source_name = (
                "pm_avail_by_branch"
                if output_name == "pm_avail_by_branch_json"
                else output_name
            )
            if source_name in source_names:
                values = batch.column(source_name).to_pylist()
            elif source_name in OPTIONAL_DEFAULTS:
                values = [OPTIONAL_DEFAULTS[source_name]] * row_count
            else:
                raise RuntimeError(
                    "Required Parquet column missing: %s" % source_name
                )
            if output_name == "pm_avail_by_branch_json":
                values = [
                    None
                    if value is None
                    else json.dumps(
                        value, separators=(",", ":"), sort_keys=True, default=str
                    )
                    for value in values
                ]
            columns.append(values)
        return COMMON_COLUMNS + spec.source_columns, columns

