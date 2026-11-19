import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, List, Optional


@dataclass(frozen=True)
class S3Object:
    dataset: str
    snapshot_date: date
    key: str
    etag: str
    size: int
    block: int
    timestamp_ms: int


class Catalog:
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
                CREATE TABLE IF NOT EXISTS scans (
                    dataset TEXT NOT NULL,
                    snapshot_date TEXT NOT NULL,
                    scanned_at TEXT NOT NULL,
                    PRIMARY KEY (dataset, snapshot_date)
                );
                CREATE TABLE IF NOT EXISTS objects (
                    dataset TEXT NOT NULL,
                    snapshot_date TEXT NOT NULL,
                    object_key TEXT NOT NULL PRIMARY KEY,
                    etag TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    block INTEGER NOT NULL,
                    timestamp_ms INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS objects_by_date
                    ON objects(dataset, snapshot_date);
                CREATE TABLE IF NOT EXISTS downloads (
                    object_key TEXT NOT NULL PRIMARY KEY,
                    etag TEXT NOT NULL,
                    local_path TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    downloaded_at TEXT NOT NULL
                );
                """
            )

    def has_scan(self, dataset: str, snapshot_date: date) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT 1 FROM scans WHERE dataset = ? AND snapshot_date = ?",
                (dataset, snapshot_date.isoformat()),
            ).fetchone()
        return row is not None

    def replace_scan(
        self, dataset: str, snapshot_date: date, objects: Iterable[S3Object]
    ) -> None:
        object_list = list(objects)
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM objects WHERE dataset = ? AND snapshot_date = ?",
                (dataset, snapshot_date.isoformat()),
            )
            connection.executemany(
                """
                INSERT INTO objects (
                    dataset, snapshot_date, object_key, etag, size, block,
                    timestamp_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        item.dataset,
                        item.snapshot_date.isoformat(),
                        item.key,
                        item.etag,
                        item.size,
                        item.block,
                        item.timestamp_ms,
                    )
                    for item in object_list
                ],
            )
            connection.execute(
                """
                INSERT INTO scans(dataset, snapshot_date, scanned_at)
                VALUES (?, ?, ?)
                ON CONFLICT(dataset, snapshot_date)
                DO UPDATE SET scanned_at = excluded.scanned_at
                """,
                (dataset, snapshot_date.isoformat(), now),
            )

    def objects_for(self, dataset: str, snapshot_date: date) -> List[S3Object]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT dataset, snapshot_date, object_key, etag, size, block,
                       timestamp_ms
                FROM objects
                WHERE dataset = ? AND snapshot_date = ?
                ORDER BY object_key
                """,
                (dataset, snapshot_date.isoformat()),
            ).fetchall()
        return [
            S3Object(
                dataset=row["dataset"],
                snapshot_date=date.fromisoformat(row["snapshot_date"]),
                key=row["object_key"],
                etag=row["etag"],
                size=row["size"],
                block=row["block"],
                timestamp_ms=row["timestamp_ms"],
            )
            for row in rows
        ]

    def record_download(self, item: S3Object, local_path: Path) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO downloads(
                    object_key, etag, local_path, size, downloaded_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(object_key) DO UPDATE SET
                    etag = excluded.etag,
                    local_path = excluded.local_path,
                    size = excluded.size,
                    downloaded_at = excluded.downloaded_at
                """,
                (
                    item.key,
                    item.etag,
                    str(local_path),
                    item.size,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )

    def downloaded_path(self, item: S3Object) -> Optional[Path]:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT local_path, size, etag FROM downloads WHERE object_key = ?
                """,
                (item.key,),
            ).fetchone()
        if row is None:
            return None
        if row["etag"] != item.etag:
            return None
        path = Path(row["local_path"])
        if not path.is_file() or path.stat().st_size != row["size"]:
            return None
        return path

    def delete_download(self, object_key: str, etag: str) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT local_path FROM downloads "
                "WHERE object_key = ? AND etag = ?",
                (object_key, etag),
            ).fetchone()
            if row is None:
                return False
            Path(row["local_path"]).unlink(missing_ok=True)
            connection.execute(
                "DELETE FROM downloads WHERE object_key = ? AND etag = ?",
                (object_key, etag),
            )
        return True

