import io
import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

import lz4.frame

from hyperliquid_etl.clickhouse_store import IngestionTask
from hyperliquid_etl.config import AppConfig, AwsConfig, ClickHouseConfig, StorageConfig
from hyperliquid_etl.node_fills import (
    BUCKET,
    DATASET,
    FILL_COLUMNS,
    IngestionCatalog,
    IngestionObject,
    NodeFillsS3,
    NodeFillsService,
    parse_block,
)


TEST_DATE = date(2026, 11, 1)
LAST_MODIFIED = datetime(2026, 11, 1, 1, tzinfo=timezone.utc)


def make_config(root: Path, batch_rows: int = 2) -> AppConfig:
    return AppConfig(
        aws=AwsConfig(profile="", region="ap-northeast-1"),
        storage=StorageConfig(
            cache_dir=root / "cache", catalog_path=root / "catalog.sqlite3"
        ),
        clickhouse=ClickHouseConfig(
            host="example",
            port=9000,
            database="laevitas",
            user="test",
            password="secret",
            batch_rows=batch_rows,
        ),
    )


def make_object(size: int = 3) -> IngestionObject:
    return IngestionObject(
        dataset=DATASET,
        bucket=BUCKET,
        source_date=TEST_DATE,
        hour=0,
        key="node_fills_by_block/hourly/20261101/0.lz4",
        etag="etag",
        size=size,
        last_modified=LAST_MODIFIED,
    )


def fill(user: str, time_ms: int, tid: int) -> list:
    return [
        user,
        {
            "coin": "BTC",
            "px": "100.25",
            "sz": "0.5",
            "side": "B",
            "time": time_ms,
            "startPosition": "1.0",
            "dir": "Open Long",
            "closedPnl": "0.0",
            "hash": "0xabc",
            "oid": 12,
            "crossed": True,
            "fee": "0.01",
            "tid": tid,
            "feeToken": "USDC",
        },
    ]


def block_line(block_number: int, events: list) -> bytes:
    return (
        json.dumps(
            {
                "local_time": "2026-11-01T00:00:00.123456789",
                "block_time": "2026-11-01T00:00:00.123456789",
                "block_number": block_number,
                "events": events,
            },
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )


class ListingClient:
    def __init__(self):
        self.calls = []

    def list_objects_v2(self, **request):
        self.calls.append(request)
        return {
            "IsTruncated": False,
            "Contents": [
                {
                    "Key": request["Prefix"] + "0.lz4",
                    "ETag": '"etag-0"',
                    "Size": 100,
                    "LastModified": LAST_MODIFIED,
                },
                {
                    "Key": request["Prefix"] + "23.lz4",
                    "ETag": '"etag-23"',
                    "Size": 200,
                    "LastModified": LAST_MODIFIED,
                },
                {
                    "Key": request["Prefix"] + "24.lz4",
                    "ETag": '"invalid"',
                    "Size": 1,
                    "LastModified": LAST_MODIFIED,
                },
            ],
        }


class ExplodingS3:
    def list_objects(self, source_date):
        raise AssertionError("cached listing should avoid S3")

    def download(self, item, destination):
        raise AssertionError("cached download should avoid S3")


class FakeClickHouse:
    def __init__(self):
        self.task = None
        self.task_history = []
        self.inserted_batches = []

    def get_ingestion_task(self, dataset, source_bucket, source_key):
        return self.task

    def record_ingestion_task(
        self,
        item,
        status,
        next_checkpoint,
        source_records_read,
        output_rows_written,
        last_error="",
    ):
        self.task = IngestionTask(
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
            last_error=last_error,
            version=len(self.task_history) + 1,
        )
        self.task_history.append(self.task)
        return self.task

    def insert_node_fills(self, column_names, columns):
        self.inserted_batches.append((tuple(column_names), columns))
        return len(columns[0])

    def ingestion_tasks_for_range(self, dataset, start, end):
        return [self.task] if self.task is not None else []


class NodeFillsTests(unittest.TestCase):
    def test_s3_listing_uses_date_prefix_and_requester_pays(self):
        client = ListingClient()
        store = NodeFillsS3(AwsConfig(profile=""), client=client)

        objects = store.list_objects(TEST_DATE)

        self.assertEqual([item.hour for item in objects], [0, 23])
        self.assertEqual(client.calls[0]["Bucket"], BUCKET)
        self.assertEqual(
            client.calls[0]["Prefix"],
            "node_fills_by_block/hourly/20261101/",
        )
        self.assertEqual(client.calls[0]["RequestPayer"], "requester")

    def test_generic_catalog_caches_scan_and_validates_download(self):
        with tempfile.TemporaryDirectory() as raw_root:
            root = Path(raw_root)
            catalog = IngestionCatalog(root / "catalog.sqlite3")
            item = make_object()
            catalog.replace_scan(DATASET, TEST_DATE, [item])
            self.assertTrue(catalog.has_scan(DATASET, TEST_DATE))
            self.assertEqual(catalog.objects_for(DATASET, TEST_DATE), [item])

            local_path = root / "0.lz4"
            local_path.write_bytes(b"abc")
            catalog.record_download(item, local_path)
            self.assertEqual(catalog.downloaded_path(item), local_path)

            changed = IngestionObject(
                dataset=item.dataset,
                bucket=item.bucket,
                source_date=item.source_date,
                hour=item.hour,
                key=item.key,
                etag="changed",
                size=item.size,
                last_modified=item.last_modified,
            )
            self.assertIsNone(catalog.downloaded_path(changed))

    def test_parse_block_preserves_nanoseconds_and_maps_fill(self):
        item = make_object()
        rows = parse_block(
            block_line(10, [fill("0xABC", 1793491200123, 99)]), item, 7
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(len(rows[0]), len(FILL_COLUMNS) - 1)
        mapped = dict(zip(FILL_COLUMNS[:-1], rows[0]))
        self.assertEqual(mapped["user"], "0xabc")
        self.assertEqual(mapped["event_time"], 1793491200123)
        self.assertEqual(mapped["block_time"] % 1_000_000_000, 123456789)
        self.assertEqual(mapped["crossed"], 1)
        self.assertEqual(mapped["source_line"], 7)
        self.assertIn('"coin":"BTC"', mapped["raw_fill_json"])

    def test_sync_streams_lz4_counts_empty_blocks_and_skips_completed(self):
        with tempfile.TemporaryDirectory() as raw_root:
            root = Path(raw_root)
            payload = b"".join(
                [
                    block_line(10, []),
                    block_line(
                        11,
                        [
                            fill("0xAAA", 1793491200001, 1),
                            fill("0xBBB", 1793491200002, 2),
                        ],
                    ),
                    block_line(12, [fill("0xCCC", 1793491200003, 3)]),
                ]
            )
            compressed = lz4.frame.compress(payload)
            item = make_object(size=len(compressed))
            local_path = root / "0.lz4"
            local_path.write_bytes(compressed)
            catalog = IngestionCatalog(root / "catalog.sqlite3")
            catalog.replace_scan(DATASET, TEST_DATE, [item])
            catalog.record_download(item, local_path)
            clickhouse = FakeClickHouse()
            service = NodeFillsService(
                make_config(root),
                catalog=catalog,
                s3=ExplodingS3(),
                clickhouse=clickhouse,
            )

            messages = service.sync(TEST_DATE, TEST_DATE)

            self.assertEqual(messages, ["sync %s: 3 fills" % item.key])
            self.assertEqual([len(batch[1][0]) for batch in clickhouse.inserted_batches], [2, 1])
            self.assertEqual(clickhouse.task.status, "completed")
            self.assertEqual(clickhouse.task.next_checkpoint, 3)
            self.assertEqual(clickhouse.task.source_records_read, 3)
            self.assertEqual(clickhouse.task.output_rows_written, 3)
            for names, columns in clickhouse.inserted_batches:
                ingested_at = columns[names.index("ingested_at")]
                self.assertEqual(len(set(ingested_at)), 1)

            second = service.sync(TEST_DATE, TEST_DATE)
            self.assertEqual(second, ["skip %s: already ingested" % item.key])
            self.assertEqual(len(clickhouse.inserted_batches), 2)

    def test_parse_failure_does_not_advance_past_buffered_rows(self):
        with tempfile.TemporaryDirectory() as raw_root:
            root = Path(raw_root)
            payload = b"".join(
                [
                    block_line(10, [fill("0xAAA", 1793491200001, 1)]),
                    b'{"block_number":11,"block_time":"bad","events":[]}\n',
                ]
            )
            compressed = lz4.frame.compress(payload)
            item = make_object(size=len(compressed))
            local_path = root / "0.lz4"
            local_path.write_bytes(compressed)
            catalog = IngestionCatalog(root / "catalog.sqlite3")
            catalog.replace_scan(DATASET, TEST_DATE, [item])
            catalog.record_download(item, local_path)
            clickhouse = FakeClickHouse()
            service = NodeFillsService(
                make_config(root, batch_rows=100),
                catalog=catalog,
                s3=ExplodingS3(),
                clickhouse=clickhouse,
            )

            with self.assertRaises(ValueError):
                service.sync(TEST_DATE, TEST_DATE)

            self.assertEqual(clickhouse.inserted_batches, [])
            self.assertEqual(clickhouse.task.status, "failed")
            self.assertEqual(clickhouse.task.next_checkpoint, 0)
            self.assertEqual(clickhouse.task.source_records_read, 0)
            self.assertEqual(clickhouse.task.output_rows_written, 0)

    def test_ddl_uses_ingested_at_and_comments_every_field(self):
        sql = (
            Path(__file__).parents[1] / "sql" / "node_fills_by_block_tables.sql"
        ).read_text(encoding="utf-8")
        self.assertNotIn("ingest_version", sql)
        self.assertIn("ENGINE = ReplacingMergeTree(ingested_at)", sql)
        column_count = sum(
            1 for line in sql.splitlines() if line.startswith("    ") and not line.startswith("        ")
        )
        comment_count = sum(
            1 for line in sql.splitlines() if line.startswith("        COMMENT '")
        )
        self.assertEqual(column_count, comment_count)


if __name__ == "__main__":
    unittest.main()
