import tempfile
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from hydro_snapshot.catalog import Catalog, S3Object
from hydro_snapshot.clickhouse_store import ClickHouseStore
from hydro_snapshot.config import (
    AppConfig,
    AwsConfig,
    ClickHouseConfig,
    StorageConfig,
)
from hydro_snapshot.constants import ALLOWED_TABLES, DATASETS
from hydro_snapshot.s3_store import S3Store
from hydro_snapshot.cli import GIB, _cost_text
from hydro_snapshot.service import SnapshotService
from hydro_snapshot.utils import date_range, format_bytes


TEST_DATE = date(2026, 11, 9)


def make_config(root: Path) -> AppConfig:
    return AppConfig(
        aws=AwsConfig(),
        storage=StorageConfig(
            cache_dir=root / "cache", catalog_path=root / "catalog.sqlite3"
        ),
        clickhouse=ClickHouseConfig(
            host="example", port=9000, database="laevitas", user="test", password="secret"
        ),
    )


class FakeS3Client:
    def __init__(self):
        self.calls = []

    def list_objects_v2(self, **request):
        self.calls.append(request)
        return {
            "IsTruncated": False,
            "Contents": [
                {
                    "Key": request["Prefix"] + "100_200.parquet",
                    "ETag": '"etag"',
                    "Size": 123,
                },
                {"Key": request["Prefix"] + "README", "Size": 1},
            ],
        }


class FakeClickHouse:
    def __init__(self, ingested=False):
        self.ingested = ingested

    def get_task(self, dataset, snapshot_date):
        if not self.ingested:
            return None
        return SimpleNamespace(status="completed")


class ExplodingS3:
    def list_objects(self, spec, snapshot_date):
        raise AssertionError("S3 must not be accessed")


class CountingS3:
    def __init__(self):
        self.calls = 0

    def list_objects(self, spec, snapshot_date):
        self.calls += 1
        return [
            S3Object(
                spec.name,
                snapshot_date,
                "%s/date=%s/100_200.parquet"
                % (spec.prefix, snapshot_date.isoformat()),
                "etag",
                123,
                100,
                200,
            )
        ]


class CostS3:
    def __init__(self, sizes):
        self.sizes = sizes

    def list_objects(self, spec, snapshot_date):
        return [
            S3Object(
                spec.name,
                snapshot_date,
                "%s/date=%s/%s_200.parquet"
                % (spec.prefix, snapshot_date.isoformat(), index),
                "etag-%s" % index,
                size,
                index,
                200,
            )
            for index, size in enumerate(self.sizes.get(snapshot_date, []), start=1)
        ]


class FakeDriver:
    def execute(self, query, params=None, **kwargs):
        return [(0,)]


class FakeArrowColumn:
    def __init__(self, values):
        self.values = values

    def to_pylist(self):
        return self.values


class FakeArrowBatch:
    def __init__(self, values):
        self.values = values
        self.num_rows = len(next(iter(values.values())))
        self.schema = SimpleNamespace(names=list(values))

    def column(self, name):
        return FakeArrowColumn(self.values[name])


class CoreTests(unittest.TestCase):
    def test_date_range_is_inclusive(self):
        self.assertEqual(
            list(date_range(date(2026, 1, 1), date(2026, 1, 3))),
            [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)],
        )
        with self.assertRaises(ValueError):
            list(date_range(date(2026, 1, 2), date(2026, 1, 1)))

    def test_format_bytes(self):
        self.assertEqual(format_bytes(1024), "1.00 KiB")

    def test_catalog_scan_and_download_validation(self):
        with tempfile.TemporaryDirectory() as raw_root:
            root = Path(raw_root)
            catalog = Catalog(root / "catalog.sqlite3")
            item = S3Object("perp", TEST_DATE, "key", "etag", 3, 100, 200)
            catalog.replace_scan("perp", TEST_DATE, [item])
            self.assertTrue(catalog.has_scan("perp", TEST_DATE))
            self.assertEqual(catalog.objects_for("perp", TEST_DATE), [item])
            local = root / "file.parquet"
            local.write_bytes(b"abc")
            catalog.record_download(item, local)
            self.assertEqual(catalog.downloaded_path(item), local)
            changed = S3Object("perp", TEST_DATE, "key", "new-etag", 3, 100, 200)
            self.assertIsNone(catalog.downloaded_path(changed))
            local.write_bytes(b"bad-size")
            self.assertIsNone(catalog.downloaded_path(item))

    def test_s3_listing_always_uses_requester_pays(self):
        client = FakeS3Client()
        store = S3Store(AwsConfig(), client=client)
        objects = store.list_objects(DATASETS["perp"], TEST_DATE)
        self.assertEqual(len(objects), 1)
        self.assertEqual(objects[0].block, 100)
        self.assertEqual(client.calls[0]["RequestPayer"], "requester")

    def test_successful_sync_skips_before_s3(self):
        with tempfile.TemporaryDirectory() as raw_root:
            root = Path(raw_root)
            service = SnapshotService(
                make_config(root),
                s3=ExplodingS3(),
                clickhouse=FakeClickHouse(ingested=True),
            )
            messages = service.sync(TEST_DATE, TEST_DATE, ["perp"])
            self.assertEqual(messages, ["skip perp 2026-11-09: already ingested"])

    def test_inspect_reuses_cached_s3_listing(self):
        with tempfile.TemporaryDirectory() as raw_root:
            root = Path(raw_root)
            s3 = CountingS3()
            service = SnapshotService(make_config(root), s3=s3)
            first = service.inspect(TEST_DATE, TEST_DATE, ["perp"])
            second = service.inspect(TEST_DATE, TEST_DATE, ["perp"])
            self.assertTrue(first[0].available)
            self.assertEqual(first, second)
            self.assertEqual(s3.calls, 1)

    def test_cost_estimate_samples_edges_and_midpoint(self):
        with tempfile.TemporaryDirectory() as raw_root:
            root = Path(raw_root)
            start = date(2026, 6, 1)
            middle = date(2026, 6, 15)
            end = date(2026, 6, 30)
            service = SnapshotService(
                make_config(root),
                s3=CostS3(
                    {
                        start: [GIB],
                        middle: [GIB, GIB],
                        end: [GIB, GIB, GIB],
                    }
                ),
            )
            estimate = service.estimate_cost(start, end, "perp")
            self.assertEqual(
                [item.snapshot_date for item in estimate.samples],
                [start, middle, end],
            )
            self.assertEqual(estimate.bytes_per_day, 2 * GIB)
            self.assertEqual(estimate.objects_per_day, 2)
            self.assertEqual(estimate.total_bytes, 60 * GIB)
            output = _cost_text(estimate, 0.09)
            self.assertIn("range        30 day(s), 3 sampled", output)
            self.assertIn("TOTAL        60.00 GiB  (60 objects)", output)
            self.assertIn("EGRESS COST  ~$5.40", output)
            single_day_output = _cost_text(
                service.estimate_cost(start, start, "perp")
            )
            self.assertIn("per day      1.000 GiB  (1 object)", single_day_output)

    def test_clickhouse_table_allowlist(self):
        config = ClickHouseConfig(
            host="example", port=9000, database="laevitas", user="test", password="secret"
        )
        store = ClickHouseStore(config, client=FakeDriver())
        self.assertEqual(
            store._table("hydromancer_snapshot_perp"),
            "laevitas.hydromancer_snapshot_perp",
        )
        with self.assertRaises(ValueError):
            store._table("unrelated_table")

    def test_clickhouse_client_does_not_enable_numpy_mode(self):
        config = ClickHouseConfig(
            host="example", port=9000, database="laevitas", user="test", password="secret"
        )
        with patch("clickhouse_driver.Client") as client_class:
            ClickHouseStore(config)
        self.assertNotIn("settings", client_class.call_args.kwargs)

    def test_allowlist_contains_only_four_project_tables(self):
        self.assertEqual(
            ALLOWED_TABLES,
            {
                "hydromancer_snapshot_perp",
                "hydromancer_snapshot_spot",
                "hydromancer_snapshot_account_values",
                "hydromancer_sync_tasks",
            },
        )

    def test_ddl_contains_no_staging_tables(self):
        sql = (Path(__file__).parents[1] / "sql" / "clickhouse_tables.sql").read_text(
            encoding="utf-8"
        )
        self.assertEqual(sql.count("CREATE TABLE IF NOT EXISTS"), 4)
        self.assertNotIn("staging", sql.lower())
        self.assertNotIn("hydromancer_ingestion_log", sql)
        self.assertEqual(sql.count("ENGINE = ReplacingMergeTree(updated_at)"), 3)
        self.assertEqual(sql.count("updated_at DateTime64(6, 'UTC')"), 3)
        for order_by in (
            "ORDER BY (snapshot_date, market, user)",
            "ORDER BY (snapshot_date, token, user)",
            "ORDER BY (snapshot_date, dex, user)",
        ):
            self.assertIn(order_by, sql)

    def test_password_is_not_in_config_repr(self):
        config = ClickHouseConfig(
            host="example", port=9000, database="laevitas", user="test", password="secret"
        )
        self.assertNotIn("secret", repr(config))

    def test_clickhouse_database_is_fixed(self):
        config = ClickHouseConfig(
            host="example", port=9000, database="other", user="test", password="secret"
        )
        with self.assertRaises(ValueError):
            ClickHouseStore(config, client=FakeDriver())

    def test_batch_adds_updated_at_version_column(self):
        with tempfile.TemporaryDirectory() as raw_root:
            service = SnapshotService(make_config(Path(raw_root)), s3=ExplodingS3())
            item = S3Object("spot", TEST_DATE, "key", "etag", 3, 100, 200)
            batch = FakeArrowBatch(
                {
                    "user": ["0x1", "0x2"],
                    "token": ["USDC", "HYPE"],
                    "balance": [1.0, 2.0],
                    "entry_value": [1.0, 2.0],
                }
            )
            names, columns = service._batch_columns(DATASETS["spot"], item, batch)
            updated_at_index = names.index("updated_at")
            self.assertEqual(len(columns[updated_at_index]), 2)
            self.assertEqual(
                columns[updated_at_index][0], columns[updated_at_index][1]
            )
            self.assertIsNotNone(columns[updated_at_index][0].tzinfo)


if __name__ == "__main__":
    unittest.main()

