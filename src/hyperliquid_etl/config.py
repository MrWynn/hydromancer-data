from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict

try:
    import tomllib  # type: ignore[attr-defined]
except ModuleNotFoundError:  # pragma: no cover - exercised on Python < 3.11
    tomllib = None  # type: ignore[assignment]


@dataclass(frozen=True)
class AwsConfig:
    profile: str = "default"
    region: str = "ap-northeast-1"
    bucket: str = "hydromancer-reservoir"


@dataclass(frozen=True)
class StorageConfig:
    cache_dir: Path
    catalog_path: Path


@dataclass(frozen=True)
class ClickHouseConfig:
    host: str
    port: int
    database: str
    user: str
    password: str = field(repr=False)
    secure: bool = False
    batch_rows: int = 100_000


@dataclass(frozen=True)
class AppConfig:
    aws: AwsConfig
    storage: StorageConfig
    clickhouse: ClickHouseConfig


def _load_toml(path: Path) -> Dict[str, Any]:
    parser = tomllib
    if parser is None:
        try:
            import tomli as parser  # type: ignore[no-redef]
        except ModuleNotFoundError as exc:
            raise RuntimeError("Python 3.9/3.10 requires the 'tomli' package") from exc
    with path.open("rb") as handle:
        return parser.load(handle)


def _resolve(base: Path, value: str) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else (base / path).resolve()


def load_config(path: Path) -> AppConfig:
    path = path.expanduser().resolve()
    raw = _load_toml(path)
    base = path.parent
    aws_raw = raw.get("aws", {})
    storage_raw = raw.get("storage", {})
    clickhouse_raw = raw.get("clickhouse", {})

    required = ("host", "port", "database", "user", "password")
    missing = [key for key in required if key not in clickhouse_raw]
    if missing:
        raise ValueError("Missing ClickHouse config keys: " + ", ".join(missing))
    if clickhouse_raw["database"] != "laevitas":
        raise ValueError("ClickHouse database must be 'laevitas'")

    cache_dir = _resolve(base, storage_raw.get("cache_dir", "./data/cache"))
    catalog_path = _resolve(
        base, storage_raw.get("catalog_path", "./data/catalog.sqlite3")
    )
    batch_rows = int(clickhouse_raw.get("batch_rows", 100_000))
    if batch_rows <= 0:
        raise ValueError("clickhouse.batch_rows must be positive")

    return AppConfig(
        aws=AwsConfig(
            profile=str(aws_raw.get("profile", "default")),
            region=str(aws_raw.get("region", "ap-northeast-1")),
            bucket=str(aws_raw.get("bucket", "hydromancer-reservoir")),
        ),
        storage=StorageConfig(cache_dir=cache_dir, catalog_path=catalog_path),
        clickhouse=ClickHouseConfig(
            host=str(clickhouse_raw["host"]),
            port=int(clickhouse_raw["port"]),
            database=str(clickhouse_raw["database"]),
            user=str(clickhouse_raw["user"]),
            password=str(clickhouse_raw["password"]),
            secure=bool(clickhouse_raw.get("secure", False)),
            batch_rows=batch_rows,
        ),
    )
