import os
import re
from datetime import date
from pathlib import Path
from typing import List, Optional

from .catalog import S3Object
from .config import AwsConfig
from .constants import DatasetSpec


SNAPSHOT_FILE_RE = re.compile(r"^(?P<block>\d+)_(?P<timestamp>\d+)\.parquet$")


class S3Store:
    def __init__(self, config: AwsConfig, client=None):
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

    def list_objects(self, spec: DatasetSpec, snapshot_date: date) -> List[S3Object]:
        prefix = "%s/date=%s/" % (spec.prefix, snapshot_date.isoformat())
        token: Optional[str] = None
        result: List[S3Object] = []
        while True:
            request = {
                "Bucket": self.config.bucket,
                "Prefix": prefix,
                "RequestPayer": "requester",
            }
            if token:
                request["ContinuationToken"] = token
            response = self.client.list_objects_v2(**request)
            for raw in response.get("Contents", []):
                name = raw["Key"].rsplit("/", 1)[-1]
                match = SNAPSHOT_FILE_RE.match(name)
                if not match:
                    continue
                result.append(
                    S3Object(
                        dataset=spec.name,
                        snapshot_date=snapshot_date,
                        key=raw["Key"],
                        etag=str(raw.get("ETag", "")).strip('"'),
                        size=int(raw["Size"]),
                        block=int(match.group("block")),
                        timestamp_ms=int(match.group("timestamp")),
                    )
                )
            if not response.get("IsTruncated"):
                break
            token = response["NextContinuationToken"]
        return sorted(result, key=lambda item: item.key)

    def download(self, item: S3Object, destination: Path) -> Path:
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(destination.name + ".part")
        try:
            response = self.client.get_object(
                Bucket=self.config.bucket,
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

