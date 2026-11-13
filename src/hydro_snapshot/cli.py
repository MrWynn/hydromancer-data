import json
from datetime import date
from pathlib import Path
from typing import List, Sequence

try:
    import typer
except ModuleNotFoundError as exc:  # pragma: no cover
    raise RuntimeError("Install project dependencies before running the CLI") from exc

from .config import load_config
from .constants import DATASETS
from .service import CostEstimate, InspectionRecord, SnapshotService
from .utils import format_bytes


app = typer.Typer(no_args_is_help=True, help="Hydromancer snapshot data tool")
GIB = 1024 ** 3
DEFAULT_USD_PER_GB = 0.0


def _datasets(value: str) -> Sequence[str]:
    names = [item.strip() for item in value.split(",") if item.strip()]
    invalid = [name for name in names if name not in DATASETS]
    if invalid:
        raise typer.BadParameter(
            "Unknown dataset(s): %s. Valid values: %s"
            % (", ".join(invalid), ", ".join(DATASETS))
        )
    if not names:
        raise typer.BadParameter("At least one dataset is required")
    return names


def _service(config_path: Path) -> SnapshotService:
    return SnapshotService(load_config(config_path))


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("Date must use YYYY-MM-DD format") from exc


def _inspection_dict(record: InspectionRecord) -> dict:
    return {
        "date": record.snapshot_date.isoformat(),
        "dataset": record.dataset,
        "status": "available" if record.available else "missing",
        "objects": record.object_count,
        "compressed_bytes": record.compressed_bytes,
        "block": record.block,
        "timestamp_ms": record.timestamp_ms,
        "downloaded": record.downloaded,
    }


def _cost_text(
    estimate: CostEstimate, usd_per_gb: float = DEFAULT_USD_PER_GB
) -> str:
    sample_gib = [item.compressed_bytes / GIB for item in estimate.samples]
    per_day_objects = "%.0f object%s" % (
        estimate.objects_per_day,
        "" if estimate.objects_per_day == 1 else "s",
    )
    total_objects = "%.0f object%s" % (
        estimate.total_objects,
        "" if estimate.total_objects == 1 else "s",
    )
    lines = [
        "dataset      %s" % estimate.dataset,
        "range        %s day(s), %s sampled"
        % (estimate.days, len(estimate.samples)),
        "per day      %.3f GiB  (%s)"
        % (estimate.bytes_per_day / GIB, per_day_objects),
        "TOTAL        %.2f GiB  (%s)"
        % (estimate.total_bytes / GIB, total_objects),
        "EGRESS COST  ~$%.2f  (@ $%.2f/GB, requester-pays)"
        % (estimate.total_bytes / GIB * usd_per_gb, usd_per_gb),
        "",
        "samples:",
    ]
    for sample in estimate.samples:
        lines.append(
            "  %s  %.3f GiB  %s obj"
            % (
                sample.snapshot_date,
                sample.compressed_bytes / GIB,
                sample.object_count,
            )
        )
    if estimate.days > len(estimate.samples):
        lines.extend(
            [
                "",
                "note: extrapolated from %s sampled day(s) "
                "(min %.3f / max %.3f GiB). Actual volume varies by date."
                % (len(estimate.samples), min(sample_gib), max(sample_gib)),
            ]
        )
    empty_dates = [
        str(item.snapshot_date) for item in estimate.samples if not item.object_count
    ]
    if empty_dates:
        lines.extend(
            [
                "",
                "WARNING: sampled day(s) found no objects: %s. "
                "This estimate may be too low." % ", ".join(empty_dates),
            ]
        )
    lines.extend(
        [
            "",
            "note: excludes S3 LIST/GET request charges; actual billed cost is "
            "available from AWS Cost Explorer/CUR after usage.",
        ]
    )
    return "\n".join(lines)


@app.command("cost")
def cost_command(
    dataset: str = typer.Option(..., "--dataset", help="Comma-separated datasets"),
    start: str = typer.Option(..., "--start", help="Inclusive UTC start date"),
    end: str = typer.Option(..., "--end", help="Inclusive UTC end date"),
    usd_per_gb: float = typer.Option(
        DEFAULT_USD_PER_GB,
        "--usd-per-gb",
        help="Data transfer rate; default is same-region S3 to EC2",
    ),
    config: Path = typer.Option(Path("config.toml"), exists=True, dir_okay=False),
) -> None:
    service = _service(config)
    estimates = [
        service.estimate_cost(_date(start), _date(end), name)
        for name in _datasets(dataset)
    ]
    typer.echo("\n\n".join(_cost_text(item, usd_per_gb) for item in estimates))


@app.command("inspect")
def inspect_command(
    start: str = typer.Option(..., "--from", help="Inclusive UTC start date"),
    end: str = typer.Option(..., "--to", help="Inclusive UTC end date"),
    dataset: str = typer.Option(
        "perp,spot,account_values", "--dataset", help="Comma-separated datasets"
    ),
    refresh: bool = typer.Option(False, help="Refresh cached S3 listings"),
    output_format: str = typer.Option("table", "--format", help="table or json"),
    config: Path = typer.Option(Path("config.toml"), exists=True, dir_okay=False),
) -> None:
    records = _service(config).inspect(
        _date(start), _date(end), _datasets(dataset), refresh
    )
    if output_format == "json":
        typer.echo(json.dumps([_inspection_dict(item) for item in records], indent=2))
        return
    if output_format != "table":
        raise typer.BadParameter("--format must be table or json")
    typer.echo("date\tdataset\tstatus\tobjects\tsize\tblock\tdownloaded")
    for record in records:
        typer.echo(
            "%s\t%s\t%s\t%s\t%s\t%s\t%s"
            % (
                record.snapshot_date,
                record.dataset,
                "available" if record.available else "missing",
                record.object_count,
                format_bytes(record.compressed_bytes),
                record.block if record.block is not None else "-",
                "yes" if record.downloaded else "no",
            )
        )


@app.command("download")
def download_command(
    start: str = typer.Option(..., "--from", help="Inclusive UTC start date"),
    end: str = typer.Option(..., "--to", help="Inclusive UTC end date"),
    dataset: str = typer.Option(
        "perp,spot,account_values", "--dataset", help="Comma-separated datasets"
    ),
    yes: bool = typer.Option(False, "--yes", help="Accept requester-pays charges"),
    config: Path = typer.Option(Path("config.toml"), exists=True, dir_okay=False),
) -> None:
    service = _service(config)
    objects = service.objects_for_range(_date(start), _date(end), _datasets(dataset))
    pending = service.pending_downloads(objects)
    typer.echo(
        "%s object(s), %s to download (%s)"
        % (len(objects), len(pending), format_bytes(sum(item.size for item in pending)))
    )
    if not pending:
        return
    if not yes and not typer.confirm("Proceed with requester-pays downloads?"):
        raise typer.Abort()
    for path in service.download(objects):
        typer.echo(str(path))


@app.command("sync")
def sync_command(
    start: str = typer.Option(..., "--from", help="Inclusive UTC start date"),
    end: str = typer.Option(..., "--to", help="Inclusive UTC end date"),
    dataset: str = typer.Option(
        "perp,spot,account_values", "--dataset", help="Comma-separated datasets"
    ),
    dry_run: bool = typer.Option(False, "--dry-run"),
    yes: bool = typer.Option(False, "--yes", help="Accept writes and download costs"),
    config: Path = typer.Option(Path("config.toml"), exists=True, dir_okay=False),
) -> None:
    service = _service(config)
    datasets = _datasets(dataset)
    start_date = _date(start)
    end_date = _date(end)
    plans = service.sync(start_date, end_date, datasets, dry_run=True)
    for message in plans:
        typer.echo(message)
    if dry_run or not any(message.startswith("plan ") for message in plans):
        return
    if not yes and not typer.confirm("Proceed with downloads and ClickHouse writes?"):
        raise typer.Abort()
    for message in service.sync(start_date, end_date, datasets, dry_run=False):
        typer.echo(message)


@app.command("status")
def status_command(
    start: str = typer.Option(..., "--from", help="Inclusive UTC start date"),
    end: str = typer.Option(..., "--to", help="Inclusive UTC end date"),
    dataset: str = typer.Option(
        "perp,spot,account_values", "--dataset", help="Comma-separated datasets"
    ),
    config: Path = typer.Option(Path("config.toml"), exists=True, dir_okay=False),
) -> None:
    service = _service(config)
    datasets = _datasets(dataset)
    tasks = service.clickhouse.tasks_for_range(
        datasets, _date(start), _date(end)
    )
    typer.echo("dataset\tdate\tstatus\trows\tnext_batch\terror")
    for task in tasks:
        typer.echo(
            "%s\t%s\t%s\t%s/%s\t%s\t%s"
            % (
                task.dataset,
                task.snapshot_date,
                task.status,
                task.rows_written,
                task.total_rows,
                task.next_batch,
                task.last_error.replace("\t", " ").replace("\n", " "),
            )
        )


if __name__ == "__main__":  # pragma: no cover
    app()

