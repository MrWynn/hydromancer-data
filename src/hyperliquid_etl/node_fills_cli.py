import json
from datetime import date
from pathlib import Path

import typer

from .config import load_config
from .node_fills import NodeFillsInspection, NodeFillsService
from .utils import format_bytes


app = typer.Typer(no_args_is_help=True, help="Hyperliquid S3 node fills tool")


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("Date must use YYYY-MM-DD format") from exc


def _service(config_path: Path) -> NodeFillsService:
    return NodeFillsService(load_config(config_path))


def _inspection_dict(record: NodeFillsInspection) -> dict:
    return {
        "date": record.source_date.isoformat(),
        "status": "available" if record.object_count else "missing",
        "objects": record.object_count,
        "compressed_bytes": record.compressed_bytes,
        "downloaded_objects": record.downloaded_count,
    }


@app.command("inspect")
def inspect_command(
    start: str = typer.Option(..., "--from", help="Inclusive UTC start date"),
    end: str = typer.Option(..., "--to", help="Inclusive UTC end date"),
    refresh: bool = typer.Option(False, help="Refresh cached S3 listings"),
    output_format: str = typer.Option("table", "--format", help="table or json"),
    config: Path = typer.Option(Path("config.toml"), exists=True, dir_okay=False),
) -> None:
    records = _service(config).inspect(_date(start), _date(end), refresh)
    if output_format == "json":
        typer.echo(json.dumps([_inspection_dict(item) for item in records], indent=2))
        return
    if output_format != "table":
        raise typer.BadParameter("--format must be table or json")
    typer.echo("date\tstatus\tobjects\tsize\tdownloaded")
    for record in records:
        typer.echo(
            "%s\t%s\t%s\t%s\t%s/%s"
            % (
                record.source_date,
                "available" if record.object_count else "missing",
                record.object_count,
                format_bytes(record.compressed_bytes),
                record.downloaded_count,
                record.object_count,
            )
        )


@app.command("sync")
def sync_command(
    start: str = typer.Option(..., "--from", help="Inclusive UTC start date"),
    end: str = typer.Option(..., "--to", help="Inclusive UTC end date"),
    dry_run: bool = typer.Option(False, "--dry-run"),
    yes: bool = typer.Option(False, "--yes", help="Accept writes and download costs"),
    config: Path = typer.Option(Path("config.toml"), exists=True, dir_okay=False),
) -> None:
    service = _service(config)
    start_date = _date(start)
    end_date = _date(end)
    plans = service.sync(start_date, end_date, dry_run=True)
    for message in plans:
        typer.echo(message)
    if dry_run or not any(message.startswith("plan ") for message in plans):
        return
    if not yes and not typer.confirm("Proceed with downloads and ClickHouse writes?"):
        raise typer.Abort()
    for message in service.sync(start_date, end_date, dry_run=False):
        typer.echo(message)


@app.command("status")
def status_command(
    start: str = typer.Option(..., "--from", help="Inclusive UTC start date"),
    end: str = typer.Option(..., "--to", help="Inclusive UTC end date"),
    config: Path = typer.Option(Path("config.toml"), exists=True, dir_okay=False),
) -> None:
    tasks = _service(config).status(_date(start), _date(end))
    typer.echo("date\thour\tstatus\trows\tnext_line\terror")
    tasks = sorted(
        tasks,
        key=lambda item: (
            item.source_date,
            int(item.source_key.rsplit("/", 1)[-1].removesuffix(".lz4")),
        ),
    )
    for task in tasks:
        hour = task.source_key.rsplit("/", 1)[-1].removesuffix(".lz4")
        typer.echo(
            "%s\t%s\t%s\t%s\t%s\t%s"
            % (
                task.source_date,
                hour,
                task.status,
                task.output_rows_written,
                task.next_checkpoint,
                task.last_error.replace("\t", " ").replace("\n", " "),
            )
        )
