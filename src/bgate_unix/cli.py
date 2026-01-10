"""CLI for bgate-unix.

Exposes the deduplication engine to the shell with high-performance defaults.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer
from loguru import logger
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

from bgate_unix import __version__
from bgate_unix.engine import DedupeResult, FileDeduplicator

app = typer.Typer(
    name="bgate",
    help="High-performance Unix file deduplication engine.",
    add_completion=False,
    rich_markup_mode="rich",
)
console = Console()


def setup_logging(verbose: bool, json_mode: bool = False) -> None:
    """Configure loguru for terminal output. Stderr for logs if JSON mode."""
    logger.remove()
    level = "ERROR" if json_mode and not verbose else ("DEBUG" if verbose else "WARNING")

    logger.add(
        RichHandler(
            rich_tracebacks=True,
            console=Console(stderr=True),
            show_time=False,
            show_path=verbose,  # Hide path:line unless verbose
            markup=True,
        ),
        format="{message}",
        level=level,
    )


def version_callback(value: bool):
    """Print the version and exit."""
    if value:
        console.print(f"bgate-unix v{__version__}")
        raise typer.Exit()


@app.callback()
def main(
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Enable debug logging.")] = False,
    _version: Annotated[
        bool,
        typer.Option(
            "--version", callback=version_callback, is_eager=True, help="Show version and exit."
        ),
    ] = False,
) -> None:
    """Fingerprinting gatekeeper for high-volume Unix pipelines."""
    setup_logging(verbose)


@app.command()
def scan(
    ctx: typer.Context,
    path: Annotated[Path, typer.Argument(help="File or directory to scan.")],
    db: Annotated[Path, typer.Option("--db", help="Path to SQLite index database.")] = Path(
        "dedupe.db"
    ),
    processing_dir: Annotated[
        Path | None, typer.Option("--into", help="Directory to move unique files into.")
    ] = None,
    move: Annotated[
        bool, typer.Option("--move", help="AUTHORIZE file moves. Default is dry-run (read-only).")
    ] = False,
    recursive: Annotated[bool, typer.Option("--recursive", "-r", help="Recursive scan.")] = False,
    tags: Annotated[
        list[str] | None, typer.Option("--tag", help="Add metadata tags (format: key:value).")
    ] = None,
    ignore: Annotated[
        list[str] | None, typer.Option("--ignore", "-i", help="Additional patterns to ignore.")
    ] = None,
    json_output: Annotated[
        bool, typer.Option("--json", help="Output results in JSON format.")
    ] = False,
) -> None:
    """Scan files for duplicates and optionally move unique files."""
    verbose = ctx.parent.params.get("verbose", False) if ctx and ctx.parent else False
    setup_logging(verbose, json_mode=json_output)

    if not path.exists():
        if json_output:
            print(json.dumps({"error": f"Path {path} does not exist"}))
        else:
            console.print(
                f'[bold red]Error:[/bold red] Path [yellow]"{path}"[/yellow] does not exist.'
            )
        raise typer.Exit(1)

    # Parse tags
    parsed_tags = {}
    if tags:
        for tag in tags:
            if ":" not in tag:
                if json_output:
                    print(json.dumps({"error": f"Invalid tag format: {tag}. Use key:value"}))
                else:
                    console.print(
                        f"[bold red]Error:[/bold red] Invalid tag format: {tag}. Use key:value"
                    )
                raise typer.Exit(1)
            key, value = tag.split(":", 1)
            parsed_tags[key.strip()] = value.strip()

    # Safety logic: Default is dry-run unless --move is specified
    is_dry_run = not move
    active_processing_dir = processing_dir if move else None

    if not json_output:
        if is_dry_run and processing_dir:
            console.print("[bold yellow]Dry Run Mode: No files will be moved.[/bold yellow]")
            console.print("[dim]Pass --move to execute changes.[/dim]\n")
        elif is_dry_run:
            console.print(
                "[bold yellow]Read-Only Mode: Scanning for duplicates only.[/bold yellow]\n"
            )

    try:
        with FileDeduplicator(db, processing_dir=active_processing_dir) as deduper:
            results = []

            if not json_output:
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    TimeElapsedColumn(),
                    console=console,
                    transient=True,
                ) as progress:
                    task = progress.add_task(f'Scanning "{path}"...', total=None)

                    if path.is_file():
                        res = deduper.process_file(path, tags=parsed_tags)
                        results.append(res)
                    else:
                        for result in deduper.process_directory(
                            path, recursive=recursive, ignore_patterns=ignore, tags=parsed_tags
                        ):
                            results.append(result)
                            progress.update(
                                task,
                                description=f'Scanning: [cyan]"{result.original_path.name}"[/cyan]',
                            )
            else:
                if path.is_file():
                    results.append(deduper.process_file(path, tags=parsed_tags))
                else:
                    results = list(
                        deduper.process_directory(
                            path, recursive=recursive, ignore_patterns=ignore, tags=parsed_tags
                        )
                    )

            # Compile counts
            unique_list = [r for r in results if r.result == DedupeResult.UNIQUE]
            dupes_list = [r for r in results if r.result == DedupeResult.DUPLICATE]
            skipped_list = [r for r in results if r.result == DedupeResult.SKIPPED]

            if json_output:
                output = {
                    "summary": {
                        "unique": len(unique_list),
                        "duplicate": len(dupes_list),
                        "skipped": len(skipped_list),
                        "total": len(results),
                    },
                    "results": [
                        {
                            "original_path": str(r.original_path),
                            "stored_path": str(r.path) if r.path else None,
                            "result": r.result.value,
                            "tier": r.tier,
                            "duplicate_of": str(r.duplicate_of) if r.duplicate_of else None,
                            "tags": r.tags if hasattr(r, "tags") else None,
                            "error": r.error,
                        }
                        for r in results
                    ],
                }
                print(json.dumps(output, indent=2))
            else:
                table = Table(title="Deduplication Summary", box=None, show_header=True)
                table.add_column("Result", style="cyan")
                table.add_column("Count", justify="right", style="magenta")

                table.add_row("Unique", str(len(unique_list)))
                table.add_row("Duplicate", str(len(dupes_list)))
                table.add_row("Skipped", str(len(skipped_list)))

                console.print(table)

                if is_dry_run and processing_dir and len(unique_list) > 0:
                    console.print(
                        f'\n[bold yellow]Dry run summary:[/bold yellow] {len(unique_list)} files would be moved to "{processing_dir}"'
                    )
                elif processing_dir and len(unique_list) > 0:
                    console.print(
                        f'\n[bold green]Success:[/bold green] {len(unique_list)} files moved to "{processing_dir}"'
                    )

    except Exception as e:
        if json_output:
            print(json.dumps({"error": str(e)}))
        else:
            logger.error("Scan failed: {}", e)
        raise typer.Exit(1) from e


@app.command()
def recover(
    db: Annotated[Path, typer.Option("--db", help="Path to SQLite index database.")] = Path(
        "dedupe.db"
    ),
) -> None:
    """Attempt to recover orphaned files from previous interrupted operations."""
    try:
        with FileDeduplicator(db) as deduper:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                progress.add_task("Recovering orphans...", total=None)
                recovery = deduper.recover_orphans()

            if recovery["total"] == 0:
                console.print("[bold green]Zero orphans found.[/bold green]")
            else:
                console.print(
                    f"Recovery complete: [green]{recovery['recovered']}[/green] recovered, "
                    f"[red]{recovery['failed']}[/red] failed."
                )
    except Exception as e:
        logger.error("Recovery failed: {}", e)
        raise typer.Exit(1) from e


@app.command()
def stats(
    ctx: typer.Context,
    db: Annotated[Path, typer.Option("--db", help="Path to SQLite index database.")] = Path(
        "dedupe.db"
    ),
    json_output: Annotated[
        bool, typer.Option("--json", help="Output results in JSON format.")
    ] = False,
) -> None:
    """Show database statistics and index health."""
    verbose = ctx.parent.params.get("verbose", False) if ctx and ctx.parent else False
    setup_logging(verbose, json_mode=json_output)
    try:
        with FileDeduplicator(db) as deduper:
            s = deduper.stats
            if json_output:
                print(json.dumps(s, indent=2))
            else:
                table = Table(title=f"Index: [cyan]{db.name}[/cyan]", box=None)
                table.add_column("Metric", style="cyan")
                table.add_column("Value", justify="right", style="magenta")

                table.add_row("Unique Sizes", str(s["unique_sizes"]))
                table.add_row("Fringe Entries", str(s["fringe_entries"]))
                table.add_row("Full Entries", str(s["full_entries"]))
                table.add_row("Schema Version", f"v{s['schema_version']}")
                table.add_row("Pending Orphans", str(s["orphan_count"]))
                table.add_row("Pending Journal", str(s["pending_journal"]))

                console.print(table)
    except Exception as e:
        if json_output:
            print(json.dumps({"error": str(e)}))
        else:
            logger.error("Failed to fetch stats: {}", e)
        raise typer.Exit(1) from e


if __name__ == "__main__":
    app()
