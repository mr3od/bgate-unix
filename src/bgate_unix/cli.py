"""CLI for bgate-unix.

Exposes the deduplication engine to the shell with high-performance defaults.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated

import typer
from loguru import logger
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn
from rich.table import Table

from bgate_unix.engine import DedupeResult, FileDeduplicator

app = typer.Typer(
    name="bgate",
    help="High-performance Unix file deduplication engine.",
    add_completion=False,
    rich_markup_mode="rich",
)
console = Console()


def setup_logging(verbose: bool) -> None:
    """Configure loguru to use RichHandler for beautiful terminal output."""
    logger.remove()
    level = "DEBUG" if verbose else "INFO"
    logger.add(
        RichHandler(rich_tracebacks=True, console=console, show_time=False),
        format="{message}",
        level=level,
    )


@app.callback()
def main(
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Enable debug logging.")] = False,
) -> None:
    """Fingerprinting gatekeeper for high-volume Unix pipelines."""
    setup_logging(verbose)


@app.command()
def scan(
    path: Annotated[Path, typer.Argument(help="File or directory to scan.")],
    db: Annotated[Path, typer.Option("--db", help="Path to SQLite index database.")] = Path(
        "dedupe.db"
    ),
    processing_dir: Annotated[
        Path | None, typer.Option("--into", help="Move unique files into this directory.")
    ] = None,
    recursive: Annotated[bool, typer.Option("--recursive", "-r", help="Recursive scan.")] = False,
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Show what would happen without moving files.")
    ] = False,
) -> None:
    """Scan files for duplicates and optionally move unique files."""
    if not path.exists():
        console.print(f"[bold red]Error:[/bold red] Path [yellow]{path}[/yellow] does not exist.")
        raise typer.Exit(1)

    if dry_run:
        console.print("[bold yellow]Dry run enabled. No files will be moved.[/bold yellow]\n")

    try:
        # If dry_run is true, we don't pass processing_dir to the engine for moving
        active_processing_dir = None if dry_run else processing_dir

        with FileDeduplicator(db, processing_dir=active_processing_dir) as deduper:
            if path.is_file():
                results = [deduper.process_file(path)]
            else:
                results = []
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TaskProgressColumn(),
                    console=console,
                ) as progress:
                    task = progress.add_task(f"Scanning {path}...", total=None)
                    for result in deduper.process_directory(path, recursive=recursive):
                        results.append(result)
                        progress.update(
                            task, advance=1, description=f"Scanning: {result.original_path.name}"
                        )
                    progress.update(task, description="Scan complete.")

            # Summary Table
            table = Table(title="Deduplication Summary", box=None, show_header=True)
            table.add_column("Result", style="cyan")
            table.add_column("Count", justify="right", style="magenta")

            unique = sum(1 for r in results if r.result == DedupeResult.UNIQUE)
            dupes = sum(1 for r in results if r.result == DedupeResult.DUPLICATE)
            skipped = sum(1 for r in results if r.result == DedupeResult.SKIPPED)

            table.add_row("Unique", str(unique))
            table.add_row("Duplicate", str(dupes))
            table.add_row("Skipped", str(skipped))

            console.print(table)

            if dry_run and unique > 0:
                console.print(
                    f"\n[bold yellow]Dry run summary:[/bold yellow] {unique} files would be moved to {processing_dir}"
                )
            elif processing_dir and unique > 0:
                console.print(
                    f"\n[bold green]Success:[/bold green] {unique} files moved to {processing_dir}"
                )

    except Exception as e:
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
    db: Annotated[Path, typer.Option("--db", help="Path to SQLite index database.")] = Path(
        "dedupe.db"
    ),
) -> None:
    """Show database statistics and index health."""
    try:
        with FileDeduplicator(db) as deduper:
            s = deduper.stats

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
        logger.error("Failed to fetch stats: {}", e)
        raise typer.Exit(1) from e


if __name__ == "__main__":
    app()
