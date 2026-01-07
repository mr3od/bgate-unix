"""Core deduplication engine with tiered short-circuit logic."""

from __future__ import annotations

import logging
import os
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING

import xxhash

from byte_gate.db import DedupeDatabase, uint64_to_signed

if TYPE_CHECKING:
    from collections.abc import Iterator

logger = logging.getLogger(__name__)

# Constants
FRINGE_SIZE = 8 * 1024  # 8KB for edge reads
HDD_FRINGE_SIZE = 16 * 1024  # 16KB sequential for HDD
CHUNK_SIZE = 256 * 1024  # 256KB chunks for full hash


class DedupeResult(Enum):
    """Result of deduplication check."""

    UNIQUE = "unique"
    DUPLICATE = "duplicate"
    SKIPPED = "skipped"  # Empty files


@dataclass(frozen=True, slots=True)
class ProcessResult:
    """Result of processing a single file."""

    path: Path
    result: DedupeResult
    tier: int  # Which tier determined the result (0-3)
    duplicate_of: Path | None = None
    error: str | None = None


def atomic_move(src: Path, dest: Path) -> None:
    """Atomically move a file from src to dest.

    Uses Path.replace for atomic operation on POSIX systems.
    Creates destination directory if needed.

    Args:
        src: Source file path.
        dest: Destination file path.

    Raises:
        OSError: If move operation fails.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    src.replace(dest)


def _compute_fringe_hash(file_path: Path, file_size: int, optimize_for_hdd: bool) -> int:
    """Compute fringe hash from file edges.

    Args:
        file_path: Path to file.
        file_size: Size of file in bytes.
        optimize_for_hdd: If True, read sequential 16KB instead of edges.

    Returns:
        Signed 64-bit fringe hash.
    """
    hasher = xxhash.xxh64()

    try:
        with file_path.open("rb") as f:
            if optimize_for_hdd:
                # HDD mode: sequential read of first 16KB
                data = f.read(HDD_FRINGE_SIZE)
                hasher.update(data)
            else:
                # SSD mode: read first 8KB + last 8KB
                first_chunk = f.read(FRINGE_SIZE)
                hasher.update(first_chunk)

                if file_size > FRINGE_SIZE * 2:
                    # File large enough for distinct last chunk
                    f.seek(-FRINGE_SIZE, os.SEEK_END)
                    last_chunk = f.read(FRINGE_SIZE)
                    hasher.update(last_chunk)
                elif file_size > FRINGE_SIZE:
                    # File between 8KB and 16KB - read remaining
                    last_chunk = f.read()
                    hasher.update(last_chunk)

            # Include file size in hash for additional uniqueness
            hasher.update(file_size.to_bytes(8, "little"))

    except OSError as e:
        raise OSError(f"Failed to read file for fringe hash: {file_path}") from e

    return uint64_to_signed(hasher.intdigest())


def _compute_full_hash(file_path: Path) -> int:
    """Compute full content hash using streaming xxHash64.

    Args:
        file_path: Path to file.

    Returns:
        Signed 64-bit content hash.
    """
    hasher = xxhash.xxh64()

    try:
        with file_path.open("rb") as f:
            while chunk := f.read(CHUNK_SIZE):
                hasher.update(chunk)
    except OSError as e:
        raise OSError(f"Failed to read file for full hash: {file_path}") from e

    return uint64_to_signed(hasher.intdigest())


class FileDeduplicator:
    """High-performance file deduplicator with tiered short-circuit logic.

    Implements a 4-tier deduplication strategy:
    - Tier 0: Skip empty files (size == 0)
    - Tier 1: Size-based lookup (unique size = unique file)
    - Tier 2: Fringe hash (first 8KB + last 8KB + size)
    - Tier 3: Full content hash (xxHash64)

    Args:
        db_path: Path to SQLite database file.
        processing_dir: Directory for unique files after processing.
        optimize_for_hdd: If True, use sequential reads for fringe hash.
    """

    def __init__(
        self,
        db_path: Path | str,
        processing_dir: Path | str | None = None,
        optimize_for_hdd: bool = False,
    ) -> None:
        self._db = DedupeDatabase(db_path)
        self._processing_dir = Path(processing_dir) if processing_dir else None
        self._optimize_for_hdd = optimize_for_hdd
        self._connected = False

    def connect(self) -> None:
        """Connect to database."""
        self._db.connect()
        self._connected = True

    def close(self) -> None:
        """Close database connection."""
        self._db.close()
        self._connected = False

    def __enter__(self) -> FileDeduplicator:
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, *args: object) -> None:
        """Context manager exit."""
        self.close()

    def _ensure_connected(self) -> None:
        """Ensure database is connected."""
        if not self._connected:
            raise RuntimeError("Deduplicator not connected. Use connect() or context manager.")

    def _log_duplicate(self, incoming: Path, stored: Path) -> None:
        """Log duplicate detection with structured format."""
        timestamp = datetime.now(UTC).isoformat()
        logger.info(
            "[%s] [%s] is a duplicate of [%s]. Rejecting.",
            timestamp,
            incoming,
            stored,
        )

    def process_file(self, file_path: Path | str) -> ProcessResult:
        """Process a single file through the deduplication tiers.

        Args:
            file_path: Path to file to process.

        Returns:
            ProcessResult with deduplication outcome.
        """
        self._ensure_connected()
        file_path = Path(file_path)

        try:
            if not file_path.exists():
                return ProcessResult(
                    path=file_path,
                    result=DedupeResult.SKIPPED,
                    tier=0,
                    error="File does not exist",
                )

            file_size = file_path.stat().st_size

            # Tier 0: Empty file check
            if file_size == 0:
                return ProcessResult(
                    path=file_path,
                    result=DedupeResult.SKIPPED,
                    tier=0,
                )

            # Tier 1: Size uniqueness check
            if not self._db.size_exists(file_size):
                return self._register_unique(file_path, file_size, tier=1)

            # Tier 2: Fringe hash check
            fringe_hash = _compute_fringe_hash(file_path, file_size, self._optimize_for_hdd)
            existing_path = self._db.fringe_lookup(fringe_hash, file_size)

            if existing_path is None:
                return self._register_unique(file_path, file_size, fringe_hash, tier=2)

            # Tier 3: Full hash verification
            full_hash = _compute_full_hash(file_path)
            existing_full = self._db.full_lookup(full_hash)

            if existing_full is None:
                return self._register_unique(file_path, file_size, fringe_hash, full_hash, tier=3)

            # Confirmed duplicate
            duplicate_path = Path(existing_full)
            self._log_duplicate(file_path, duplicate_path)
            return ProcessResult(
                path=file_path,
                result=DedupeResult.DUPLICATE,
                tier=3,
                duplicate_of=duplicate_path,
            )

        except OSError as e:
            logger.exception("OS error processing file: %s", file_path)
            return ProcessResult(
                path=file_path,
                result=DedupeResult.SKIPPED,
                tier=0,
                error=str(e),
            )
        except sqlite3.Error as e:
            logger.exception("Database error processing file: %s", file_path)
            return ProcessResult(
                path=file_path,
                result=DedupeResult.SKIPPED,
                tier=0,
                error=str(e),
            )

    def _register_unique(
        self,
        file_path: Path,
        file_size: int,
        fringe_hash: int | None = None,
        full_hash: int | None = None,
        tier: int = 1,
    ) -> ProcessResult:
        """Register a unique file in the database.

        Args:
            file_path: Path to unique file.
            file_size: File size in bytes.
            fringe_hash: Fringe hash if computed.
            full_hash: Full hash if computed.
            tier: Tier at which uniqueness was determined.

        Returns:
            ProcessResult indicating unique file.
        """
        # Determine final storage path
        if self._processing_dir:
            dest_path = self._processing_dir / file_path.name
            # Handle name collisions
            counter = 1
            while dest_path.exists():
                dest_path = self._processing_dir / f"{file_path.stem}_{counter}{file_path.suffix}"
                counter += 1
            atomic_move(file_path, dest_path)
            storage_path = str(dest_path)
        else:
            storage_path = str(file_path)

        # For Tier 1 unique, we need to compute hashes for storage
        if fringe_hash is None:
            source = Path(storage_path)
            fringe_hash = _compute_fringe_hash(source, file_size, self._optimize_for_hdd)

        if full_hash is None:
            source = Path(storage_path)
            full_hash = _compute_full_hash(source)

        # Update all indexes
        self._db.add_size(file_size)
        self._db.add_fringe(fringe_hash, file_size, storage_path)
        self._db.add_full(full_hash, storage_path)

        return ProcessResult(
            path=file_path,
            result=DedupeResult.UNIQUE,
            tier=tier,
        )

    def process_directory(
        self,
        directory: Path | str,
        recursive: bool = True,
    ) -> Iterator[ProcessResult]:
        """Process all files in a directory.

        Args:
            directory: Directory to process.
            recursive: If True, process subdirectories.

        Yields:
            ProcessResult for each file.
        """
        self._ensure_connected()
        directory = Path(directory)

        if not directory.is_dir():
            raise ValueError(f"Not a directory: {directory}")

        pattern = "**/*" if recursive else "*"
        for file_path in directory.glob(pattern):
            if file_path.is_file():
                yield self.process_file(file_path)

    def is_duplicate(self, file_path: Path | str) -> bool:
        """Quick check if a file is a duplicate.

        Args:
            file_path: Path to file to check.

        Returns:
            True if file is a duplicate, False otherwise.
        """
        result = self.process_file(file_path)
        return result.result == DedupeResult.DUPLICATE

    @property
    def stats(self) -> dict[str, int]:
        """Get database statistics."""
        self._ensure_connected()
        conn = self._db.connection

        size_count = conn.execute("SELECT COUNT(*) FROM size_index").fetchone()[0]
        fringe_count = conn.execute("SELECT COUNT(*) FROM fringe_index").fetchone()[0]
        full_count = conn.execute("SELECT COUNT(*) FROM full_index").fetchone()[0]

        return {
            "unique_sizes": size_count,
            "fringe_entries": fringe_count,
            "full_entries": full_count,
        }
