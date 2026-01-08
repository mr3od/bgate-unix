"""Core deduplication engine with tiered short-circuit logic.

Unix-only implementation using os.link/unlink for atomic moves.
Absolute trust model: xxh128 collisions are treated as impossible.
"""

from __future__ import annotations

import errno
import os
import stat
import sys
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Self

import xxhash
from loguru import logger

from bgate_unix.db import DedupeDatabase

if TYPE_CHECKING:
    from collections.abc import Iterator

# Unix-only enforcement
if sys.platform == "win32":
    sys.exit("bgate-unix is Unix-only. Windows is not supported.")

# Constants
FRINGE_SIZE = 64 * 1024  # 64KB for edge reads
CHUNK_SIZE = 256 * 1024  # 256KB chunks for all storage types


class DedupeResult(Enum):
    """Result of deduplication check."""

    UNIQUE = "unique"
    DUPLICATE = "duplicate"
    SKIPPED = "skipped"


@dataclass(frozen=True, slots=True)
class ProcessResult:
    """Result of processing a single file."""

    path: Path
    result: DedupeResult
    tier: Literal[0, 1, 2, 3]
    duplicate_of: Path | None = None
    error: str | None = None


def atomic_move(src: Path, dest: Path) -> None:
    """Atomically move a file using link/unlink pattern.

    os.link fails if dest exists, providing atomic overwrite protection.

    Args:
        src: Source file path.
        dest: Destination file path.

    Raises:
        FileExistsError: If destination already exists.
        OSError: If cross-device move attempted or other failure.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.link(src, dest)
    except OSError as e:
        if e.errno == errno.EXDEV:
            raise OSError(
                f"Cross-device move failed: {src} -> {dest}. "
                "Source and destination must be on the same filesystem."
            ) from e
        raise
    src.unlink()


def _compute_fringe_hash(file_path: Path, file_size: int) -> bytes:
    """Compute fringe hash from file edges.

    Reads first 64KB, and if file > 64KB, also reads last 64KB (non-overlapping).

    Args:
        file_path: Path to file.
        file_size: Size of file in bytes.

    Returns:
        Raw 8-byte digest from xxh64.
    """
    hasher = xxhash.xxh64()

    try:
        with file_path.open("rb") as f:
            first_chunk = f.read(FRINGE_SIZE)
            hasher.update(first_chunk)

            if file_size > FRINGE_SIZE:
                f.seek(-min(FRINGE_SIZE, file_size - FRINGE_SIZE), os.SEEK_END)
                last_chunk = f.read()
                hasher.update(last_chunk)

            hasher.update(file_size.to_bytes(8, "little"))
    except OSError as e:
        raise OSError(f"Failed to read file for fringe hash: {file_path}") from e

    return hasher.digest()


def _compute_full_hash(file_path: Path) -> bytes:
    """Compute full content hash using xxHash128.

    Args:
        file_path: Path to file.

    Returns:
        Raw 16-byte digest from xxh128.
    """
    hasher = xxhash.xxh128()

    try:
        with file_path.open("rb") as f:
            while chunk := f.read(CHUNK_SIZE):
                hasher.update(chunk)
    except OSError as e:
        raise OSError(f"Failed to read file for full hash: {file_path}") from e

    return hasher.digest()


class FileDeduplicator:
    """High-performance file deduplicator with tiered short-circuit logic.

    Implements a 4-tier deduplication strategy:
    - Tier 0: Skip empty files (size == 0)
    - Tier 1: Size-based lookup (unique size = unique file)
    - Tier 2: Fringe hash (first 64KB + last 64KB + size)
    - Tier 3: Full content hash (xxHash128) - absolute identity

    Args:
        db_path: Path to SQLite database file.
        processing_dir: Directory for unique files after processing.
    """

    def __init__(
        self,
        db_path: Path | str,
        processing_dir: Path | str | None = None,
    ) -> None:
        self._db = DedupeDatabase(db_path)
        self._processing_dir = Path(processing_dir) if processing_dir else None
        self._connected = False

    def connect(self) -> None:
        """Connect to database and run recovery."""
        self._db.connect()
        self._connected = True

        self._check_emergency_orphans()

        recovery_count = self._recover_from_journal()
        if recovery_count > 0:
            logger.info("Recovered {} incomplete operations from journal", recovery_count)

        recovery = self.recover_orphans()
        if recovery["total"] > 0:
            logger.info(
                "Orphan recovery: {} recovered, {} failed",
                recovery["recovered"],
                recovery["failed"],
            )

    def close(self) -> None:
        self._db.close()
        self._connected = False

    def __enter__(self) -> Self:
        self.connect()
        return self

    def __exit__(self, *args: object) -> bool | None:
        self.close()
        return None

    def _ensure_connected(self) -> None:
        if not self._connected:
            raise RuntimeError("Deduplicator not connected. Use connect() or context manager.")

    def _validate_path(self, path: Path) -> tuple[bool, str | None]:
        """Validate path for security and accessibility.

        Note: Symlinks are not followed by process_directory (uses follow_symlinks=False).
        This validation is for direct process_file() calls only.
        """
        try:
            path_str = str(path)
            if not path_str or "\x00" in path_str:
                return False, "Invalid file path"

            # Reject symlinks - they're skipped by directory scanner anyway
            if path.is_symlink():
                return False, "Symlinks not supported"

            if not path.is_file():
                return False, "Not a regular file"

            if not os.access(path, os.R_OK):
                return False, "File not readable"

            mode = path.stat().st_mode
            if stat.S_ISCHR(mode) or stat.S_ISBLK(mode):
                return False, "Device file not allowed"

            return True, None
        except OSError as e:
            return False, str(e)

    def process_file(
        self, file_path: Path | str, stat_result: os.stat_result | None = None
    ) -> ProcessResult:
        """Process a single file through the deduplication tiers."""
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

            valid, error = self._validate_path(file_path)
            if not valid:
                logger.warning("Path validation failed for {}: {}", file_path, error)
                return ProcessResult(
                    path=file_path,
                    result=DedupeResult.SKIPPED,
                    tier=0,
                    error=f"Validation failed: {error}",
                )

            file_size = stat_result.st_size if stat_result else file_path.stat().st_size
            return self._process_file(file_path, file_size)

        except OSError as e:
            logger.exception("OS error processing file: {}", file_path)
            return ProcessResult(
                path=file_path,
                result=DedupeResult.SKIPPED,
                tier=0,
                error=str(e),
            )
        except Exception as e:
            logger.exception("Error processing file: {}", file_path)
            return ProcessResult(
                path=file_path,
                result=DedupeResult.SKIPPED,
                tier=0,
                error=str(e),
            )

    def _process_file(self, file_path: Path, file_size: int) -> ProcessResult:
        """Core processing logic."""
        # Tier 0: Skip empty files
        if file_size == 0:
            return ProcessResult(path=file_path, result=DedupeResult.SKIPPED, tier=0)

        # Tier 1: Size uniqueness
        if not self._db.size_exists(file_size):
            return self._register_unique(file_path, file_size, tier=1)

        # Tier 2: Fringe hash
        fringe_hash = _compute_fringe_hash(file_path, file_size)
        existing_fringe = self._db.fringe_lookup(fringe_hash, file_size)

        if existing_fringe is None:
            return self._register_unique(file_path, file_size, fringe_hash, tier=2)

        # Tier 3: Full hash - absolute identity
        full_hash = _compute_full_hash(file_path)
        existing_full = self._db.full_lookup(full_hash)

        if existing_full is None:
            return self._register_unique(file_path, file_size, fringe_hash, full_hash, tier=3)

        # Duplicate found
        duplicate_path = Path(existing_full)
        logger.info("[{}] is a duplicate of [{}]", file_path, duplicate_path)
        return ProcessResult(
            path=file_path,
            result=DedupeResult.DUPLICATE,
            tier=3,
            duplicate_of=duplicate_path,
        )

    def _register_unique(
        self,
        file_path: Path,
        file_size: int,
        fringe_hash: bytes | None = None,
        full_hash: bytes | None = None,
        tier: Literal[1, 2, 3] = 1,
    ) -> ProcessResult:
        """Register a unique file in the database.

        Three-phase approach:
        1. Journal intent (in transaction)
        2. File move via link/unlink (outside transaction)
        3. Register in index (in transaction)
        """
        dest_path: Path | None = None
        storage_path: str
        journal_id: int | None = None
        max_retries = 5

        if self._processing_dir:
            # Phase 1: Journal the intent
            for attempt in range(max_retries):
                if full_hash is not None:
                    unique_name = f"{full_hash.hex()[:16]}{file_path.suffix}"
                else:
                    unique_name = f"{uuid.uuid4().hex[:16]}{file_path.suffix}"

                if attempt > 0:
                    unique_name = (
                        f"{Path(unique_name).stem}_{uuid.uuid4().hex[:8]}{file_path.suffix}"
                    )

                dest_path = self._processing_dir / unique_name

                self._db.begin_transaction()
                try:
                    journal_id = self._db.journal_move(str(file_path), str(dest_path), file_size)
                    self._db.update_move_phase(journal_id, "moving")
                    self._db.commit()
                except Exception:
                    self._db.rollback()
                    raise

                # Phase 2: File move (outside transaction)
                try:
                    atomic_move(file_path, dest_path)
                    break
                except FileExistsError:
                    self._db.begin_transaction()
                    try:
                        self._db.update_move_phase(journal_id, "failed")
                        self._db.commit()
                    except Exception:
                        self._db.rollback()
                    if attempt == max_retries - 1:
                        raise
                    continue
                except Exception:
                    self._db.begin_transaction()
                    try:
                        self._db.update_move_phase(journal_id, "failed")
                        self._db.commit()
                    except Exception:
                        self._db.rollback()
                    raise

            storage_path = str(dest_path)
        else:
            storage_path = str(file_path)

        # Phase 3: Register in index
        try:
            self._db.begin_transaction()
            try:
                if journal_id is not None:
                    self._db.update_move_phase(journal_id, "completed")

                if fringe_hash is None:
                    fringe_hash = _compute_fringe_hash(Path(storage_path), file_size)

                if full_hash is None:
                    full_hash = _compute_full_hash(Path(storage_path))

                self._db.add_size(file_size)
                self._db.add_fringe(fringe_hash, file_size, storage_path)
                self._db.add_full(full_hash, storage_path)

                self._db.commit()
            except Exception:
                self._db.rollback()
                raise

        except Exception:
            # Rollback file move on DB failure
            if dest_path is not None and dest_path.exists():
                try:
                    atomic_move(dest_path, file_path)
                except OSError:
                    logger.warning("Failed to rollback file move: {} -> {}", dest_path, file_path)
                    try:
                        self._db.add_orphan(str(file_path), str(dest_path), file_size)
                    except Exception:
                        self._write_emergency_orphan(file_path, dest_path, file_size)
            raise

        return ProcessResult(path=file_path, result=DedupeResult.UNIQUE, tier=tier)

    def _write_emergency_orphan(
        self, original_path: Path, orphan_path: Path, file_size: int
    ) -> None:
        """Write orphan info to emergency file when DB is unavailable.

        Uses JSON for safe serialization (filenames can contain any character).
        """
        import json

        emergency_file = self._db.db_path.parent / "emergency_orphans.jsonl"
        try:
            with emergency_file.open("a") as f:
                record = {
                    "timestamp": datetime.now(UTC).isoformat(),
                    "original_path": str(original_path),
                    "orphan_path": str(orphan_path),
                    "file_size": file_size,
                }
                f.write(json.dumps(record) + "\n")
            logger.error("DB unavailable - wrote orphan to emergency file: {}", emergency_file)
        except OSError:
            logger.critical(
                "CRITICAL: Cannot write orphan record anywhere! "
                "File at {} needs manual recovery to {}",
                orphan_path,
                original_path,
            )

    def _check_emergency_orphans(self) -> None:
        """Check for emergency orphans file and import if present."""
        # Check both old format (.txt) and new format (.jsonl)
        for filename in ["emergency_orphans.jsonl", "emergency_orphans.txt"]:
            emergency_file = self._db.db_path.parent / filename
            if emergency_file.exists():
                try:
                    line_count = sum(1 for _ in emergency_file.open())
                    logger.warning(
                        "Emergency orphans file found at {} with {} entries",
                        emergency_file,
                        line_count,
                    )
                    imported = self._import_emergency_orphans(emergency_file)
                    if imported > 0:
                        logger.info("Imported {} emergency orphan records", imported)
                except OSError as e:
                    logger.error("Failed to read emergency orphans file: {}", e)

    def _import_emergency_orphans(self, emergency_file: Path) -> int:
        """Import emergency orphan records into the database."""
        import json

        imported = 0
        remaining_lines: list[str] = []
        is_jsonl = emergency_file.suffix == ".jsonl"

        try:
            with emergency_file.open() as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        if is_jsonl:
                            record = json.loads(line)
                            original_path = record["original_path"]
                            orphan_path = record["orphan_path"]
                            file_size = record["file_size"]
                        else:
                            # Legacy pipe-delimited format
                            parts = line.split("|")
                            if len(parts) < 4:
                                remaining_lines.append(line)
                                continue
                            original_path = parts[1]
                            orphan_path = parts[2]
                            file_size = int(parts[3])

                        if Path(orphan_path).exists():
                            self._db.add_orphan(original_path, orphan_path, file_size)
                            imported += 1
                        else:
                            logger.warning("Emergency orphan no longer exists: {}", orphan_path)
                    except (ValueError, KeyError, json.JSONDecodeError) as e:
                        logger.warning("Failed to parse emergency orphan line: {} ({})", line, e)
                        remaining_lines.append(line)

            if not remaining_lines and imported > 0:
                emergency_file.unlink()
                logger.info("Removed emergency orphans file after successful import")
            elif remaining_lines:
                with emergency_file.open("w") as f:
                    for line in remaining_lines:
                        f.write(line + "\n")

        except OSError as e:
            logger.error("Failed to import emergency orphans: {}", e)

        return imported

    def _recover_from_journal(self) -> int:
        """Recover from incomplete journal entries.

        Critical: Files in 'moving' state have been physically moved but NOT indexed.
        We must move them back to source so they can be re-processed and properly indexed.
        """
        incomplete = self._db.get_incomplete_journal_entries()
        recovered = 0

        for entry in incomplete:
            source = Path(entry["source_path"])
            dest = Path(entry["dest_path"])
            phase = entry["phase"]
            journal_id = entry["id"]

            if phase == "planned":
                # Move never started - just mark as failed
                self._db.update_move_phase(journal_id, "failed")
                recovered += 1
            elif phase == "moving":
                # File was moved but index registration never completed.
                # Must rollback to source for re-processing.
                if dest.exists() and not source.exists():
                    try:
                        atomic_move(dest, source)
                        self._db.update_move_phase(journal_id, "failed")
                        logger.info("Rolled back incomplete move: {} -> {}", dest, source)
                        recovered += 1
                    except OSError as e:
                        logger.error(
                            "Critical: Cannot rollback move {} -> {}: {}. "
                            "File exists in processing_dir but is NOT indexed!",
                            dest,
                            source,
                            e,
                        )
                elif source.exists() and not dest.exists():
                    # Move never happened - mark failed
                    self._db.update_move_phase(journal_id, "failed")
                    recovered += 1
                elif source.exists() and dest.exists():
                    # Partial state - remove dest, keep source
                    try:
                        dest.unlink()
                        self._db.update_move_phase(journal_id, "failed")
                        recovered += 1
                    except OSError:
                        logger.error("Cannot clean up partial move: {} -> {}", source, dest)
                else:
                    # Both missing - manual intervention occurred
                    self._db.update_move_phase(journal_id, "failed")
                    recovered += 1

        return recovered

    def process_directory(
        self,
        directory: Path | str,
        recursive: bool = True,
    ) -> Iterator[ProcessResult]:
        """Process all files in a directory."""
        self._ensure_connected()
        directory = Path(directory)

        if not directory.is_dir():
            raise ValueError(f"Not a directory: {directory}")

        yield from self._process_directory_scandir(directory, recursive)

    def _process_directory_scandir(
        self, directory: Path, recursive: bool
    ) -> Iterator[ProcessResult]:
        """Process directory using scandir for efficient stat access."""
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    try:
                        if entry.is_file(follow_symlinks=False):
                            stat_result = entry.stat(follow_symlinks=False)
                            yield self.process_file(Path(entry.path), stat_result)
                        elif recursive and entry.is_dir(follow_symlinks=False):
                            yield from self._process_directory_scandir(Path(entry.path), recursive)
                    except OSError as e:
                        logger.warning("Error accessing {}: {}", entry.path, e)
        except OSError as e:
            logger.warning("Error scanning directory {}: {}", directory, e)

    def is_duplicate(self, file_path: Path | str) -> bool:
        """Quick check if a file is a duplicate."""
        result = self.process_file(file_path)
        return result.result == DedupeResult.DUPLICATE

    def recover_orphans(self) -> dict[str, int]:
        """Attempt to recover orphaned files."""
        self._ensure_connected()
        orphans = self._db.get_pending_orphans()
        recovered = 0
        failed = 0

        for orphan in orphans:
            orphan_path = Path(orphan["orphan_path"])
            original_path = Path(orphan["original_path"])

            try:
                if orphan_path.exists():
                    atomic_move(orphan_path, original_path)
                    self._db.update_orphan_status(orphan["id"], "recovered")
                    recovered += 1
                else:
                    self._db.update_orphan_status(orphan["id"], "failed")
                    failed += 1
            except OSError:
                self._db.update_orphan_status(orphan["id"], "failed")
                failed += 1

        return {"recovered": recovered, "failed": failed, "total": len(orphans)}

    def list_orphans(self) -> list[dict]:
        """List all pending orphan records."""
        self._ensure_connected()
        return self._db.get_pending_orphans()

    @property
    def stats(self) -> dict[str, int | str]:
        """Get database and engine statistics."""
        self._ensure_connected()
        db = self._db.db

        size_count = db.execute("SELECT COUNT(*) FROM size_index").fetchone()[0]
        fringe_count = db.execute("SELECT COUNT(*) FROM fringe_index").fetchone()[0]
        full_count = db.execute("SELECT COUNT(*) FROM full_index").fetchone()[0]

        return {
            "unique_sizes": size_count,
            "fringe_entries": fringe_count,
            "full_entries": full_count,
            "schema_version": self._db.schema_version,
            "orphan_count": self._db.get_orphan_count(),
            "pending_journal": self._db.get_pending_journal_count(),
        }
