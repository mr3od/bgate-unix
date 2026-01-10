"""Core deduplication engine with tiered short-circuit logic.

Unix-only implementation using os.link/unlink for atomic moves.
Absolute trust model: xxh128 collisions are treated as impossible.
"""

from __future__ import annotations

import contextlib
import errno
import json
import os
import signal
import stat
import sys
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Self

import xxhash
from loguru import logger

from bgate_unix.db import DedupeDatabase

if TYPE_CHECKING:
    from collections.abc import Generator, Iterator

# Unix-only enforcement
if sys.platform == "win32":
    sys.exit("bgate-unix is Unix-only. Windows is not supported.")

# Constants
FRINGE_SIZE = 64 * 1024  # 64KB for edge reads
CHUNK_SIZE = 256 * 1024  # 256KB chunks for all storage types
DEFAULT_IGNORES = {
    ".git",
    "node_modules",
    "vendor",
    "__pycache__",
    ".DS_Store",
    ".venv",
    "venv",
    ".idea",
    ".vscode",
}

# Signal handling for critical sections
_deferred_signal: tuple[int, object] | None = None


class DedupeResult(Enum):
    """Result of deduplication check."""

    UNIQUE = "unique"
    DUPLICATE = "duplicate"
    SKIPPED = "skipped"


@dataclass(frozen=True, slots=True)
class ProcessResult:
    """Result of processing a single file."""

    path: Path
    original_path: Path
    result: DedupeResult
    tier: Literal[0, 1, 2, 3]
    stored_path: Path | None = None
    duplicate_of: Path | None = None
    tags: dict[str, str] | None = None
    error: str | None = None


def _deferred_signal_handler(signum: int, frame: object) -> None:
    """Store signal for later delivery after critical section completes."""
    global _deferred_signal
    _deferred_signal = (signum, frame)


@contextmanager
def critical_section() -> Generator[None, None, None]:
    """Context manager that defers SIGINT/SIGTERM until critical I/O completes.

    Ensures atomic_move operations complete fully before honoring interrupts.
    """
    global _deferred_signal
    _deferred_signal = None

    old_sigint = signal.signal(signal.SIGINT, _deferred_signal_handler)
    old_sigterm = signal.signal(signal.SIGTERM, _deferred_signal_handler)

    try:
        yield
    finally:
        signal.signal(signal.SIGINT, old_sigint)
        signal.signal(signal.SIGTERM, old_sigterm)

        if _deferred_signal is not None:
            signum, frame = _deferred_signal
            _deferred_signal = None
            logger.warning("Deferred signal {} received, re-raising after critical section", signum)
            # Re-raise the signal to the original handler
            if signum == signal.SIGINT and old_sigint not in (signal.SIG_IGN, signal.SIG_DFL):
                old_sigint(signum, frame)  # type: ignore[operator]
            elif signum == signal.SIGTERM and old_sigterm not in (signal.SIG_IGN, signal.SIG_DFL):
                old_sigterm(signum, frame)  # type: ignore[operator]
            else:
                # Default behavior - re-raise
                signal.raise_signal(signum)


def _fsync_dir(dir_path: Path) -> None:
    """Sync a directory to ensure metadata changes are durable.

    Critical for power-loss safety: without this, directory entry changes
    (file moves) may not survive a crash even if the file data is written.
    """
    fd = os.open(dir_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_move(src: Path, dest: Path) -> None:
    """Atomically move file with full directory durability.

    Uses critical_section to defer signals until the move completes.
    Ensures durability by syncing the parent of every newly created directory
    in the path, plus the destination and source directories.

    Args:
        src: Source file path.
        dest: Destination file path.

    Raises:
        FileExistsError: If destination already exists.
        OSError: If cross-device move attempted or other failure.
    """
    parent = dest.parent

    # 1. Detect which directories need creation outside critical section
    dirs_to_sync_parents_of: list[Path] = []
    curr = parent
    while not curr.exists():
        dirs_to_sync_parents_of.append(curr)
        prev = curr
        curr = curr.parent
        # Robust root check: stop if we hit root or can't go higher
        if curr == prev:
            break

    with critical_section():
        # 2. Create directories (inside critical section to prevent partial state on SIGINT)
        if dirs_to_sync_parents_of:
            parent.mkdir(parents=True, exist_ok=True)

        try:
            os.link(src, dest)
        except OSError as e:
            if e.errno == errno.EXDEV:
                raise OSError(
                    f"Cross-device move failed: {src} -> {dest}. "
                    "Source and destination must be on the same filesystem."
                ) from e
            raise

        # 3. Durability: Sync parent of every new directory
        # CRITICAL: Do NOT suppress errors here. If we can't persist the directory structure,
        # we must not proceed to unlink the source.
        for d in reversed(dirs_to_sync_parents_of):
            _fsync_dir(d.parent)

        # 4. Sync dest directory (to persist the file link)
        _fsync_dir(parent)

        # 5. Remove source
        src.unlink()

        # 6. Sync source directory to ensure unlink is durable
        _fsync_dir(src.parent)


def _compute_fringe_hash(file_path: Path, _file_size: int = 0) -> bytes:
    """Compute fringe hash from file edges.

    Reads first 64KB, and if file > 64KB, also reads last 64KB.
    If file size is between 64KB and 128KB, the chunks will overlap.
    Uses actual file size from the open file descriptor to avoid TOCTOU issues.

    Args:
        file_path: Path to file.
        _file_size: Deprecated, kept for API compatibility. Actual size from FD is used.

    Returns:
        Raw 8-byte digest from xxh64.
    """
    hasher = xxhash.xxh64()

    try:
        with file_path.open("rb") as f:
            # Get authoritative size from file descriptor to avoid TOCTOU
            actual_size = f.seek(0, os.SEEK_END)
            f.seek(0)

            first_chunk = f.read(FRINGE_SIZE)
            hasher.update(first_chunk)

            if actual_size > FRINGE_SIZE:
                # Overlap allowed spec: always read last 64KB (even if overlapping)
                seek_pos = max(0, actual_size - FRINGE_SIZE)
                f.seek(seek_pos)
                last_chunk = f.read(FRINGE_SIZE)
                hasher.update(last_chunk)

            # Use actual size from FD, not the passed estimate
            hasher.update(actual_size.to_bytes(8, "little"))
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

        Symlinks are explicitly not supported:
        - process_directory() filters them out via is_file(follow_symlinks=False)
        - Direct process_file() calls are rejected here

        This prevents symlink-based attacks and ensures consistent behavior.
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
        self,
        file_path: Path | str,
        stat_result: os.stat_result | None = None,
        tags: dict[str, str] | None = None,
    ) -> ProcessResult:
        """Process a single file through the deduplication tiers."""
        self._ensure_connected()
        file_path = Path(file_path)

        try:
            if not file_path.exists():
                return ProcessResult(
                    path=file_path,
                    original_path=file_path,
                    result=DedupeResult.SKIPPED,
                    tier=0,
                    error="File does not exist",
                )

            valid, error = self._validate_path(file_path)
            if not valid:
                logger.warning("Path validation failed for {}: {}", file_path, error)
                return ProcessResult(
                    path=file_path,
                    original_path=file_path,
                    result=DedupeResult.SKIPPED,
                    tier=0,
                    error=f"Validation failed: {error}",
                )

            file_size = stat_result.st_size if stat_result else file_path.stat().st_size
            return self._process_file(file_path, file_size, tags)

        except OSError as e:
            logger.exception("OS error processing file: {}", file_path)
            return ProcessResult(
                path=file_path,
                original_path=file_path,
                result=DedupeResult.SKIPPED,
                tier=0,
                error=str(e),
            )
        except Exception as e:
            logger.exception("Error processing file: {}", file_path)
            return ProcessResult(
                path=file_path,
                original_path=file_path,
                result=DedupeResult.SKIPPED,
                tier=0,
                error=str(e),
            )

    def _process_file(
        self, file_path: Path, file_size: int, tags: dict[str, str] | None = None
    ) -> ProcessResult:
        """Core processing logic."""
        # Tier 0: Skip empty files
        if file_size == 0:
            return ProcessResult(
                path=file_path,
                original_path=file_path,
                result=DedupeResult.SKIPPED,
                tier=0,
                tags=tags,
            )

        # Tier 1: Size uniqueness
        if not self._db.size_exists(file_size):
            return self._register_unique(file_path, file_size, tier=1, tags=tags)

        # Tier 2: Fringe hash
        fringe_hash = _compute_fringe_hash(file_path, file_size)
        existing_fringe = self._db.fringe_lookup(fringe_hash, file_size)

        if existing_fringe is None:
            return self._register_unique(file_path, file_size, fringe_hash, tier=2, tags=tags)

        # Tier 3: Full hash - absolute identity
        full_hash = _compute_full_hash(file_path)
        existing_full = self._db.full_lookup(full_hash)

        if existing_full is None:
            return self._register_unique(
                file_path, file_size, fringe_hash, full_hash, tier=3, tags=tags
            )

        # Self-check: Prevent "duplicate of self" reports
        existing_path = Path(existing_full)
        if existing_path.resolve() == file_path.resolve():
            return ProcessResult(
                path=file_path,
                original_path=file_path,
                result=DedupeResult.UNIQUE,
                tier=3,
                stored_path=file_path,
                tags=tags,
            )

        # Duplicate found
        logger.info("[{}] is a duplicate of [{}]", file_path, existing_path)
        return ProcessResult(
            path=file_path,
            original_path=file_path,
            result=DedupeResult.DUPLICATE,
            tier=3,
            duplicate_of=existing_path,
            tags=tags,
        )

    def _register_unique(
        self,
        file_path: Path,
        file_size: int,
        fringe_hash: bytes | None = None,
        full_hash: bytes | None = None,
        tier: Literal[1, 2, 3] = 1,
        tags: dict[str, str] | None = None,
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
                # v0.3.0 Sharding: 2-level hex (e.g. processing/aa/bbcc...)
                if full_hash is not None:
                    hex_val = full_hash.hex()
                    shard = hex_val[:2]
                    unique_name = f"{hex_val[2:16]}{file_path.suffix}"
                else:
                    hex_val = uuid.uuid4().hex
                    shard = hex_val[:2]
                    unique_name = f"{hex_val[2:16]}{file_path.suffix}"

                if attempt > 0:
                    unique_name = (
                        f"{Path(unique_name).stem}_{uuid.uuid4().hex[:8]}{file_path.suffix}"
                    )

                dest_dir = self._processing_dir / shard
                dest_path = dest_dir / unique_name

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
                    # Durable shard creation: atomic_move will create parents, but we want to ensure
                    # the shard directory entry itself is durable in the processing_dir.
                    # Performance: Only fsync processing_dir if we ACTUALLY created a new shard dir.
                    try:
                        dest_dir.mkdir(exist_ok=False)
                        # New directory created - must sync parent to ensure entry is durable
                        _fsync_dir(self._processing_dir)
                    except FileExistsError:
                        # Directory already exists - no parent sync needed
                        pass
                    except OSError as e:
                        # CRITICAL: If mkdir fails unexpectedly (e.g. permission), we must NOT
                        # fall back to atomic_move because we haven't synced the parent directory.
                        # We must "fail fast" to ensure durability guarantees.
                        logger.error("Shard pre-create failed for {}: {}", dest_dir, e)
                        raise

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
            conflict_detected = False
            self._db.begin_transaction()
            try:
                # 3a. Update journal if needed
                if journal_id is not None:
                    self._db.update_move_phase(journal_id, "completed")

                # 3b. Calculate hashes if missing (idempotent)
                if fringe_hash is None:
                    fringe_hash = _compute_fringe_hash(Path(storage_path), file_size)
                if full_hash is None:
                    full_hash = _compute_full_hash(Path(storage_path))

                # 3c. Insert shared metadata
                self._db.add_size(file_size)
                self._db.add_fringe(fringe_hash, file_size, storage_path)

                # 3d. Insert full hash - check strict uniqueness
                metadata_json = json.dumps(tags) if tags else None
                if self._db.add_full(full_hash, storage_path, metadata_json):
                    # Success
                    self._db.commit()
                else:
                    # Conflict: prevent commit, rollback this transaction fully
                    self._db.rollback()
                    conflict_detected = True

            except Exception:
                self._db.rollback()
                raise

            # Handle conflict OUTSIDE the transaction block
            if conflict_detected:
                return self._handle_duplicate_conflict(
                    file_path, dest_path, full_hash, file_size, journal_id
                )

        except Exception:
            # DB failure fallback (e.g. connection lost during begin_transaction)
            self._handle_move_rollback(file_path, dest_path, file_size, journal_id)
            raise

        return ProcessResult(
            path=dest_path if dest_path else file_path,
            original_path=file_path,
            result=DedupeResult.UNIQUE,
            tier=tier,
            stored_path=dest_path if self._processing_dir else None,
            tags=tags,
        )

    def _handle_duplicate_conflict(
        self,
        file_path: Path,
        dest_path: Path | None,
        full_hash: bytes,
        file_size: int,
        original_journal_id: int | None = None,
    ) -> ProcessResult:
        """Handle race condition where file became duplicate during move."""
        # 4a. Move file back to source (rollback the move)
        self._handle_move_rollback(file_path, dest_path, file_size, original_journal_id)

        # 4b. Register as duplicate
        existing_path = self._db.full_lookup(full_hash)
        return ProcessResult(
            path=file_path,
            original_path=file_path,
            result=DedupeResult.DUPLICATE,
            tier=3,
            duplicate_of=Path(existing_path) if existing_path else None,
        )

    def _handle_move_rollback(
        self,
        file_path: Path,
        dest_path: Path | None,
        file_size: int,
        original_journal_id: int | None = None,
    ) -> None:
        """Rollback a file move: move dest back to source."""
        if dest_path and dest_path.exists():
            # Create a NEW journal entry for the rollback move
            # This ensures the rollback itself is crash-safe
            rollback_journal_id: int | None = None
            self._db.begin_transaction()
            try:
                rollback_journal_id = self._db.journal_move(
                    str(dest_path), str(file_path), file_size
                )
                self._db.update_move_phase(rollback_journal_id, "moving")
                self._db.commit()
            except Exception:
                self._db.rollback()
                rollback_journal_id = None

            try:
                atomic_move(dest_path, file_path)
                if rollback_journal_id is not None:
                    self._db.begin_transaction()
                    try:
                        self._db.update_move_phase(rollback_journal_id, "completed")
                        self._db.commit()
                    except Exception:
                        self._db.rollback()
            except OSError:
                logger.warning("Failed to rollback file move: {} -> {}", dest_path, file_path)
                if rollback_journal_id is not None:
                    self._db.begin_transaction()
                    try:
                        self._db.update_move_phase(rollback_journal_id, "failed")
                        self._db.commit()
                    except Exception:
                        self._db.rollback()
                try:
                    self._db.add_orphan(str(file_path), str(dest_path), file_size)
                except Exception:
                    self._write_emergency_orphan(file_path, dest_path, file_size)

        # After attempting rollback, mark the ORIGINAL journal entry as failed
        # This prevents it from being stuck in "moving" state
        if original_journal_id is not None:
            try:
                self._db.begin_transaction()
                self._db.update_move_phase(original_journal_id, "failed")
                self._db.commit()
            except Exception:
                self._db.rollback()
                pass

    def _write_emergency_orphan(
        self, original_path: Path, orphan_path: Path, file_size: int
    ) -> None:
        """Write orphan info to emergency file when DB is unavailable.

        Uses unbuffered I/O with O_APPEND for atomic writes and fsync for durability.
        Includes metadata for debugging and manual recovery.
        """
        import getpass
        import json
        import socket

        emergency_file = self._db.db_path.parent / "emergency_orphans.jsonl"
        try:
            record = {
                "timestamp": datetime.now(UTC).isoformat(),
                "hostname": socket.gethostname(),
                "user": getpass.getuser(),
                "pid": os.getpid(),
                "original_path": str(original_path),
                "orphan_path": str(orphan_path),
                "file_size": file_size,
                "db_path": str(self._db.db_path),
                "version": "0.3.0",
            }
            # Use unbuffered I/O with O_APPEND for atomic single-syscall writes
            fd = os.open(emergency_file, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
            try:
                os.write(fd, (json.dumps(record, separators=(",", ":")) + "\n").encode())
                os.fsync(fd)
            finally:
                os.close(fd)
            _fsync_dir(emergency_file.parent)
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
                    with emergency_file.open("r") as f:
                        line_count = sum(1 for _ in f)
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
                _fsync_dir(emergency_file.parent)
                logger.info("Removed emergency orphans file after successful import")
            elif remaining_lines:
                # Performance: batch write all lines, flush once, fsync once
                # Safety (Opus Fix): Use atomic temp file -> rename to avoid partial writes
                temp_file = emergency_file.with_suffix(".tmp")
                with temp_file.open("w") as f:
                    for line in remaining_lines:
                        f.write(line + "\n")
                    f.flush()
                    os.fsync(f.fileno())

                # Sync directory for temp file creation
                _fsync_dir(temp_file.parent)

                # Atomic replace
                temp_file.replace(emergency_file)

                # Sync directory for replace
                _fsync_dir(emergency_file.parent)

        except OSError as e:
            logger.error("Failed to import emergency orphans: {}", e)

        return imported

    def _recover_from_journal(self) -> int:
        """Recover from incomplete journal entries.

        Uses atomic operations to avoid TOCTOU races.
        Files in 'moving' state have been physically moved but NOT indexed.
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
                self._db.begin_transaction()
                try:
                    self._db.update_move_phase(journal_id, "failed")
                    self._db.commit()
                except Exception:
                    self._db.rollback()
                recovered += 1
            elif phase == "moving":
                # Attempt atomic rollback without exists() checks (TOCTOU-safe)
                try:
                    # Try to create hard link back to source
                    os.link(dest, source)
                    # CRITICAL: Sync source directory BEFORE unlinking dest
                    _fsync_dir(source.parent)
                    dest.unlink()
                    with contextlib.suppress(FileNotFoundError, OSError):
                        _fsync_dir(dest.parent)

                    self._db.begin_transaction()
                    try:
                        self._db.update_move_phase(journal_id, "failed")
                        self._db.commit()
                    except Exception:
                        self._db.rollback()

                    logger.info("Rolled back incomplete move: {} -> {}", dest, source)
                    recovered += 1
                except FileExistsError:
                    # Source already exists - link() never completed or was interrupted
                    # Safe to remove dest if it exists
                    with contextlib.suppress(FileNotFoundError):
                        dest.unlink()
                    # Sync dest parent if it exists
                    with contextlib.suppress(FileNotFoundError, OSError):
                        _fsync_dir(dest.parent)

                    self._db.begin_transaction()
                    try:
                        self._db.update_move_phase(journal_id, "failed")
                        self._db.commit()
                    except Exception:
                        self._db.rollback()

                    recovered += 1
                except FileNotFoundError:
                    # Dest doesn't exist - move never happened or manual cleanup
                    self._db.begin_transaction()
                    try:
                        self._db.update_move_phase(journal_id, "failed")
                        self._db.commit()
                    except Exception:
                        self._db.rollback()

                    recovered += 1
                except OSError as e:
                    if e.errno == errno.EXDEV:
                        logger.error(
                            "Cannot rollback cross-device move: {} -> {}. "
                            "Manual intervention required.",
                            dest,
                            source,
                        )
                    else:
                        logger.error(
                            "Critical: Cannot rollback move {} -> {}: {}. "
                            "File may exist in processing_dir but is NOT indexed!",
                            dest,
                            source,
                            e,
                        )
                    # Don't mark as failed - needs manual review

        return recovered

    def process_directory(
        self,
        directory: Path | str,
        recursive: bool = True,
        ignore_patterns: list[str] | None = None,
        tags: dict[str, str] | None = None,
    ) -> Iterator[ProcessResult]:
        """Process all files in a directory."""
        self._ensure_connected()
        directory = Path(directory)

        if not directory.is_dir():
            raise ValueError(f"Not a directory: {directory}")

        # Combine default ignores with user patterns
        ignores = DEFAULT_IGNORES.copy()
        if ignore_patterns:
            ignores.update(ignore_patterns)

        # Load .bgateignore if it exists
        bgateignore_path = directory / ".bgateignore"
        if bgateignore_path.exists():
            try:
                with open(bgateignore_path, "r") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#"):
                            ignores.add(line)
            except Exception as e:
                logger.warning("Failed to read .bgateignore: {}", e)

        yield from self._process_directory_scandir(directory, recursive, ignores, tags)

    def _process_directory_scandir(
        self,
        directory: Path,
        recursive: bool,
        ignores: set[str],
        tags: dict[str, str] | None = None,
    ) -> Iterator[ProcessResult]:
        """Process directory using scandir for efficient stat access."""
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    # Skip ignored files/directories early
                    if entry.name in ignores:
                        continue

                    try:
                        if entry.is_file(follow_symlinks=False):
                            stat_result = entry.stat(follow_symlinks=False)
                            yield self.process_file(Path(entry.path), stat_result, tags=tags)
                        elif recursive and entry.is_dir(follow_symlinks=False):
                            yield from self._process_directory_scandir(
                                Path(entry.path), recursive, ignores, tags
                            )
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
