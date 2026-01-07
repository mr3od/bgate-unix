"""Database schema and connection management for byte-gate."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

# SQLite signed 64-bit integer bounds
SQLITE_INT64_MAX = (1 << 63) - 1
SQLITE_INT64_MIN = -(1 << 63)


def uint64_to_signed(value: int) -> int:
    """Convert unsigned 64-bit integer to signed for SQLite storage.

    xxhash returns unsigned 64-bit integers, but SQLite stores signed integers.
    This converts the unsigned value to its signed representation.
    """
    if value > SQLITE_INT64_MAX:
        return value - (1 << 64)
    return value


def signed_to_uint64(value: int) -> int:
    """Convert signed 64-bit integer back to unsigned."""
    if value < 0:
        return value + (1 << 64)
    return value


class DedupeDatabase:
    """SQLite database for deduplication index with optimized pragmas."""

    def __init__(self, db_path: Path | str) -> None:
        """Initialize database connection with performance pragmas.

        Args:
            db_path: Path to SQLite database file.
        """
        self._db_path = Path(db_path)
        self._conn: sqlite3.Connection | None = None

    def connect(self) -> None:
        """Establish database connection and apply pragmas."""
        try:
            self._conn = sqlite3.connect(
                self._db_path,
                isolation_level=None,  # Autocommit for explicit transaction control
                check_same_thread=False,
            )
            self._apply_pragmas()
            self._create_schema()
        except sqlite3.Error as e:
            raise RuntimeError(f"Failed to connect to database: {e}") from e

    def _apply_pragmas(self) -> None:
        """Apply performance-optimized SQLite pragmas."""
        if self._conn is None:
            return

        pragmas = [
            "PRAGMA journal_mode = WAL",
            "PRAGMA synchronous = NORMAL",
            "PRAGMA cache_size = -64000",  # 64MB cache
            "PRAGMA temp_store = MEMORY",
            "PRAGMA mmap_size = 268435456",  # 256MB mmap
        ]
        for pragma in pragmas:
            self._conn.execute(pragma)

    def _create_schema(self) -> None:
        """Create database tables if they don't exist."""
        if self._conn is None:
            return

        # Size index for Tier 1 lookups
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS size_index (
                file_size INTEGER PRIMARY KEY,
                count INTEGER NOT NULL DEFAULT 1
            ) WITHOUT ROWID
        """)

        # Fringe hash index for Tier 2 lookups (edges + size)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS fringe_index (
                fringe_hash INTEGER NOT NULL,
                file_size INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                PRIMARY KEY (fringe_hash, file_size)
            ) WITHOUT ROWID
        """)

        # Full hash index for Tier 3 lookups
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS full_index (
                full_hash INTEGER PRIMARY KEY,
                file_path TEXT NOT NULL
            ) WITHOUT ROWID
        """)

    def close(self) -> None:
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> DedupeDatabase:
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, *args: object) -> None:
        """Context manager exit."""
        self.close()

    @property
    def connection(self) -> sqlite3.Connection:
        """Get active database connection."""
        if self._conn is None:
            raise RuntimeError("Database not connected")
        return self._conn

    # Tier 1: Size operations
    def size_exists(self, file_size: int) -> bool:
        """Check if a file of this size already exists."""
        cursor = self.connection.execute(
            "SELECT 1 FROM size_index WHERE file_size = ?", (file_size,)
        )
        return cursor.fetchone() is not None

    def add_size(self, file_size: int) -> None:
        """Add or increment size count."""
        self.connection.execute(
            """
            INSERT INTO size_index (file_size, count) VALUES (?, 1)
            ON CONFLICT(file_size) DO UPDATE SET count = count + 1
            """,
            (file_size,),
        )

    # Tier 2: Fringe hash operations
    def fringe_lookup(self, fringe_hash: int, file_size: int) -> str | None:
        """Look up existing file by fringe hash and size.

        Args:
            fringe_hash: Signed 64-bit fringe hash.
            file_size: File size in bytes.

        Returns:
            Path to existing file if found, None otherwise.
        """
        cursor = self.connection.execute(
            "SELECT file_path FROM fringe_index WHERE fringe_hash = ? AND file_size = ?",
            (fringe_hash, file_size),
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def add_fringe(self, fringe_hash: int, file_size: int, file_path: str) -> None:
        """Add fringe hash entry."""
        self.connection.execute(
            "INSERT OR IGNORE INTO fringe_index (fringe_hash, file_size, file_path) VALUES (?, ?, ?)",
            (fringe_hash, file_size, file_path),
        )

    # Tier 3: Full hash operations
    def full_lookup(self, full_hash: int) -> str | None:
        """Look up existing file by full content hash.

        Args:
            full_hash: Signed 64-bit full content hash.

        Returns:
            Path to existing file if found, None otherwise.
        """
        cursor = self.connection.execute(
            "SELECT file_path FROM full_index WHERE full_hash = ?", (full_hash,)
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def add_full(self, full_hash: int, file_path: str) -> None:
        """Add full hash entry."""
        self.connection.execute(
            "INSERT OR IGNORE INTO full_index (full_hash, file_path) VALUES (?, ?)",
            (full_hash, file_path),
        )

    def get_all_paths(self) -> Iterator[str]:
        """Iterate over all stored file paths."""
        cursor = self.connection.execute("SELECT DISTINCT file_path FROM full_index")
        for row in cursor:
            yield row[0]
