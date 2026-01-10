"""Database schema and connection management for bgate-unix.

Uses sqlite-utils for schema management and BLOB-based hash storage.
Schema v3 - implements mandatory schema_version tracking.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Self

from loguru import logger
from sqlite_utils import Database

if TYPE_CHECKING:
    from collections.abc import Iterator

# Unix-only enforcement
if sys.platform == "win32":
    sys.exit("bgate-unix is Unix-only. Windows is not supported.")

CURRENT_SCHEMA_VERSION = 4


class DedupeDatabase:
    """SQLite database for deduplication index using sqlite-utils."""

    def __init__(self, db_path: Path | str) -> None:
        self._db_path = Path(db_path)
        self._db: Database | None = None

    @property
    def db_path(self) -> Path:
        return self._db_path

    def connect(self) -> None:
        """Establish database connection and initialize schema."""
        self._db = Database(self._db_path)
        self._apply_pragmas()

        # Safety: Check for legacy tables without version tracking
        tables = self._db.table_names()
        if tables and "schema_version" not in tables:
            logger.error(
                "Legacy or incompatible database detected (missing schema_version). "
                "Manual migration or a fresh database is required."
            )
            sys.exit(1)

        self._create_schema()
        self._enforce_schema_version()

    def _apply_pragmas(self) -> None:
        if self._db is None:
            return
        conn = self._db.conn
        if conn is None:
            return
        # Set isolation_level to None for manual transaction control
        conn.isolation_level = None
        self._db.execute("PRAGMA journal_mode = WAL")
        self._db.execute("PRAGMA synchronous = FULL")
        self._db.execute("PRAGMA busy_timeout = 5000")
        self._db.execute("PRAGMA cache_size = -64000")
        self._db.execute("PRAGMA temp_store = MEMORY")
        self._db.execute("PRAGMA mmap_size = 268435456")

    def _enforce_schema_version(self) -> None:
        """Handle schema migrations and version enforcement."""
        current_version = self.schema_version

        if current_version < CURRENT_SCHEMA_VERSION:
            logger.info(
                "Migrating database from v{} to v{}", current_version, CURRENT_SCHEMA_VERSION
            )
            self._migrate_schema(current_version)
        elif current_version > CURRENT_SCHEMA_VERSION:
            logger.error(
                "Database schema version is newer than supported! "
                "Expected v{}, found v{}. "
                "Please update bgate-unix or use a compatible database.",
                CURRENT_SCHEMA_VERSION,
                current_version,
            )
            sys.exit(1)

    def _migrate_schema(self, from_version: int) -> None:
        """Apply schema migrations."""
        if from_version < 4:
            # Add metadata column to full_index
            logger.info("Adding metadata column to full_index table")
            self._db.execute("ALTER TABLE full_index ADD COLUMN metadata TEXT")

        # Update schema version
        self._db.execute(
            "INSERT INTO schema_version (version, applied_at) VALUES (?, ?)",
            [CURRENT_SCHEMA_VERSION, datetime.now(UTC).isoformat()],
        )
        logger.info("Schema migration to v{} completed", CURRENT_SCHEMA_VERSION)

    def _create_schema(self) -> None:
        if self._db is None:
            return

        # Size index for Tier 1
        if "size_index" not in self._db.table_names():
            self._db.execute("""
                CREATE TABLE size_index (
                    file_size INTEGER PRIMARY KEY
                ) WITHOUT ROWID
            """)

        # Fringe hash index for Tier 2 (BLOB)
        if "fringe_index" not in self._db.table_names():
            self._db.execute("""
                CREATE TABLE fringe_index (
                    fringe_hash BLOB NOT NULL,
                    file_size INTEGER NOT NULL,
                    file_path TEXT NOT NULL,
                    PRIMARY KEY (fringe_hash, file_size)
                ) WITHOUT ROWID
            """)

        # Full hash index for Tier 3 (BLOB) with metadata
        if "full_index" not in self._db.table_names():
            self._db.execute("""
                CREATE TABLE full_index (
                    full_hash BLOB PRIMARY KEY,
                    file_path TEXT NOT NULL,
                    metadata TEXT
                ) WITHOUT ROWID
            """)

        # Orphan registry for crash recovery
        if "orphan_registry" not in self._db.table_names():
            self._db.execute("""
                CREATE TABLE orphan_registry (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    original_path TEXT NOT NULL,
                    orphan_path TEXT NOT NULL,
                    file_size INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    recovered_at TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    UNIQUE(orphan_path)
                )
            """)

        # Move journal for crash recovery
        if "move_journal" not in self._db.table_names():
            self._db.execute("""
                CREATE TABLE move_journal (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_path TEXT NOT NULL,
                    dest_path TEXT NOT NULL,
                    file_size INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    phase TEXT NOT NULL DEFAULT 'planned',
                    completed_at TEXT
                )
            """)

        # Schema version
        if "schema_version" not in self._db.table_names():
            self._db.execute("""
                CREATE TABLE schema_version (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                )
            """)
            self._db.execute(
                "INSERT INTO schema_version (version, applied_at) VALUES (?, ?)",
                [CURRENT_SCHEMA_VERSION, datetime.now(UTC).isoformat()],
            )

    def close(self) -> None:
        if self._db:
            self._db.close()
            self._db = None

    def __enter__(self) -> Self:
        self.connect()
        return self

    def __exit__(self, *args: object) -> bool | None:
        self.close()
        return None

    @property
    def db(self) -> Database:
        if self._db is None:
            raise RuntimeError("Database not connected")
        return self._db

    @property
    def schema_version(self) -> int:
        try:
            row = self.db.execute("SELECT MAX(version) FROM schema_version").fetchone()
            return row[0] if row and row[0] else 0
        except Exception:
            return 0

    # Tier 1: Size operations
    def size_exists(self, file_size: int) -> bool:
        row = self.db.execute(
            "SELECT 1 FROM size_index WHERE file_size = ?", [file_size]
        ).fetchone()
        return row is not None

    def add_size(self, file_size: int) -> None:
        self.db.execute(
            "INSERT OR IGNORE INTO size_index (file_size) VALUES (?)",
            [file_size],
        )

    # Tier 2: Fringe hash operations (BLOB)
    def fringe_lookup(self, fringe_hash: bytes, file_size: int) -> str | None:
        row = self.db.execute(
            "SELECT file_path FROM fringe_index WHERE fringe_hash = ? AND file_size = ?",
            [fringe_hash, file_size],
        ).fetchone()
        return row[0] if row else None

    def add_fringe(self, fringe_hash: bytes, file_size: int, file_path: str) -> bool:
        cursor = self.db.execute(
            """
            INSERT INTO fringe_index (fringe_hash, file_size, file_path)
            VALUES (?, ?, ?)
            ON CONFLICT DO NOTHING
            """,
            [fringe_hash, file_size, file_path],
        )
        return cursor.rowcount > 0

    # Tier 3: Full hash operations (BLOB)
    def full_lookup(self, full_hash: bytes) -> str | None:
        row = self.db.execute(
            "SELECT file_path FROM full_index WHERE full_hash = ?", [full_hash]
        ).fetchone()
        return row[0] if row else None

    def add_full(self, full_hash: bytes, file_path: str, metadata: str | None = None) -> bool:
        cursor = self._db.execute(
            """
            INSERT INTO full_index (full_hash, file_path, metadata)
            VALUES (?, ?, ?)
            ON CONFLICT DO NOTHING
            """,
            [full_hash, file_path, metadata],
        )
        return cursor.rowcount > 0

    def get_all_paths(self) -> Iterator[str]:
        for row in self.db.execute("SELECT DISTINCT file_path FROM full_index"):
            yield row[0]

    # Orphan registry
    def add_orphan(self, original_path: str, orphan_path: str, file_size: int) -> int:
        self.db.execute(
            """
            INSERT INTO orphan_registry (original_path, orphan_path, file_size, created_at, status)
            VALUES (?, ?, ?, ?, 'pending')
            ON CONFLICT(orphan_path) DO NOTHING
            """,
            [original_path, orphan_path, file_size, datetime.now(UTC).isoformat()],
        )
        # Fetch the ID (either newly inserted or existing) to ensure idempotency
        row = self.db.execute(
            "SELECT id FROM orphan_registry WHERE orphan_path = ?", [orphan_path]
        ).fetchone()
        return row[0] if row else 0

    def update_orphan_status(self, orphan_id: int, status: str) -> None:
        recovered_at = datetime.now(UTC).isoformat() if status != "pending" else None
        self.db.execute(
            "UPDATE orphan_registry SET status = ?, recovered_at = ? WHERE id = ?",
            [status, recovered_at, orphan_id],
        )

    def get_pending_orphans(self) -> list[dict]:
        rows = self.db.execute(
            """
            SELECT id, original_path, orphan_path, file_size, created_at
            FROM orphan_registry WHERE status = 'pending'
            """
        ).fetchall()
        return [
            {
                "id": r[0],
                "original_path": r[1],
                "orphan_path": r[2],
                "file_size": r[3],
                "created_at": r[4],
            }
            for r in rows
        ]

    def get_orphan_count(self) -> int:
        row = self.db.execute(
            "SELECT COUNT(*) FROM orphan_registry WHERE status = 'pending'"
        ).fetchone()
        return row[0] if row else 0

    # Move journal
    def journal_move(self, source_path: str, dest_path: str, file_size: int) -> int:
        cursor = self.db.execute(
            """
            INSERT INTO move_journal (source_path, dest_path, file_size, created_at, phase)
            VALUES (?, ?, ?, ?, 'planned')
            """,
            [source_path, dest_path, file_size, datetime.now(UTC).isoformat()],
        )
        return cursor.lastrowid or 0

    def update_move_phase(self, journal_id: int, phase: str) -> None:
        completed_at = datetime.now(UTC).isoformat() if phase in ("completed", "failed") else None
        self.db.execute(
            "UPDATE move_journal SET phase = ?, completed_at = ? WHERE id = ?",
            [phase, completed_at, journal_id],
        )

    def get_incomplete_journal_entries(self) -> list[dict]:
        rows = self.db.execute(
            """
            SELECT id, source_path, dest_path, file_size, created_at, phase
            FROM move_journal WHERE phase NOT IN ('completed', 'failed')
            """
        ).fetchall()
        return [
            {
                "id": r[0],
                "source_path": r[1],
                "dest_path": r[2],
                "file_size": r[3],
                "created_at": r[4],
                "phase": r[5],
            }
            for r in rows
        ]

    def get_pending_journal_count(self) -> int:
        row = self.db.execute(
            "SELECT COUNT(*) FROM move_journal WHERE phase NOT IN ('completed', 'failed')"
        ).fetchone()
        return row[0] if row else 0

    def begin_transaction(self) -> None:
        conn = self.db.conn
        if conn is not None and not conn.in_transaction:
            conn.execute("BEGIN IMMEDIATE")

    def commit(self) -> None:
        conn = self.db.conn
        if conn is not None and conn.in_transaction:
            conn.execute("COMMIT")

    def rollback(self) -> None:
        conn = self.db.conn
        if conn is not None and conn.in_transaction:
            conn.execute("ROLLBACK")
