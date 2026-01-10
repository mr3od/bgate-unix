"""Comprehensive test suite for bgate-unix deduplication engine."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

from bgate_unix.db import DedupeDatabase
from bgate_unix.engine import (
    CHUNK_SIZE,
    FRINGE_SIZE,
    DedupeResult,
    FileDeduplicator,
    atomic_move,
)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def db_path(temp_dir: Path) -> Path:
    """Create a temporary database path."""
    return temp_dir / "test.db"


@pytest.fixture
def deduplicator(db_path: Path):
    """Create a connected FileDeduplicator instance."""
    with FileDeduplicator(db_path) as deduper:
        yield deduper


@pytest.fixture
def inbound_dir(temp_dir: Path) -> Path:
    """Create inbound directory for file processing."""
    inbound = temp_dir / "inbound"
    inbound.mkdir()
    return inbound


@pytest.fixture
def processing_dir(temp_dir: Path) -> Path:
    """Create processing directory for unique files."""
    processing = temp_dir / "processing"
    processing.mkdir()
    return processing


class TestAtomicMove:
    """Test atomic file move operation."""

    def test_basic_move(self, temp_dir: Path):
        """Basic file move should work."""
        src = temp_dir / "source.txt"
        dest = temp_dir / "dest.txt"
        src.write_text("test content")

        atomic_move(src, dest)

        assert not src.exists()
        assert dest.exists()
        assert dest.read_text() == "test content"

    def test_creates_parent_dirs(self, temp_dir: Path):
        """Should create parent directories if needed."""
        src = temp_dir / "source.txt"
        dest = temp_dir / "nested" / "deep" / "dest.txt"
        src.write_text("test")

        atomic_move(src, dest)

        assert dest.exists()
        assert dest.read_text() == "test"

    def test_refuses_overwrite(self, temp_dir: Path):
        """Should refuse to overwrite existing destination file."""
        src = temp_dir / "source.txt"
        dest = temp_dir / "dest.txt"
        src.write_text("new content")
        dest.write_text("old content")

        with pytest.raises(FileExistsError):
            atomic_move(src, dest)

        # Source should still exist, dest unchanged
        assert src.read_text() == "new content"
        assert dest.read_text() == "old content"

    def test_cross_device_move_raises_exdev(self, temp_dir: Path, monkeypatch):
        """Cross-device moves should raise clear OSError with EXDEV context."""
        import errno

        src = temp_dir / "source.txt"
        dest = temp_dir / "dest.txt"
        src.write_text("test content")

        def mock_link(_src_path, _dest_path):
            raise OSError(errno.EXDEV, "Cross-device link")

        monkeypatch.setattr("os.link", mock_link)

        with pytest.raises(OSError, match="Cross-device move failed"):
            atomic_move(src, dest)

        # Source should still exist after failed cross-device attempt
        assert src.exists()
        assert src.read_text() == "test content"


class TestDedupeDatabase:
    """Test database operations with BLOB-based hashes."""

    def test_connection(self, db_path: Path):
        """Database should connect and create schema."""
        with DedupeDatabase(db_path) as db:
            assert db.db is not None

    def test_size_operations(self, db_path: Path):
        """Size index operations should work correctly."""
        with DedupeDatabase(db_path) as db:
            assert not db.size_exists(1000)
            db.add_size(1000)
            assert db.size_exists(1000)

    def test_fringe_operations_blob(self, db_path: Path):
        """Fringe index operations should work with BLOB hashes."""
        with DedupeDatabase(db_path) as db:
            fringe_hash = b"\x01\x02\x03\x04\x05\x06\x07\x08"
            assert db.fringe_lookup(fringe_hash, 1000) is None
            db.add_fringe(fringe_hash, 1000, "/path/to/file")
            assert db.fringe_lookup(fringe_hash, 1000) == "/path/to/file"

    def test_full_operations_blob(self, db_path: Path):
        """Full hash index operations should work with BLOB hashes."""
        with DedupeDatabase(db_path) as db:
            full_hash = b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10"
            assert db.full_lookup(full_hash) is None
            db.add_full(full_hash, "/path/to/file")
            assert db.full_lookup(full_hash) == "/path/to/file"

    def test_schema_version(self, db_path: Path):
        """Schema version should be set correctly."""
        with DedupeDatabase(db_path) as db:
            assert db.schema_version == 4

    def test_move_journal(self, db_path: Path):
        """Move journal operations should work correctly."""
        with DedupeDatabase(db_path) as db:
            journal_id = db.journal_move("/src/file.txt", "/dest/file.txt", 1000)
            assert journal_id > 0

            entries = db.get_incomplete_journal_entries()
            assert len(entries) == 1
            assert entries[0]["phase"] == "planned"

            db.update_move_phase(journal_id, "completed")
            entries = db.get_incomplete_journal_entries()
            assert len(entries) == 0


class TestTier0EmptyFiles:
    """Test Tier 0: Empty file handling."""

    def test_empty_file_skipped(self, deduplicator: FileDeduplicator, temp_dir: Path):
        """Empty files should be skipped."""
        empty_file = temp_dir / "empty.txt"
        empty_file.touch()

        result = deduplicator.process_file(empty_file)

        assert result.result == DedupeResult.SKIPPED
        assert result.tier == 0

    def test_nonexistent_file_skipped(self, deduplicator: FileDeduplicator, temp_dir: Path):
        """Non-existent files should be skipped with error."""
        result = deduplicator.process_file(temp_dir / "nonexistent.txt")

        assert result.result == DedupeResult.SKIPPED
        assert result.error is not None


class TestTier1SizeCheck:
    """Test Tier 1: Size-based deduplication."""

    def test_unique_size_is_unique(self, deduplicator: FileDeduplicator, temp_dir: Path):
        """File with unique size should be marked unique at Tier 1."""
        file1 = temp_dir / "file1.txt"
        file1.write_bytes(b"a" * 100)

        result = deduplicator.process_file(file1)

        assert result.result == DedupeResult.UNIQUE
        assert result.tier == 1

    def test_different_sizes_both_unique(self, deduplicator: FileDeduplicator, temp_dir: Path):
        """Files with different sizes should both be unique."""
        file1 = temp_dir / "file1.txt"
        file2 = temp_dir / "file2.txt"
        file1.write_bytes(b"a" * 100)
        file2.write_bytes(b"b" * 200)

        result1 = deduplicator.process_file(file1)
        result2 = deduplicator.process_file(file2)

        assert result1.result == DedupeResult.UNIQUE
        assert result2.result == DedupeResult.UNIQUE


class TestTier2FringeHash:
    """Test Tier 2: Fringe hash deduplication."""

    def test_same_size_different_content_unique(
        self, deduplicator: FileDeduplicator, temp_dir: Path
    ):
        """Files with same size but different content should be unique."""
        file1 = temp_dir / "file1.txt"
        file2 = temp_dir / "file2.txt"
        file1.write_bytes(os.urandom(100))
        file2.write_bytes(os.urandom(100))

        result1 = deduplicator.process_file(file1)
        result2 = deduplicator.process_file(file2)

        assert result1.result == DedupeResult.UNIQUE
        assert result2.result == DedupeResult.UNIQUE
        assert result2.tier >= 2

    def test_large_file_fringe_hash(self, deduplicator: FileDeduplicator, temp_dir: Path):
        """Large files should use fringe hash correctly."""
        file1 = temp_dir / "large1.bin"
        file2 = temp_dir / "large2.bin"

        head = os.urandom(FRINGE_SIZE)
        tail = os.urandom(FRINGE_SIZE)
        middle1 = b"M" * FRINGE_SIZE
        middle2 = b"X" * FRINGE_SIZE

        content1 = head + middle1 + tail
        content2 = head + middle2 + tail

        file1.write_bytes(content1)
        file2.write_bytes(content2)

        result1 = deduplicator.process_file(file1)
        result2 = deduplicator.process_file(file2)

        # Same edges but different middle - fringe hash same, full hash different
        assert result1.result == DedupeResult.UNIQUE
        assert result2.result == DedupeResult.UNIQUE


class TestTier3FullHash:
    """Test Tier 3: Full content hash deduplication (xxHash128)."""

    def test_exact_duplicate_detected(self, deduplicator: FileDeduplicator, temp_dir: Path):
        """Exact binary duplicates should be detected."""
        file1 = temp_dir / "original.txt"
        file2 = temp_dir / "duplicate.txt"
        content = os.urandom(100)
        file1.write_bytes(content)
        file2.write_bytes(content)

        result1 = deduplicator.process_file(file1)
        result2 = deduplicator.process_file(file2)

        assert result1.result == DedupeResult.UNIQUE
        assert result2.result == DedupeResult.DUPLICATE
        assert result2.duplicate_of == file1

    def test_large_duplicate_detected(self, deduplicator: FileDeduplicator, temp_dir: Path):
        """Large exact duplicates should be detected."""
        file1 = temp_dir / "large1.bin"
        file2 = temp_dir / "large2.bin"

        content = os.urandom(CHUNK_SIZE * 3)
        file1.write_bytes(content)
        file2.write_bytes(content)

        result1 = deduplicator.process_file(file1)
        result2 = deduplicator.process_file(file2)

        assert result1.result == DedupeResult.UNIQUE
        assert result2.result == DedupeResult.DUPLICATE


class TestProcessingDirectory:
    """Test file movement to processing directory."""

    def test_unique_file_moved(self, db_path: Path, inbound_dir: Path, processing_dir: Path):
        """Unique files should be moved to processing directory."""
        with FileDeduplicator(db_path, processing_dir=processing_dir) as deduper:
            src_file = inbound_dir / "unique.txt"
            src_file.write_bytes(os.urandom(100))

            result = deduper.process_file(src_file)

            assert result.result == DedupeResult.UNIQUE
            assert not src_file.exists()
            files_in_processing = [p for p in processing_dir.rglob("*") if p.is_file()]
            assert len(files_in_processing) == 1
            assert files_in_processing[0].suffix == ".txt"

    def test_duplicate_not_moved(self, db_path: Path, inbound_dir: Path, processing_dir: Path):
        """Duplicate files should not be moved."""
        with FileDeduplicator(db_path, processing_dir=processing_dir) as deduper:
            content = os.urandom(100)

            file1 = inbound_dir / "original.txt"
            file1.write_bytes(content)
            deduper.process_file(file1)

            file2 = inbound_dir / "duplicate.txt"
            file2.write_bytes(content)
            result = deduper.process_file(file2)

            assert result.result == DedupeResult.DUPLICATE
            assert file2.exists()

    def test_name_collision_handling(self, db_path: Path, inbound_dir: Path, processing_dir: Path):
        """Should handle multiple unique files with hash-based naming."""
        with FileDeduplicator(db_path, processing_dir=processing_dir) as deduper:
            file1 = inbound_dir / "file.txt"
            file1.write_bytes(os.urandom(100))
            deduper.process_file(file1)

            file2 = inbound_dir / "file.txt"
            file2.write_bytes(os.urandom(100))
            deduper.process_file(file2)

            files_in_processing = [p for p in processing_dir.rglob("*") if p.is_file()]
            assert len(files_in_processing) == 2
            assert all(f.suffix == ".txt" for f in files_in_processing)


class TestDirectoryProcessing:
    """Test batch directory processing."""

    def test_process_directory(self, deduplicator: FileDeduplicator, temp_dir: Path):
        """Should process all files in directory."""
        test_dir = temp_dir / "batch"
        test_dir.mkdir()

        content = os.urandom(100)
        (test_dir / "file1.txt").write_bytes(content)
        (test_dir / "file2.txt").write_bytes(os.urandom(100))
        (test_dir / "file3.txt").write_bytes(content)  # Duplicate of file1

        results = list(deduplicator.process_directory(test_dir))

        assert len(results) == 3
        unique_count = sum(1 for r in results if r.result == DedupeResult.UNIQUE)
        dup_count = sum(1 for r in results if r.result == DedupeResult.DUPLICATE)
        assert unique_count == 2
        assert dup_count == 1

    def test_recursive_processing(self, deduplicator: FileDeduplicator, temp_dir: Path):
        """Should process subdirectories when recursive=True."""
        test_dir = temp_dir / "recursive"
        test_dir.mkdir()
        (test_dir / "sub").mkdir()

        (test_dir / "file1.txt").write_bytes(os.urandom(100))
        (test_dir / "sub" / "file2.txt").write_bytes(os.urandom(100))

        results = list(deduplicator.process_directory(test_dir, recursive=True))
        assert len(results) == 2


class TestStats:
    """Test statistics reporting."""

    def test_stats_after_processing(self, deduplicator: FileDeduplicator, temp_dir: Path):
        """Stats should reflect processed files."""
        file1 = temp_dir / "file1.txt"
        file2 = temp_dir / "file2.txt"
        file1.write_bytes(os.urandom(100))
        file2.write_bytes(os.urandom(200))

        deduplicator.process_file(file1)
        deduplicator.process_file(file2)

        stats = deduplicator.stats
        assert stats["unique_sizes"] == 2
        assert stats["full_entries"] == 2
        assert stats["schema_version"] == 4
        assert "pending_journal" in stats


class TestErrorHandling:
    """Test error handling scenarios."""

    def test_unconnected_raises(self, db_path: Path):
        """Operations without connection should raise."""
        deduper = FileDeduplicator(db_path)
        with pytest.raises(RuntimeError, match="not connected"):
            deduper.process_file(Path("/some/file"))

    def test_invalid_directory_raises(self, deduplicator: FileDeduplicator, temp_dir: Path):
        """Processing non-directory should raise."""
        file_path = temp_dir / "not_a_dir.txt"
        file_path.write_text("test")

        with pytest.raises(ValueError, match="Not a directory"):
            list(deduplicator.process_directory(file_path))


class TestJournalRecovery:
    """Test journal-based recovery."""

    def test_journal_recovery_on_connect(self, db_path: Path):
        """Should recover from incomplete journal entries on connect."""
        with DedupeDatabase(db_path) as db:
            db.journal_move("/src/file.txt", "/dest/file.txt", 1000)

        with FileDeduplicator(db_path) as deduper:
            stats = deduper.stats
            assert stats["pending_journal"] == 0
