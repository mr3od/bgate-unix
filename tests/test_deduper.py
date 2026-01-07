"""Comprehensive test suite for byte-gate deduplication engine."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

from byte_gate.db import DedupeDatabase, signed_to_uint64, uint64_to_signed
from byte_gate.engine import (
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


class TestIntegerConversion:
    """Test signed/unsigned 64-bit integer conversion."""

    def test_small_positive(self):
        """Small positive values should remain unchanged."""
        assert uint64_to_signed(100) == 100
        assert signed_to_uint64(100) == 100

    def test_max_signed(self):
        """Maximum signed value should remain unchanged."""
        max_signed = (1 << 63) - 1
        assert uint64_to_signed(max_signed) == max_signed
        assert signed_to_uint64(max_signed) == max_signed

    def test_overflow_to_negative(self):
        """Values above max signed should become negative."""
        # 2^63 should become -2^63
        value = 1 << 63
        signed = uint64_to_signed(value)
        assert signed < 0
        assert signed_to_uint64(signed) == value

    def test_max_unsigned(self):
        """Maximum unsigned value should convert correctly."""
        max_unsigned = (1 << 64) - 1
        signed = uint64_to_signed(max_unsigned)
        assert signed == -1
        assert signed_to_uint64(signed) == max_unsigned

    def test_roundtrip(self):
        """Conversion should be reversible."""
        test_values = [0, 1, 1000, (1 << 63) - 1, 1 << 63, (1 << 64) - 1]
        for value in test_values:
            signed = uint64_to_signed(value)
            unsigned = signed_to_uint64(signed)
            assert unsigned == value


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

    def test_overwrites_existing(self, temp_dir: Path):
        """Should overwrite existing destination file."""
        src = temp_dir / "source.txt"
        dest = temp_dir / "dest.txt"
        src.write_text("new content")
        dest.write_text("old content")

        atomic_move(src, dest)

        assert dest.read_text() == "new content"


class TestDedupeDatabase:
    """Test database operations."""

    def test_connection(self, db_path: Path):
        """Database should connect and create schema."""
        with DedupeDatabase(db_path) as db:
            assert db.connection is not None

    def test_size_operations(self, db_path: Path):
        """Size index operations should work correctly."""
        with DedupeDatabase(db_path) as db:
            assert not db.size_exists(1000)
            db.add_size(1000)
            assert db.size_exists(1000)

    def test_fringe_operations(self, db_path: Path):
        """Fringe index operations should work correctly."""
        with DedupeDatabase(db_path) as db:
            assert db.fringe_lookup(12345, 1000) is None
            db.add_fringe(12345, 1000, "/path/to/file")
            assert db.fringe_lookup(12345, 1000) == "/path/to/file"

    def test_full_operations(self, db_path: Path):
        """Full hash index operations should work correctly."""
        with DedupeDatabase(db_path) as db:
            assert db.full_lookup(99999) is None
            db.add_full(99999, "/path/to/file")
            assert db.full_lookup(99999) == "/path/to/file"

    def test_negative_hash_storage(self, db_path: Path):
        """Should handle negative (signed) hash values."""
        with DedupeDatabase(db_path) as db:
            negative_hash = -9223372036854775808  # Min signed 64-bit
            db.add_full(negative_hash, "/test/path")
            assert db.full_lookup(negative_hash) == "/test/path"


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
        file1.write_bytes(b"a" * 100)
        file2.write_bytes(b"b" * 100)

        result1 = deduplicator.process_file(file1)
        result2 = deduplicator.process_file(file2)

        assert result1.result == DedupeResult.UNIQUE
        assert result2.result == DedupeResult.UNIQUE
        # Second file should be determined at Tier 2 or 3
        assert result2.tier >= 2

    def test_large_file_fringe_hash(self, deduplicator: FileDeduplicator, temp_dir: Path):
        """Large files should use fringe hash correctly."""
        # Create file larger than 2 * FRINGE_SIZE
        file1 = temp_dir / "large1.bin"
        file2 = temp_dir / "large2.bin"

        content1 = b"A" * FRINGE_SIZE + b"M" * FRINGE_SIZE + b"Z" * FRINGE_SIZE
        content2 = b"A" * FRINGE_SIZE + b"X" * FRINGE_SIZE + b"Z" * FRINGE_SIZE

        file1.write_bytes(content1)
        file2.write_bytes(content2)

        result1 = deduplicator.process_file(file1)
        result2 = deduplicator.process_file(file2)

        # Same edges but different middle - fringe hash same, full hash different
        assert result1.result == DedupeResult.UNIQUE
        assert result2.result == DedupeResult.UNIQUE


class TestTier3FullHash:
    """Test Tier 3: Full content hash deduplication."""

    def test_exact_duplicate_detected(self, deduplicator: FileDeduplicator, temp_dir: Path):
        """Exact binary duplicates should be detected."""
        file1 = temp_dir / "original.txt"
        file2 = temp_dir / "duplicate.txt"
        content = b"This is the exact same content"
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

        # Create file larger than chunk size
        content = os.urandom(CHUNK_SIZE * 3)
        file1.write_bytes(content)
        file2.write_bytes(content)

        result1 = deduplicator.process_file(file1)
        result2 = deduplicator.process_file(file2)

        assert result1.result == DedupeResult.UNIQUE
        assert result2.result == DedupeResult.DUPLICATE


class TestHDDOptimization:
    """Test HDD optimization mode."""

    def test_hdd_mode_sequential_read(self, db_path: Path, temp_dir: Path):
        """HDD mode should use sequential reads."""
        with FileDeduplicator(db_path, optimize_for_hdd=True) as deduper:
            file1 = temp_dir / "file1.bin"
            file2 = temp_dir / "file2.bin"

            # Files with same first 16KB but different endings
            content1 = b"A" * 20000 + b"B" * 10000
            content2 = b"A" * 20000 + b"C" * 10000

            file1.write_bytes(content1)
            file2.write_bytes(content2)

            result1 = deduper.process_file(file1)
            result2 = deduper.process_file(file2)

            # Both should be unique (full hash will differ)
            assert result1.result == DedupeResult.UNIQUE
            assert result2.result == DedupeResult.UNIQUE


class TestProcessingDirectory:
    """Test file movement to processing directory."""

    def test_unique_file_moved(self, db_path: Path, inbound_dir: Path, processing_dir: Path):
        """Unique files should be moved to processing directory."""
        with FileDeduplicator(db_path, processing_dir=processing_dir) as deduper:
            src_file = inbound_dir / "unique.txt"
            src_file.write_text("unique content")

            result = deduper.process_file(src_file)

            assert result.result == DedupeResult.UNIQUE
            assert not src_file.exists()
            assert (processing_dir / "unique.txt").exists()

    def test_duplicate_not_moved(self, db_path: Path, inbound_dir: Path, processing_dir: Path):
        """Duplicate files should not be moved."""
        with FileDeduplicator(db_path, processing_dir=processing_dir) as deduper:
            # First file - unique
            file1 = inbound_dir / "original.txt"
            file1.write_text("same content")
            deduper.process_file(file1)

            # Second file - duplicate
            file2 = inbound_dir / "duplicate.txt"
            file2.write_text("same content")
            result = deduper.process_file(file2)

            assert result.result == DedupeResult.DUPLICATE
            assert file2.exists()  # Duplicate stays in place

    def test_name_collision_handling(self, db_path: Path, inbound_dir: Path, processing_dir: Path):
        """Should handle name collisions in processing directory."""
        with FileDeduplicator(db_path, processing_dir=processing_dir) as deduper:
            # Create file with same name but different content
            file1 = inbound_dir / "file.txt"
            file1.write_text("content 1")
            deduper.process_file(file1)

            file2 = inbound_dir / "file.txt"
            file2.write_text("content 2")
            deduper.process_file(file2)

            # Both should exist with different names
            assert (processing_dir / "file.txt").exists()
            assert (processing_dir / "file_1.txt").exists()


class TestDirectoryProcessing:
    """Test batch directory processing."""

    def test_process_directory(self, deduplicator: FileDeduplicator, temp_dir: Path):
        """Should process all files in directory."""
        test_dir = temp_dir / "batch"
        test_dir.mkdir()

        (test_dir / "file1.txt").write_text("content 1")
        (test_dir / "file2.txt").write_text("content 2")
        (test_dir / "file3.txt").write_text("content 1")  # Duplicate of file1

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

        (test_dir / "file1.txt").write_text("content 1")
        (test_dir / "sub" / "file2.txt").write_text("content 2")

        results = list(deduplicator.process_directory(test_dir, recursive=True))
        assert len(results) == 2


class TestStats:
    """Test statistics reporting."""

    def test_stats_after_processing(self, deduplicator: FileDeduplicator, temp_dir: Path):
        """Stats should reflect processed files."""
        file1 = temp_dir / "file1.txt"
        file2 = temp_dir / "file2.txt"
        file1.write_text("content 1")
        file2.write_text("different content 2")  # Different size

        deduplicator.process_file(file1)
        deduplicator.process_file(file2)

        stats = deduplicator.stats
        assert stats["unique_sizes"] == 2
        assert stats["full_entries"] == 2


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
