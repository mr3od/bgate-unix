"""Durability and crash safety tests for bgate-unix."""

from __future__ import annotations

import signal
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from bgate_unix.db import DedupeDatabase
from bgate_unix.engine import (
    FRINGE_SIZE,
    DedupeResult,
    FileDeduplicator,
    _compute_fringe_hash,
    atomic_move,
)


@pytest.fixture
def temp_dir(tmp_path):
    return tmp_path


@pytest.fixture
def db_path(temp_dir):
    return temp_dir / "test.db"


@pytest.fixture
def processing_dir(temp_dir):
    d = temp_dir / "processing"
    d.mkdir()
    return d


class TestAtomicMoveDurability:
    """Test durability guarantees of atomic_move."""

    def test_fsync_ordering(self, temp_dir):
        """Verify strict fsync ordering: link -> fsync(parents) -> fsync(dest) -> unlink -> fsync(src)."""
        src = temp_dir / "src.txt"
        dest = temp_dir / "subdir" / "dest.txt"
        # DO NOT pre-create subdir, let atomic_move create it to test grandparent fsync
        src.write_text("content")

        # Mock os.link, os.unlink, _fsync_dir
        # Use a manager to verify strict ordering of calls across different mocks
        manager = MagicMock()

        with (
            patch("os.link") as mock_link,
            patch("bgate_unix.engine._fsync_dir") as mock_fsync,
            patch.object(Path, "unlink") as mock_unlink,
        ):
            manager.attach_mock(mock_link, "link")
            manager.attach_mock(mock_fsync, "fsync")
            manager.attach_mock(mock_unlink, "unlink")

            atomic_move(src, dest)

            # Expected exact sequence (deep paths)
            # atomic_move(src, dest) where dest = subdir/dest.txt
            # subdir is new, so it should sync subdir.parent (temp_dir)
            expected_calls = [
                call.link(src, dest),
                call.fsync(temp_dir),  # Persist 'subdir' entry
                call.fsync(dest.parent),  # Persist 'dest.txt' link
                call.unlink(),
                call.fsync(src.parent),
            ]
            manager.assert_has_calls(expected_calls)

    def test_fsync_even_if_same_parent(self, temp_dir):
        """Verify fsync(src.parent) happens even if src.parent == dest.parent."""
        src = temp_dir / "src.txt"
        dest = temp_dir / "dest.txt"
        src.write_text("content")

        with (
            patch("os.link"),
            patch("bgate_unix.engine._fsync_dir") as mock_fsync,
            patch.object(Path, "unlink"),
        ):
            atomic_move(src, dest)

            # verify _fsync_dir was called TWICE with the same directory
            assert mock_fsync.call_count == 2
            assert mock_fsync.call_args_list[0] == call(dest.parent)
            assert mock_fsync.call_args_list[1] == call(src.parent)
            assert src.parent == dest.parent


class TestJournalHygiene:
    """Test journal state correctness during conflicts and partial failures."""

    def test_add_full_conflict_marks_journal_failed(self, db_path, processing_dir, temp_dir):
        """If add_full returns False (duplicate), journal entry must terminate as 'failed'."""
        src = temp_dir / "source.txt"
        src.write_bytes(b"content")

        # Pre-seed DB with "existing" entry to force add_full failure
        with DedupeDatabase(db_path) as db:
            db.connect()
            db.add_full(b"fake_full_hash", "/existing/path")

        # Mock _compute_full_hash to return the collision
        with (
            patch("bgate_unix.engine._compute_full_hash", return_value=b"fake_full_hash"),
            patch("bgate_unix.engine._compute_fringe_hash", return_value=b"fake_fringe"),
            FileDeduplicator(db_path, processing_dir=processing_dir) as deduper,
        ):
            # process_file should return DUPLICATE
            result = deduper.process_file(src)
            assert str(result.result.value) == "duplicate"

            # Verify journal state
            # We expect 2 entries:
            # 1. The original move (marked "failed" because index failed)
            # 2. The rollback move (marked "completed")
            entries = deduper._db.db.execute("SELECT * FROM move_journal ORDER BY id").fetchall()
            assert len(entries) == 2

            # Entry 1: Original move (failed)
            assert entries[0][5] == "failed"

            # Entry 2: Rollback move (completed)
            assert entries[1][5] == "completed"
            assert entries[1][1] == entries[0][2]  # Rollback source == Original dest
            assert entries[1][2] == entries[0][1]  # Rollback dest == Original source

            # Verify pending journal entries is 0
            assert deduper._db.get_pending_journal_count() == 0

    def test_rollback_on_add_full_conflict(self, db_path, processing_dir, temp_dir):
        """File should be moved BACK if add_full fails."""
        src = temp_dir / "source.txt"
        src.write_bytes(b"content")

        with DedupeDatabase(db_path) as db:
            db.connect()
            # Register collision
            db.add_full(b"fake_hash", "/existing")

        with (
            patch("bgate_unix.engine._compute_full_hash", return_value=b"fake_hash"),
            FileDeduplicator(db_path, processing_dir=processing_dir) as deduper,
        ):
            deduper.process_file(src)

            # File should still be at src
            assert src.exists()
            assert src.read_bytes() == b"content"

            # Processing dir should be empty (moved back)
            # With sharding, empty directories might remain, but no files should exist
            files = [p for p in processing_dir.rglob("*") if p.is_file()]
            assert not files


class TestFringeHashSpec:
    """Test fringe hash specification compliance."""

    def test_tail_read_is_bounded(self, temp_dir):
        """Tail read should use explicit size, not read() to end."""
        path = temp_dir / "large.bin"
        # Make a file larger than fringe size
        size = FRINGE_SIZE * 3
        path.write_bytes(b"x" * size)

        # Mock Path.open using patch.object on the class specific to the file
        with patch("pathlib.Path.open") as mock_open:
            mock_file = MagicMock()
            mock_open.return_value.__enter__.return_value = mock_file

            # Setup mock file behavior
            mock_file.seek.return_value = size  # size of file
            mock_file.read.return_value = b"x" * FRINGE_SIZE

            # We need to compute hash on 'path', which is a Path object
            _compute_fringe_hash(path)

            # Check read calls
            # First read: start of file
            assert mock_file.read.call_args_list[0] == call(FRINGE_SIZE)

            # Second read: tail of file
            # MUST be called with explicit size, NOT empty args
            assert mock_file.read.call_args_list[1] == call(FRINGE_SIZE)

    def test_fringe_overlap_spec(self, temp_dir):
        """Verify overlap logic: 70KB file should read last 64KB (overlap 58KB)."""
        path = temp_dir / "overlap.bin"
        # 64KB + 6KB = 71680 bytes
        # Overlap allowed: seek should be safe_pos = size - 64KB = 6KB
        size = FRINGE_SIZE + (6 * 1024)
        path.write_bytes(b"x" * size)

        with patch("pathlib.Path.open") as mock_open:
            mock_file = MagicMock()
            mock_open.return_value.__enter__.return_value = mock_file
            mock_file.seek.return_value = size
            mock_file.read.return_value = b"x" * FRINGE_SIZE

            _compute_fringe_hash(path)

            # Verify seek position for the second chunk
            # 1st seek: 0 (head)
            # 2nd seek: size - FRINGE_SIZE (tail with overlap)
            # calls: seek(0), seek(delta)

            # It might seek 0 twice if it rewinds? check impl
            # Impl: 1. read head (implicit pos 0 or seek 0?)
            # Impl uses 'with open' -> starts at 0.
            # Impl lines:
            # first_chunk = f.read(FRINGE_SIZE) -> pos 64KB
            # if actual_size > ...:
            #     seek_pos = ...
            #     f.seek(seek_pos)

            # So seek is called once for tail.
            assert mock_file.seek.call_count >= 1
            # Check the seek(tail_pos) call
            expected_seek = size - FRINGE_SIZE
            mock_file.seek.assert_called_with(expected_seek)


class TestSignalGuards:
    """Test signal handling during critical sections."""

    def test_deferred_signal(self):
        """Signal received during critical section should be deferred."""
        from bgate_unix.engine import _deferred_signal, critical_section

        # Verify no signal pending initially
        assert _deferred_signal is None

        # We start a critical section
        # We need to ensure we don't actually kill the process when the signal is re-raised
        # So we mock the original handler content

        with patch("signal.signal") as mock_signal_func:
            # We want critical_section to capture the current handlers, which will be our mocks
            mock_original_handler = MagicMock()
            mock_signal_func.return_value = mock_original_handler

            with critical_section():
                # Manually trigger the handler that critical_section installed
                # types: signal_func.call_args[0][1] is likely the handler
                # We need to find the handler installed for SIGINT

                # Iterate calls to find the one for SIGINT
                handler = None
                for args in mock_signal_func.call_args_list:
                    if args[0][0] == signal.SIGINT:
                        handler = args[0][1]
                        break

                assert handler is not None, "SIGINT handler not installed"

                # Simulate receiving SIGINT
                handler(signal.SIGINT, None)

                # Assert it was caught and stored in _deferred_signal
                # We need to check the global variable in the module
                import bgate_unix.engine

                assert bgate_unix.engine._deferred_signal == (signal.SIGINT, None)

            # After exit, it should have tried to call the old handler
            mock_original_handler.assert_called_with(signal.SIGINT, None)


class TestLayout:
    """Test filesystem layout structure."""

    def test_sharding_structure(self, db_path, processing_dir, temp_dir):
        """Verify files are moved into hash-based 2-char shards."""
        src = temp_dir / "shard_me.txt"
        src.write_bytes(b"content")

        with FileDeduplicator(db_path, processing_dir=processing_dir) as deduper:
            deduper.process_file(src)

            # Inspect processing dir content
            # Should have at least one shard directory (2-char hex)
            items = list(processing_dir.iterdir())
            subdirs = [i for i in items if i.is_dir() and len(i.name) == 2]
            assert len(subdirs) >= 1
            shard_dir = subdirs[0]

            # Shard dir name must be 2 chars (hex)
            assert len(shard_dir.name) == 2

            # File inside shard
            files = list(shard_dir.iterdir())
            assert len(files) == 1


class TestOrphanIdempotency:
    """Test orphan registry idempotency."""

    def test_duplicate_orphan_paths_ignored(self, db_path):
        """Verify duplicate orphan_path entries are silently ignored."""
        with DedupeDatabase(db_path) as db:
            db.connect()

            # First insert should succeed
            id1 = db.add_orphan("/original/path1", "/orphan/file.txt", 100)
            assert id1 > 0

            # Second insert with SAME orphan_path should be ignored (ON CONFLICT DO NOTHING)
            _ = db.add_orphan("/original/path2", "/orphan/file.txt", 200)

            # Verify only ONE orphan entry exists
            orphans = db.get_pending_orphans()
            assert len(orphans) == 1
            assert orphans[0]["orphan_path"] == "/orphan/file.txt"
            assert orphans[0]["original_path"] == "/original/path1"  # First one wins

    def test_different_orphan_paths_allowed(self, db_path):
        """Verify different orphan_path entries are allowed."""
        with DedupeDatabase(db_path) as db:
            db.connect()

            id1 = db.add_orphan("/original/path1", "/orphan/file1.txt", 100)
            id2 = db.add_orphan("/original/path2", "/orphan/file2.txt", 200)

            assert id1 > 0
            assert id2 > 0
            assert id1 != id2

            orphans = db.get_pending_orphans()
            assert len(orphans) == 2

    def test_process_result_api_shape(self, db_path, processing_dir, temp_dir):
        """Verify ProcessResult contains both original and stored paths (GPT Point 1)."""
        src = temp_dir / "unique.txt"
        src.write_bytes(b"content")

        with FileDeduplicator(db_path, processing_dir=processing_dir) as deduper:
            result = deduper.process_file(src)

            assert result.result == DedupeResult.UNIQUE
            assert result.original_path == src
            assert result.stored_path is not None
            assert str(result.stored_path).startswith(str(processing_dir))
            # result.path should now be the stored path (GPT Option B style)
            assert result.path == result.stored_path

    def test_emergency_rewrite_atomic_safe(self, db_path):
        """Verify emergency orphan rewrite doesn't lose data on crash (Opus HIGH)."""
        emergency_file = db_path.parent / "emergency_orphans.jsonl"
        # Create a mix of valid and invalid lines
        # Valid line for engine to "import"
        # Invalid line to force "rewrite" of remaining lines
        content = (
            '{"original_path": "/ext/orig", "orphan_path": "/ext/lost", "file_size": 100}\n'
            '{"invalid": "json"}\n'
        )
        emergency_file.write_text(content)

        # Mock add_orphan to avoid needing real files
        # Simulate crash during rename
        with (
            patch("bgate_unix.engine.DedupeDatabase.add_orphan"),
            patch("pathlib.Path.replace", side_effect=OSError("Simulated crash")),
            patch("bgate_unix.engine.logger"),
        ):
            deduper = FileDeduplicator(db_path)
            # connect() triggers _check_emergency_orphans
            # It catches the OSError internally, so no raises()
            deduper.connect()

        # Original content must remain intact because replace didn't happen
        assert emergency_file.read_text() == content
