"""
Performance and correctness validation tests for v0.3.0 optimizations.
Focuses on fsync counts, sharding behavior, and batch I/O.
"""

import contextlib
import tempfile
from pathlib import Path
from unittest.mock import call, patch

import pytest

from bgate_unix.engine import FileDeduplicator


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as td:
        yield Path(td)


class TestPerfOptimizations:
    def test_shard_fsync_optimization(self, temp_dir):
        """Verify fsync processing_dir happens ONLY when shard is created."""
        db_path = temp_dir / "db.sqlite"
        processing_dir = temp_dir / "processing"
        processing_dir.mkdir()

        # Test 1: New shard creation -> Should fsync parent
        src = temp_dir / "file1.txt"
        src.write_bytes(b"content1")

        with (
            patch("bgate_unix.engine._fsync_dir") as mock_fsync,
            patch("bgate_unix.engine.atomic_move"),
        ):
            deduper = FileDeduplicator(db_path, processing_dir=processing_dir)
            deduper.connect()
            deduper.process_file(src)

            # Should have synced processing_dir (shard created)
            assert call(processing_dir) in mock_fsync.call_args_list

        # Test 2: Existing shard creation -> Should NOT fsync parent
        src2 = temp_dir / "file2.txt"
        src2.write_bytes(b"content2")

        # Refactor: Avoid global Path.mkdir patch.
        # Deterministically force a specific shard ID by mocking uuid or hash computation.
        # If hash is not pre-computed, code uses UUID.
        # Let's mock uuid.uuid4 to return a known hex.

        known_uuid_hex = "aa" + "0" * 30
        shard_name = "aa"
        (processing_dir / shard_name).mkdir(exist_ok=True)

        with (
            patch("bgate_unix.engine._fsync_dir") as mock_fsync,
            patch("bgate_unix.engine.atomic_move"),
            patch("uuid.uuid4") as mock_uuid,
        ):
            mock_uuid.return_value.hex = known_uuid_hex
            deduper.process_file(src2)

            # Should NOT have synced processing_dir because mkdir logic handles existing dir
            # Note: since we didn't mock mkdir globally, real mkdir runs and raises FileExistsError naturally
            assert call(processing_dir) not in mock_fsync.call_args_list

    def test_emergency_orphan_batch_fsync(self, temp_dir):
        """Verify emergency orphan rewrite uses single batch fsync."""
        processing_dir = temp_dir / "processing"
        processing_dir.mkdir()

        # Create a dummy emergency file with invalid lines to force rewrite
        emergency_file = processing_dir / "emergency_orphans.jsonl"
        emergency_file.write_text("invalid_json\nanother_invalid\n")

        # We need to ensure the mocks correctly handle the file path being opened/synced
        with (
            patch("bgate_unix.engine._fsync_dir") as mock_fsync_dir,
            patch("os.fsync") as mock_os_fsync,
        ):
            # Since _check_emergency_orphans looks for file in db_path.parent
            # We must ensure db_path is in processing_dir or emergency file is in db_path.parent
            # In engine.py: emergency_file = self._db.db_path.parent / filename
            # So let's make sure db_path is adjacent to where we put the emergency file
            # or move the emergency file to db_path.parent

            # Re-setup paths to match engine logic
            real_db_path = processing_dir / "db.sqlite"

            deduper = FileDeduplicator(real_db_path, processing_dir=processing_dir)
            deduper.connect()

            # Assertions:
            # 1. atomic_move/fsync tests might run during connect/recover, ignore those.
            # 2. Key check: os.fsync called exactly ONCE regarding the file descriptor

            # Use strict verification on the rewrite block
            # Logic: imports 0, remaining 2 lines -> rewrite

            # os.fsync(fd) should be called once (for the flush)
            assert mock_os_fsync.call_count == 1

            # processing_dir should be synced once (after rewrite)
            # processing_dir should be synced once (after rewrite)
            assert call(processing_dir) in mock_fsync_dir.call_args_list

    def test_emergency_orphan_rewrite_failure_preservation(self, temp_dir):
        """Verify original emergency orphan file is preserved if rewrite crashes."""
        processing_dir = temp_dir / "processing"
        processing_dir.mkdir()

        # Valid JSON line to trick it into "importing" something?
        # Actually _check_emergency_orphans iterates.
        # If we have invalid lines, they are added to remaining_lines.
        # We need remaining_lines to be non-empty to trigger rewrite.

        emergency_file = processing_dir / "emergency_orphans.jsonl"
        original_content = "invalid_1\ninvalid_2\n"
        emergency_file.write_text(original_content)

        db_path = processing_dir / "db.sqlite"

        # Simulate crash during temp file write or replace
        # We can patch pathlib.Path.open to raise exception when opening .tmp file
        # Or patch temp_file.replace

        with (
            patch("pathlib.Path.replace", side_effect=OSError("Simulated Crash")),
            patch("bgate_unix.engine._fsync_dir"),  # Suppress real fsync calls
            patch("os.fsync"),
        ):
            deduper = FileDeduplicator(db_path, processing_dir=processing_dir)

            # This will trigger rewrite but fail at replace step
            with contextlib.suppress(OSError):
                deduper.connect()

            # Original file should still maintain content because replace failed
            assert emergency_file.exists()
            assert emergency_file.read_text() == original_content


class TestShardFailureLogging:
    def test_shard_pre_create_failure_logs(self, temp_dir, caplog):
        """Verify debug log emitted when shard pre-create fails."""
        import logging

        caplog.set_level(logging.DEBUG)

        db_path = temp_dir / "db.sqlite"
        processing_dir = temp_dir / "processing"
        processing_dir.mkdir()
        src = temp_dir / "test.txt"
        src.write_bytes(b"data")

        # Mock mkdir to raise PermissionError (OSError)
        with (
            patch("pathlib.Path.mkdir", side_effect=PermissionError("Denied")),
            patch("bgate_unix.engine.atomic_move"),
            patch("bgate_unix.engine.logger") as mock_logger,
        ):
            deduper = FileDeduplicator(db_path, processing_dir=processing_dir)
            deduper.connect()
            # process_file calls _process_file, which calls _register_unique
            # src file needs to exist for process_file to proceed beyond validation
            with contextlib.suppress(Exception):
                deduper.process_file(src)

            # Check logs via mock
            # Now we expect ERROR level log
            assert mock_logger.error.called
            args = mock_logger.error.call_args[0]
            assert "Shard pre-create failed" in args[0]
            assert "Denied" in str(args[2])
