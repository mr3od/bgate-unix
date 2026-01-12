"""bgate-unix: High-performance Unix file deduplication engine."""

from bgate_unix.engine import DedupeResult, FileDeduplicator, ProcessResult

__version__ = "0.5.1"
__all__ = ["DedupeResult", "FileDeduplicator", "ProcessResult"]
