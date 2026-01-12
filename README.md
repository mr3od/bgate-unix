# bgate-unix

High-performance Unix file deduplication engine with tiered short-circuit logic.

[![PyPI](https://img.shields.io/pypi/v/bgate-unix.svg)](https://pypi.org/project/bgate-unix/)
[![CI](https://github.com/mr3od/bgate-unix/actions/workflows/ci.yml/badge.svg)](https://github.com/mr3od/bgate-unix/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Overview

bgate-unix is a fingerprinting gatekeeper that performs strict binary identity deduplication using tiered short-circuit logic. Designed for high-volume Unix pipelines where disk I/O is the bottleneck.

**Key Features:**
- Sub-millisecond duplicate rejection via O(1) index lookups
- Journaled file moves with crash recovery
- BLOB-based xxHash128 storage for collision-proof identity
- Atomic `link/unlink` moves (no TOCTOU races)

## The 4-Tier Engine

```
Incoming File
     │
     ▼
┌─────────────────────────────────────────┐
│  TIER 0: Empty Check                    │
│  file_size == 0 → SKIP                  │
│  Cost: stat() only                      │
└─────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────┐
│  TIER 1: Size Uniqueness                │
│  Size not in DB → UNIQUE                │
│  Cost: SQLite lookup                    │
└─────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────┐
│  TIER 2: Fringe Hash (xxh64)            │
│  First 64KB + Last 64KB + size          │
│  (Last 64KB overlaps if file < 128KB)   │
│  Hash not in DB → UNIQUE                │
│  Cost: 128KB read max                   │
└─────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────┐
│  TIER 3: Full Hash (xxh128)             │
│  Entire file in 256KB chunks            │
│  Hash in DB → DUPLICATE                 │
│  Hash not in DB → UNIQUE                │
│  Cost: Full file read                   │
└─────────────────────────────────────────┘
```

## Installation

### As a CLI Tool (Recommended)
Install globally in an isolated environment using [uv](https://docs.astral.sh/uv/):

```bash
uv tool install bgate-unix
```

Verify it works:
```bash
bgate --help
```

### As a Library
```bash
uv add bgate-unix
# or
pip install bgate-unix
```

**Requirements:** Unix-based OS (Linux, macOS, BSD). Windows is not supported.

## CLI Usage

`bgate-unix` provides a high-performance CLI for pipeline integration.

```bash
# Scan and move unique files to vault (Active Mode)
bgate scan ./incoming --into ./vault --recursive --move

# Read-only scan (default behavior)
bgate scan ./incoming --recursive

# Show index statistics
bgate stats --db dedupe.db

# Recover from an interrupted session
bgate recover --db dedupe.db
```

## Quick Start

### As a CLI tool

```bash
# Install
uv tool install bgate-unix

# Scan and move unique files to tiered storage (Active Mode)
bgate scan ./incoming --into ./vault --recursive --move
```

### As a Library

```python
from pathlib import Path
from bgate_unix import FileDeduplicator
from bgate_unix.engine import DedupeResult

with FileDeduplicator("dedupe.db") as deduper:
    result = deduper.process_file("incoming/document.pdf")
    
    match result.result:
        case DedupeResult.UNIQUE:
            print(f"New file (tier {result.tier})")
        case DedupeResult.DUPLICATE:
            print(f"Duplicate of {result.duplicate_of}")
        case DedupeResult.SKIPPED:
            print(f"Skipped: {result.error or 'empty'}")
```

## Usage

### File Movement Pipeline

Unique files are atomically moved to a processing directory:

```python
from pathlib import Path
from bgate_unix import FileDeduplicator

with FileDeduplicator("index.db", processing_dir=Path("processed/")) as deduper:
    for result in deduper.process_directory("inbound/", recursive=True):
        if result.result == DedupeResult.UNIQUE:
            # result.path is the new location in processed/
            # result.original_path is the source location
            # result.stored_path is also the new location (explicit field)
            print(f"Moved: {result.original_path.name} -> {result.stored_path.name}")
```

**Important:** `processing_dir` must be on the same filesystem as source files (required for atomic `os.link`).

### Batch Processing

```python
from bgate_unix import FileDeduplicator
from bgate_unix.engine import DedupeResult

with FileDeduplicator("index.db") as deduper:
    results = list(deduper.process_directory("incoming/", recursive=True))
    
    unique = sum(1 for r in results if r.result == DedupeResult.UNIQUE)
    dupes = sum(1 for r in results if r.result == DedupeResult.DUPLICATE)
    
    print(f"Unique: {unique}, Duplicates: {dupes}")
    print(f"Stats: {deduper.stats}")
```

### Database & Recovery

- **Strict Schema Enforcement**: Engines will hard-stop if a database version mismatch is detected.
- **Orphan Recovery**: If a crash occurs during file moves, orphaned files are automatically recovered on next connect.
- **Emergency Logging**: If the database becomes unavailable during a critical I/O operation, orphan records are written to an atomic `.jsonl` log file for manual recovery.

## Technical Details

### Threat Model & Hashing

bgate-unix is designed for **trusted internal pipelines**.

- **xxHash128**: Used as an extremely low-collision identifier for high-volume data (2^128 range). For trusted inputs, collisions are treated as mathematically impossible.
- **Deduplication Priority**: Speed and durability are prioritized over security.
- **Not for Adversarial Input**: If you are processing untrusted/malicious files where hash collisions could be intentionally engineered, use a cryptographically secure mode (like BLAKE3 or SHA-256) which may be added in future versions.

### Sharded Storage Layout

Unique files are stored in a 2-level hex-sharded structure inside `processing_dir`:
- Path: `{processing_dir}/{id[0:2]}/{id[2:16]}{original_suffix}`
- Note: `id` is the full content hash when available (Tier 3), otherwise a unique UUID (Tier 1/2) to preserve "Move-then-Hash" performance.
- Example: `processed/a3/bc4f91e2d0f8.pdf`

### Database Schema

SQLite with BLOB-based hash storage:

```sql
-- Tier 1: Size lookup (existence set)
CREATE TABLE size_index (
    file_size INTEGER PRIMARY KEY
) WITHOUT ROWID;

-- Tier 2: Fringe hash (BLOB)
CREATE TABLE fringe_index (
    fringe_hash BLOB NOT NULL,
    file_size INTEGER NOT NULL,
    file_path TEXT NOT NULL,
    PRIMARY KEY (fringe_hash, file_size)
) WITHOUT ROWID;

-- Tier 3: Full hash (BLOB)
CREATE TABLE full_index (
    full_hash BLOB PRIMARY KEY,
    file_path TEXT NOT NULL
) WITHOUT ROWID;

-- Crash recovery tables
CREATE TABLE orphan_registry (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    original_path TEXT NOT NULL,
    orphan_path TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    recovered_at TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    UNIQUE(orphan_path)
);

CREATE TABLE move_journal (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_path TEXT NOT NULL,
    dest_path TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    phase TEXT NOT NULL DEFAULT 'planned',
    completed_at TEXT
);

CREATE TABLE schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);
```

Pragmas: `WAL` mode, `synchronous=FULL`, 64MB cache, 256MB mmap.

### Atomic File Moves

Uses hard-link + unlink (`os.link` / `Path.unlink`) for atomic same-filesystem moves.

#### Durability Guarantees
- **Signal Deferral**: SIGINT/SIGTERM signals are deferred during critical move operations using `critical_section()`.
- **Fsync Ordering**: File and directory durability is strictly enforced:
  1. After linking destination, newly created parent directories are fsynced (top-down).
  2. The destination directory is fsynced to persist the new link.
  3. The source file is unlinked.
  4. The source directory is fsynced to persist the removal.
- **FS Enforcement**: Cross-device moves are explicitly rejected (`EXDEV` error) to maintain atomicity.

### Crash Recovery

Move operations use phase-based journaling: `planned → moving → completed`.

On startup, the engine automatically recovers incomplete entries:
- **`planned`**: Move never started → Marked as `failed`.
- **`moving`**: File may have been moved but not yet indexed → Engine attempts atomic rollback (link back to source + fsync + unlink destination).

## Benchmarks

Performance benchmarks on production datasets demonstrate bgate-unix's efficiency for enterprise workloads.

### Test Environment

**Hardware:** AWS EC2 ARM64 instance  
**Storage:** Amazon Elastic Block Store (NVMe SSD)  
**OS:** Debian GNU/Linux (ARM64)  
**Dataset:** 24.68 GB production data pipeline files  

### Results

| Metric | Value |
|--------|-------|
| **Dataset Size** | 24.68 GB, 9,174 files |
| **Processing Time** | 274.96 seconds (~4.6 minutes) |
| **Bandwidth** | 89.1 MB/sec |
| **File Throughput** | 28.8 files/sec (moved) |
| **Files Moved** | 7,932 unique files (23.92 GB) |
| **Deduplication** | 13.5% duplicates found (1,242 files) |
| **Idempotency** | ✅ 0 files moved on subsequent runs |

### Performance Analysis

- **Excellent bandwidth** on large datasets (89.1 MB/sec)
- **Consistent throughput** across different file sizes
- **Production-ready** performance for enterprise workloads
- **Perfect idempotency** - no unnecessary operations on re-runs
- **Effective deduplication** with 13.5% duplicate detection
- **I/O optimized** - performance bottleneck is disk throughput, not CPU cycles (as designed)

### Running Benchmarks

Use the included benchmark script to test performance on your data:

```bash
# Run benchmark with idempotency test
./scripts/benchmark.sh /path/to/source /path/to/vault

# Example output:
# 🚀 bgate-unix Move Operation Benchmark
# FIRST RUN: 89.1 MB/sec, 7,932 files moved
# IDEMPOTENCY TEST: ✅ 0 files moved (perfect idempotency)
```

**Note:** Source and vault must be on the same filesystem for atomic operations.

## Development

```bash
git clone https://github.com/mr3od/bgate-unix.git
cd bgate-unix
uv sync --dev

# Run tests
uv run pytest

# Lint
uv run ruff check .
uv run ruff format .

# Type check
uv run ty check src/
```

## License

MIT
