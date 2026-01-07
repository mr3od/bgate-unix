# byte-gate

High-performance Layer 0 binary deduplication with tiered short-circuit logic.

[![CI](https://github.com/mr3od/byte-gate/actions/workflows/ci.yml/badge.svg)](https://github.com/mr3od/byte-gate/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Overview

byte-gate is a "gatekeeper" that performs strict binary identity deduplication (exact byte-match) using tiered short-circuit logic to minimize disk I/O costs. Designed for local network file processing pipelines where disk I/O is the bottleneck.

### Why Layer 0?

Processing costs are expensive. Whether you're running OCR, AI indexing, or complex transcoding, the most expensive file is the one you've already processed. byte-gate ensures that:

- **Most unique files** are verified via metadata (Tier 1) in microseconds
- **Files with size collisions** are verified via 16KB "Fringe" checks (Tier 2)
- **Only potential duplicates** require a full disk read (Tier 3)

This reduces downstream processing costs and storage overhead significantly without external dependencies or cloud access.

## The 4-Tier Short-Circuit Engine

```
┌─────────────────────────────────────────────────────────────────┐
│                    Incoming File                                │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  TIER 0: Empty Check                                            │
│  ─────────────────                                              │
│  • Check: file_size == 0                                        │
│  • Action: Skip immediately                                     │
│  • I/O Cost: stat() only                                        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  TIER 1: Size Uniqueness                                        │
│  ───────────────────────                                        │
│  • Check: Is this file size in the database?                    │
│  • If NO → File is UNIQUE (short-circuit!)                      │
│  • I/O Cost: SQLite lookup only                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  TIER 2: Fringe Hash                                            │
│  ───────────────────                                            │
│  • Read: First 8KB + Last 8KB + file_size                       │
│  • Hash: xxHash64 of combined data                              │
│  • Check: (fringe_hash, file_size) in database?                 │
│  • If NO → File is UNIQUE (short-circuit!)                      │
│  • I/O Cost: 16KB read (or 16KB sequential for HDD mode)        │
│                                                                 │
│  HDD Optimization: Set optimize_for_hdd=True to read            │
│  sequential 16KB instead of seeking to file end                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  TIER 3: Full Content Hash                                      │
│  ─────────────────────────                                      │
│  • Read: Entire file in 256KB chunks                            │
│  • Hash: xxHash64 of full content                               │
│  • Check: full_hash in database?                                │
│  • If NO → File is UNIQUE                                       │
│  • If YES → File is DUPLICATE                                   │
│  • I/O Cost: Full file read                                     │
└─────────────────────────────────────────────────────────────────┘
```

## Installation

```bash
# Using uv (recommended)
uv add byte-gate

# Using pip
pip install byte-gate
```

## Quick Start

```python
from pathlib import Path
from fast_gate import FileDeduplicator

# Basic usage
with FileDeduplicator("dedupe.db") as deduper:
    result = deduper.process_file("incoming/document.pdf")

    if result.result.value == "unique":
        print(f"New file! Determined at Tier {result.tier}")
    elif result.result.value == "duplicate":
        print(f"Duplicate of: {result.duplicate_of}")
```

## Usage Examples

### Basic Deduplication

```python
from byte_gate import FileDeduplicator
from byte_gate.engine import DedupeResult

with FileDeduplicator("index.db") as deduper:
    result = deduper.process_file("/path/to/file.bin")

    match result.result:
        case DedupeResult.UNIQUE:
            print(f"Unique file (tier {result.tier})")
        case DedupeResult.DUPLICATE:
            print(f"Duplicate of {result.duplicate_of}")
        case DedupeResult.SKIPPED:
            print(f"Skipped: {result.error or 'empty file'}")
```

### With File Movement Pipeline

```python
from pathlib import Path
from byte_gate import FileDeduplicator

# Unique files are atomically moved to processing/
with FileDeduplicator(
    db_path="index.db",
    processing_dir=Path("processing/"),
    optimize_for_hdd=False  # Set True for spinning disks
) as deduper:
    for result in deduper.process_directory("inbound/"):
        print(f"{result.path.name}: {result.result.value}")
```

### HDD Optimization

For spinning disks, enable HDD mode to avoid seek latency:

```python
# HDD mode reads sequential 16KB instead of first 8KB + last 8KB
deduper = FileDeduplicator("index.db", optimize_for_hdd=True)
```

### Batch Processing

```python
from byte_gate import FileDeduplicator
from byte_gate.engine import DedupeResult

with FileDeduplicator("index.db") as deduper:
    results = list(deduper.process_directory("incoming/", recursive=True))

    unique = sum(1 for r in results if r.result == DedupeResult.UNIQUE)
    dupes = sum(1 for r in results if r.result == DedupeResult.DUPLICATE)

    print(f"Processed: {len(results)} files")
    print(f"Unique: {unique}, Duplicates: {dupes}")
    print(f"Stats: {deduper.stats}")
```

## Technical Details

### Database Schema

SQLite with optimized pragmas:

- `journal_mode = WAL` - Write-ahead logging for concurrency
- `synchronous = NORMAL` - Balance durability/performance
- `cache_size = -64000` - 64MB page cache

Tables use `WITHOUT ROWID` for clustered index performance:

```sql
-- Tier 1: Size lookup
CREATE TABLE size_index (
    file_size INTEGER PRIMARY KEY,
    count INTEGER NOT NULL
) WITHOUT ROWID;

-- Tier 2: Fringe hash lookup
CREATE TABLE fringe_index (
    fringe_hash INTEGER NOT NULL,
    file_size INTEGER NOT NULL,
    file_path TEXT NOT NULL,
    PRIMARY KEY (fringe_hash, file_size)
) WITHOUT ROWID;

-- Tier 3: Full hash lookup
CREATE TABLE full_index (
    full_hash INTEGER PRIMARY KEY,
    file_path TEXT NOT NULL
) WITHOUT ROWID;
```

### Hash Storage

Hashes are stored as **signed 64-bit integers** for SQLite compatibility:

- xxHash64 returns unsigned 64-bit integers
- Values > 2^63-1 are converted to negative signed integers
- Conversion is transparent and reversible

### Atomic Operations

File moves use `os.replace()` for POSIX atomicity, ensuring no partial writes or race conditions in the processing pipeline.

## Development

```bash
# Clone and setup
git clone https://github.com/mr3od/byte-gate.git
cd byte-gate
uv sync --dev

# Run tests
uv run pytest

# Lint and format
uv run ruff check .
uv run ruff format .

# Type check
uv run ty check src/
```
