# bgate-unix

High-performance Unix file deduplication engine with tiered short-circuit logic.

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

```bash
# Using uv (recommended)
uv add bgate-unix

# Using pip
pip install bgate-unix
```

**Requirements:** Unix-based OS (Linux, macOS, BSD). Windows is not supported.

## Quick Start

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
        if result.result.value == "unique":
            # File has been moved to processed/
            print(f"Moved: {result.path.name}")
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

### Orphan Recovery

If a crash occurs during file moves, orphaned files are automatically recovered on next connect:

```python
with FileDeduplicator("index.db") as deduper:
    # Automatic recovery happens in connect()
    orphans = deduper.list_orphans()
    print(f"Pending orphans: {len(orphans)}")
```

## Technical Details

### Absolute Trust Model

bgate-unix uses xxHash128 for full content hashing. With 2^128 possible values, collisions are treated as mathematically impossible. If a hash exists in the database, the file is definitively a duplicate—no byte-by-byte verification needed.

### Database Schema

SQLite with BLOB-based hash storage:

```sql
-- Tier 1: Size lookup
CREATE TABLE size_index (
    file_size INTEGER PRIMARY KEY,
    count INTEGER NOT NULL
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
```

Pragmas: `WAL` mode, `synchronous=FULL`, 64MB cache, 256MB mmap.

### Atomic File Moves

Uses Unix `os.link()` + `os.unlink()` pattern:
- `os.link()` fails if destination exists (no overwrites)
- `os.link()` fails across filesystems (`EXDEV` error)
- Three-phase journaling: Journal → Move → Register

### Crash Recovery

Move operations are journaled before execution:
1. **Journal intent** (committed transaction)
2. **Perform move** (outside transaction)
3. **Register in index** (committed transaction)

On startup, incomplete journal entries are recovered automatically.

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
