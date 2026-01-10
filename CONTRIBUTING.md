# Contributing to bgate-unix

Thanks for your interest in contributing to bgate-unix! This guide will help you get started.

## Development Setup

### Prerequisites

- Python 3.11 or higher
- [uv](https://docs.astral.sh/uv/) package manager
- Unix-based OS (Linux, macOS, BSD)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/mr3od/bgate-unix.git
cd bgate-unix
```

2. Install dependencies with uv:
```bash
uv sync --dev
```

## Development Workflow

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with verbose output
uv run pytest -v

# Run specific test
uv run pytest tests/test_deduper.py::TestTier1SizeCheck
```

### Code Quality

```bash
# Format code
uv run ruff format .

# Check formatting
uv run ruff format --check .

# Run linter
uv run ruff check .

# Auto-fix linting issues
uv run ruff check --fix .
```

### Type Checking

```bash
uv run ty check src/
```

### Running All Checks

```bash
uv run ruff format .
uv run ruff check .
uv run ty check src/
uv run pytest
```

## Code Standards

- **Type Hints:** All functions must have complete type annotations
- **Docstrings:** Public APIs should have docstrings
- **Error Handling:** Handle `OSError` appropriately
- **Testing:** Add tests for new features
- **Line Length:** Maximum 100 characters

## Project Structure

```
bgate-unix/
├── src/bgate_unix/      # Main package
│   ├── __init__.py
│   ├── engine.py        # Core deduplication logic
│   ├── db.py            # Database layer
│   └── cli.py           # Typer CLI implementation
├── tests/
│   ├── test_deduper.py         # Core functional tests
│   ├── test_durability.py      # Crash recovery & atomicity tests
│   └── test_perf_correctness.py # Performance & correctness logic
├── pyproject.toml
└── README.md
```

## Pull Request Process

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Make your changes
4. Run all checks
5. Commit with clear messages
6. Open a Pull Request

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
