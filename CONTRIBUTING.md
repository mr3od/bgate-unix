# Contributing to fast-gate

Thanks for your interest in contributing to fast-gate! This guide will help you get started.

## Development Setup

### Prerequisites

- Python 3.11 or higher
- [uv](https://docs.astral.sh/uv/) package manager

### Installation

1. Clone the repository:
```bash
git clone https://github.com/mr3od/fast-gate.git
cd fast-gate
```

2. Install dependencies with uv:
```bash
uv sync --dev
```

This will create a virtual environment and install all dependencies including dev tools (pytest, ruff, ty).

## Development Workflow

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with verbose output
uv run pytest -v

# Run specific test file
uv run pytest tests/test_deduper.py

# Run specific test class or function
uv run pytest tests/test_deduper.py::TestTier1SizeCheck
```

### Code Quality

We use `ruff` for linting and formatting:

```bash
# Format code
uv run ruff format .

# Check formatting without changes
uv run ruff format --check .

# Run linter
uv run ruff check .

# Auto-fix linting issues
uv run ruff check --fix .
```

### Type Checking

We use `ty` for strict type checking:

```bash
# Type check the source code
uv run ty check src/
```

### Running All Checks

Before submitting a PR, ensure all checks pass:

```bash
# Format
uv run ruff format .

# Lint
uv run ruff check .

# Type check
uv run ty check src/

# Test
uv run pytest
```

## Code Standards

- **Type Hints:** All functions must have complete type annotations
- **Docstrings:** Public APIs should have docstrings explaining purpose and parameters
- **Error Handling:** Handle `OSError` and `sqlite3.Error` appropriately
- **Testing:** Add tests for new features and bug fixes
- **Line Length:** Maximum 100 characters (enforced by ruff)

## Project Structure

```
fast-gate/
├── src/fast_gate/       # Main package
│   ├── __init__.py      # Package exports
│   ├── engine.py        # Core deduplication logic
│   ├── db.py            # Database layer
│   └── py.typed         # PEP 561 marker
├── tests/               # Test suite
│   └── test_deduper.py  # Comprehensive tests
├── pyproject.toml       # Project configuration
└── README.md            # Documentation
```

## Pull Request Process

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/your-feature-name`
3. Make your changes
4. Run all checks (format, lint, type check, test)
5. Commit with clear messages: `git commit -m "Add feature: description"`
6. Push to your fork: `git push origin feature/your-feature-name`
7. Open a Pull Request with a clear description

## Reporting Issues

When reporting bugs, please include:

- Python version (`python --version`)
- uv version (`uv --version`)
- Operating system
- Minimal code to reproduce the issue
- Expected vs actual behavior

## Questions?

Feel free to open an issue for questions or discussions about the project.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
