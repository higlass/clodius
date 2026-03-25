# Clodius

A Python library and CLI tool for aggregating large genomic datasets into tile-based formats for display at multiple resolutions (used by [HiGlass](https://higlass.io)).

## Project Structure

- `clodius/` — main package
  - `cli/` — Click-based CLI commands (`aggregate.py`, `convert.py`)
  - `tiles/` — tile generation modules per file type (bigwig, cooler, bed, etc.)
  - `models/` — Pydantic data models
- `test/` — pytest tests mirroring the source layout
- `test/sample_data/` — small sample files used by tests

## Development Setup

```shell
pip install -e ".[dev]"
```

## Common Commands

Run all tests:
```shell
pytest
```

Run a specific test:
```shell
pytest test/cli_test.py::test_clodius_aggregate_bedgraph
```

Lint:
```shell
flake8 clodius
```

## Key Conventions

- **Linting**: flake8 (configured via `pyproject.toml`)
- **Tests**: pytest with coverage (`pytest --cov=clodius`)
- **Build**: hatchling
- **Main branch**: `develop` (use this as the base for PRs)
- **Python packaging**: `pyproject.toml` (no `setup.py`)
