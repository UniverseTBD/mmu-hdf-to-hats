# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Converts Multimodal Universe (MMU) astronomical datasets from HDF5 format into HATS (Hierarchical Adaptive Triangular Search) format — a spatially-indexed Parquet format for efficient cross-matching on Hugging Face.

## Common Commands

```bash
# Install dependencies (uses uv package manager, requires Python 3.13+)
uv venv .venv && uv pip install '.[dev]'

# Run the full conversion pipeline
uv run python ./main.py \
  --transformer=sdss \
  --input=<HDF5_URI> \
  --output=./hats \
  --name=mmu_sdss \
  --tmp-dir=./tmp \
  --max-rows=8192

# Debug mode (single worker, single thread — enables breakpoints)
uv run python ./main.py --debug ...

# Verify a single catalog transformer against reference implementation
source .venv/bin/activate && python verify.py <catalog_name>

# Run unit tests
source .venv/bin/activate && pytest tests/
```

## Architecture

### Transformer Pattern

The core abstraction is `BaseTransformer` in `catalog_functions/utils.py`. Each catalog has a corresponding `catalog_functions/<name>_transformer.py` that implements:
- `create_schema()` → PyArrow schema definition
- `dataset_to_table(data)` → HDF5 → PyArrow Table conversion

New catalogs are added by creating a new `*_transformer.py` file. Transformers are auto-discovered by `main.py` via `pkgutil.walk_packages`.

### Data Flow

```
HDF5 files → MMUReader (chunks) → Transformer.dataset_to_table() → PyArrow Tables
  → HATS-import pipeline (HEALPix spatial indexing) → Parquet output
```

### Key Utility

`np_to_pyarrow_array()` in `catalog_functions/utils.py` handles numpy→PyArrow conversion for 1D/2D arrays with proper offset calculation. Used extensively in transformers.

### Verification Pipeline

`verify.py` is a Click CLI that for each catalog: downloads test data, processes it via both the HF `datasets` library (reference) and the transformer, then compares outputs using `verification/compare.py`. CI runs this for 14 catalogs in a matrix.

## Conventions

- Transformer classes use feature-group class attributes (e.g., `FLOAT_FEATURES`, `DOUBLE_FEATURES`) to declaratively define column mappings.
- Complex nested data (spectra, lightcurves, images) uses `pa.struct()` types.
- RA/Dec coordinate columns are required for HATS spatial indexing; some catalogs need `--ra`/`--dec` flags when column names differ.
