#!/usr/bin/env bash
set -e
set -u

# Verify we're in the correct directory
if [[ ! -f "main.py" ]] || [[ ! -d "catalog_functions" ]]; then
    echo "Error: Run from toy-mmu directory"
    exit 1
fi

export PYTHONPATH=".:..${PYTHONPATH:+:$PYTHONPATH}"

echo "=== Downloading Gaia test data ==="
./verification/download_gaia.sh

echo "=== Processing Gaia with datasets ==="
uv run --with-requirements=verification/requirements.in python verification/process_gaia_using_datasets.py

echo "=== Transforming Gaia to parquet ==="
source .venv/bin/activate && python transform_scripts/transform_gaia_to_parquet.py

echo "=== Comparing transformed vs datasets output ==="
source .venv/bin/activate && python verification/compare.py \
    data/gaia_hp1631_transformed.parquet \
    data/MultimodalUniverse/v1/gaia_with_coordinates \
    --ignore-missing-columns=healpix \
    --forbidden-columns=healpix

echo "=== Gaia verification PASSED ==="
