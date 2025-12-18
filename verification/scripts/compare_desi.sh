#!/usr/bin/env bash
set -e  # Exit on any error
set -u  # Exit on undefined variables

# Verify we're in the mmu-to-hats directory
if [[ ! -f "main.py" ]] || [[ ! -d "catalog_functions" ]]; then
    echo "Error: This script must be run from the mmu-to-hats directory"
    exit 1
fi

# Set PYTHONPATH to include both current dir (for catalog_functions) and parent (for mmu)
export PYTHONPATH=".:..${PYTHONPATH:+:$PYTHONPATH}"

./verification/download_desi.sh
uv run --with-requirements=verification/requirements.in python verification/process_desi_using_datasets.py
# source .venv/bin/activate && python verification/process_desi_using_datasets.py
source .venv/bin/activate && python transform_scripts/transform_desi_to_parquet.py
source .venv/bin/activate && python verification/compare.py data/desi_hp626_transformed.parquet data/MultimodalUniverse/v1/desi_with_coordinates
