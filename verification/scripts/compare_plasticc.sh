#!/usr/bin/env bash
set -e  # Exit on any error
set -u  # Exit on undefined variables

# Verify we're in the mmu-to-hats directory
if [[ ! -f "main.py" ]] || [[ ! -d "catalog_functions" ]]; then
    echo "Error: This script must be run from the mmu-to-hats directory"
    exit 1
fi

echo "=== Step 1: Download PLAsTiCC sample data ==="
./verification/download_plasticc.sh

echo ""
echo "=== Step 2: Process using datasets library (v3.6) ==="
uv run --with-requirements=verification/requirements.in python verification/process_plasticc_using_datasets.py

echo ""
echo "=== Step 3: Transform using PLAsTiCCTransformer ==="
source .venv/bin/activate && python -m transform_scripts.transform_plasticc_to_parquet

echo ""
echo "=== Step 4: Compare outputs ==="
source .venv/bin/activate && python verification/compare.py \
    data/MultimodalUniverse/v1/plasticc_datasets_output \
    data/plasticc_hp1378_transformed.parquet

echo ""
echo "=== Verification complete! ==="
