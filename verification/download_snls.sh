#!/bin/bash
# execute from toy-mmu directory
wget -r -np -nH --cut-dirs=1 -R "index.html*" -q https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/snls/data/healpix=0714/
mkdir -p data/MultimodalUniverse/v1/snls
cp catalog_download_scripts/snls.py data/MultimodalUniverse/v1/snls/snls.py
