#!/bin/bash
# execute from mmu-to-hats directory
# Downloads spectra/healpix=1339 (~19MB, 1 file)
wget -r -np -nH --cut-dirs=1 -R "index.html*" -q https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/chandra/spectra/healpix=1339/
# already checked in under catalog_download_scripts/
cp catalog_download_scripts/chandra.py data/MultimodalUniverse/v1/chandra/chandra.py
