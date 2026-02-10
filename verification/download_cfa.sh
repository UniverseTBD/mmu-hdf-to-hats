#!/bin/bash
# execute from mmu-to-hats directory
# Downloads cfa3/healpix=0146 (~19KB, 3 files)
wget -r -np -nH --cut-dirs=1 -R "index.html*" -q https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/cfa/cfa3/healpix=0146/
wget -q -P data/MultimodalUniverse/v1/cfa/ https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/cfa/cfa.py
