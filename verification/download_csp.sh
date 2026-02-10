#!/bin/bash
# execute from mmu-to-hats directory
# Downloads csp/healpix=1113 (~32KB, 3 files)
wget -r -np -nH --cut-dirs=1 -R "index.html*" -q https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/csp/csp/healpix=1113/
wget -q -P data/MultimodalUniverse/v1/csp/ https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/csp/csp.py
