#!/bin/bash
# execute from mmu-to-hats directory
wget -r -np -nH --cut-dirs=1 -R "index.html*" -q https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/foundation/foundation_dr1/healpix=1628/
wget -r -np -nH --cut-dirs=1 -R "index.html*" -q https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/foundation/foundation.py
