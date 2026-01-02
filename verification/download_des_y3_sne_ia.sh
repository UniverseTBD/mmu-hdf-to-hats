#!/bin/bash
# execute from mmu-to-hats directory
wget -r -np -nH --cut-dirs=1 -R "index.html*" -q https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/des_y3_sne_ia/des_y3_sne_ia/healpix=1105/
wget -r -np -nH --cut-dirs=1 -R "index.html*" -q https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/des_y3_sne_ia/des_y3_sne_ia.py
