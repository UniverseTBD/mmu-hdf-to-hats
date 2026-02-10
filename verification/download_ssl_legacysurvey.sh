#!/bin/bash
# execute from mmu-to-hats directory
# Downloads north/healpix=125 (~38MB, 1 file)
wget -r -np -nH --cut-dirs=1 -R "index.html*" -q https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/ssl_legacysurvey/north/healpix=125/
wget -q -P data/MultimodalUniverse/v1/ssl_legacysurvey/ https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/ssl_legacysurvey/ssl_legacysurvey.py
