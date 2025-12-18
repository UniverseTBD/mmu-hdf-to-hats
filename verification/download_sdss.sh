# execute from mmu-to-hats directory
cp catalog_download_scripts/sdss.py data/MultimodalUniverse/v1/sdss/sdss.py
wget -r -np -nH --cut-dirs=1 -R "index.html*" -q https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/sdss/sdss/healpix=583/
# For some reason this file is access restricted, other <catalog>.py files should work though
# wget -r -np -nH --cut-dirs=1 -R "index.html*" -q https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/sdss/sdss.py
