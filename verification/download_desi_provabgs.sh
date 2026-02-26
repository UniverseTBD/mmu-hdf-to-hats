# execute from mmu-to-hats directory
wget -r -np -nH --cut-dirs=1 -R "index.html*" -q https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/desi_provabgs/datafiles/healpix=669/
# For some reason this file is access restricted, other <catalog>.py files should work though
wget -r -np -nH --cut-dirs=1 -R "index.html*" -q https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/desi_provabgs/desi_provabgs.py
