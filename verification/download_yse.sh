# execute from mmu-to-hats directory
wget -r -np -nH --cut-dirs=1 -R "index.html*" -q https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/yse/yse_dr1/healpix=0584/
# For some reason this file is access restricted, other <catalog>.py files should work though
wget -r -np -nH --cut-dirs=1 -R "index.html*" -q https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/yse/yse.py
