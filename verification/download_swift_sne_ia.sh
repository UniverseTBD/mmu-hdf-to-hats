# execute from mmu-to-hats directory
wget -r -np -nH --cut-dirs=1 -R "index.html*" -q https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/swift_sne_ia/data/healpix=2158/
# For some reason this file is access restricted, other <catalog>.py files should work though
wget -r -np -nH --cut-dirs=1 -R "index.html*" -q https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/swift_sne_ia/swift_sne_ia.py
