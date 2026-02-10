# execute from mmu-to-hats directory
# Downloads healpix=1378 (has both train and test files)
wget -r -np -nH --cut-dirs=1 -R "index.html*" -q https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/plasticc/data/healpix=1378/
wget -q -P data/MultimodalUniverse/v1/plasticc/ https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/plasticc/plasticc.py
