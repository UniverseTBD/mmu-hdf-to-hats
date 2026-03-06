# execute from mmu-to-hats directory
wget -r -np -nH --cut-dirs=1 -R "index.html*" -q https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/btsbot/data/healpix=0313/
# For some reason this file is access restricted, other <catalog>.py files should work though
wget -r -np -nH --cut-dirs=1 -R "index.html*" -q https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/btsbot/btsbot.py
