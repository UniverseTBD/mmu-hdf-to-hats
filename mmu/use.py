from datasets import load_dataset_builder
from mmu.utils import crossmatch_streaming

# Load builders (not full datasets)
hsc_builder = load_dataset_builder("data/hsc/", name="pdr3_dud_22.5")
desi_builder = load_dataset_builder("data/desi/", name="edr_sv3")

# Create streaming cross-matched dataset
matched = crossmatch_streaming(
    hsc_builder,
    desi_builder,
    matching_radius=1.0
)

# Use like any iterable dataset
for example in matched:
    # Process matched data
    hsc_image = example['image']
    desi_spectrum = example['spectrum']
    separation = example['separation_arcsec']
