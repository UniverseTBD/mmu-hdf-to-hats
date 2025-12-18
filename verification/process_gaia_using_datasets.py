# NOTE: run with datasets==3.6
# uv run --with-requirements=verification/requirements.in python verification/process_gaia_using_datasets.py
from datasets import load_dataset_builder

# Load the dataset descriptions from local copy of the data
gaia = load_dataset_builder("data/MultimodalUniverse/v1/gaia", trust_remote_code=True)
gaia.download_and_prepare()

# Gaia HDF5 files already contain ra, dec, healpix - no catalog join needed (unlike other datasets)
gaia_train = gaia.as_dataset(split="train")
gaia_train.save_to_disk("data/MultimodalUniverse/v1/gaia_with_coordinates")
