# NOTE: run with datasets==3.6
# uv run --with-requirements=verification/requirements.in python verification/process_jwst_using_datasets.py
from datasets import load_dataset_builder
from utils import get_catalog

# Load the dataset descriptions from local copy of the data
jwst = load_dataset_builder("data/MultimodalUniverse/v1/jwst/", trust_remote_code=True)
jwst.download_and_prepare()


jwst_train = jwst.as_dataset(split="train")
# num_shards=1 arguments necessary to process the one and only example
jwst_train.save_to_disk("data/MultimodalUniverse/v1/jwst_with_coordinates", num_shards=1, num_proc=1)