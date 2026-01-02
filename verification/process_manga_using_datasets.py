# NOTE: run with datasets==3.6
# uv run --with-requirements=verification/requirements.in python verification/process_manga_using_datasets.py
from datasets import load_dataset_builder
from utils import get_catalog

# Load the dataset descriptions from local copy of the data
manga = load_dataset_builder("data/MultimodalUniverse/v1/manga/", trust_remote_code=True)
manga.download_and_prepare()


manga_train = manga.as_dataset(split="train")
# num_shards=1 arguments necessary to process the one and only example
manga_train.save_to_disk("data/MultimodalUniverse/v1/manga_with_coordinates", num_shards=1, num_proc=1)
