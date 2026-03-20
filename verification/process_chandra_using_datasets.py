# NOTE: use datasets==3.6 for this
# Run this first
# uv run --with-requirements=verification/requirements.in python verification/process_chandra_using_datasets.py
from datasets import load_dataset_builder
from utils import get_catalog

# Load the dataset descriptions from local copy of the data
chandra = load_dataset_builder(
    "data/MultimodalUniverse/v1/chandra", trust_remote_code=True
)
chandra.download_and_prepare()

chandra_catalog = get_catalog(chandra)


def match_chandra_catalog_object_ids(example, catalog):
    # object_id is int64 in HDF5 but string in dataset output
    example_obj_id = int(example["object_id"])
    catalog_entry = catalog[catalog["object_id"] == example_obj_id]
    assert len(catalog_entry) == 1, f"Expected 1 entry for {example_obj_id}, got {len(catalog_entry)}"
    return {
        **example,
        "ra": catalog_entry["ra"][0],
        "dec": catalog_entry["dec"][0],
    }


chandra_train = chandra.as_dataset(split="train")
chandra_mapped = chandra_train.map(
    lambda example: match_chandra_catalog_object_ids(example, chandra_catalog)
)
chandra_mapped.save_to_disk("data/MultimodalUniverse/v1/chandra_with_coordinates")
print(f"Saved {len(chandra_mapped)} examples to data/MultimodalUniverse/v1/chandra_with_coordinates")
