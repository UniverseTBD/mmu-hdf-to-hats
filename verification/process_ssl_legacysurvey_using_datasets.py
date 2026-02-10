# NOTE: use datasets==3.6 for this
# Run this first
# uv run --with-requirements=verification/requirements.in python verification/process_ssl_legacysurvey_using_datasets.py
from datasets import load_dataset_builder
from utils import get_catalog

# Load the dataset descriptions from local copy of the data
ssl_ls = load_dataset_builder(
    "data/MultimodalUniverse/v1/ssl_legacysurvey", trust_remote_code=True
)
ssl_ls.download_and_prepare()

ssl_ls_catalog = get_catalog(ssl_ls)


def match_ssl_legacysurvey_catalog_object_ids(example, catalog):
    # object_id is int64 in HDF5 but string in dataset output
    example_obj_id = int(example["object_id"])
    catalog_entry = catalog[catalog["object_id"] == example_obj_id]
    assert len(catalog_entry) == 1, f"Expected 1 entry for {example_obj_id}, got {len(catalog_entry)}"
    return {
        **example,
        "ra": catalog_entry["ra"][0],
        "dec": catalog_entry["dec"][0],
    }


ssl_ls_train = ssl_ls.as_dataset(split="train")
ssl_ls_mapped = ssl_ls_train.map(
    lambda example: match_ssl_legacysurvey_catalog_object_ids(example, ssl_ls_catalog)
)
ssl_ls_mapped.save_to_disk("data/MultimodalUniverse/v1/ssl_legacysurvey_with_coordinates")
print(f"Saved {len(ssl_ls_mapped)} examples to data/MultimodalUniverse/v1/ssl_legacysurvey_with_coordinates")
