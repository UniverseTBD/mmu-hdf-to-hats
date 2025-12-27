# NOTE: use datasets==3.6 for this
# Run this first
# uv run --with-requirements=verification/requirements.in python verification/process_snls_using_datasets.py
from datasets import load_dataset_builder
from utils import get_catalog

# Load the dataset descriptions from local copy of the data
snls = load_dataset_builder("data/MultimodalUniverse/v1/snls", trust_remote_code=True)
snls.download_and_prepare()

snls_catalog = get_catalog(snls)


def match_snls_catalog_object_ids(example, catalog):
    # SNLS object_id is returned as string from _generate_examples
    example_obj_id = example["object_id"]
    catalog_entry = catalog[catalog["object_id"] == example_obj_id]
    assert len(catalog_entry) == 1, f"Expected 1 entry for {example_obj_id}, got {len(catalog_entry)}"
    return {
        **example,
        "ra": catalog_entry["ra"][0],
        "dec": catalog_entry["dec"][0],
        "healpix": catalog_entry["healpix"][0],
    }


snls_train = snls.as_dataset(split="train")
snls_mapped = snls_train.map(
    lambda example: match_snls_catalog_object_ids(example, snls_catalog)
)
snls_mapped.save_to_disk("data/MultimodalUniverse/v1/snls_with_coordinates")
