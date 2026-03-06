# NOTE: use datasets==3.6 for this
# Run this first
# uv run --with-requirements=verification/requirements.in python verification/process_foundation_using_datasets.py
from datasets import load_dataset_builder
from utils import get_catalog

# Load the dataset descriptions from local copy of the data
foundation = load_dataset_builder(
    "data/MultimodalUniverse/v1/foundation", trust_remote_code=True
)
foundation.download_and_prepare()

foundation_catalog = get_catalog(foundation)


def match_foundation_catalog_object_ids(example, catalog):
    # Foundation object_id is returned as string from _generate_examples
    example_obj_id = example["object_id"]
    catalog_entry = catalog[catalog["object_id"] == example_obj_id]
    assert len(catalog_entry) == 1, f"Expected 1 entry for {example_obj_id}, got {len(catalog_entry)}"
    return {
        **example,
        "ra": catalog_entry["ra"][0],
        "dec": catalog_entry["dec"][0],
    }


foundation_train = foundation.as_dataset(split="train")
foundation_mapped = foundation_train.map(
    lambda example: match_foundation_catalog_object_ids(example, foundation_catalog)
)
foundation_mapped.save_to_disk("data/MultimodalUniverse/v1/foundation_with_coordinates")
