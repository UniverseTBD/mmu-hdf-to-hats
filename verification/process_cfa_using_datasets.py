# NOTE: use datasets==3.6 for this
# Run this first
# uv run --with-requirements=verification/requirements.in python verification/process_cfa_using_datasets.py
from datasets import load_dataset_builder
from utils import get_catalog

# Load the dataset descriptions from local copy of the data (cfa3 config)
cfa = load_dataset_builder(
    "data/MultimodalUniverse/v1/cfa", trust_remote_code=True, name="cfa3"
)
cfa.download_and_prepare()

cfa_catalog = get_catalog(cfa)


def match_cfa_catalog_object_ids(example, catalog):
    # CFA object_id is returned as string from _generate_examples
    example_obj_id = example["object_id"]
    catalog_entry = catalog[catalog["object_id"] == example_obj_id]
    assert len(catalog_entry) == 1, f"Expected 1 entry for {example_obj_id}, got {len(catalog_entry)}"
    return {
        **example,
        "ra": catalog_entry["ra"][0],
        "dec": catalog_entry["dec"][0],
    }


cfa_train = cfa.as_dataset(split="train")
cfa_mapped = cfa_train.map(
    lambda example: match_cfa_catalog_object_ids(example, cfa_catalog)
)
cfa_mapped.save_to_disk("data/MultimodalUniverse/v1/cfa_with_coordinates")
print(f"Saved {len(cfa_mapped)} examples to data/MultimodalUniverse/v1/cfa_with_coordinates")
