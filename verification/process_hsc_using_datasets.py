# NOTE: use datasets==3.6 for this
# Run this first
# uv run --with-requirements=verification/requirements.in python verification/process_hsc_using_datasets.py
from datasets import load_dataset_builder
from utils import get_catalog

# Load the dataset descriptions from local copy of the data
hsc = load_dataset_builder("data/MultimodalUniverse/v1/hsc", trust_remote_code=True)
hsc.download_and_prepare()

hsc_catalog = get_catalog(hsc)


def match_hsc_catalog_object_ids(example, catalog):
    # HSC object_id is returned as string from _generate_examples, but catalog has int64
    example_obj_id = int(example["object_id"])
    catalog_entry = catalog[catalog["object_id"] == example_obj_id]
    assert len(catalog_entry) == 1, f"Expected 1 entry for {example_obj_id}, got {len(catalog_entry)}"
    return {
        **example,
        "ra": catalog_entry["ra"][0],
        "dec": catalog_entry["dec"][0],
    }


hsc_train = hsc.as_dataset(split="train")
hsc_mapped = hsc_train.map(
    lambda example: match_hsc_catalog_object_ids(example, hsc_catalog)
)
hsc_mapped.save_to_disk("data/MultimodalUniverse/v1/hsc_with_coordinates")
