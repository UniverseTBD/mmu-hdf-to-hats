# NOTE: run with datasets==3.6
# uv run --with-requirements=verification/requirements.in python verification/process_legacysurvey_using_datasets.py
from datasets import load_dataset_builder, concatenate_datasets
from mmu.utils import get_catalog
from astropy.table import vstack

# Load the dataset descriptions from local copy of the data
legacysurvey = load_dataset_builder("data/MultimodalUniverse/v1/legacysurvey", trust_remote_code=True)
legacysurvey.download_and_prepare()

legacysurvey_catalog = get_catalog(legacysurvey)


def match_legacysurvey_catalog_object_ids(example, catalog):
    if isinstance(example["object_id"], int):
        example_obj_id = example["object_id"]
    elif isinstance(example["object_id"], str):
        example_obj_id = example["object_id"].strip("b'")
    else:
        raise ValueError("Unexpected type for object_id")
    catalog_entry = catalog[catalog["object_id"] == example_obj_id]
    assert len(catalog_entry) == 1
    return {
        **example,
        "ra": catalog_entry["ra"][0],
        "dec": catalog_entry["dec"][0],
    }


legacysurvey_train = legacysurvey.as_dataset(split="train")
legacysurvey_mapped = legacysurvey_train.map(
    lambda example: match_legacysurvey_catalog_object_ids(example, legacysurvey_catalog)
)
legacysurvey_mapped.save_to_disk("data/MultimodalUniverse/v1/legacysurvey_with_coordinates")
