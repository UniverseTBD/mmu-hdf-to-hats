# NOTE: use datasets==3.6 for this
# Run this first
# uv run --with-requirements=verification/requirements.in python verification/process_gz10_using_datasets.py
from datasets import load_dataset_builder
from utils import get_catalog

# Load the dataset descriptions from local copy of the data
gz10 = load_dataset_builder("data/MultimodalUniverse/v1/gz10", trust_remote_code=True)
gz10.download_and_prepare()

gz10_catalog = get_catalog(gz10)


def match_gz10_catalog_object_ids(example, catalog):
    # GZ10 object_id is returned as string from _generate_examples, but catalog has int64
    example_obj_id = int(example["object_id"])
    catalog_entry = catalog[catalog["object_id"] == example_obj_id]
    assert len(catalog_entry) == 1, f"Expected 1 entry for {example_obj_id}, got {len(catalog_entry)}"
    return {
        **example,
        "ra": catalog_entry["ra"][0],
        "dec": catalog_entry["dec"][0],
    }


gz10_train = gz10.as_dataset(split="train")
gz10_mapped = gz10_train.map(
    lambda example: match_gz10_catalog_object_ids(example, gz10_catalog)
)
gz10_mapped.save_to_disk("data/MultimodalUniverse/v1/gz10_with_coordinates")
