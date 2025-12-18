# NOTE: use datasets==3.6 for this
# Run this first
# uv pip install -r requirements.txt
# ./download_desi_hsc.sh
from datasets import load_dataset_builder, concatenate_datasets
from mmu.utils import get_catalog

# Load the dataset descriptions from local copy of the data
desi = load_dataset_builder("data/MultimodalUniverse/v1/desi", trust_remote_code=True)
desi.download_and_prepare()

desi_catalog = get_catalog(desi)


def match_desi_catalog_object_ids(example, catalog):
    if isinstance(example["object_id"], int):
        example_obj_id = example["object_id"]
    elif isinstance(example["object_id"], str):
        example_obj_id = example["object_id"].strip("b'")
    else:
        raise ValueError("Unexpected type for object_id")
    catalog_entry = catalog[catalog["object_id"] == int(example_obj_id)]
    assert len(catalog_entry) == 1
    return {
        **example,
        "ra": catalog_entry["ra"][0],
        "dec": catalog_entry["dec"][0],
        "healpix": catalog_entry["healpix"][0],
    }


desi_train = desi.as_dataset(split="train")
desi_mapped = desi_train.map(
    lambda example: match_desi_catalog_object_ids(example, desi_catalog)
)
desi_mapped.save_to_disk("data/MultimodalUniverse/v1/desi_with_coordinates")
