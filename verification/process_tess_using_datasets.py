# NOTE: use datasets==3.6 for this
# Run this first
# uv pip install -r requirements.txt
# ./download_tess_hsc.sh
from datasets import load_dataset_builder, concatenate_datasets
from mmu.utils import get_catalog

# Load the dataset descriptions from local copy of the data
tess = load_dataset_builder("data/MultimodalUniverse/v1/tess", trust_remote_code=True)
tess.download_and_prepare()

tess_catalog = get_catalog(tess, keys=["object_id", "RA", "DEC"])


def match_tess_catalog_object_ids(example, catalog):
    example_obj_id = example["object_id"].strip("b'")
    catalog_entry = catalog[catalog["object_id"] == int(example_obj_id)]
    assert len(catalog_entry) == 1
    return {
        **example,
        "RA": catalog_entry["RA"][0],
        "DEC": catalog_entry["DEC"][0],
    }


tess_train = tess.as_dataset(split="train")
tess_mapped = tess_train.map(
    lambda example: match_tess_catalog_object_ids(example, tess_catalog)
)
tess_mapped.save_to_disk("data/MultimodalUniverse/v1/tess_with_coordinates")
