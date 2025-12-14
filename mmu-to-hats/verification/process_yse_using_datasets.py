# NOTE: use datasets==3.6 for this
# Run this first
# uv pip install -r requirements.txt
# ./download_yse_hsc.sh
from datasets import load_dataset_builder, concatenate_datasets
from mmu.utils import get_catalog

# Load the dataset descriptions from local copy of the data
yse = load_dataset_builder("data/MultimodalUniverse/v1/yse", trust_remote_code=True)
yse.download_and_prepare()

yse_catalog = get_catalog(yse)


def match_yse_catalog_object_ids(example, catalog):
    example_obj_id = example["object_id"].strip("b'")
    catalog_entry = catalog[catalog["object_id"] == example_obj_id]
    assert len(catalog_entry) == 1
    return {
        **example,
        "ra": catalog_entry["ra"][0],
        "dec": catalog_entry["dec"][0],
        "healpix": catalog_entry["healpix"][0],
    }


yse_train = yse.as_dataset(split="train")
yse_mapped = yse_train.map(
    lambda example: match_yse_catalog_object_ids(example, yse_catalog)
)
yse_mapped.save_to_disk("data/MultimodalUniverse/v1/yse_with_coordinates")
