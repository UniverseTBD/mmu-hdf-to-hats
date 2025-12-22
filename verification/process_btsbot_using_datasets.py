# NOTE: use datasets==3.6 for this
# Run this first
# uv pip install -r requirements.txt
# ./download_btsbot.sh
from datasets import load_dataset_builder, concatenate_datasets
from mmu.utils import get_catalog
from astropy.table import vstack

# Load the dataset descriptions from local copy of the data
desi = load_dataset_builder("data/MultimodalUniverse/v1/btsbot", trust_remote_code=True)
desi.download_and_prepare()

train_catalog = get_catalog(desi, split="train")
test_catalog = get_catalog(desi, split="test")
val_catalog = get_catalog(desi, split="val")
# concat astropy tables
desi_catalog = vstack([train_catalog, test_catalog, val_catalog])


def add_ra_dec(example, catalog):
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


splits = []
for split in ["train", "test", "val"]:
    desi_train = desi.as_dataset(split=split)
    desi_mapped = desi_train.map(
        lambda example: add_ra_dec(example, desi_catalog)
    )
    print("Length of split", split, "is", len(desi_mapped))
    splits.append(desi_mapped)
table = concatenate_datasets(splits)
table.save_to_disk("data/MultimodalUniverse/v1/btsbot_with_coordinates")
