# NOTE: use datasets==3.6 for this
# Run this first
# uv pip install -r requirements.txt
# ./download_swift_sne_ia_hsc.sh
from datasets import load_dataset_builder, concatenate_datasets
from utils import get_catalog

# Load the dataset descriptions from local copy of the data
swift_sne_ia = load_dataset_builder(
    "data/MultimodalUniverse/v1/swift_sne_ia", trust_remote_code=True
)
swift_sne_ia.download_and_prepare()

swift_sne_ia_catalog = get_catalog(swift_sne_ia)


def match_swift_sne_ia_catalog_object_ids(example, catalog):
    example_obj_id = example["object_id"].strip("b'")
    catalog_entry = catalog[catalog["object_id"] == example_obj_id]
    assert len(catalog_entry) == 1
    return {
        **example,
        "ra": catalog_entry["ra"][0],
        "dec": catalog_entry["dec"][0],
    }


swift_sne_ia_train = swift_sne_ia.as_dataset(split="train")
swift_sne_ia_mapped = swift_sne_ia_train.map(
    lambda example: match_swift_sne_ia_catalog_object_ids(example, swift_sne_ia_catalog)
)
swift_sne_ia_mapped.save_to_disk(
    "data/MultimodalUniverse/v1/swift_sne_ia_with_coordinates"
)
