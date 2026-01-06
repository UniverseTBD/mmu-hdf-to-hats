# NOTE: use datasets==3.6 for this
# Run this first
# uv run --with-requirements=verification/requirements.in python verification/process_ps1_sne_ia_using_datasets.py
from datasets import load_dataset_builder
from utils import get_catalog

# Load the dataset descriptions from local copy of the data
ps1_sne_ia = load_dataset_builder(
    "data/MultimodalUniverse/v1/ps1_sne_ia", trust_remote_code=True
)
ps1_sne_ia.download_and_prepare()

ps1_sne_ia_catalog = get_catalog(ps1_sne_ia)


def match_ps1_sne_ia_catalog_object_ids(example, catalog):
    # PS1 SNe Ia object_id is returned as string from _generate_examples
    example_obj_id = example["object_id"]
    catalog_entry = catalog[catalog["object_id"] == example_obj_id]
    assert len(catalog_entry) == 1, f"Expected 1 entry for {example_obj_id}, got {len(catalog_entry)}"
    return {
        **example,
        "ra": catalog_entry["ra"][0],
        "dec": catalog_entry["dec"][0],
    }


ps1_sne_ia_train = ps1_sne_ia.as_dataset(split="train")
ps1_sne_ia_mapped = ps1_sne_ia_train.map(
    lambda example: match_ps1_sne_ia_catalog_object_ids(example, ps1_sne_ia_catalog)
)
ps1_sne_ia_mapped.save_to_disk("data/MultimodalUniverse/v1/ps1_sne_ia_with_coordinates")
