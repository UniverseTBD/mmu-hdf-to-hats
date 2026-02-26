# NOTE: use datasets==3.6 for this
# Run this first
# uv run --with-requirements=verification/requirements.in python verification/process_csp_using_datasets.py
from datasets import load_dataset_builder

# Load the dataset descriptions from local copy of the data
csp = load_dataset_builder(
    "data/MultimodalUniverse/v1/csp", trust_remote_code=True
)
csp.download_and_prepare()

csp_train = csp.as_dataset(split="train")
csp_train.save_to_disk("data/MultimodalUniverse/v1/csp_with_coordinates")
print(f"Saved {len(csp_train)} examples to data/MultimodalUniverse/v1/csp_with_coordinates")
