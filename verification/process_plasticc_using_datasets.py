# NOTE: run with datasets==3.6
# uv run --with-requirements=verification/requirements.in python verification/process_plasticc_using_datasets.py
from datasets import load_dataset_builder

# Load using train_only config (default)
plasticc = load_dataset_builder(
    "data/MultimodalUniverse/v1/plasticc",
    trust_remote_code=True,
    name="train_only",
)
plasticc.download_and_prepare()

plasticc_train = plasticc.as_dataset(split="train")
plasticc_train.save_to_disk("data/MultimodalUniverse/v1/plasticc_datasets_output")
print(f"Saved {len(plasticc_train)} examples to data/MultimodalUniverse/v1/plasticc_datasets_output")
