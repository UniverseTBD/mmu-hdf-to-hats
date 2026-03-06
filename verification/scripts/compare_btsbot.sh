./verification/download_btsbot.sh
uv run --with-requirements=verification/requirements.in python verification/process_btsbot_using_datasets.py
python -m transform_scripts.transform_btsbot_to_parquet
python verification/compare.py data/btsbot_hp0313_transformed.parquet data/MultimodalUniverse/v1/btsbot_with_coordinates/
