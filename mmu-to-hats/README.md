# Convert MMU datasets to HATS

`./main.py` converts an MMU dataset in HDF5 format to HATS. Example usage:

```shell
uv run python ./main.py \
  --input=https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/sdss/sdss/ \
  --output=./hats \
  --name=mmu_sdss_sdss \
  --tmp-dir=./tmp \
  --max-rows=8192
```

Run with `--help` to see all available options.

This will create a dataset at `./hats/mmu_sdss_sdss`.
While it is technically possible to specify a Hugging Face organization URI as the output
(e.g. `--output=hf://datasets/LSDB/` to place the data in the `LSDB/mmu_sdss_sdss` repository),
in practice this may fail for large datasets due to the commit rate limit of 128 per hour.

It is therefore recommended to create the dataset locally and upload it using the Hugging Face client:

```shell
uvx --from huggingface-hub hf upload-large-folder \
  --repo-type=dataset --num-workers=16 \
  LSDB/mmu_sdss_sdss ./hats/mmu_sdss_sdss
```

Here is an example of the result dataset: https://huggingface.co/datasets/LSDB/mmu_sdss_sdss/tree/main

# Transformation classes
The idea is to have one transformation class for each catalog. This class should follow a given structure as outlined in the `catalog_functions.base_transformer.BaseTransformer` class and override its abstract methods.
Note that we need to return a `pyarrow.Table` with exactly the same output as the <catalog>.py script. Read how to check this in the paragraph below. An example is provided for sdss, the relevant files are:
 - catalog_functions/sdss_transformer.py
 - verification/download_sdss.sh
 - process_sdss_using_datasets.py

# Verification of a tranformation class
There is an example implemenation for sdss. For data generation do:
 - `uv run --with-requirements=verification/requirements.in python verification/process_sdss_using_datasets.py`, this will install datasets==3.6 and run the processing using datasets, no need to create another venv. Note that you'll need numpy>1 for the other jobs, so it is not feasible to install from `verification/requirements.in` in your working virtualenv
 - then run `python catalog_functions/sdss_transformer.py`
 - both of these jobs will create their own parquet files in the data folder
- afterwards make sure that the created files match: `python verification/compare.py`
