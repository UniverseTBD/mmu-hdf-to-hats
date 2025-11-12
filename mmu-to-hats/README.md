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