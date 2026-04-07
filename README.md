# MMU HDF5 to HATS Converter

[![NeurIPS](https://img.shields.io/badge/NeurIPS-2024---?logo=https%3A%2F%2Fneurips.cc%2Fstatic%2Fcore%2Fimg%2FNeurIPS-logo.svg&labelColor=68448B&color=b3b3b3)](https://openreview.net/forum?id=EWm9zR5Qy1) [![arXiv](https://img.shields.io/badge/arXiv-2412.02527---?logo=arXiv&labelColor=b31b1b&color=grey)](https://arxiv.org/abs/2412.02527) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

This repository contains code for converting [Multimodal Universe](https://github.com/MultimodalUniverse/MultimodalUniverse) datasets from HDF5 format to HATS format, to eventually ease the use of server-side crossmatching on Hugging Face as part of the [MMU-Streaming project](https://discord.gg/caqxB63DPd).

## Convert MMU datasets to HATS

`./main.py` converts an MMU dataset in HDF5 format to HATS. Example usage:

```shell
uv run python ./main.py \
  --transformer=sdss \
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

## Debugging workflow

Since dask is running in parallel it is notoriously hard to debug. In order to set breakpoints it's easiest to use only one runner, you can do that using the `debug` flag:

```bash
python ./main.py \
  --input=https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/sdss/sdss/healpix=583/ \
  --output=./hats \
  --name=mmu_sdss_sdss \
  --tmp-dir=./tmp \
  --max-rows=8192 \
  --debug
```

Note that choosing a concrete healpix that is rather small (<1GB) speeds up the process a lot, since reading via HTTP is typically slower than from disk and depending on the individual connection.

## MaNGA reader note

Most MMU HDF5 catalogs in this repo are "flat" files with top-level datasets such as `ra` and `dec`.
MaNGA is different: each top-level key is an object group like `8726-1901`, and scalar metadata such as
`ra`, `dec`, and `object_id` live inside that group.

That grouped layout works fine for the transformer itself, but it breaks the generic HDF5 reader used by
the HATS import path because `hats-import` first asks the reader for only `ra` and `dec` during pixel mapping.

To handle this cleanly, transformers can now provide their own reader via `build_reader()`.
The default reader remains `MMUReader`, while `MaNGATransformer` uses `MangaGroupReader`, which:

- builds per-object scalar tables from grouped HDF5 files when only `ra` and `dec` are requested
- passes grouped chunks through to the transformer for full-row conversion

### Manual MaNGA validation

Download the sample used by the verification flow:

```shell
./verification/download_manga.sh
```

Run the HATS conversion through the main CLI:

```shell
python ./main.py \
  --transformer=manga \
  --input=./data/MultimodalUniverse/v1/manga/manga \
  --output=./hats \
  --name=mmu_manga_manga \
  --tmp-dir=./tmp \
  --max-rows=8192 \
  --debug
```

For the single downloaded sample, the resulting catalog should complete successfully and
`./hats/mmu_manga_manga/mmu_manga_manga/hats.properties` should report `hats_nrows=1`.

## Transformation classes

The idea is to have one transformation class for each catalog. This class should follow a given structure as outlined in the `catalog_functions.base_transformer.BaseTransformer` class and override its abstract methods.
Note that we need to return a `pyarrow.Table` with exactly the same output as the `<catalog>.py` script. Read how to check this in the verification section below. An example is provided for SDSS, the relevant files are:

- `catalog_functions/sdss_transformer.py`
- `verification/download_sdss.sh`
- `process_sdss_using_datasets.py`

Please note that most of the classes are vibe-coded and not verified yet.
The only verified transformation class is SDSS.

## Verification of a transformation class

There is an example implementation for SDSS. For data generation:

1. Run the datasets-based processing:
   ```shell
   uv run --with-requirements=verification/requirements.in python verification/process_sdss_using_datasets.py
   ```
   This will install `datasets==3.6` and run the processing using datasets, no need to create another venv. Note that you'll need `numpy>1` for the other jobs, so it is not feasible to install from `verification/requirements.in` in your working virtualenv.

2. Run the transform script:
   ```shell
   python transform_scripts/transform_<catalog>_to_parquet.py
   ```
   (Legacy: `python catalog_functions/sdss_transformer.py`.) This script needs to be written first but can be copy-pasted. Adaptations may be needed to the function in the script, so that the `object_id`s match.

3. Both of these jobs will create their own parquet files in the data folder.

4. Make sure the created files match:
   ```shell
   python verification/compare.py
   ```

5. Add the paths to the files in `verify.py`.

6. Once you've followed all these steps, you can simply do:
   ```shell
   python verify.py <catalog_name>
   ```

Caveats:
- btsbot contains `test_*`, `train_*`, `val_*` files
