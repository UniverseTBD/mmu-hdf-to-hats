---
configs:
- config_name: default
  data_dir: mmu_yse_dr1/dataset
tags:
- astronomy
license: cc-by-4.0
pretty_name: mmu_yse_dr1
size_categories:
- 1K<n<10K
---

<div align="center">
<img src="example_figure.png" width="600">
</div>

# mmu_yse_dr1 HATS Catalog Collection

This is the collection of HATS catalogs representing mmu_yse_dr1.

This dataset is part of the [Multimodal Universe](https://github.com/MultimodalUniverse/MultimodalUniverse),
a large-scale collection of multimodal astronomical data. For full details, see the paper:
[The Multimodal Universe: Enabling Large-Scale Machine Learning with 100TBs of Astronomical Scientific Data](https://arxiv.org/abs/2412.02527).

### Access the catalog

We recommend the use of the [LSDB](https://lsdb.io) Python framework to access HATS catalogs.
LSDB can be installed via `pip install lsdb` or `conda install conda-forge::lsdb`,
see more details [in the docs](https://docs.lsdb.io/).
The following code provides a minimal example of opening this catalog:

```python
import lsdb

# Full sky coverage.
catalog = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_yse_dr1")
# One-degree cone.
catalog = lsdb.open_catalog(
    "https://huggingface.co/datasets/UniverseTBD/mmu_yse_dr1",
    search_filter=lsdb.ConeSearch(ra=142.0, dec=36.0, radius_arcsec=3600.0),
)
```

Each catalog in this collection is represented as a separate [Apache Parquet dataset](https://arrow.apache.org/docs/python/dataset.html) and can be accessed with a variety of tools, including `pandas`, `pyarrow`, `dask`, `Spark`, `DuckDB`.

### File structure

This catalog is represented by the following files and directories:

- [`collection.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_yse_dr1/collection.properties) � textual metadata file describing the HATS collection of catalogs
- [`mmu_yse_dr1`](https://huggingface.co/datasets/UniverseTBD/mmu_yse_dr1/mmu_yse_dr1) � main HATS catalog directory
  - [`dataset/`](https://huggingface.co/datasets/UniverseTBD/mmu_yse_dr1/mmu_yse_dr1/dataset/) � Apache Parquet dataset directory for the main catalog
    - ... parquet metadata and data files in sub directories ...
  - [`hats.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_yse_dr1/mmu_yse_dr1/hats.properties) � textual metadata file describing the main HATS catalog
  - [`partition_info.csv`](https://huggingface.co/datasets/UniverseTBD/mmu_yse_dr1/mmu_yse_dr1/partition_info.csv) � CSV file with a list of catalog HEALPix tiles (catalog partitions)
  - [`skymap.fits`](https://huggingface.co/datasets/UniverseTBD/mmu_yse_dr1/mmu_yse_dr1/skymap.fits) � HEALPix skymap FITS file with row-counts per HEALPix tile of fixed order 10
- [`mmu_yse_dr1_10arcs/`](https://huggingface.co/datasets/UniverseTBD/mmu_yse_dr1/mmu_yse_dr1_10arcs) � default margin catalog to ensure data completeness in cross-matching, the margin threshold is 10.0 arcseconds
  - ... margin catalog files and directories ...

### Catalog metadata

Metadata of the main HATS catalog, excluding margins and indexes:

| **Number of rows** | **Number of columns** | **Number of partitions** | **Size on disk** | **HATS Builder** |
| --- | --- | --- | --- | --- |
| 2,003 | 7 | 312 | 195.5 MiB | hats-import v0.7.3, hats v0.7.3 |


### Catalog columns

The main HATS catalog contains the following columns:

| **Name** |  **`_healpix_29`** | **`lightcurve.band`** | **`lightcurve.time`** | **`lightcurve.flux`** | **`lightcurve.flux_err`** | **`redshift`** | **`host_log_mass`** | **`ra`** | **`dec`** | **`obj_type`** | **`object_id`** |
| --- |  --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **Data Type** |  int64 | list[string] | list[float] | list[float] | list[float] | float | float | double | double | string | string |
| **Nested?** |  � | lightcurve | lightcurve | lightcurve | lightcurve | � | � | � | � | � | � |
| **Value count** |  2,003 | 269,628 | 269,628 | 269,628 | 269,628 | 2,003 | 2,003 | 2,003 | 2,003 | 2,003 | 2,003 |
| **Example row** |  399200651177352460 | [X, X, X, X, X, X, X, X, X, X, X, � (168 total)] | [5.887e+04, 5.887e+04, 5.887e+04, � (168 total)] | [328.3, 435.8, 580.4, 681.8, 773, � (168 total)] | [76.91, 74.19, 56.67, 59.37, 67.84, � (168 total)] | 0.14 | -99 | 141.9 | 35.53 | SN-Ia-norm | 2020azn |
| **Minimum value** |  1126237237925017 | X | -99.0 | -0.0 | -0.0 | -99.0 | -99.0 | 0.3768570125102997 | -29.100194931030273 | LBV | 2019lbi |
| **Maximum value** |  3458687311145453838 | z | 59839.5703125 | 1408506.75 | 97039.4609375 | 0.5249999761581421 | -99.0 | 359.86798095703125 | 74.1773452758789 | TDE | 2021zzv |


"Nested" indicates whether the column is stored as a nested field inside another "struct" column.


"Value count" may be different from the total number of rows for nested columns: each nested element is counted as a single value.




### Crossmatch with another catalog

HATS catalogs can be efficiently crossmatched using [LSDB](https://lsdb.io),
which leverages the HEALPix partitioning to avoid loading the full datasets into memory:

```python
import lsdb

mmu_yse_dr1 = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_yse_dr1")
other = lsdb.open_catalog("https://huggingface.co/datasets/<org>/<other_catalog>")

crossmatched = mmu_yse_dr1.crossmatch(other, radius_arcsec=1.0)
print(crossmatched)
```

See the [LSDB documentation](https://docs.lsdb.io/) for more details on crossmatching and other operations.

### Dataset-specific context

**Original survey**  
This dataset is based on the [Young Supernova Experiment Data Release 1 (YSE DR1)](https://arxiv.org/pdf/2211.07128) and contains a collection of 1975 supernova light curves.

**Data modality**  
The dataset consists of light curve data including time of observation, flux, flux error, and band pass filter information (griz bands). Additional metadata includes supernova classification, coordinates, redshift, and host galaxy mass.

**Typical use cases**  
The dataset can be used for photometric classification of supernovae.

**Caveats**  
No additional preprocessing or selection criteria beyond those in the original release are described.

**Citation**  
The dataset is released under the CC BY 4.0 license. Any further distribution
of this work must maintain attribution to the author(s) and the title of the work, journal
citation and DOI.
