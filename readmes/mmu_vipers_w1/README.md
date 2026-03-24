
---
configs:
- config_name: default
  data_dir: mmu_vipers_w1/dataset
tags:
- astronomy
license: cc-by-4.0
pretty_name: mmu_vipers_w1
size_categories:
- 10K<n<100K
---

# mmu_vipers_w1 HATS Catalog Collection

This is the collection of HATS catalogs representing mmu_vipers_w1.

### Access the catalog

We recommend the use of the [LSDB](https://lsdb.io) Python framework to access HATS catalogs.
LSDB can be installed via `pip install lsdb` or `conda install conda-forge::lsdb`,
see more details [in the docs](https://docs.lsdb.io/).
The following code provides a minimal example of opening this catalog:

```python
import lsdb

# Full sky coverage.
catalog = lsdb.open_catalog("https://huggingface.co/datasets/LSDB/mmu_vipers_w1")
# One-degree cone.
catalog = lsdb.open_catalog(
    "https://huggingface.co/datasets/LSDB/mmu_vipers_w1",
    search_filter=lsdb.ConeSearch(ra=36.0, dec=-6.0, radius_arcsec=3600.0),
)
```

Each catalog in this collection is represented as a separate [Apache Parquet dataset](https://arrow.apache.org/docs/python/dataset.html) and can be accessed with a variety of tools, including `pandas`, `pyarrow`, `dask`, `Spark`, `DuckDB`.

### File structure

This catalog is represented by the following files and directories:

- [`collection.properties`](https://huggingface.co/datasets/LSDB/mmu_vipers_w1/collection.properties) — textual metadata file describing the HATS collection of catalogs
- [`mmu_vipers_w1`](https://huggingface.co/datasets/LSDB/mmu_vipers_w1/mmu_vipers_w1) — main HATS catalog directory
  - [`dataset/`](https://huggingface.co/datasets/LSDB/mmu_vipers_w1/mmu_vipers_w1/dataset/) — Apache Parquet dataset directory for the main catalog
    - ... parquet metadata and data files in sub directories ...
  - [`hats.properties`](https://huggingface.co/datasets/LSDB/mmu_vipers_w1/mmu_vipers_w1/hats.properties) — textual metadata file describing the main HATS catalog
  - [`partition_info.csv`](https://huggingface.co/datasets/LSDB/mmu_vipers_w1/mmu_vipers_w1/partition_info.csv) — CSV file with a list of catalog HEALPix tiles (catalog partitions)
  - [`skymap.fits`](https://huggingface.co/datasets/LSDB/mmu_vipers_w1/mmu_vipers_w1/skymap.fits) — HEALPix skymap FITS file with row-counts per HEALPix tile of fixed order 10
- [`mmu_vipers_w1_10arcs/`](https://huggingface.co/datasets/LSDB/mmu_vipers_w1/mmu_vipers_w1_10arcs) — default margin catalog to ensure data completeness in cross-matching, the margin threshold is 10.0 arcseconds
  - ... margin catalog files and directories ...

### Catalog metadata

Metadata of the main HATS catalog, excluding margins and indexes:

| **Number of rows** | **Number of columns** | **Number of partitions** | **Size on disk** | **HATS Builder** |
| --- | --- | --- | --- | --- |
| 60,528 | 9 | 20 | 447.3 MiB | hats-import v0.7.1, hats v0.7.1 |


### Catalog columns

The main HATS catalog contains the following columns:

| **Name** |  **`_healpix_29`** | **`spectrum.flux`** | **`spectrum.ivar`** | **`spectrum.lambda`** | **`spectrum.mask`** | **`REDSHIFT`** | **`REDFLAG`** | **`EXPTIME`** | **`NORM`** | **`MAG`** | **`ra`** | **`dec`** | **`object_id`** |
| --- |  --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **Data Type** |  int64 | list[float] | list[float] | list[float] | list[float] | float | float | float | float | float | float | float | string |
| **Nested?** |  — | spectrum | spectrum | spectrum | spectrum | — | — | — | — | — | — | — | — |
| **Value count** |  60,528 | 33,714,096 | 33,714,096 | 33,714,096 | 33,714,096 | 60,528 | 60,528 | 60,528 | 60,528 | 60,528 | 60,528 | 60,528 | 60,528 |
| **Example row** |  1244368854977221770 | [0.1494, 0.1505, 0.1503, 0.1502, … (557 total)] | [1.471e-17, 1.777e-17, 2.024e-17, … (557 total)] | [5514, 5521, 5529, 5536, 5543, … (557 total)] | [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, … (557 total)] | 0.8462 | 0.8462 | 540 | 6.217 | 21.93 | 36.11 | -5.501 | 116024139.0 |
| **Minimum value** |  1243931779419333962 | -92.83397674560547 | -3.101720243426774e-19 | 5514.27978515625 | -0.0 | -0.0 | -0.0 | 540.0 | -99.0 | 15.56879997253418 | 30.189260482788086 | -5.980062007904053 | 101121877.0 |
| **Maximum value** |  2589547514245821801 | 986.7094116210938 | 1.3494081035675311e-14 | 9484.1201171875 | 3.0 | 4.558800220489502 | 4.558800220489502 | 540.0 | 1385.6070556640625 | 23.991300582885742 | 38.80224609375 | -4.171539783477783 | 127112147.0 |


"Nested" indicates whether the column is stored as a nested field inside another "struct" column.


"Value count" may be different from the total number of rows for nested columns: each nested element is counted as a single value.




### Crossmatch with another catalog

HATS catalogs can be efficiently crossmatched using [LSDB](https://lsdb.io),
which leverages the HEALPix partitioning to avoid loading the full datasets into memory:

```python
import lsdb

mmu_vipers_w1 = lsdb.open_catalog("https://huggingface.co/datasets/LSDB/mmu_vipers_w1")
other = lsdb.open_catalog("https://huggingface.co/datasets/<org>/<other_catalog>")

crossmatched = mmu_vipers_w1.crossmatch(other, radius_arcsec=1.0)
print(crossmatched)
```

See the [LSDB documentation](https://docs.lsdb.io/) for more details on crossmatching and other operations.
