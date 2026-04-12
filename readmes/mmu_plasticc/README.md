
---
configs:
- config_name: default
  data_dir: mmu_plasticc/dataset
tags:
- astronomy
license: cc-by-4.0
pretty_name: mmu_plasticc
size_categories:
- 1M<n<10M
---

# mmu_plasticc HATS Catalog Collection

This is the collection of HATS catalogs representing mmu_plasticc.

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
catalog = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_plasticc")
# One-degree cone.
catalog = lsdb.open_catalog(
    "https://huggingface.co/datasets/UniverseTBD/mmu_plasticc",
    search_filter=lsdb.ConeSearch(ra=330.0, dec=-6.0, radius_arcsec=3600.0),
)
```

Each catalog in this collection is represented as a separate [Apache Parquet dataset](https://arrow.apache.org/docs/python/dataset.html) and can be accessed with a variety of tools, including `pandas`, `pyarrow`, `dask`, `Spark`, `DuckDB`.

### File structure

This catalog is represented by the following files and directories:

- [`collection.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_plasticc/collection.properties) — textual metadata file describing the HATS collection of catalogs
- [`mmu_plasticc`](https://huggingface.co/datasets/UniverseTBD/mmu_plasticc/mmu_plasticc) — main HATS catalog directory
  - [`dataset/`](https://huggingface.co/datasets/UniverseTBD/mmu_plasticc/mmu_plasticc/dataset/) — Apache Parquet dataset directory for the main catalog
    - ... parquet metadata and data files in sub directories ...
  - [`hats.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_plasticc/mmu_plasticc/hats.properties) — textual metadata file describing the main HATS catalog
  - [`partition_info.csv`](https://huggingface.co/datasets/UniverseTBD/mmu_plasticc/mmu_plasticc/partition_info.csv) — CSV file with a list of catalog HEALPix tiles (catalog partitions)
  - [`skymap.fits`](https://huggingface.co/datasets/UniverseTBD/mmu_plasticc/mmu_plasticc/skymap.fits) — HEALPix skymap FITS file with row-counts per HEALPix tile of fixed order 10
- [`mmu_plasticc_10arcs/`](https://huggingface.co/datasets/UniverseTBD/mmu_plasticc/mmu_plasticc_10arcs) — default margin catalog to ensure data completeness in cross-matching, the margin threshold is 10.0 arcseconds
  - ... margin catalog files and directories ...

### Catalog metadata

Metadata of the main HATS catalog, excluding margins and indexes:

| **Number of rows** | **Number of columns** | **Number of partitions** | **Size on disk** | **HATS Builder** |
| --- | --- | --- | --- | --- |
| 7,001,476 | 8 | 1,509 | 3.9 GiB | hats-import v0.7.3, hats v0.7.3 |


### Catalog columns

The main HATS catalog contains the following columns:

| **Name** |  **`_healpix_29`** | **`lightcurve.band`** | **`lightcurve.time`** | **`lightcurve.flux`** | **`lightcurve.flux_err`** | **`hostgal_photoz`** | **`hostgal_specz`** | **`redshift`** | **`ra`** | **`dec`** | **`obj_type`** | **`object_id`** |
| --- |  --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **Data Type** |  int64 | list[string] | list[float] | list[float] | list[float] | float | float | float | double | double | string | string |
| **Nested?** |  — | lightcurve | lightcurve | lightcurve | lightcurve | — | — | — | — | — | — | — |
| **Value count** |  7,001,476 | 5,849,936,616 | 5,849,936,616 | 5,849,936,616 | 5,849,936,616 | 7,001,476 | 7,001,476 | 7,001,476 | 7,001,476 | 7,001,476 | 7,001,476 | 7,001,476 |
| **Example row** |  1310263451060905914 | [u, u, u, u, u, u, u, u, u, u, u, … (816 total)] | [5.979e+04, 5.982e+04, 5.982e+04, … (816 total)] | [-13.15, 7.379, 2.236, -2.3, … (816 total)] | [13.62, 8.448, 8.948, 13.9, 10.11, … (816 total)] | 0.545 | -9 | 0.551 | 330.5 | -6.129 | SNII | 75227491 |
| **Minimum value** |  64871186081855 | Y | -0.0 | -8935484.0 | -0.0 | -0.0 | -9.0 | -0.0 | -0.0 | -64.7609 | AGN | 100000051 |
| **Maximum value** |  3458741424076622916 | z | 60674.26171875 | 13675792.0 | 13791667.0 | 3.0 | 3.444999933242798 | 3.446000099182129 | 359.8242 | 4.1815 | TDE | 99999980 |

"ra" and "dec" are random numbers and not real coordinates.


"Nested" indicates whether the column is stored as a nested field inside another "struct" column.


"Value count" may be different from the total number of rows for nested columns: each nested element is counted as a single value.




### Crossmatch with another catalog

HATS catalogs can be efficiently crossmatched using [LSDB](https://lsdb.io),
which leverages the HEALPix partitioning to avoid loading the full datasets into memory:

```python
import lsdb

mmu_plasticc = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_plasticc")
other = lsdb.open_catalog("https://huggingface.co/datasets/<org>/<other_catalog>")

crossmatched = mmu_plasticc.crossmatch(other, radius_arcsec=1.0)
print(crossmatched)
```

See the [LSDB documentation](https://docs.lsdb.io/) for more details on crossmatching and other operations.
