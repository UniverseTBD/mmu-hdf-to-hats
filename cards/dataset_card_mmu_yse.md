

# mmu_yse HATS Catalog Collection

This is the collection of HATS catalogs representing mmu_yse.

### Access the catalog

We recommend the use of the [LSDB](https://lsdb.io) Python framework to access HATS catalogs.
LSDB can be installed via `pip install lsdb` or `conda install conda-forge::lsdb`,
see more details [in the docs](https://docs.lsdb.io/).
The following code provides a minimal example of opening this catalog:

```python
import lsdb

# Full sky coverage.
catalog = lsdb.open_catalog("<PATH>")
# One-degree cone.
catalog = lsdb.open_catalog(
    "<PATH>",
    search_filter=lsdb.ConeSearch(ra=142.0, dec=36.0, radius_arcsec=3600.0),
)
```

Each catalog in this collection is represented as a separate [Apache Parquet dataset](https://arrow.apache.org/docs/python/dataset.html) and can be accessed with a variety of tools, including `pandas`, `pyarrow`, `dask`, `Spark`, `DuckDB`.

### File structure

This catalog is represented by the following files and directories:

- [`collection.properties`](<PATH>/collection.properties) — textual metadata file describing the HATS collection of catalogs
- [`mmu_yse`](mmu_yse) — main HATS catalog directory
  - [`dataset/`](mmu_yse/dataset/) — Apache Parquet dataset directory for the main catalog
    - ... parquet metadata and data files in sub directories ...
  - [`hats.properties`](mmu_yse/hats.properties) — textual metadata file describing the main HATS catalog
  - [`partition_info.csv`](mmu_yse/partition_info.csv) — CSV file with a list of catalog HEALPix tiles (catalog partitions)
  - [`skymap.fits`](mmu_yse/skymap.fits) — HEALPix skymap FITS file with row-counts per HEALPix tile of fixed order 10
- [`mmu_yse_10arcs/`](mmu_yse_10arcs) — default margin catalog to ensure data completeness in cross-matching, the margin threshold is 10.0 arcseconds
  - ... margin catalog files and directories ...

### Catalog metadata

Metadata of the main HATS catalog, excluding margins and indexes:

| **Number of rows** | **Number of columns** | **Number of partitions** | **Size on disk** | **HATS Builder** |
| --- | --- | --- | --- | --- |
| 2,003 | 7 | 312 | 3.6 MiB | hats-import v0.9.0, hats v0.9.0 |


### Catalog columns

The main HATS catalog contains the following columns:

| **Name** |  **`_healpix_29`** | **`lightcurve.band`** | **`lightcurve.time`** | **`lightcurve.flux`** | **`lightcurve.flux_err`** | **`redshift`** | **`host_log_mass`** | **`ra`** | **`dec`** | **`obj_type`** | **`object_id`** |
| --- |  --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **Data Type** |  int64 | list[string] | list[float] | list[float] | list[float] | float | float | double | double | string | string |
| **Nested?** |  — | lightcurve | lightcurve | lightcurve | lightcurve | — | — | — | — | — | — |
| **Value count** |  2,003 | 269,628 | 269,628 | 269,628 | 269,628 | 2,003 | 2,003 | 2,003 | 2,003 | 2,003 | 2,003 |
| **Example row** |  399200651177352460 | [X, X, X, X, X, X, X, X, X, X, X, … (168 total)] | [5.887e+04, 5.887e+04, 5.887e+04, … (168 total)] | [328.3, 435.8, 580.4, 681.8, 773, … (168 total)] | [76.91, 74.19, 56.67, 59.37, 67.84, … (168 total)] | 0.14 | -99 | 141.9 | 35.53 | SN-Ia-norm | 2020azn |
| **Minimum value** |  1126237237925017 | X | -99.0 | -0.0 | -0.0 | -99.0 | -99.0 | 0.3768570125102997 | -29.100194931030273 | LBV | 2019lbi |
| **Maximum value** |  3458687311145453838 | z | 59839.5703125 | 1408506.75 | 97039.4609375 | 0.5249999761581421 | -99.0 | 359.86798095703125 | 74.1773452758789 | TDE | 2021zzv |


"Nested" indicates whether the column is stored as a nested field inside another "struct" column.


"Value count" may be different from the total number of rows for nested columns: each nested element is counted as a single value.



