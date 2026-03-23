

# mmu_csp HATS Catalog Collection

This is the collection of HATS catalogs representing mmu_csp.

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
    search_filter=lsdb.ConeSearch(ra=139.0, dec=30.0, radius_arcsec=3600.0),
)
```

Each catalog in this collection is represented as a separate [Apache Parquet dataset](https://arrow.apache.org/docs/python/dataset.html) and can be accessed with a variety of tools, including `pandas`, `pyarrow`, `dask`, `Spark`, `DuckDB`.

### File structure

This catalog is represented by the following files and directories:

- [`collection.properties`](<PATH>/collection.properties) — textual metadata file describing the HATS collection of catalogs
- [`mmu_csp`](mmu_csp) — main HATS catalog directory
  - [`dataset/`](mmu_csp/dataset/) — Apache Parquet dataset directory for the main catalog
    - ... parquet metadata and data files in sub directories ...
  - [`hats.properties`](mmu_csp/hats.properties) — textual metadata file describing the main HATS catalog
  - [`partition_info.csv`](mmu_csp/partition_info.csv) — CSV file with a list of catalog HEALPix tiles (catalog partitions)
  - [`skymap.fits`](mmu_csp/skymap.fits) — HEALPix skymap FITS file with row-counts per HEALPix tile of fixed order 10
- [`mmu_csp_10arcs/`](mmu_csp_10arcs) — default margin catalog to ensure data completeness in cross-matching, the margin threshold is 10.0 arcseconds
  - ... margin catalog files and directories ...

### Catalog metadata

Metadata of the main HATS catalog, excluding margins and indexes:

| **Number of rows** | **Number of columns** | **Number of partitions** | **Size on disk** | **HATS Builder** |
| --- | --- | --- | --- | --- |
| 134 | 6 | 122 | 794.0 KiB | hats-import v0.9.0, hats v0.9.0 |


### Catalog columns

The main HATS catalog contains the following columns:

| **Name** |  **`_healpix_29`** | **`lightcurve.band`** | **`lightcurve.time`** | **`lightcurve.mag`** | **`lightcurve.mag_err`** | **`redshift`** | **`ra`** | **`dec`** | **`spec_class`** | **`object_id`** |
| --- |  --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **Data Type** |  int64 | list[string] | list[float] | list[float] | list[float] | float | double | double | string | string |
| **Nested?** |  — | lightcurve | lightcurve | lightcurve | lightcurve | — | — | — | — | — |
| **Value count** |  134 | 36,120 | 36,120 | 36,120 | 36,120 | 134 | 134 | 134 | 134 | 134 |
| **Example row** |  349459259295784838 | [B, B, B, B, B, B, B, B, B, B, B, … (144 total)] | [1938, 1939, 1943, 1946, 1947, … (144 total)] | [16.01, 15.93, 15.78, 15.85, 15.89, … (144 total)] | [0.007, 0.007, 0.007, 0.007, 0.007, … (144 total)] | 0.0211 | 138.8 | 29.74 | SN Ia | SN2009cz |
| **Minimum value** |  70274246734234 | B | -0.0 | -0.0 | -0.0 | 0.003700000001117587 | 1.0079580545425415 | -80.17755889892578 | SN Ia | SN2004dt |
| **Maximum value** |  3410168382924176502 | u | 2305.530029296875 | 22.347000122070312 | 0.20000000298023224 | 0.08349999785423279 | 359.6354064941406 | 29.735305786132812 | SN Ia | SN2010ae |


"Nested" indicates whether the column is stored as a nested field inside another "struct" column.


"Value count" may be different from the total number of rows for nested columns: each nested element is counted as a single value.



