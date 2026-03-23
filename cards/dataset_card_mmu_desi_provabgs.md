

# mmu_desi_provabgs HATS Catalog Collection

This is the collection of HATS catalogs representing mmu_desi_provabgs.

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
    search_filter=lsdb.ConeSearch(ra=245.0, dec=43.0, radius_arcsec=3600.0),
)
```

Each catalog in this collection is represented as a separate [Apache Parquet dataset](https://arrow.apache.org/docs/python/dataset.html) and can be accessed with a variety of tools, including `pandas`, `pyarrow`, `dask`, `Spark`, `DuckDB`.

### File structure

This catalog is represented by the following files and directories:

- [`collection.properties`](<PATH>/collection.properties) — textual metadata file describing the HATS collection of catalogs
- [`mmu_desi_provabgs`](mmu_desi_provabgs) — main HATS catalog directory
  - [`dataset/`](mmu_desi_provabgs/dataset/) — Apache Parquet dataset directory for the main catalog
    - ... parquet metadata and data files in sub directories ...
  - [`hats.properties`](mmu_desi_provabgs/hats.properties) — textual metadata file describing the main HATS catalog
  - [`partition_info.csv`](mmu_desi_provabgs/partition_info.csv) — CSV file with a list of catalog HEALPix tiles (catalog partitions)
  - [`skymap.fits`](mmu_desi_provabgs/skymap.fits) — HEALPix skymap FITS file with row-counts per HEALPix tile of fixed order 10
- [`mmu_desi_provabgs_10arcs/`](mmu_desi_provabgs_10arcs) — default margin catalog to ensure data completeness in cross-matching, the margin threshold is 10.0 arcseconds
  - ... margin catalog files and directories ...

### Catalog metadata

Metadata of the main HATS catalog, excluding margins and indexes:

| **Number of rows** | **Number of columns** | **Number of partitions** | **Size on disk** | **HATS Builder** |
| --- | --- | --- | --- | --- |
| 238,516 | 22 | 78 | 1.2 GiB | hats-import v0.9.0, hats v0.9.0 |


### Catalog columns

The main HATS catalog contains the following columns:

| **Name** |  **`_healpix_29`** | **`ra`** | **`dec`** | **`PROVABGS_MCMC`** | **`PROVABGS_THETA_BF`** | **`LOG_MSTAR`** | **`Z_HP`** | **`Z_MW`** | **`TAGE_MW`** | **`AVG_SFR`** | **`ZERR`** | **`TSNR2_BGS`** | **`MAG_G`** | **`MAG_R`** | **`MAG_Z`** | **`MAG_W1`** | **`FIBMAG_R`** | **`HPIX_64`** | **`PROVABGS_Z_MAX`** | **`SCHLEGEL_COLOR`** | **`PROVABGS_W_ZFAIL`** | **`PROVABGS_W_FIBASSIGN`** | **`object_id`** |
| --- |  --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **Data Type** |  int64 | double | double | list<element: list<element: float>> | list<element: float> | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | string |
| **Value count** |  238,516 | 238,516 | 238,516 | *N/A* | 3,100,708 | 238,516 | 238,516 | 238,516 | 238,516 | 238,516 | 238,516 | 238,516 | 238,516 | 238,516 | 238,516 | 238,516 | 238,516 | 238,516 | 238,516 | 238,516 | 238,516 | 238,516 | 238,516 |
| **Example row** |  691192699993178773 | 245 | 43.46 | [[11.36, 0.5826, 0.2381, 0.1031, … (13 total)], … (100 total)] | [11.35, 0.902, 0.0334, 0.02257, … (13 total)] | 11.11 | 0.3145 | 0.005167 | 9.374 | 1.161 | 7.852e-05 | 2639 | 20.51 | 19.14 | 18.36 | 17.95 | 20.52 | 9822 | 0.3531 | -0.03429 | 1.015 | 1 | 39633140951548685 |
| **Minimum value** |  643521053811880247 | 148.40325927734375 | -2.3291468620300293 | *N/A* | -2.0 | 6.238491058349609 | 1.4423111679207068e-05 | 4.4905984395882115e-05 | 0.014555543661117554 | 8.957725782920284e-14 | 2.9781909915982396e-07 | 223.89047241210938 | 12.553780555725098 | 12.053372383117676 | 11.390754699707031 | 11.869658470153809 | 14.953400611877441 | 9144.0 | 0.0007990878657437861 | -23.969953536987305 | 1.0 | 1.0 | 39627733927462296 |
| **Maximum value** |  1981011982237869960 | 273.93377685546875 | 67.75138854980469 | *N/A* | 13.269999504089355 | 12.770240783691406 | 0.5997362732887268 | 0.04490434378385544 | 12.506500244140625 | 2095.118896484375 | 0.0006934804259799421 | 205831.9375 | 22.785625457763672 | 20.299989700317383 | 21.06035804748535 | 40.0 | 22.896602630615234 | 28151.0 | 0.6000000238418579 | 6.442105293273926 | 3.547720432281494 | 129.0 | 39633470523181151 |



"Value count" may be different from the total number of rows for nested columns: each nested element is counted as a single value.



