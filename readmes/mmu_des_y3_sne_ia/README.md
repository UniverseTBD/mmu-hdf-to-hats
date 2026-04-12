

# mmu_des_y3_sne_ia HATS Catalog Collection

This is the collection of HATS catalogs representing mmu_des_y3_sne_ia.

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
catalog = lsdb.open_catalog("<PATH>")
# One-degree cone.
catalog = lsdb.open_catalog(
    "<PATH>",
    search_filter=lsdb.ConeSearch(ra=35.0, dec=-6.0, radius_arcsec=3600.0),
)
```

Each catalog in this collection is represented as a separate [Apache Parquet dataset](https://arrow.apache.org/docs/python/dataset.html) and can be accessed with a variety of tools, including `pandas`, `pyarrow`, `dask`, `Spark`, `DuckDB`.

### File structure

This catalog is represented by the following files and directories:

- [`collection.properties`](<PATH>/collection.properties) — textual metadata file describing the HATS collection of catalogs
- [`mmu_des_y3_sne_ia`](mmu_des_y3_sne_ia) — main HATS catalog directory
  - [`dataset/`](mmu_des_y3_sne_ia/dataset/) — Apache Parquet dataset directory for the main catalog
    - ... parquet metadata and data files in sub directories ...
  - [`hats.properties`](mmu_des_y3_sne_ia/hats.properties) — textual metadata file describing the main HATS catalog
  - [`partition_info.csv`](mmu_des_y3_sne_ia/partition_info.csv) — CSV file with a list of catalog HEALPix tiles (catalog partitions)
  - [`skymap.fits`](mmu_des_y3_sne_ia/skymap.fits) — HEALPix skymap FITS file with row-counts per HEALPix tile of fixed order 10
- [`mmu_des_y3_sne_ia_10arcs/`](mmu_des_y3_sne_ia_10arcs) — default margin catalog to ensure data completeness in cross-matching, the margin threshold is 10.0 arcseconds
  - ... margin catalog files and directories ...

### Catalog metadata

Metadata of the main HATS catalog, excluding margins and indexes:

| **Number of rows** | **Number of columns** | **Number of partitions** | **Size on disk** | **HATS Builder** |
| --- | --- | --- | --- | --- |
| 248 | 7 | 11 | 304.0 KiB | hats-import v0.9.0, hats v0.9.0 |


### Catalog columns

The main HATS catalog contains the following columns:

| **Name** |  **`_healpix_29`** | **`lightcurve.band`** | **`lightcurve.time`** | **`lightcurve.flux`** | **`lightcurve.flux_err`** | **`redshift`** | **`host_log_mass`** | **`ra`** | **`dec`** | **`obj_type`** | **`object_id`** |
| --- |  --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **Data Type** |  int64 | list[string] | list[float] | list[float] | list[float] | float | float | double | double | string | string |
| **Nested?** |  — | lightcurve | lightcurve | lightcurve | lightcurve | — | — | — | — | — | — |
| **Value count** |  248 | 23,172 | 23,172 | 23,172 | 23,172 | 248 | 248 | 248 | 248 | 248 | 248 |
| **Example row** |  1243942473816715777 | [g, g, g, g, g, g, g, g, g, g, g, … (112 total)] | [5.694e+04, 5.695e+04, 5.696e+04, … (112 total)] | [1.838, 3.156, -1.974, -0.876, … (112 total)] | [4.258, 6.417, 3.601, 4.023, 5.391, … (112 total)] | 0.2883 | 10.38 | 34.83 | -5.789 | Ia | DES_1313594 |
| **Minimum value** |  1243402345506524657 | g | -99.0 | -465.260009765625 | -0.0 | 0.05962910130620003 | 2.0 | 6.669886112213135 | -44.91709899902344 | Ia | DES_1248677 |
| **Maximum value** |  2542315152193303737 | z | 57428.0546875 | 4431.10205078125 | 134.26699829101562 | 0.8485189080238342 | 11.562000274658203 | 55.340694427490234 | 0.9379519820213318 | Ia | DES_1347120 |


"Nested" indicates whether the column is stored as a nested field inside another "struct" column.


"Value count" may be different from the total number of rows for nested columns: each nested element is counted as a single value.



