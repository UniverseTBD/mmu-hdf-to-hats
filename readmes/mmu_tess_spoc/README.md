
---
configs:
- config_name: default
  data_dir: mmu_tess_spoc/dataset
tags:
- astronomy
license: cc-by-4.0
pretty_name: mmu_tess_spoc
size_categories:
- 1M<n<10M
---

# mmu_tess_spoc HATS Catalog Collection

This is the collection of HATS catalogs representing mmu_tess_spoc.

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
catalog = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_tess_spoc")
# One-degree cone.
catalog = lsdb.open_catalog(
    "https://huggingface.co/datasets/UniverseTBD/mmu_tess_spoc",
    search_filter=lsdb.ConeSearch(ra=20.0, dec=61.0, radius_arcsec=3600.0),
)
```

Each catalog in this collection is represented as a separate [Apache Parquet dataset](https://arrow.apache.org/docs/python/dataset.html) and can be accessed with a variety of tools, including `pandas`, `pyarrow`, `dask`, `Spark`, `DuckDB`.

### File structure

This catalog is represented by the following files and directories:

- [`collection.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_tess_spoc/collection.properties) — textual metadata file describing the HATS collection of catalogs
- [`mmu_tess_spoc`](https://huggingface.co/datasets/UniverseTBD/mmu_tess_spoc/mmu_tess_spoc) — main HATS catalog directory
  - [`dataset/`](https://huggingface.co/datasets/UniverseTBD/mmu_tess_spoc/mmu_tess_spoc/dataset/) — Apache Parquet dataset directory for the main catalog
    - ... parquet metadata and data files in sub directories ...
  - [`hats.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_tess_spoc/mmu_tess_spoc/hats.properties) — textual metadata file describing the main HATS catalog
  - [`partition_info.csv`](https://huggingface.co/datasets/UniverseTBD/mmu_tess_spoc/mmu_tess_spoc/partition_info.csv) — CSV file with a list of catalog HEALPix tiles (catalog partitions)
  - [`skymap.fits`](https://huggingface.co/datasets/UniverseTBD/mmu_tess_spoc/mmu_tess_spoc/skymap.fits) — HEALPix skymap FITS file with row-counts per HEALPix tile of fixed order 10
- [`mmu_tess_spoc_10arcs/`](https://huggingface.co/datasets/UniverseTBD/mmu_tess_spoc/mmu_tess_spoc_10arcs) — default margin catalog to ensure data completeness in cross-matching, the margin threshold is 10.0 arcseconds
  - ... margin catalog files and directories ...

### Catalog metadata

Metadata of the main HATS catalog, excluding margins and indexes:

| **Number of rows** | **Number of columns** | **Number of partitions** | **Size on disk** | **HATS Builder** |
| --- | --- | --- | --- | --- |
| 1,122,883 | 4 | 1,084 | 64.4 GiB | hats-import v0.7.3, hats v0.7.3 |


### Catalog columns

The main HATS catalog contains the following columns:

| **Name** |  **`_healpix_29`** | **`lightcurve.time`** | **`lightcurve.flux`** | **`lightcurve.flux_err`** | **`ra`** | **`dec`** | **`object_id`** |
| --- |  --- | --- | --- | --- | --- | --- | --- |
| **Data Type** |  int64 | list[float] | list[float] | list[float] | double | double | string |
| **Nested?** |  — | lightcurve | lightcurve | lightcurve | — | — | — |
| **Value count** |  1,122,883 | 12,180,623,431 | 12,180,623,431 | 12,180,623,431 | 1,122,883 | 1,122,883 | 1,122,883 |
| **Example row** |  255238671391160662 | [2882, 2882, 2882, 2882, 2882, … (11673 total)] | [1.448e+04, 1.447e+04, 1.448e+04, … (11673 total)] | [13.36, 13.35, 13.35, 13.35, … (11673 total)] | 19.52 | 60.78 | 54374808 |
| **Minimum value** |  34898257120247167 | -0.0 | -544389.875 | -0.0 | 0.0002006297289342 | -89.8896741927853 | 10000003212 |
| **Maximum value** |  3423932021944551628 | 3208.14306640625 | 60974292.0 | 682.621337890625 | 359.999893323771 | 89.8323304826145 | 9993478 |


"Nested" indicates whether the column is stored as a nested field inside another "struct" column.


"Value count" may be different from the total number of rows for nested columns: each nested element is counted as a single value.




### Crossmatch with another catalog

HATS catalogs can be efficiently crossmatched using [LSDB](https://lsdb.io),
which leverages the HEALPix partitioning to avoid loading the full datasets into memory:

```python
import lsdb

mmu_tess_spoc = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_tess_spoc")
other = lsdb.open_catalog("https://huggingface.co/datasets/<org>/<other_catalog>")

crossmatched = mmu_tess_spoc.crossmatch(other, radius_arcsec=1.0)
print(crossmatched)
```

See the [LSDB documentation](https://docs.lsdb.io/) for more details on crossmatching and other operations.

### Dataset-specific context

**Original survey**  
This dataset is based on the NASA Transiting Exoplanet Survey Satellite (TESS), an all-sky photometric survey observing millions of sources to discover exoplanets and study variable stars.

**Data modality**  
The dataset consists of light curve data (brightness over time) including object_id, time (BTJD; Barycenter corrected TESS Julian Date), flux (electrons per second) and flux_error (electrons per second). It contains light curves for approximately 1,120,000 stars observed in selected sectors. 


Stars are typically observed for about 27 days per sector and may be observed multiple times across different sectors. Observations have a cadence between 20 seconds and 30 minutes.

**Typical use cases**  
TESS light curves have been used in machine learning applications such as light curve classification (e.g. transiting exoplanets, pulsating stars, eclipsing binaries) and stellar parameter estimation.

**Caveats**  
The dataset includes data from selected observing sectors. Light curves are processed using the PDC_SAP flux, with additional cleaning applied through quality flag filtering to remove potentially anomalous data points.

**Citation**  
The data are publicly available through the Mikulski Archive for Space Telescopes (MAST) and are released under the CC BY 4.0 license.
