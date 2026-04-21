---
configs:
- config_name: default
  data_dir: mmu_csp_csp/dataset
tags:
- astronomy
license: cc-by-4.0
pretty_name: mmu_csp_csp
size_categories:
- n<1K
---

<div align="center">
<img src="example_figure.png" width="600">
</div>

# mmu_csp_csp HATS Catalog Collection

This is the collection of HATS catalogs representing mmu_csp_csp.

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
catalog = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_csp_csp")
# One-degree cone.
catalog = lsdb.open_catalog(
    "https://huggingface.co/datasets/UniverseTBD/mmu_csp_csp",
    search_filter=lsdb.ConeSearch(ra=139.0, dec=30.0, radius_arcsec=3600.0),
)
```

Each catalog in this collection is represented as a separate [Apache Parquet dataset](https://arrow.apache.org/docs/python/dataset.html) and can be accessed with a variety of tools, including `pandas`, `pyarrow`, `dask`, `Spark`, `DuckDB`.

### File structure

This catalog is represented by the following files and directories:

- [`collection.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_csp_csp/collection.properties) � textual metadata file describing the HATS collection of catalogs
- [`mmu_csp_csp`](https://huggingface.co/datasets/UniverseTBD/mmu_csp_csp/mmu_csp_csp) � main HATS catalog directory
  - [`dataset/`](https://huggingface.co/datasets/UniverseTBD/mmu_csp_csp/mmu_csp_csp/dataset/) � Apache Parquet dataset directory for the main catalog
    - ... parquet metadata and data files in sub directories ...
  - [`hats.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_csp_csp/mmu_csp_csp/hats.properties) � textual metadata file describing the main HATS catalog
  - [`partition_info.csv`](https://huggingface.co/datasets/UniverseTBD/mmu_csp_csp/mmu_csp_csp/partition_info.csv) � CSV file with a list of catalog HEALPix tiles (catalog partitions)
  - [`skymap.fits`](https://huggingface.co/datasets/UniverseTBD/mmu_csp_csp/mmu_csp_csp/skymap.fits) � HEALPix skymap FITS file with row-counts per HEALPix tile of fixed order 10
- [`mmu_csp_csp_10arcs/`](https://huggingface.co/datasets/UniverseTBD/mmu_csp_csp/mmu_csp_csp_10arcs) � default margin catalog to ensure data completeness in cross-matching, the margin threshold is 10.0 arcseconds
  - ... margin catalog files and directories ...

### Catalog metadata

Metadata of the main HATS catalog, excluding margins and indexes:

| **Number of rows** | **Number of columns** | **Number of partitions** | **Size on disk** | **HATS Builder** |
| --- | --- | --- | --- | --- |
| 134 | 6 | 122 | 192.8 MiB | hats-import v0.7.3, hats v0.7.3 |


### Catalog columns

The main HATS catalog contains the following columns:

| **Name** |  **`_healpix_29`** | **`lightcurve.band`** | **`lightcurve.time`** | **`lightcurve.mag`** | **`lightcurve.mag_err`** | **`redshift`** | **`ra`** | **`dec`** | **`spec_class`** | **`object_id`** |
| --- |  --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **Data Type** |  int64 | list[string] | list[float] | list[float] | list[float] | float | double | double | string | string |
| **Nested?** |  � | lightcurve | lightcurve | lightcurve | lightcurve | � | � | � | � | � |
| **Value count** |  134 | 36,120 | 36,120 | 36,120 | 36,120 | 134 | 134 | 134 | 134 | 134 |
| **Example row** |  349459259295784838 | [B, B, B, B, B, B, B, B, B, B, B, � (144 total)] | [1938, 1939, 1943, 1946, 1947, � (144 total)] | [16.01, 15.93, 15.78, 15.85, 15.89, � (144 total)] | [0.007, 0.007, 0.007, 0.007, 0.007, � (144 total)] | 0.0211 | 138.8 | 29.74 | SN Ia | SN2009cz |
| **Minimum value** |  70274246734234 | B | -0.0 | -0.0 | -0.0 | 0.003700000001117587 | 1.0079580545425415 | -80.17755889892578 | SN Ia | SN2004dt |
| **Maximum value** |  3410168382924176502 | u | 2305.530029296875 | 22.347000122070312 | 0.20000000298023224 | 0.08349999785423279 | 359.6354064941406 | 29.735305786132812 | SN Ia | SN2010ae |


"Nested" indicates whether the column is stored as a nested field inside another "struct" column.


"Value count" may be different from the total number of rows for nested columns: each nested element is counted as a single value.




### Crossmatch with another catalog

HATS catalogs can be efficiently crossmatched using [LSDB](https://lsdb.io),
which leverages the HEALPix partitioning to avoid loading the full datasets into memory:

```python
import lsdb

mmu_csp_csp = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_csp_csp")
other = lsdb.open_catalog("https://huggingface.co/datasets/<org>/<other_catalog>")

crossmatched = mmu_csp_csp.crossmatch(other, radius_arcsec=1.0)
print(crossmatched)
```

See the [LSDB documentation](https://docs.lsdb.io/) for more details on crossmatching and other operations.

### Dataset-specific context

**Original survey**  
This dataset is based on the third data release of the first stage of the Carnegie Supernova Project (CSP) and contains light curves of 134 spectroscopically confirmed Type Ia supernovae observed between 2004 and 2009.

**Data modality**  
The dataset consists of light curve data. The data fields are identical to those of the CfA datasets, with measurements provided in the ugriBVYJH bands.

**Typical use cases**  
The dataset has been used in many studies, including those mentioned in the CfA section, as well as several CSP studies.

**Caveats**  
The dataset uses different photometric bands (ugriBVYJH) while keeping the same data fields as the CfA datasets. No preprocessing has been applied.

**Citation**  
The dataset is released under the CC BY 4.0 license. Users should cite the corresponding data release paper and may access the data through the [CSP data products page](https://csp.obs.carnegiescience.edu/data).
