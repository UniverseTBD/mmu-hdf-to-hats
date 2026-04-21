
---
configs:
- config_name: default
  data_dir: mmu_cfa_cfa4/dataset
tags:
- astronomy
license: cc-by-4.0
pretty_name: mmu_cfa_cfa4
size_categories:
- n<1K
---

# mmu_cfa_cfa4 HATS Catalog Collection

This is the collection of HATS catalogs representing mmu_cfa_cfa4.

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
catalog = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_cfa_cfa4")
# One-degree cone.
catalog = lsdb.open_catalog(
    "https://huggingface.co/datasets/UniverseTBD/mmu_cfa_cfa4",
    search_filter=lsdb.ConeSearch(ra=44.0, dec=47.0, radius_arcsec=3600.0),
)
```

Each catalog in this collection is represented as a separate [Apache Parquet dataset](https://arrow.apache.org/docs/python/dataset.html) and can be accessed with a variety of tools, including `pandas`, `pyarrow`, `dask`, `Spark`, `DuckDB`.

### File structure

This catalog is represented by the following files and directories:

- [`collection.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_cfa_cfa4/collection.properties) � textual metadata file describing the HATS collection of catalogs
- [`mmu_cfa_cfa4`](https://huggingface.co/datasets/UniverseTBD/mmu_cfa_cfa4/mmu_cfa_cfa4) � main HATS catalog directory
  - [`dataset/`](https://huggingface.co/datasets/UniverseTBD/mmu_cfa_cfa4/mmu_cfa_cfa4/dataset/) � Apache Parquet dataset directory for the main catalog
    - ... parquet metadata and data files in sub directories ...
  - [`hats.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_cfa_cfa4/mmu_cfa_cfa4/hats.properties) � textual metadata file describing the main HATS catalog
  - [`partition_info.csv`](https://huggingface.co/datasets/UniverseTBD/mmu_cfa_cfa4/mmu_cfa_cfa4/partition_info.csv) � CSV file with a list of catalog HEALPix tiles (catalog partitions)
  - [`skymap.fits`](https://huggingface.co/datasets/UniverseTBD/mmu_cfa_cfa4/mmu_cfa_cfa4/skymap.fits) � HEALPix skymap FITS file with row-counts per HEALPix tile of fixed order 10
- [`mmu_cfa_cfa4_10arcs/`](https://huggingface.co/datasets/UniverseTBD/mmu_cfa_cfa4/mmu_cfa_cfa4_10arcs) � default margin catalog to ensure data completeness in cross-matching, the margin threshold is 10.0 arcseconds
  - ... margin catalog files and directories ...

### Catalog metadata

Metadata of the main HATS catalog, excluding margins and indexes:

| **Number of rows** | **Number of columns** | **Number of partitions** | **Size on disk** | **HATS Builder** |
| --- | --- | --- | --- | --- |
| 92 | 5 | 87 | 192.5 MiB | hats-import v0.7.3, hats v0.7.3 |


### Catalog columns

The main HATS catalog contains the following columns:

| **Name** |  **`_healpix_29`** | **`lightcurve.band`** | **`lightcurve.time`** | **`lightcurve.mag`** | **`lightcurve.mag_err`** | **`ra`** | **`dec`** | **`obj_type`** | **`object_id`** |
| --- |  --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **Data Type** |  int64 | list[string] | list[float] | list[float] | list[float] | double | double | string | string |
| **Nested?** |  � | lightcurve | lightcurve | lightcurve | lightcurve | � | � | � | � |
| **Value count** |  92 | 8,640 | 8,640 | 8,640 | 8,640 | 92 | 92 | 92 | 92 |
| **Example row** |  217215479864952347 | [B, B, B, B, B, B, B, B, B, B, B, B, � (90 total)] | [5.475e+04, 5.475e+04, 5.475e+04, � (90 total)] | [17.83, 17.78, 17.82, 17.89, 18.52, � (90 total)] | [0.0288, 0.0324, 0.0336, 0.058, � (90 total)] | 44.49 | 46.87 | SN Ia | SN2008gb |
| **Minimum value** |  39270986388764360 | B | -0.0 | -0.0 | -0.0 | -0.0 | -62.31468963623047 | ? | SN2006ct |
| **Maximum value** |  3427434955430110561 | u' | 55631.4375 | 21.64620018005371 | 0.4964999854564667 | 359.65594482421875 | 71.4156723022461 | SN Ia-pec | SNsnf20080522011 |


"Nested" indicates whether the column is stored as a nested field inside another "struct" column.


"Value count" may be different from the total number of rows for nested columns: each nested element is counted as a single value.




### Crossmatch with another catalog

HATS catalogs can be efficiently crossmatched using [LSDB](https://lsdb.io),
which leverages the HEALPix partitioning to avoid loading the full datasets into memory:

```python
import lsdb

mmu_cfa_cfa4 = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_cfa_cfa4")
other = lsdb.open_catalog("https://huggingface.co/datasets/<org>/<other_catalog>")

crossmatched = mmu_cfa_cfa4.crossmatch(other, radius_arcsec=1.0)
print(crossmatched)
```

See the [LSDB documentation](https://docs.lsdb.io/) for more details on crossmatching and other operations.

### Dataset-specific context

**Original survey**  
This dataset is based on the Harvard-Smithsonian Center for Astrophysics (CfA) Supernova Group observations and contains light curves of Type Ia supernovae observed between 2000 and 2011.

**Data modality**  
The dataset consists of light curve data for 94 Type Ia supernovae in the u’UBVr’i’ bands. Each sample includes sky coordinates (ra, dec), object identifiers and classification, and a light curve with time (modified Julian date), band, magnitude (mag), and magnitude error (mag_err).

**Typical use cases**  
Type Ia supernovae from this dataset are used in cosmological studies and are included in large supernova compilations such as Pantheon and related datasets.

**Caveats**  
The dataset follows the CfA data structure. The flux and flux_err fields are replaced by mag and mag_err fields, which present the original photometric measurements in the standard photometric system. It represents an additional sample of Type Ia supernovae with a slightly different band configuration.

**Citation**  
Data are obtained from the [CfA Supernova Archive](https://lweb.cfa.harvard.edu/supernova/). Users should cite the corresponding CfA4 publication and follow the archive acknowledgement guidelines.
