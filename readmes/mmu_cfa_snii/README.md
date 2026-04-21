
<div align="center">
<img src="example_figure.png" width="600">
</div>

---
configs:
- config_name: default
  data_dir: mmu_cfa_snii/dataset
tags:
- astronomy
license: cc-by-4.0
pretty_name: mmu_cfa_snii
size_categories:
- n<1K
---

# mmu_cfa_snii HATS Catalog Collection

This is the collection of HATS catalogs representing mmu_cfa_snii.

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
catalog = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_cfa_snii")
# One-degree cone.
catalog = lsdb.open_catalog(
    "https://huggingface.co/datasets/UniverseTBD/mmu_cfa_snii",
    search_filter=lsdb.ConeSearch(ra=41.0, dec=37.0, radius_arcsec=3600.0),
)
```

Each catalog in this collection is represented as a separate [Apache Parquet dataset](https://arrow.apache.org/docs/python/dataset.html) and can be accessed with a variety of tools, including `pandas`, `pyarrow`, `dask`, `Spark`, `DuckDB`.

### File structure

This catalog is represented by the following files and directories:

- [`collection.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_cfa_snii/collection.properties) � textual metadata file describing the HATS collection of catalogs
- [`mmu_cfa_snii`](https://huggingface.co/datasets/UniverseTBD/mmu_cfa_snii/mmu_cfa_snii) � main HATS catalog directory
  - [`dataset/`](https://huggingface.co/datasets/UniverseTBD/mmu_cfa_snii/mmu_cfa_snii/dataset/) � Apache Parquet dataset directory for the main catalog
    - ... parquet metadata and data files in sub directories ...
  - [`hats.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_cfa_snii/mmu_cfa_snii/hats.properties) � textual metadata file describing the main HATS catalog
  - [`partition_info.csv`](https://huggingface.co/datasets/UniverseTBD/mmu_cfa_snii/mmu_cfa_snii/partition_info.csv) � CSV file with a list of catalog HEALPix tiles (catalog partitions)
  - [`skymap.fits`](https://huggingface.co/datasets/UniverseTBD/mmu_cfa_snii/mmu_cfa_snii/skymap.fits) � HEALPix skymap FITS file with row-counts per HEALPix tile of fixed order 10
- [`mmu_cfa_snii_10arcs/`](https://huggingface.co/datasets/UniverseTBD/mmu_cfa_snii/mmu_cfa_snii_10arcs) � default margin catalog to ensure data completeness in cross-matching, the margin threshold is 10.0 arcseconds
  - ... margin catalog files and directories ...

### Catalog metadata

Metadata of the main HATS catalog, excluding margins and indexes:

| **Number of rows** | **Number of columns** | **Number of partitions** | **Size on disk** | **HATS Builder** |
| --- | --- | --- | --- | --- |
| 64 | 5 | 62 | 192.4 MiB | hats-import v0.7.3, hats v0.7.3 |


### Catalog columns

The main HATS catalog contains the following columns:

| **Name** |  **`_healpix_29`** | **`lightcurve.band`** | **`lightcurve.time`** | **`lightcurve.mag`** | **`lightcurve.mag_err`** | **`ra`** | **`dec`** | **`obj_type`** | **`object_id`** |
| --- |  --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **Data Type** |  int64 | list[string] | list[float] | list[float] | list[float] | double | double | string | string |
| **Nested?** |  � | lightcurve | lightcurve | lightcurve | lightcurve | � | � | � | � |
| **Value count** |  64 | 16,291 | 16,291 | 16,291 | 16,291 | 64 | 64 | 64 | 64 |
| **Example row** |  166916028924337739 | [B, B, B, B, B, B, B, B, B, B, B, � (748 total)] | [5.435e+04, 5.435e+04, 5.435e+04, � (748 total)] | [14.11, 14.23, 14.35, 14.73, 14.85, � (748 total)] | [0.014, 0.014, 0.014, 0.014, 0.014, � (748 total)] | 40.87 | 37.35 | SN Ib/c | SN2007gr |
| **Minimum value** |  27677490352178670 | B | -0.0 | -0.0 | -0.0 | 0.3315800130367279 | -25.708139419555664 | SN II-pec | SN2001ej |
| **Maximum value** |  2870298439381265425 | u' | 55244.17578125 | 22.079999923706055 | 0.7300000190734863 | 350.2640686035156 | 74.57247161865234 | SN Ic-pec | SN2009jf |


"Nested" indicates whether the column is stored as a nested field inside another "struct" column.


"Value count" may be different from the total number of rows for nested columns: each nested element is counted as a single value.




### Crossmatch with another catalog

HATS catalogs can be efficiently crossmatched using [LSDB](https://lsdb.io),
which leverages the HEALPix partitioning to avoid loading the full datasets into memory:

```python
import lsdb

mmu_cfa_snii = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_cfa_snii")
other = lsdb.open_catalog("https://huggingface.co/datasets/<org>/<other_catalog>")

crossmatched = mmu_cfa_snii.crossmatch(other, radius_arcsec=1.0)
print(crossmatched)
```

See the [LSDB documentation](https://docs.lsdb.io/) for more details on crossmatching and other operations.

### Dataset-specific context

**Original survey**  
This dataset is based on the Harvard-Smithsonian Center for Astrophysics (CfA) Supernova Group observations and contains light curves of Type II supernovae observed between 2000 and 2011.

**Data modality**  
The dataset consists of light curve data for 60 Type II supernovae in the UBVRIu’r’i’JHKs bands. Each sample includes sky coordinates (ra, dec), object identifiers and classification, and a light curve with time (modified Julian date), band, magnitude (mag), and magnitude error (mag_err).

**Typical use cases**  
The dataset is used in studies of Type II supernovae and their light curve properties.

**Caveats**  
The dataset follows the CfA data structure. The flux and flux_err fields are replaced by mag and mag_err fields, which present the original photometric measurements in the standard photometric system. It represents a different supernova class compared to Type Ia datasets.

**Citation**  
Data are obtained from the [CfA Supernova Archive](https://lweb.cfa.harvard.edu/supernova/). Users should cite the corresponding CfA-SNII publication and follow the archive acknowledgement guidelines.
