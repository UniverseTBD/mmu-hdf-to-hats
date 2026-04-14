
---
configs:
- config_name: default
  data_dir: mmu_chandra_spectra/dataset
tags:
- astronomy
license: cc-by-4.0
pretty_name: mmu_chandra_spectra
size_categories:
- 100K<n<1M
---

# mmu_chandra_spectra HATS Catalog Collection

This is the collection of HATS catalogs representing mmu_chandra_spectra.

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
catalog = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_chandra_spectra")
# One-degree cone.
catalog = lsdb.open_catalog(
    "https://huggingface.co/datasets/UniverseTBD/mmu_chandra_spectra",
    search_filter=lsdb.ConeSearch(ra=134.0, dec=14.0, radius_arcsec=3600.0),
)
```

Each catalog in this collection is represented as a separate [Apache Parquet dataset](https://arrow.apache.org/docs/python/dataset.html) and can be accessed with a variety of tools, including `pandas`, `pyarrow`, `dask`, `Spark`, `DuckDB`.

### File structure

This catalog is represented by the following files and directories:

- [`collection.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_chandra_spectra/collection.properties) � textual metadata file describing the HATS collection of catalogs
- [`mmu_chandra_spectra`](https://huggingface.co/datasets/UniverseTBD/mmu_chandra_spectra/mmu_chandra_spectra) � main HATS catalog directory
  - [`dataset/`](https://huggingface.co/datasets/UniverseTBD/mmu_chandra_spectra/mmu_chandra_spectra/dataset/) � Apache Parquet dataset directory for the main catalog
    - ... parquet metadata and data files in sub directories ...
  - [`hats.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_chandra_spectra/mmu_chandra_spectra/hats.properties) � textual metadata file describing the main HATS catalog
  - [`partition_info.csv`](https://huggingface.co/datasets/UniverseTBD/mmu_chandra_spectra/mmu_chandra_spectra/partition_info.csv) � CSV file with a list of catalog HEALPix tiles (catalog partitions)
  - [`skymap.fits`](https://huggingface.co/datasets/UniverseTBD/mmu_chandra_spectra/mmu_chandra_spectra/skymap.fits) � HEALPix skymap FITS file with row-counts per HEALPix tile of fixed order 10
- [`mmu_chandra_spectra_10arcs/`](https://huggingface.co/datasets/UniverseTBD/mmu_chandra_spectra/mmu_chandra_spectra_10arcs) � default margin catalog to ensure data completeness in cross-matching, the margin threshold is 10.0 arcseconds
  - ... margin catalog files and directories ...

### Catalog metadata

Metadata of the main HATS catalog, excluding margins and indexes:

| **Number of rows** | **Number of columns** | **Number of partitions** | **Size on disk** | **HATS Builder** |
| --- | --- | --- | --- | --- |
| 128,900 | 13 | 2,257 | 276.5 MiB | hats-import v0.7.3, hats v0.7.3 |


### Catalog columns

The main HATS catalog contains the following columns:

| **Name** |  **`_healpix_29`** | **`spectrum.ene_center_bin`** | **`spectrum.ene_high_bin`** | **`spectrum.ene_low_bin`** | **`spectrum.flux`** | **`spectrum.flux_error`** | **`flux_aper_b`** | **`flux_bb_aper_b`** | **`flux_significance_b`** | **`hard_hm`** | **`hard_hs`** | **`hard_ms`** | **`var_index_b`** | **`var_prob_b`** | **`name`** | **`object_id`** | **`ra`** | **`dec`** |
| --- |  --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **Data Type** |  int64 | list[float] | list[float] | list[float] | list[float] | list[float] | float | float | float | float | float | float | float | float | string | string | double | double |
| **Nested?** |  � | spectrum | spectrum | spectrum | spectrum | spectrum | � | � | � | � | � | � | � | � | � | � | � | � |
| **Value count** |  128,900 | 4,364,568 | 4,364,568 | 4,364,568 | 4,364,568 | 4,364,568 | 128,900 | 128,900 | 128,900 | 128,900 | 128,900 | 128,900 | 128,900 | 128,900 | 128,900 | 128,900 | 128,900 | 128,900 |
| **Example row** |  304334048313525994 | [0.657, 0.9198, 1.102, 1.241, 1.372, � (17 total)] | [0.803, 1.037, 1.168, 1.314, 1.431, � (17 total)] | [0.511, 0.803, 1.037, 1.168, 1.314, � (17 total)] | [0.001425, 0.002141, 0.003167, � (17 total)] | [0.0009772, 0.001293, 0.002171, � (17 total)] | 7.578e-14 | 7.276e-14 | 7.918 | 0.2598 | 0.1337 | -0.1337 | 0 | 0.1263 | 2CXO J085446.9+140843 | 47877 | 133.7 | 14.15 |
| **Minimum value** |  55043007421035 | 0.5182999968528748 | 0.525600016117096 | 0.5109999775886536 | -0.1565576046705246 | 1.8854531163015054e-06 | -0.0 | 1.7774323594850266e-15 | 4.047618865966797 | -0.9993754029273987 | -0.9993754029273987 | -0.9993754029273987 | -0.0 | -0.0 | 2CXO J000002.4+004444 | 0 | 0.01018931969673531 | -86.63160308051602 |
| **Maximum value** |  3456929663501242754 | 7.978899955749512 | 7.986199855804443 | 7.97160005569458 | 8.14805793762207 | 0.8432331681251526 | 5.414789727709035e-10 | 2.89389456842315e-10 | 415.9320983886719 | 0.9993754029273987 | 0.9993754029273987 | 0.9993754029273987 | 10.0 | 1.0 | 2CXO J235953.4-093655 | 99999 | 359.9726737313183 | 89.37200563200145 |


"Nested" indicates whether the column is stored as a nested field inside another "struct" column.


"Value count" may be different from the total number of rows for nested columns: each nested element is counted as a single value.




### Crossmatch with another catalog

HATS catalogs can be efficiently crossmatched using [LSDB](https://lsdb.io),
which leverages the HEALPix partitioning to avoid loading the full datasets into memory:

```python
import lsdb

mmu_chandra_spectra = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_chandra_spectra")
other = lsdb.open_catalog("https://huggingface.co/datasets/<org>/<other_catalog>")

crossmatched = mmu_chandra_spectra.crossmatch(other, radius_arcsec=1.0)
print(crossmatched)
```

See the [LSDB documentation](https://docs.lsdb.io/) for more details on crossmatching and other operations.

### Dataset-specific context

**Original survey**  
This dataset is based on the Chandra X-ray Observatory, which studies X-ray emission from extreme environments in the Universe, such as black holes, supernova remnants, and active stars.

**Data modality**  
The dataset consists of X-ray spectra (photon energy vs count rate) in the energy range 0.5–8 keV, derived from individual photon event data recorded by the telescope. Each sample includes energy bins, count rates, associated errors, and source identifiers and coordinates. In addition, the dataset provides derived features that characterize the sources, such as flux measurements, signal-to-noise ratio, spectral hardness ratios, and variability indicators over time.

**Typical use cases**  
The dataset can be used to study X-ray emission from astrophysical sources and to analyze their physical properties. It has also been used in machine learning applications such as classification of X-ray sources.

**Caveats**  
The dataset includes only sources with at least 40 total counts and a signal-to-noise ratio of 4 or higher. The spectra are derived from photon event data through the application of the instrument’s spectral response and processing pipelines.

**Citation**  
This dataset uses data from the Chandra Source Catalog (CSC, version 2.1), which is publicly available. Users should follow the official guidelines from the Chandra X-ray Center (CXC) when acknowledging and citing the data.
