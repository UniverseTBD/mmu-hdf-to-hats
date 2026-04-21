
---
configs:
- config_name: default
  data_dir: mmu_foundation/dataset
tags:
- astronomy
license: cc-by-4.0
pretty_name: mmu_foundation
size_categories:
- n<1K
---

# mmu_foundation HATS Catalog Collection

This is the collection of HATS catalogs representing mmu_foundation.

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
catalog = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_foundation")
# One-degree cone.
catalog = lsdb.open_catalog(
    "https://huggingface.co/datasets/UniverseTBD/mmu_foundation",
    search_filter=lsdb.ConeSearch(ra=126.0, dec=25.0, radius_arcsec=3600.0),
)
```

Each catalog in this collection is represented as a separate [Apache Parquet dataset](https://arrow.apache.org/docs/python/dataset.html) and can be accessed with a variety of tools, including `pandas`, `pyarrow`, `dask`, `Spark`, `DuckDB`.

### File structure

This catalog is represented by the following files and directories:

- [`collection.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_foundation/collection.properties) � textual metadata file describing the HATS collection of catalogs
- [`mmu_foundation`](https://huggingface.co/datasets/UniverseTBD/mmu_foundation/mmu_foundation) � main HATS catalog directory
  - [`dataset/`](https://huggingface.co/datasets/UniverseTBD/mmu_foundation/mmu_foundation/dataset/) � Apache Parquet dataset directory for the main catalog
    - ... parquet metadata and data files in sub directories ...
  - [`hats.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_foundation/mmu_foundation/hats.properties) � textual metadata file describing the main HATS catalog
  - [`partition_info.csv`](https://huggingface.co/datasets/UniverseTBD/mmu_foundation/mmu_foundation/partition_info.csv) � CSV file with a list of catalog HEALPix tiles (catalog partitions)
  - [`skymap.fits`](https://huggingface.co/datasets/UniverseTBD/mmu_foundation/mmu_foundation/skymap.fits) � HEALPix skymap FITS file with row-counts per HEALPix tile of fixed order 10
- [`mmu_foundation_10arcs/`](https://huggingface.co/datasets/UniverseTBD/mmu_foundation/mmu_foundation_10arcs) � default margin catalog to ensure data completeness in cross-matching, the margin threshold is 10.0 arcseconds
  - ... margin catalog files and directories ...

### Catalog metadata

Metadata of the main HATS catalog, excluding margins and indexes:

| **Number of rows** | **Number of columns** | **Number of partitions** | **Size on disk** | **HATS Builder** |
| --- | --- | --- | --- | --- |
| 360 | 7 | 171 | 193.0 MiB | hats-import v0.7.3, hats v0.7.3 |


### Catalog columns

The main HATS catalog contains the following columns:

| **Name** |  **`_healpix_29`** | **`lightcurve.band`** | **`lightcurve.time`** | **`lightcurve.flux`** | **`lightcurve.flux_err`** | **`redshift`** | **`host_log_mass`** | **`ra`** | **`dec`** | **`obj_type`** | **`object_id`** |
| --- |  --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **Data Type** |  int64 | list[string] | list[float] | list[float] | list[float] | float | float | double | double | string | string |
| **Nested?** |  � | lightcurve | lightcurve | lightcurve | lightcurve | � | � | � | � | � | � |
| **Value count** |  360 | 17,008 | 17,008 | 17,008 | 17,008 | 360 | 360 | 360 | 360 | 360 | 360 |
| **Example row** |  339588456743818884 | [g, g, g, g, g, g, g, g, g, g, g, g, � (56 total)] | [5.778e+04, 5.779e+04, 5.78e+04, � (56 total)] | [1219, 682.2, 547.6, 129.3, 88.42, � (56 total)] | [95.98, 95.54, 193, 90.31, 76.68, 0, � (56 total)] | 0.09284 | 9.137 | 126.1 | 25 | Ia | iPTF17dz |
| **Minimum value** |  3547481393604017 | g | -99.0 | -2181.111083984375 | -0.0 | 0.004886799957603216 | 6.0 | 1.4732304811477661 | -28.45744514465332 | Ia | 2016W |
| **Maximum value** |  3424691708467754140 | z | 57970.5625 | 544487.625 | 7568.98779296875 | 0.10942360013723373 | 11.739999771118164 | 358.35638427734375 | 84.6783218383789 | Ia | iPTF17dz |


"Nested" indicates whether the column is stored as a nested field inside another "struct" column.


"Value count" may be different from the total number of rows for nested columns: each nested element is counted as a single value.




### Crossmatch with another catalog

HATS catalogs can be efficiently crossmatched using [LSDB](https://lsdb.io),
which leverages the HEALPix partitioning to avoid loading the full datasets into memory:

```python
import lsdb

mmu_foundation = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_foundation")
other = lsdb.open_catalog("https://huggingface.co/datasets/<org>/<other_catalog>")

crossmatched = mmu_foundation.crossmatch(other, radius_arcsec=1.0)
print(crossmatched)
```

See the [LSDB documentation](https://docs.lsdb.io/) for more details on crossmatching and other operations.

### Dataset-specific context

**Original survey**  
This dataset is based on the Foundation Data Release 3 (Foundation DR3) and contains a collection of 180 spectroscopically confirmed Type Ia supernova light curves.

**Data modality**  
The dataset consists of light curve data including time of observation, flux, flux error, and band pass filter information (griz bands). Additional metadata includes supernova classification, coordinates, redshift, and host galaxy mass.

**Typical use cases**  
The dataset can be used for photometric redshift prediction and light curve inpainting.

**Caveats**  
The data is collected from the Pantheon+ compilation, which applies a series of selection cuts that define the final sample.

**Citation**  
Users should cite the original Foundation DR3 data and the Pantheon+ compilation. The data is accessed through the [compilation’s GitHub](https://github.com/PantheonPlusSH0ES/DataRelease/tree/main/Pantheon%2B_Data/1_DATA/photometry/Foundation_DJ17)
