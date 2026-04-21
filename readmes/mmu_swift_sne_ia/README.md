---
configs:
- config_name: default
  data_dir: mmu_swift_sne_ia/dataset
tags:
- astronomy
license: cc-by-4.0
pretty_name: mmu_swift_sne_ia
size_categories:
- n<1K
---

<div align="center">
<img src="example_figure.png" width="600">
</div>

# mmu_swift_sne_ia HATS Catalog Collection

This is the collection of HATS catalogs representing mmu_swift_sne_ia.

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
catalog = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_swift_sne_ia")
# One-degree cone.
catalog = lsdb.open_catalog(
    "https://huggingface.co/datasets/UniverseTBD/mmu_swift_sne_ia",
    search_filter=lsdb.ConeSearch(ra=139.0, dec=30.0, radius_arcsec=3600.0),
)
```

Each catalog in this collection is represented as a separate [Apache Parquet dataset](https://arrow.apache.org/docs/python/dataset.html) and can be accessed with a variety of tools, including `pandas`, `pyarrow`, `dask`, `Spark`, `DuckDB`.

### File structure

This catalog is represented by the following files and directories:

- [`collection.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_swift_sne_ia/collection.properties) � textual metadata file describing the HATS collection of catalogs
- [`mmu_swift_sne_ia`](https://huggingface.co/datasets/UniverseTBD/mmu_swift_sne_ia/mmu_swift_sne_ia) � main HATS catalog directory
  - [`dataset/`](https://huggingface.co/datasets/UniverseTBD/mmu_swift_sne_ia/mmu_swift_sne_ia/dataset/) � Apache Parquet dataset directory for the main catalog
    - ... parquet metadata and data files in sub directories ...
  - [`hats.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_swift_sne_ia/mmu_swift_sne_ia/hats.properties) � textual metadata file describing the main HATS catalog
  - [`partition_info.csv`](https://huggingface.co/datasets/UniverseTBD/mmu_swift_sne_ia/mmu_swift_sne_ia/partition_info.csv) � CSV file with a list of catalog HEALPix tiles (catalog partitions)
  - [`skymap.fits`](https://huggingface.co/datasets/UniverseTBD/mmu_swift_sne_ia/mmu_swift_sne_ia/skymap.fits) � HEALPix skymap FITS file with row-counts per HEALPix tile of fixed order 10
- [`mmu_swift_sne_ia_10arcs/`](https://huggingface.co/datasets/UniverseTBD/mmu_swift_sne_ia/mmu_swift_sne_ia_10arcs) � default margin catalog to ensure data completeness in cross-matching, the margin threshold is 10.0 arcseconds
  - ... margin catalog files and directories ...

### Catalog metadata

Metadata of the main HATS catalog, excluding margins and indexes:

| **Number of rows** | **Number of columns** | **Number of partitions** | **Size on disk** | **HATS Builder** |
| --- | --- | --- | --- | --- |
| 234 | 7 | 109 | 192.7 MiB | hats-import v0.7.3, hats v0.7.3 |


### Catalog columns

The main HATS catalog contains the following columns:

| **Name** |  **`_healpix_29`** | **`lightcurve.band`** | **`lightcurve.time`** | **`lightcurve.flux`** | **`lightcurve.flux_err`** | **`redshift`** | **`host_log_mass`** | **`ra`** | **`dec`** | **`obj_type`** | **`object_id`** |
| --- |  --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **Data Type** |  int64 | list[string] | list[float] | list[float] | list[float] | float | float | double | double | string | string |
| **Nested?** |  � | lightcurve | lightcurve | lightcurve | lightcurve | � | � | � | � | � | � |
| **Value count** |  234 | 22,596 | 22,596 | 22,596 | 22,596 | 234 | 234 | 234 | 234 | 234 | 234 |
| **Example row** |  349459259295965761 | [B, B, B, B, B, B, B, B, B, B, B, B, � (78 total)] | [5.494e+04, 5.494e+04, 5.494e+04, � (78 total)] | [3.32e+04, 3.855e+04, 4.04e+04, � (78 total)] | [1988, 2130, 2270, 2509, 2587, 2459, � (78 total)] | 0.02203 | 9.726 | 138.8 | 29.74 | Ia | 2009cz |
| **Minimum value** |  43005875302682221 | B | -99.0 | -0.0 | -0.0 | 0.0011059000389650464 | -0.0 | 6.962006092071533 | -69.50639343261719 | Ia | 2005am |
| **Maximum value** |  3442626109715665663 | X | 59412.43359375 | 5727960.0 | 147718.0 | 0.060903098434209824 | 9.922599792480469 | 359.02728271484375 | 78.60652923583984 | Ia | iPTF14bdn |


"Nested" indicates whether the column is stored as a nested field inside another "struct" column.


"Value count" may be different from the total number of rows for nested columns: each nested element is counted as a single value.




### Crossmatch with another catalog

HATS catalogs can be efficiently crossmatched using [LSDB](https://lsdb.io),
which leverages the HEALPix partitioning to avoid loading the full datasets into memory:

```python
import lsdb

mmu_swift_sne_ia = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_swift_sne_ia")
other = lsdb.open_catalog("https://huggingface.co/datasets/<org>/<other_catalog>")

crossmatched = mmu_swift_sne_ia.crossmatch(other, radius_arcsec=1.0)
print(crossmatched)
```

See the [LSDB documentation](https://docs.lsdb.io/) for more details on crossmatching and other operations.

### Dataset-specific context

**Original survey**  
This dataset is based on the Swift Type Ia Supernova (Swift SN Ia) dataset and contains a collection of 117 spectroscopically confirmed Type Ia supernova light curves.

**Data modality**  
The dataset consists of light curve data including time of observation, flux, flux error, and band pass filter information (UVw1, u, b, UVw2, v, UVm2). Additional metadata includes supernova classification, coordinates, redshift, and host galaxy mass.

**Typical use cases**  
The dataset can be used for photometric redshift prediction and light curve inpainting.

**Caveats**  
The data is collected from the Pantheon+ compilation, which applies a series of selection cuts.

**Citation**  
The dataset is released under the GNU Lesser General Public License. Users should attribute the original data source appropriately.
