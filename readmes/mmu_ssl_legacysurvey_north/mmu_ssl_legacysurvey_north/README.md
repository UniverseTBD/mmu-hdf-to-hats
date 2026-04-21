
---
configs:
- config_name: default
  data_dir: mmu_ssl_legacysurvey_north/dataset
tags:
- astronomy
license: cc-by-4.0
pretty_name: mmu_ssl_legacysurvey_north
size_categories:
- 10M<n<100M
---

# mmu_ssl_legacysurvey_north HATS Catalog Collection

This is the collection of HATS catalogs representing mmu_ssl_legacysurvey_north.

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
catalog = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_ssl_legacysurvey_north")
# One-degree cone.
catalog = lsdb.open_catalog(
    "https://huggingface.co/datasets/UniverseTBD/mmu_ssl_legacysurvey_north",
    search_filter=lsdb.ConeSearch(ra=150.0, dec=40.0, radius_arcsec=3600.0),
)
```

Each catalog in this collection is represented as a separate [Apache Parquet dataset](https://arrow.apache.org/docs/python/dataset.html) and can be accessed with a variety of tools, including `pandas`, `pyarrow`, `dask`, `Spark`, `DuckDB`.

### File structure

This catalog is represented by the following files and directories:

- [`collection.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_ssl_legacysurvey_north/collection.properties) � textual metadata file describing the HATS collection of catalogs
- [`mmu_ssl_legacysurvey_north`](https://huggingface.co/datasets/UniverseTBD/mmu_ssl_legacysurvey_north/mmu_ssl_legacysurvey_north) � main HATS catalog directory
  - [`dataset/`](https://huggingface.co/datasets/UniverseTBD/mmu_ssl_legacysurvey_north/mmu_ssl_legacysurvey_north/dataset/) � Apache Parquet dataset directory for the main catalog
    - ... parquet metadata and data files in sub directories ...
  - [`hats.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_ssl_legacysurvey_north/mmu_ssl_legacysurvey_north/hats.properties) � textual metadata file describing the main HATS catalog
  - [`partition_info.csv`](https://huggingface.co/datasets/UniverseTBD/mmu_ssl_legacysurvey_north/mmu_ssl_legacysurvey_north/partition_info.csv) � CSV file with a list of catalog HEALPix tiles (catalog partitions)
  - [`skymap.fits`](https://huggingface.co/datasets/UniverseTBD/mmu_ssl_legacysurvey_north/mmu_ssl_legacysurvey_north/skymap.fits) � HEALPix skymap FITS file with row-counts per HEALPix tile of fixed order 10
- [`mmu_ssl_legacysurvey_north_10arcs/`](https://huggingface.co/datasets/UniverseTBD/mmu_ssl_legacysurvey_north/mmu_ssl_legacysurvey_north_10arcs) � default margin catalog to ensure data completeness in cross-matching, the margin threshold is 10.0 arcseconds
  - ... margin catalog files and directories ...

### Catalog metadata

Metadata of the main HATS catalog, excluding margins and indexes:

| **Number of rows** | **Number of columns** | **Number of partitions** | **Size on disk** | **HATS Builder** |
| --- | --- | --- | --- | --- |
| 14,174,203 | 15 | 5,488 | 3.4 TiB | hats-import v0.7.3, hats v0.7.3 |


### Catalog columns

The main HATS catalog contains the following columns:

| **Name** |  **`_healpix_29`** | **`image.band`** | **`image.flux`** | **`image.psf_fwhm`** | **`image.scale`** | **`ebv`** | **`flux_g`** | **`flux_r`** | **`flux_z`** | **`fiberflux_g`** | **`fiberflux_r`** | **`fiberflux_z`** | **`psfdepth_g`** | **`psfdepth_r`** | **`psfdepth_z`** | **`z_spec`** | **`ra`** | **`dec`** | **`object_id`** |
| --- |  --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **Data Type** |  int64 | list[string] | list[list<element: list<element: float>>] | list[float] | list[float] | float | float | float | float | float | float | float | float | float | float | float | double | double | string |
| **Nested?** |  � | image | image | image | image | � | � | � | � | � | � | � | � | � | � | � | � | � | � |
| **Value count** |  14,174,203 | 42,522,609 | *N/A* | 42,522,609 | 42,522,609 | 14,174,203 | 14,174,203 | 14,174,203 | 14,174,203 | 14,174,203 | 14,174,203 | 14,174,203 | 14,174,203 | 14,174,203 | 14,174,203 | 14,174,203 | 14,174,203 | 14,174,203 | 14,174,203 |
| **Example row** |  401650157984434242 | [des-g, des-r, des-z] | [[[-0.001399, 0.0001793, 0.001422, � (152 total)], � (152 total)], � � | [2.012, 2.21, 1.323] | [0.262, 0.262, 0.262] | 0.01309 | 0.9361 | 4.839 | 14.18 | 0.4654 | 2.406 | 7.05 | 443.5 | 128.6 | 98.56 | 0.5121 | 150.3 | 39.65 | 12092771 |
| **Minimum value** |  132680257016861193 | des-g | *N/A* | 0.6329100728034973 | 0.2619999945163727 | 0.0024021391291171312 | -2269.253662109375 | -6554.19677734375 | 10.0 | -0.0 | -0.0 | 0.011576710268855095 | -0.0 | -0.0 | -0.0 | -99.0 | 35.705657958984375 | -1.5527414083480835 | 0 |
| **Maximum value** |  2305843006203526511 | des-z | *N/A* | 3.8228883743286133 | 0.2619999945163727 | 0.455355167388916 | 312806.15625 | 541284.5 | 433497.625 | 2976.06005859375 | 247616.359375 | 3174.939208984375 | 3746.2158203125 | 1059.060791015625 | 747.9191284179688 | 1.2718158960342407 | 356.4070129394531 | 84.76776123046875 | 9999999 |


"Nested" indicates whether the column is stored as a nested field inside another "struct" column.


"Value count" may be different from the total number of rows for nested columns: each nested element is counted as a single value.




### Crossmatch with another catalog

HATS catalogs can be efficiently crossmatched using [LSDB](https://lsdb.io),
which leverages the HEALPix partitioning to avoid loading the full datasets into memory:

```python
import lsdb

mmu_ssl_legacysurvey_north = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_ssl_legacysurvey_north")
other = lsdb.open_catalog("https://huggingface.co/datasets/<org>/<other_catalog>")

crossmatched = mmu_ssl_legacysurvey_north.crossmatch(other, radius_arcsec=1.0)
print(crossmatched)
```

See the [LSDB documentation](https://docs.lsdb.io/) for more details on crossmatching and other operations.

### Dataset-specific context

**Original survey**  
This dataset is based on the northern sky component of the Legacy Surveys Data Release 9. It includes (g, r, z) images from the Beijing-Arizona Sky Survey (BASS) and the Mayall z-band Legacy Survey (MzLS), processed through the Legacy Surveys pipeline.

**Data modality**  
The dataset consists of 15 million galaxy image cutouts (152 × 152 pixels) in three optical bands (g, r, z) at a pixel scale of 0.262 arcsec. Each image is associated with measurements from the Legacy Survey catalog.

**Typical use cases**  
This dataset was used to build a self-supervised representation learning model, to identify strong gravitational lensing, and to develop an image-spectrum contrastive learning model.

**Caveats**  
The dataset includes galaxies selected based on specific criteria (extended objects, magnitude cuts, and quality flags). No further processing is applied beyond the original dataset.

**Citation**  
Users should cite the data compilation paper and include the official acknowledgment provided by the [Legacy Surveys](https://www.legacysurvey.org/acknowledgment/#toc-entry-2)
