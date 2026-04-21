
<div align="center">
<img src="example_figure.png" width="600">
</div>

---
configs:
- config_name: default
  data_dir: mmu_jwst_gdn/dataset
tags:
- astronomy
license: cc-by-4.0
pretty_name: mmu_jwst_gdn
size_categories:
- 10K<n<100K
---

# mmu_jwst_gdn HATS Catalog Collection

This is the collection of HATS catalogs representing mmu_jwst_gdn.

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
catalog = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_jwst_gdn")
# One-degree cone.
catalog = lsdb.open_catalog(
    "https://huggingface.co/datasets/UniverseTBD/mmu_jwst_gdn",
    search_filter=lsdb.ConeSearch(ra=189.0, dec=62.0, radius_arcsec=3600.0),
)
```

Each catalog in this collection is represented as a separate [Apache Parquet dataset](https://arrow.apache.org/docs/python/dataset.html) and can be accessed with a variety of tools, including `pandas`, `pyarrow`, `dask`, `Spark`, `DuckDB`.

### File structure

This catalog is represented by the following files and directories:

- [`collection.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_jwst_gdn/collection.properties) — textual metadata file describing the HATS collection of catalogs
- [`mmu_jwst_gdn`](https://huggingface.co/datasets/UniverseTBD/mmu_jwst_gdn/mmu_jwst_gdn) — main HATS catalog directory
  - [`dataset/`](https://huggingface.co/datasets/UniverseTBD/mmu_jwst_gdn/mmu_jwst_gdn/dataset/) — Apache Parquet dataset directory for the main catalog
    - ... parquet metadata and data files in sub directories ...
  - [`hats.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_jwst_gdn/mmu_jwst_gdn/hats.properties) — textual metadata file describing the main HATS catalog
  - [`partition_info.csv`](https://huggingface.co/datasets/UniverseTBD/mmu_jwst_gdn/mmu_jwst_gdn/partition_info.csv) — CSV file with a list of catalog HEALPix tiles (catalog partitions)
  - [`skymap.fits`](https://huggingface.co/datasets/UniverseTBD/mmu_jwst_gdn/mmu_jwst_gdn/skymap.fits) — HEALPix skymap FITS file with row-counts per HEALPix tile of fixed order 10
- [`mmu_jwst_gdn_10arcs/`](https://huggingface.co/datasets/UniverseTBD/mmu_jwst_gdn/mmu_jwst_gdn_10arcs) — default margin catalog to ensure data completeness in cross-matching, the margin threshold is 10.0 arcseconds
  - ... margin catalog files and directories ...

### Catalog metadata

Metadata of the main HATS catalog, excluding margins and indexes:

| **Number of rows** | **Number of columns** | **Number of partitions** | **Size on disk** | **HATS Builder** |
| --- | --- | --- | --- | --- |
| 29,435 | 11 | 11 | 9.2 GiB | hats-import v0.7.3, hats v0.7.3 |


### Catalog columns

The main HATS catalog contains the following columns:

| **Name** |  **`_healpix_29`** | **`image.band`** | **`image.flux`** | **`image.ivar`** | **`image.mask`** | **`image.psf_fwhm`** | **`image.scale`** | **`mag_auto`** | **`flux_radius`** | **`flux_auto`** | **`fluxerr_auto`** | **`cxx_image`** | **`cyy_image`** | **`cxy_image`** | **`object_id`** | **`ra`** | **`dec`** |
| --- |  --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **Data Type** |  int64 | list[string] | list[list<element: list<element: float>>] | list[list<element: list<element: float>>] | list[list<element: list<element: bool>>] | list[float] | list[float] | float | float | float | float | float | float | float | string | double | double |
| **Nested?** |  — | image | image | image | image | image | image | — | — | — | — | — | — | — | — | — | — |
| **Value count** |  29,435 | 206,045 | *N/A* | *N/A* | *N/A* | 206,045 | 206,045 | 29,435 | 29,435 | 29,435 | 29,435 | 29,435 | 29,435 | 29,435 | 29,435 | 29,435 | 29,435 |
| **Example row** |  791790208707182804 | [f090w, f115w, f150w, f200w, f277w, … (7 total)] | [[[0.0003051, 0.01758, 0.007788, … (96 total)], … (96 total)], … (7 t… | [[[6395, 5902, 5432, 5561, 5916, 6041, … (96 total)], … (96 total)], … | [[[True, True, True, True, True, True, … (96 total)], … (96 total)], … | [0.033, 0.04, 0.05, 0.066, 0.092, … (7 total)] | [0.02, 0.02, 0.02, 0.02, 0.04, 0.04, … (7 total)] | 27.21 | 3.845 | 0.03933 | 0.000739 | 0.1824 | 0.1795 | 0.06391 | 7251073904309204425 | 189.2 | 62.13 |
| **Minimum value** |  791790207886246552 | f090w | *N/A* | *N/A* | *N/A* | 0.032999999821186066 | 0.019999999552965164 | 17.300647735595703 | 0.5765308141708374 | 0.02993936277925968 | 0.0005087985191494226 | 2.9649861971847713e-05 | 3.4777178370859474e-05 | -2.554218053817749 | 7251073904309204381 | 188.96725600580982 | 62.12745097303882 |
| **Maximum value** |  791799302667255993 | f444w | *N/A* | *N/A* | *N/A* | 0.14499999582767487 | 0.03999999910593033 | 27.4999942779541 | 403.9623107910156 | 359.7336730957031 | 2.076092481613159 | 2.7494616508483887 | 2.032952070236206 | 2.0402920246124268 | 7251073904309274726 | 189.40077013564573 | 62.33559761881421 |


"Nested" indicates whether the column is stored as a nested field inside another "struct" column.


"Value count" may be different from the total number of rows for nested columns: each nested element is counted as a single value.




### Crossmatch with another catalog

HATS catalogs can be efficiently crossmatched using [LSDB](https://lsdb.io),
which leverages the HEALPix partitioning to avoid loading the full datasets into memory:

```python
import lsdb

mmu_jwst_gdn = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_jwst_gdn")
other = lsdb.open_catalog("https://huggingface.co/datasets/<org>/<other_catalog>")

crossmatched = mmu_jwst_gdn.crossmatch(other, radius_arcsec=1.0)
print(crossmatched)
```

See the [LSDB documentation](https://docs.lsdb.io/) for more details on crossmatching and other operations.

### Dataset-specific context

**Original survey**  
This dataset is based on the James Webb Space Telescope (JWST) NIRCam observations from early deep field surveys.

**Data modality**  
The dataset consists of fixed-size image cutouts (96×96 pixels) centered on sources from photometric catalogs. The images are multi-band, with 6 or 7 filters covering wavelengths from approximately 0.9μm to 4.4μm.

**Typical use cases**  
Images from these JWST deep field surveys have been used in a large number of scientific publications, including machine learning applications.

**Caveats**  
Different surveys have different wavelength coverage, and missing bands are represented as arrays of zeros to simplify data loading.

**Citation**  
The data are in the public domain. The dataset uses data products retrieved from the Dawn JWST Archive (DJA), an initiative of the Cosmic Dawn Center (DAWN).
