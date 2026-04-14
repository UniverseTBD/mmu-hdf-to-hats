
---
configs:
- config_name: default
  data_dir: mmu_jwst_ceers/dataset
tags:
- astronomy
license: cc-by-4.0
pretty_name: mmu_jwst_ceers
size_categories:
- 10K<n<100K
---

# mmu_jwst_ceers HATS Catalog Collection

This is the collection of HATS catalogs representing mmu_jwst_ceers.

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
catalog = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_jwst_ceers")
# One-degree cone.
catalog = lsdb.open_catalog(
    "https://huggingface.co/datasets/UniverseTBD/mmu_jwst_ceers",
    search_filter=lsdb.ConeSearch(ra=215.0, dec=53.0, radius_arcsec=3600.0),
)
```

Each catalog in this collection is represented as a separate [Apache Parquet dataset](https://arrow.apache.org/docs/python/dataset.html) and can be accessed with a variety of tools, including `pandas`, `pyarrow`, `dask`, `Spark`, `DuckDB`.

### File structure

This catalog is represented by the following files and directories:

- [`collection.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_jwst_ceers/collection.properties) — textual metadata file describing the HATS collection of catalogs
- [`mmu_jwst_ceers`](https://huggingface.co/datasets/UniverseTBD/mmu_jwst_ceers/mmu_jwst_ceers) — main HATS catalog directory
  - [`dataset/`](https://huggingface.co/datasets/UniverseTBD/mmu_jwst_ceers/mmu_jwst_ceers/dataset/) — Apache Parquet dataset directory for the main catalog
    - ... parquet metadata and data files in sub directories ...
  - [`hats.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_jwst_ceers/mmu_jwst_ceers/hats.properties) — textual metadata file describing the main HATS catalog
  - [`partition_info.csv`](https://huggingface.co/datasets/UniverseTBD/mmu_jwst_ceers/mmu_jwst_ceers/partition_info.csv) — CSV file with a list of catalog HEALPix tiles (catalog partitions)
  - [`skymap.fits`](https://huggingface.co/datasets/UniverseTBD/mmu_jwst_ceers/mmu_jwst_ceers/skymap.fits) — HEALPix skymap FITS file with row-counts per HEALPix tile of fixed order 10
- [`mmu_jwst_ceers_10arcs/`](https://huggingface.co/datasets/UniverseTBD/mmu_jwst_ceers/mmu_jwst_ceers_10arcs) — default margin catalog to ensure data completeness in cross-matching, the margin threshold is 10.0 arcseconds
  - ... margin catalog files and directories ...

### Catalog metadata

Metadata of the main HATS catalog, excluding margins and indexes:

| **Number of rows** | **Number of columns** | **Number of partitions** | **Size on disk** | **HATS Builder** |
| --- | --- | --- | --- | --- |
| 24,518 | 11 | 9 | 8.2 GiB | hats-import v0.7.3, hats v0.7.3 |


### Catalog columns

The main HATS catalog contains the following columns:

| **Name** |  **`_healpix_29`** | **`image.band`** | **`image.flux`** | **`image.ivar`** | **`image.mask`** | **`image.psf_fwhm`** | **`image.scale`** | **`mag_auto`** | **`flux_radius`** | **`flux_auto`** | **`fluxerr_auto`** | **`cxx_image`** | **`cyy_image`** | **`cxy_image`** | **`object_id`** | **`ra`** | **`dec`** |
| --- |  --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **Data Type** |  int64 | list[string] | list[list<element: list<element: float>>] | list[list<element: list<element: float>>] | list[list<element: list<element: bool>>] | list[float] | list[float] | float | float | float | float | float | float | float | string | double | double |
| **Nested?** |  — | image | image | image | image | image | image | — | — | — | — | — | — | — | — | — | — |
| **Value count** |  24,518 | 171,626 | *N/A* | *N/A* | *N/A* | 171,626 | 171,626 | 24,518 | 24,518 | 24,518 | 24,518 | 24,518 | 24,518 | 24,518 | 24,518 | 24,518 | 24,518 |
| **Example row** |  804002297459820835 | [f090w, f115w, f150w, f200w, f277w, … (7 total)] | [[[0.01465, 0.03999, 0.0095, … (96 total)], … (96 total)], … (7 total… | [[[1606, 1537, 1521, 1697, 1743, 1692, … (96 total)], … (96 total)], … | [[[True, True, True, True, True, True, … (96 total)], … (96 total)], … | [0.033, 0.04, 0.05, 0.066, 0.092, … (7 total)] | [0.04, 0.04, 0.04, 0.04, 0.04, 0.04, … (7 total)] | 21.62 | 2.014 | 6.875 | 0.0014 | 0.0496 | 0.06944 | -0.001268 | 1757963689505803332 | 214.7 | 52.75 |
| **Minimum value** |  804001887516808636 | f090w | *N/A* | *N/A* | *N/A* | 0.032999999821186066 | 0.03999999910593033 | 17.203704833984375 | 1.6584279537200928 | 0.047454044222831726 | 0.0005687509546987712 | 1.5739618902443908e-05 | 3.137264502584003e-05 | -2.257167339324951 | 1757963689505748225 | 214.69148780378492 | 52.72084420518769 |
| **Maximum value** |  804019014425927361 | f444w | *N/A* | *N/A* | *N/A* | 0.14499999582767487 | 0.03999999910593033 | 26.99991798400879 | 336.2449645996094 | 476.9996032714844 | 18.44452667236328 | 1.9025871753692627 | 1.8590304851531982 | 0.965598464012146 | 1757963689505829844 | 215.17615155042228 | 53.02129352482981 |


"Nested" indicates whether the column is stored as a nested field inside another "struct" column.


"Value count" may be different from the total number of rows for nested columns: each nested element is counted as a single value.




### Crossmatch with another catalog

HATS catalogs can be efficiently crossmatched using [LSDB](https://lsdb.io),
which leverages the HEALPix partitioning to avoid loading the full datasets into memory:

```python
import lsdb

mmu_jwst_ceers = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_jwst_ceers")
other = lsdb.open_catalog("https://huggingface.co/datasets/<org>/<other_catalog>")

crossmatched = mmu_jwst_ceers.crossmatch(other, radius_arcsec=1.0)
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
