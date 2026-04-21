---
configs:
- config_name: default
  data_dir: mmu_jwst_primer_cosmos/dataset
tags:
- astronomy
license: cc-by-4.0
pretty_name: mmu_jwst_primer_cosmos
size_categories:
- 10K<n<100K
---

<div align="center">
<img src="example_figure.png" width="600">
</div>

# mmu_jwst_primer_cosmos HATS Catalog Collection

This is the collection of HATS catalogs representing mmu_jwst_primer_cosmos.

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
catalog = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_jwst_primer_cosmos")
# One-degree cone.
catalog = lsdb.open_catalog(
    "https://huggingface.co/datasets/UniverseTBD/mmu_jwst_primer_cosmos",
    search_filter=lsdb.ConeSearch(ra=150.0, dec=2.0, radius_arcsec=3600.0),
)
```

Each catalog in this collection is represented as a separate [Apache Parquet dataset](https://arrow.apache.org/docs/python/dataset.html) and can be accessed with a variety of tools, including `pandas`, `pyarrow`, `dask`, `Spark`, `DuckDB`.

### File structure

This catalog is represented by the following files and directories:

- [`collection.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_jwst_primer_cosmos/collection.properties) — textual metadata file describing the HATS collection of catalogs
- [`mmu_jwst_primer_cosmos`](https://huggingface.co/datasets/UniverseTBD/mmu_jwst_primer_cosmos/mmu_jwst_primer_cosmos) — main HATS catalog directory
  - [`dataset/`](https://huggingface.co/datasets/UniverseTBD/mmu_jwst_primer_cosmos/mmu_jwst_primer_cosmos/dataset/) — Apache Parquet dataset directory for the main catalog
    - ... parquet metadata and data files in sub directories ...
  - [`hats.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_jwst_primer_cosmos/mmu_jwst_primer_cosmos/hats.properties) — textual metadata file describing the main HATS catalog
  - [`partition_info.csv`](https://huggingface.co/datasets/UniverseTBD/mmu_jwst_primer_cosmos/mmu_jwst_primer_cosmos/partition_info.csv) — CSV file with a list of catalog HEALPix tiles (catalog partitions)
  - [`skymap.fits`](https://huggingface.co/datasets/UniverseTBD/mmu_jwst_primer_cosmos/mmu_jwst_primer_cosmos/skymap.fits) — HEALPix skymap FITS file with row-counts per HEALPix tile of fixed order 10
- [`mmu_jwst_primer_cosmos_10arcs/`](https://huggingface.co/datasets/UniverseTBD/mmu_jwst_primer_cosmos/mmu_jwst_primer_cosmos_10arcs) — default margin catalog to ensure data completeness in cross-matching, the margin threshold is 10.0 arcseconds
  - ... margin catalog files and directories ...

### Catalog metadata

Metadata of the main HATS catalog, excluding margins and indexes:

| **Number of rows** | **Number of columns** | **Number of partitions** | **Size on disk** | **HATS Builder** |
| --- | --- | --- | --- | --- |
| 51,058 | 11 | 15 | 15.1 GiB | hats-import v0.7.3, hats v0.7.3 |


### Catalog columns

The main HATS catalog contains the following columns:

| **Name** |  **`_healpix_29`** | **`image.band`** | **`image.flux`** | **`image.ivar`** | **`image.mask`** | **`image.psf_fwhm`** | **`image.scale`** | **`mag_auto`** | **`flux_radius`** | **`flux_auto`** | **`fluxerr_auto`** | **`cxx_image`** | **`cyy_image`** | **`cxy_image`** | **`object_id`** | **`ra`** | **`dec`** |
| --- |  --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **Data Type** |  int64 | list[string] | list[list<element: list<element: float>>] | list[list<element: list<element: float>>] | list[list<element: list<element: bool>>] | list[float] | list[float] | float | float | float | float | float | float | float | string | double | double |
| **Nested?** |  — | image | image | image | image | image | image | — | — | — | — | — | — | — | — | — | — |
| **Value count** |  51,058 | 357,406 | *N/A* | *N/A* | *N/A* | 357,406 | 357,406 | 51,058 | 51,058 | 51,058 | 51,058 | 51,058 | 51,058 | 51,058 | 51,058 | 51,058 | 51,058 |
| **Example row** |  1918117816734487009 | [f090w, f115w, f150w, f200w, f277w, … (7 total)] | [[[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, … (96 total)], … (96 total)], … | [[[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, … (96 total)], … (96 total)], … | [[[False, False, False, False, False, … (96 total)], … (96 total)], …… | [0.033, 0.04, 0.05, 0.066, 0.092, … (7 total)] | [0.04, 0.04, 0.04, 0.04, 0.04, 0.04, … (7 total)] | 26.48 | 2.555 | 0.07643 | 0.003804 | 0.2659 | 0.3691 | 0.1555 | 4779921206430939062 | 150 | 2.158 |
| **Minimum value** |  1918035224610977248 | f090w | *N/A* | *N/A* | *N/A* | 0.032999999821186066 | 0.03999999910593033 | 15.154260635375977 | 1.093169927597046 | 0.04745382070541382 | 0.0009272033930756152 | 5.68464383832179e-06 | 1.775023883965332e-05 | -1.4645973443984985 | -1793941820439658733 | 150.0225356463053 | 2.142978850173105 |
| **Maximum value** |  1918143149034739361 | f444w | *N/A* | *N/A* | *N/A* | 0.14499999582767487 | 0.03999999910593033 | 26.99997901916504 | 558.0476684570312 | 3149.892578125 | 46.66569519042969 | 2.4375405311584473 | 1.9571294784545898 | 1.3657711744308472 | 4779921206430994078 | 150.22745237191987 | 2.506566292130767 |


"Nested" indicates whether the column is stored as a nested field inside another "struct" column.


"Value count" may be different from the total number of rows for nested columns: each nested element is counted as a single value.




### Crossmatch with another catalog

HATS catalogs can be efficiently crossmatched using [LSDB](https://lsdb.io),
which leverages the HEALPix partitioning to avoid loading the full datasets into memory:

```python
import lsdb

mmu_jwst_primer_cosmos = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_jwst_primer_cosmos")
other = lsdb.open_catalog("https://huggingface.co/datasets/<org>/<other_catalog>")

crossmatched = mmu_jwst_primer_cosmos.crossmatch(other, radius_arcsec=1.0)
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
