
---
configs:
- config_name: default
  data_dir: mmu_btsbot/dataset
tags:
- astronomy
license: cc-by-4.0
pretty_name: mmu_btsbot
size_categories:
- 100K<n<1M
---

# mmu_btsbot HATS Catalog Collection

This is the collection of HATS catalogs representing mmu_btsbot.

### Access the catalog

We recommend the use of the [LSDB](https://lsdb.io) Python framework to access HATS catalogs.
LSDB can be installed via `pip install lsdb` or `conda install conda-forge::lsdb`,
see more details [in the docs](https://docs.lsdb.io/).
The following code provides a minimal example of opening this catalog:

```python
import lsdb

# Full sky coverage.
catalog = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_btsbot")
# One-degree cone.
catalog = lsdb.open_catalog(
    "https://huggingface.co/datasets/UniverseTBD/mmu_btsbot",
    search_filter=lsdb.ConeSearch(ra=1.0, dec=78.0, radius_arcsec=3600.0),
)
```

Each catalog in this collection is represented as a separate [Apache Parquet dataset](https://arrow.apache.org/docs/python/dataset.html) and can be accessed with a variety of tools, including `pandas`, `pyarrow`, `dask`, `Spark`, `DuckDB`.

### File structure

This catalog is represented by the following files and directories:

- [`collection.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_btsbot/collection.properties) — textual metadata file describing the HATS collection of catalogs
- [`mmu_btsbot`](https://huggingface.co/datasets/UniverseTBD/mmu_btsbot/mmu_btsbot) — main HATS catalog directory
  - [`dataset/`](https://huggingface.co/datasets/UniverseTBD/mmu_btsbot/mmu_btsbot/dataset/) — Apache Parquet dataset directory for the main catalog
    - ... parquet metadata and data files in sub directories ...
  - [`hats.properties`](https://huggingface.co/datasets/UniverseTBD/mmu_btsbot/mmu_btsbot/hats.properties) — textual metadata file describing the main HATS catalog
  - [`partition_info.csv`](https://huggingface.co/datasets/UniverseTBD/mmu_btsbot/mmu_btsbot/partition_info.csv) — CSV file with a list of catalog HEALPix tiles (catalog partitions)
  - [`skymap.fits`](https://huggingface.co/datasets/UniverseTBD/mmu_btsbot/mmu_btsbot/skymap.fits) — HEALPix skymap FITS file with row-counts per HEALPix tile of fixed order 10
- [`mmu_btsbot_10arcs/`](https://huggingface.co/datasets/UniverseTBD/mmu_btsbot/mmu_btsbot_10arcs) — default margin catalog to ensure data completeness in cross-matching, the margin threshold is 10.0 arcseconds
  - ... margin catalog files and directories ...

### Catalog metadata

Metadata of the main HATS catalog, excluding margins and indexes:

| **Number of rows** | **Number of columns** | **Number of partitions** | **Size on disk** | **HATS Builder** |
| --- | --- | --- | --- | --- |
| 818,214 | 92 | 2,078 | 18.9 GiB | hats-import v0.7.1, hats v0.7.1 |


### Catalog columns

The main HATS catalog contains the following columns:

| **Name** |  **`_healpix_29`** | **`band`** | **`view`** | **`array`** | **`scale`** | **`jd`** | **`diffmaglim`** | **`magpsf`** | **`sigmapsf`** | **`chipsf`** | **`magap`** | **`sigmagap`** | **`distnr`** | **`magnr`** | **`chinr`** | **`sharpnr`** | **`sky`** | **`magdiff`** | **`fwhm`** | **`classtar`** | **`mindtoedge`** | **`seeratio`** | **`magapbig`** | **`sigmagapbig`** | **`sgmag1`** | **`srmag1`** | **`simag1`** | **`szmag1`** | **`sgscore1`** | **`distpsnr1`** | **`jdstarthist`** | **`scorr`** | **`sgmag2`** | **`srmag2`** | **`simag2`** | **`szmag2`** | **`sgscore2`** | **`distpsnr2`** | **`sgmag3`** | **`srmag3`** | **`simag3`** | **`szmag3`** | **`sgscore3`** | **`distpsnr3`** | **`jdstartref`** | **`dsnrms`** | **`ssnrms`** | **`magzpsci`** | **`magzpsciunc`** | **`magzpscirms`** | **`clrcoeff`** | **`clrcounc`** | **`neargaia`** | **`neargaiabright`** | **`maggaia`** | **`maggaiabright`** | **`exptime`** | **`drb`** | **`acai_h`** | **`acai_v`** | **`acai_o`** | **`acai_n`** | **`acai_b`** | **`new_drb`** | **`peakmag`** | **`maxmag`** | **`peakmag_so_far`** | **`maxmag_so_far`** | **`age`** | **`days_since_peak`** | **`days_to_peak`** | **`ra`** | **`dec`** | **`label`** | **`fid`** | **`programid`** | **`field`** | **`nneg`** | **`nbad`** | **`ndethist`** | **`ncovhist`** | **`nmtchps`** | **`nnotdet`** | **`N`** | **`healpix`** | **`isdiffpos`** | **`is_SN`** | **`near_threshold`** | **`is_rise`** | **`OBJECT_ID_`** | **`source_set`** | **`split`** | **`object_id`** |
| --- |  --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **Data Type** |  int64 | list<element: string> | list<element: string> | list<element: list<element: list<element: float>>> | list<element: float> | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | float | double | double | int64 | int64 | int64 | int64 | int64 | int64 | int64 | int64 | int64 | int64 | int64 | int64 | bool | bool | bool | bool | string | string | string | int64 |
| **Value count** |  818,214 | 2,454,642 | 2,454,642 | *N/A* | 2,454,642 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 | 818,214 |
| **Example row** |  270210874492301921 | [r, r, r] | [science, reference, difference] | [[[0.01562, 0.01536, 0.01607, 0.01597, … (63 total)], … (63 total)], … | [1.01, 1.01, 1.01] | 2.46e+06 | 20.07 | 19.62 | 0.1505 | 1.172 | 19.69 | 0.2534 | 0.3195 | 19.37 | 3.737 | 0.527 | 0.09285 | 0.06265 | 2.08 | 0.979 | 468.5 | 1.341 | 19.48 | 0.2668 | 19.36 | 18.51 | 17.92 | 17.7 | 0.04042 | 2.737 | 2.459e+06 | 10.01 | 17.24 | 16.57 | 16.24 | 16.09 | 0.9923 | 12.25 | 20.73 | 20.13 | 19.93 | 19.83 | 0.766 | 13.17 | 2.458e+06 | 9.074 | 12.85 | 26.38 | 3.614e-06 | 0.02667 | 0.08524 | 6.388e-06 | 2.738 | -999 | 19.5 | -999 | 30 | 1 | 0.9969 | 0.0001698 | 8.935e-08 | 0.03556 | 8.846e-05 | 0.9999 | 18.36 | 20.36 | 18.36 | 20.36 | 53.82 | 41.92 | 11.9 | 0.5351 | 78.02 | 1 | 2 | 1 | 854 | 4 | 0 | 46 | 1494 | 8 | 1448 | 9 | 239 | True | True | False | False | ZTF21acdjasw | trues | train | 1776246164415015001 |
| **Minimum value** |  143192435166087 | g | difference | *N/A* | 1.0099999904632568 | 2458208.0 | 14.993193626403809 | 11.828742980957031 | 0.003804703475907445 | 0.061839427798986435 | 11.862600326538086 | 0.0010999999940395355 | 0.0005188093637116253 | 11.85200023651123 | 0.05999999865889549 | -0.7940000295639038 | -148.0614013671875 | -0.6691054701805115 | -0.9399999976158142 | -0.0 | 10.00100040435791 | -999.0 | 11.839099884033203 | 0.0010999999940395355 | -999.0 | -999.0 | -999.0 | -999.0 | -0.0 | 0.00033156777499243617 | 2458042.0 | 5.000020980834961 | -999.0 | -999.0 | -999.0 | -999.0 | -999.0 | -999.0 | -999.0 | -999.0 | -999.0 | -999.0 | -999.0 | -999.0 | 2458154.5 | -236.6349639892578 | -2.804577350616455 | 21.37965202331543 | 7.099999947968172e-07 | 0.012311999686062336 | -1.7358529567718506 | 1.4523000118060736e-06 | -999.0 | -999.0 | -999.0 | -999.0 | 30.0 | -999.0 | -0.0 | -0.0 | -0.0 | -0.0 | 2.2476720598673186e-14 | 6.094935223188713e-09 | 10.285042762756348 | 12.511996269226074 | 10.285042762756348 | 12.511996269226074 | -0.0 | -0.0 | -0.0 | 0.02237 | -30.3120473 | 0 | 1 | 1 | 202 | 0 | 0 | 1 | 1 | 1 | -62 | 1 | 0 | True | False | False | False | ZTF17aaaamwo | dims | test | 453441342515015017 |
| **Maximum value** |  3458697867235106154 | r | science | *N/A* | 1.0099999904632568 | 2460172.75 | 21.433073043823242 | 21.104284286499023 | 0.21714617311954498 | 9912.720703125 | 21.708900451660156 | 1.0729000568389893 | 26.693052291870117 | 23.976999282836914 | 13.935999870300293 | 1.4600000381469727 | 119.668701171875 | 0.9848169088363647 | 7.75 | 1.0 | 1535.1785888671875 | 11.868691444396973 | 21.631799697875977 | 1.669700026512146 | 27.165000915527344 | 27.964000701904297 | 26.252599716186523 | 22.355899810791016 | 1.0 | 29.914194107055664 | 2460166.75 | 536.3513793945312 | 27.81800079345703 | 24.626800537109375 | 23.552799224853516 | 22.370399475097656 | 1.0 | 29.998464584350586 | 27.92300033569336 | 24.05900001525879 | 24.337200164794922 | 23.570499420166016 | 1.0 | 29.999650955200195 | 2459138.75 | 7598.24658203125 | 14282.5029296875 | 27.262657165527344 | 0.6577643752098083 | 0.857014000415802 | 2.0040109157562256 | 2.592698097229004 | 89.99986267089844 | 89.99980926513672 | 21.386119842529297 | 13.999776840209961 | 30.0 | 1.0 | 1.0 | 0.9999991655349731 | 0.9999970197677612 | 0.9995384812355042 | 1.0 | 1.0 | 20.396743774414062 | 22.171342849731445 | 21.009519577026367 | 21.818437576293945 | 2128.044677734375 | 1942.9764404296875 | 2126.070068359375 | 359.9805578 | 87.9603521 | 1 | 2 | 1 | 1877 | 13 | 4 | 4314 | 10393 | 429 | 10308 | 2867 | 3071 | True | True | True | True | ZTF23aawwabr | vars | val | 2418290236115015016 |



"Value count" may be different from the total number of rows for nested columns: each nested element is counted as a single value.




### Crossmatch with another catalog

HATS catalogs can be efficiently crossmatched using [LSDB](https://lsdb.io),
which leverages the HEALPix partitioning to avoid loading the full datasets into memory:

```python
import lsdb

mmu_btsbot = lsdb.open_catalog("https://huggingface.co/datasets/UniverseTBD/mmu_btsbot")
other = lsdb.open_catalog("https://huggingface.co/datasets/<org>/<other_catalog>")

crossmatched = mmu_btsbot.crossmatch(other, radius_arcsec=1.0)
print(crossmatched)
```

See the [LSDB documentation](https://docs.lsdb.io/) for more details on crossmatching and other operations.
