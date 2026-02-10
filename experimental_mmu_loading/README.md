### Crossmatching Basic Implementation

#### Usage

```bash
uv venv
source .venv/bin/activate
uv pip install -r experimental_mmu_loading/requirements.in
python experimental_mmu_loading/demo_mmu_builder.py
```

#### What this implementation does?
This is a basic crossmatching implementation that matches two datasets from huggingface against each other without downloading any unnecessary data (though we will process unnecessary data quite a bit in this implementation, see below). Prerequisites are, that the dataset contains the columns `ra`, `dec` and `object_id` and that the datasets are partitioned by healpix.
The idea is rather simple: The datasets are stored in parquet, which is columnar oriented, so it is fairly easy and efficient to extract the columns `ra`, `dec`, `object_id`, `healpix` and `file_name`. This is enough to do crossmatching. Then we load only the files in which we found matches, and we do this leveraging huggingface's filesystem and read capabilites directly from the server, which allows us to stream data via http. Since we don't need to download a complete file in which we found matches (since 99.9% of the data within isn't interesting for us) we use parquet's `filter` functionality. This AFAIK isn't pushed down to the server but rather filtered on the user's end.

#### Where it fails?
As mentioned, we still load unnecessary data, and we do this quite a bit. In a test of mine, where I processed `hsc` and `sdss` on `healpix=1174` (which is quite large, >100GB on hsc), we still needed to go through 100 of 107 files for hsc. So this saves us some 6.xxx %, which is way less than I hoped.


#### How it can be improved?
Maybe some clever paritioning could help us, but I would assume that matches are evenly distributed across the data (as my test indicates) and therefore only true random access across rows and columns will solve this performantly. One thing would be to try hdf5 or zarr formats. For this I propose the following order of testing:
 - check if there is a possibility to leverage the format on huggingface like we do with parquet:
    ```python
    
        table = pq.read_table(
            HfFileSystem(),
            filters=pc.field("object_id").isin(pa.array(object_ids))
        )
    ```
    So that we can stream data via HTTP.
 - Create a coordinates table with the columns: `['ra', 'dec', 'object_id', 'healpix', 'file_name', 'catalog', 'index']` (index would be great to have in here for random access) and use this for crossmatching, load only the needed rows with the procedure found in the row above
 - try scale to this approach to a bunch of catalogs and healpixels
