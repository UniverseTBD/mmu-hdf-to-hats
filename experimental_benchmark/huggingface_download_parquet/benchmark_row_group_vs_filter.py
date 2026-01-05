import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub import HfFileSystem
import pyarrow.compute as pc



# Configuration
REPO_ID = "MultimodalUniverse/jwst"
FILE_PATH = "all/train-00000-of-00027.parquet"  # Example file

def find_row_group_and_download(target_ids: list[str]):
    hf_fs = HfFileSystem()
    full_path = f"datasets/{REPO_ID}/{FILE_PATH}"

    # Open parquet file
    parquet_file = pq.ParquetFile(hf_fs.open(full_path, "rb"))

    print(f"File has {parquet_file.num_row_groups} row groups")
    print()

    # Read only object_id column to find which row groups contain our targets
    object_id_table = pq.read_table(
        hf_fs.open(full_path, "rb"),
        columns=['object_id']
    )

    # Find all row indices that match any of our target IDs
    matches = pc.is_in(object_id_table['object_id'], pa.array(target_ids))
    matching_indices = pc.indices_nonzero(matches).to_pylist()

    if not matching_indices:
        print(f"✗ None of the target object_ids found in file")
        return None

    # Determine which row groups contain matches
    target_row_groups = set()
    cumulative_rows = 0

    for i in range(parquet_file.num_row_groups):
        row_group = parquet_file.metadata.row_group(i)
        rows_in_group = row_group.num_rows

        # Check if any matching index falls in this row group
        for idx in matching_indices:
            if cumulative_rows <= idx < cumulative_rows + rows_in_group:
                target_row_groups.add(i)

        cumulative_rows += rows_in_group

    print(f"Found matches in {len(target_row_groups)} row group(s): {sorted(target_row_groups)}")

    # Read only the necessary row groups and combine them
    tables = []
    for rg_idx in sorted(target_row_groups):
        table = parquet_file.read_row_group(rg_idx)
        tables.append(table)

    # Combine all row groups
    if len(tables) == 1:
        combined_table = tables[0]
    else:
        combined_table = pa.concat_tables(tables)

    # Filter to only rows matching our target IDs
    matches = pc.is_in(combined_table['object_id'], pa.array(target_ids))
    filtered_table = combined_table.filter(matches)

    if filtered_table.num_rows > 0:
        pq.write_table(filtered_table, "temp_partial.parquet")
        return filtered_table.to_pydict()
    else:
        print(f"✗ None of the target object_ids found in row groups")
        return None


def demo_load_full_file(target_ids: list[str]):
    hf_fs = HfFileSystem()
    full_path = f"datasets/{REPO_ID}/{FILE_PATH}"

    parquet_file = pq.ParquetFile(hf_fs.open(full_path, "rb"))

    print(f"File has {parquet_file.num_row_groups} row groups")
    print()

    table = pq.read_table(
        hf_fs.open(full_path, "rb"),
        filters=pc.field("object_id").isin(pa.array(target_ids))
    )
    pq.write_table(table, "temp_full.parquet")
    return table.to_pydict()


if __name__ == "__main__":
    import time
    target_ids = ["1757963689505762345", "1757963689505748225"]
    t1 = time.time()
    rows = find_row_group_and_download(target_ids)
    elapsed1 = time.time() - t1
    t2 = time.time()
    rows2 = demo_load_full_file(target_ids)
    elapsed2 = time.time() - t2
    print(f"Row group: {elapsed1:.2f}s | Full file: {elapsed2:.2f}s | Speedup: {elapsed2/elapsed1:.1f}x")
    assert rows == rows2

