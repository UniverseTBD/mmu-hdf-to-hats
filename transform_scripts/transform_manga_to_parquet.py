# run using:
# python -m transform_scripts.transform_manga_to_parquet
import pyarrow.parquet as pq
from catalog_functions.manga_transformer import MaNGATransformer

input_file = "data/MultimodalUniverse/v1/manga/manga/healpix=385/0001-of-0001.hdf5"
output_file = "data/manga_hp385_transformed.parquet"

print("Transforming HDF5 to Arrow table...")
transformer = MaNGATransformer()
table = transformer.transform_from_hdf5(input_file)
breakpoint()

print(f"\nTable shape: {table.num_rows} rows, {table.num_columns} columns")
print(f"\nSchema:\n{table.schema}")

# Write to parquet
pq.write_table(table, output_file)
print(f"\nWrote output to {output_file}")
