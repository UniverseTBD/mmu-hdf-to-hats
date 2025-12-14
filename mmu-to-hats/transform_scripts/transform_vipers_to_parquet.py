# run using:
# python -m transform_scripts.transform_sdss_to_parquet
import pyarrow.parquet as pq
from catalog_functions.vipers_transformer import VIPERSTransformer

# Example usage
input_file = "data/MultimodalUniverse/v1/vipers/vipers_w1/healpix=1107/"
output_file = "data/vipers_hp1107_transformed.parquet"

print("Transforming HDF5 to Arrow table...")
transformer = VIPERSTransformer()
table = transformer.transform_from_hdf5(input_file)

print(f"\nTable shape: {table.num_rows} rows, {table.num_columns} columns")
print(f"\nSchema:\n{table.schema}")

# Write to parquet
pq.write_table(table, output_file)
print(f"\nWrote output to {output_file}")
