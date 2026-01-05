# run using:
# python -m transform_scripts.transform_btsbot_to_parquet
import pyarrow.parquet as pq
from catalog_functions.btsbot_transformer import BTSbotTransformer

# Example usage
input_file = "data/MultimodalUniverse/v1/btsbot/data/healpix=0313/"
output_file = "data/btsbot_hp0313_transformed.parquet"

print("Transforming HDF5 to Arrow table...")
transformer = BTSbotTransformer()
table = transformer.transform_from_hdf5(input_file)

print(f"\nTable shape: {table.num_rows} rows, {table.num_columns} columns")
print(f"\nSchema:\n{table.schema}")

# Write to parquet
pq.write_table(table, output_file)
print(f"\nWrote output to {output_file}")
