# run using:
# python -m transform_scripts.transform_hsc_to_parquet
import pyarrow.parquet as pq
from catalog_functions.hsc_transformer import HSCTransformer

# Example usage
input_file = "data/MultimodalUniverse/v1/hsc/pdr3_dud_22.5/healpix=1106/"
output_file = "data/hsc_hp1106_transformed.parquet"

print("Transforming HDF5 to Arrow table...")
transformer = HSCTransformer()
table = transformer.transform_from_hdf5(input_file)

print(f"\nTable shape: {table.num_rows} rows, {table.num_columns} columns")
print(f"\nSchema:\n{table.schema}")

# Write to parquet
pq.write_table(table, output_file)
print(f"\nWrote output to {output_file}")
