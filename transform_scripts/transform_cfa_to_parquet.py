# run using:
# python -m transform_scripts.transform_cfa_to_parquet
import pyarrow.parquet as pq
from catalog_functions.cfa_transformer import CFATransformer

# Example usage - cfa3 healpix=0146
input_file = "data/MultimodalUniverse/v1/cfa/cfa3/healpix=0146/"
output_file = "data/cfa_hp0146_transformed.parquet"

print("Transforming HDF5 to Arrow table...")
transformer = CFATransformer()
table = transformer.transform_from_hdf5(input_file)

print(f"\nTable shape: {table.num_rows} rows, {table.num_columns} columns")
print(f"\nSchema:\n{table.schema}")

# Write to parquet
pq.write_table(table, output_file)
print(f"\nWrote output to {output_file}")
