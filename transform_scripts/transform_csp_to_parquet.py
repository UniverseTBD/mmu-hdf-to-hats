# run using:
# python -m transform_scripts.transform_csp_to_parquet
import pyarrow.parquet as pq
from catalog_functions.csp_transformer import CSPTransformer

# Example usage - csp healpix=1113
input_file = "data/MultimodalUniverse/v1/csp/csp/healpix=1113/"
output_file = "data/csp_hp1113_transformed.parquet"

print("Transforming HDF5 to Arrow table...")
transformer = CSPTransformer()
table = transformer.transform_from_hdf5(input_file)

print(f"\nTable shape: {table.num_rows} rows, {table.num_columns} columns")
print(f"\nSchema:\n{table.schema}")

# Write to parquet
pq.write_table(table, output_file)
print(f"\nWrote output to {output_file}")
