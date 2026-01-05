# run using:
# python -m transform_scripts.transform_snls_to_parquet
import pyarrow.parquet as pq
from catalog_functions.snls_transformer import SNLSTransformer

# Example usage
input_file = "data/MultimodalUniverse/v1/snls/data/healpix=0714/"
output_file = "data/snls_hp0714_transformed.parquet"

print("Transforming HDF5 to Arrow table...")
transformer = SNLSTransformer()
table = transformer.transform_from_hdf5(input_file)

print(f"\nTable shape: {table.num_rows} rows, {table.num_columns} columns")
print(f"\nSchema:\n{table.schema}")

# Write to parquet
pq.write_table(table, output_file)
print(f"\nWrote output to {output_file}")
