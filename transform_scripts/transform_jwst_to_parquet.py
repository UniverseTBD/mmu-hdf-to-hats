# run using:
# python -m transform_scripts/transform_jwst_to_parquet.py


import pyarrow.parquet as pq
from catalog_functions.jwst_transformer import JWSTTransformer

input_file = "data/MultimodalUniverse/v1/jwst/ngdeep/healpix=2245/001-of-001.hdf5"
output_file = "data/jwst_hp2245_transformed.parquet"

print("Transforming HDF5 to Arrow table...")
transformer = JWSTTransformer()
table = transformer.transform_from_hdf5(input_file)

print(f"\nTable shape: {table.num_rows} rows, {table.num_columns} columns")
print(f"\nSchema:\n{table.schema}")

# Write to parquet
pq.write_table(table, output_file)
print(f"\nWrote output to {output_file}")
