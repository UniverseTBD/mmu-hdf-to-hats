# run using:
# python -m transform_scripts.transform_gaia_to_parquet
import pyarrow.parquet as pq
from catalog_functions.gaia_transformer import GaiaTransformer

input_file = "data/MultimodalUniverse/v1/gaia/gaia/healpix=1631/001-of-001.hdf5"
output_file = "data/gaia_hp1631_transformed.parquet"

print("Transforming HDF5 to Arrow table...")
transformer = GaiaTransformer()
table = transformer.transform_from_hdf5(input_file)

print(f"\nTable shape: {table.num_rows} rows, {table.num_columns} columns")
print(f"\nSchema:\n{table.schema}")

# Write to parquet
pq.write_table(table, output_file)
print(f"\nWrote output to {output_file}")
