# run using:
# python -m transform_scripts.transform_chandra_to_parquet
import pyarrow.parquet as pq
from catalog_functions.chandra_transformer import ChandraTransformer

# Example usage - spectra/healpix=1339
input_file = "data/MultimodalUniverse/v1/chandra/spectra/healpix=1339/"
output_file = "data/chandra_hp1339_transformed.parquet"

print("Transforming HDF5 to Arrow table...")
transformer = ChandraTransformer()
table = transformer.transform_from_hdf5(input_file)

print(f"\nTable shape: {table.num_rows} rows, {table.num_columns} columns")
print(f"\nSchema:\n{table.schema}")

# Write to parquet
pq.write_table(table, output_file)
print(f"\nWrote output to {output_file}")
