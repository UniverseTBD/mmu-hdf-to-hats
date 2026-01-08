# run using:
# python -m transform_scripts.transform_desi_to_parquet
import pyarrow.parquet as pq
from catalog_functions.desi_provabgs_transformer import DESIPROVABGSTransformer

# Example usage
input_file = "data/MultimodalUniverse/v1/desi_provabgs/datafiles/healpix=669/001-of-001.h5"
output_file = "data/desi_provabgs_hp669_transformed.parquet"

print("Transforming HDF5 to Arrow table...")
transformer = DESIPROVABGSTransformer()
table = transformer.transform_from_hdf5(input_file)

print(f"\nTable shape: {table.num_rows} rows, {table.num_columns} columns")
print(f"\nSchema:\n{table.schema}")

# Write to parquet
pq.write_table(table, output_file)
print(f"\nWrote output to {output_file}")
