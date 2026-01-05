# run using:
# python -m transform_scripts.transform_tess_to_parquet
import pyarrow.parquet as pq
from catalog_functions.tess_transformer import TESSTransformer

# Example usage
input_file = "data/MultimodalUniverse/v1/tess/spoc/healpix=2201/"
output_file = "data/tess_hp2201_transformed.parquet"

print("Transforming HDF5 to Arrow table...")
transformer = TESSTransformer()
table = transformer.transform_from_hdf5(input_file)

print(f"\nTable shape: {table.num_rows} rows, {table.num_columns} columns")
print(f"\nSchema:\n{table.schema}")

# Write to parquet
pq.write_table(table, output_file)
print(f"\nWrote output to {output_file}")
