# run using:
# python -m transform_scripts.transform_foundation_to_parquet
import pyarrow.parquet as pq
from catalog_functions.foundation_transformer import FoundationTransformer

# Example usage
input_file = "data/MultimodalUniverse/v1/foundation/foundation_dr1/healpix=1628/"
output_file = "data/foundation_hp1628_transformed.parquet"

print("Transforming HDF5 to Arrow table...")
transformer = FoundationTransformer()
table = transformer.transform_from_hdf5(input_file)

print(f"\nTable shape: {table.num_rows} rows, {table.num_columns} columns")
print(f"\nSchema:\n{table.schema}")

# Write to parquet
pq.write_table(table, output_file)
print(f"\nWrote output to {output_file}")
