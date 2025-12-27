# run using:
# python -m transform_scripts.transform_sdss_to_parquet
import pyarrow.parquet as pq
from catalog_functions.swift_sne_ia_transformer import SwiftSNeIaTransformer

# Example usage
input_file = "data/MultimodalUniverse/v1/swift_sne_ia/data/healpix=2158/"
output_file = "data/swift_sne_ia_hp2158_transformed.parquet"

print("Transforming HDF5 to Arrow table...")
transformer = SwiftSNeIaTransformer()
table = transformer.transform_from_hdf5(input_file)

print(f"\nTable shape: {table.num_rows} rows, {table.num_columns} columns")
print(f"\nSchema:\n{table.schema}")

# Write to parquet
pq.write_table(table, output_file)
print(f"\nWrote output to {output_file}")
