# run using:
# python -m transform_scripts.transform_gz10_to_parquet
import pyarrow.parquet as pq
from catalog_functions.gz10_transformer import GZ10Transformer

# Example usage
input_file = "data/MultimodalUniverse/v1/gz10/datafiles/healpix=513/"
output_file = "data/gz10_hp513_transformed.parquet"

print("Transforming HDF5 to Arrow table...")
transformer = GZ10Transformer()
table = transformer.transform_from_hdf5(input_file)

print(f"\nTable shape: {table.num_rows} rows, {table.num_columns} columns")
print(f"\nSchema:\n{table.schema}")

# Write to parquet
pq.write_table(table, output_file)
print(f"\nWrote output to {output_file}")
