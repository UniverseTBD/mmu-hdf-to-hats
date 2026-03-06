# run using:
# python -m transform_scripts.transform_yse_to_parquet
import pyarrow.parquet as pq
from catalog_functions.yse_transformer import YSETransformer

# Example usage
input_file = "data/MultimodalUniverse/v1/yse/yse_dr1/healpix=0584/"
output_file = "data/yse_hp584_transformed.parquet"

print("Transforming HDF5 to Arrow table...")
transformer = YSETransformer()
table = transformer.transform_from_hdf5(input_file)

print(f"\nTable shape: {table.num_rows} rows, {table.num_columns} columns")
print(f"\nSchema:\n{table.schema}")

# Write to parquet
pq.write_table(table, output_file)
print(f"\nWrote output to {output_file}")
