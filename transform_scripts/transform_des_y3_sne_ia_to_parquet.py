# run using:
# python -m transform_scripts.transform_des_y3_sne_ia_to_parquet
import pyarrow.parquet as pq
from catalog_functions.des_y3_sne_ia_transformer import DESY3SNEIaTransformer

# Example usage
input_file = "data/MultimodalUniverse/v1/des_y3_sne_ia/des_y3_sne_ia/healpix=1105/"
output_file = "data/des_y3_sne_ia_hp1105_transformed.parquet"

print("Transforming HDF5 to Arrow table...")
transformer = DESY3SNEIaTransformer()
table = transformer.transform_from_hdf5(input_file)

print(f"\nTable shape: {table.num_rows} rows, {table.num_columns} columns")
print(f"\nSchema:\n{table.schema}")

# Write to parquet
pq.write_table(table, output_file)
print(f"\nWrote output to {output_file}")
