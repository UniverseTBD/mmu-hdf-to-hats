# run using:
# python -m transform_scripts.transform_ps1_sne_ia_to_parquet
import pyarrow.parquet as pq
from catalog_functions.ps1_sne_ia_transformer import PS1SNeIaTransformer

# Example usage
input_file = "data/MultimodalUniverse/v1/ps1_sne_ia/ps1_sne_ia/healpix=1105/"
output_file = "data/ps1_sne_ia_hp1105_transformed.parquet"

print("Transforming HDF5 to Arrow table...")
transformer = PS1SNeIaTransformer()
table = transformer.transform_from_hdf5(input_file)

print(f"\nTable shape: {table.num_rows} rows, {table.num_columns} columns")
print(f"\nSchema:\n{table.schema}")

# Write to parquet
pq.write_table(table, output_file)
print(f"\nWrote output to {output_file}")
