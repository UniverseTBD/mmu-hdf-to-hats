# run using:
# python -m transform_scripts.transform_legacysurvey_to_parquet
import pyarrow.parquet as pq
from catalog_functions.legacysurvey_transformer import LegacySurveyTransformer

# Example usage
input_file = "data/MultimodalUniverse/v1/legacysurvey/dr10_south_21/healpix=1981/001-of-001.hdf5"
output_file = "data/legacysurvey_hp1981_transformed.parquet"

print("Transforming HDF5 to Arrow table...")
transformer = LegacySurveyTransformer()
table = transformer.transform_from_hdf5(input_file)

print(f"\nTable shape: {table.num_rows} rows, {table.num_columns} columns")
print(f"\nSchema:\n{table.schema}")

# Write to parquet
pq.write_table(table, output_file)
print(f"\nWrote output to {output_file}")
