# run using:
# python -m transform_scripts.transform_ssl_legacysurvey_to_parquet
import pyarrow.parquet as pq
from catalog_functions.ssl_legacysurvey_transformer import SSLLegacySurveyTransformer

# Example usage - north/healpix=125
input_file = "data/MultimodalUniverse/v1/ssl_legacysurvey/north/healpix=125/"
output_file = "data/ssl_legacysurvey_hp125_transformed.parquet"

print("Transforming HDF5 to Arrow table...")
transformer = SSLLegacySurveyTransformer()
table = transformer.transform_from_hdf5(input_file)

print(f"\nTable shape: {table.num_rows} rows, {table.num_columns} columns")
print(f"\nSchema:\n{table.schema}")

# Write to parquet
pq.write_table(table, output_file)
print(f"\nWrote output to {output_file}")
