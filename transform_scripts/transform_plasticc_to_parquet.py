# run using: python transform_scripts/transform_plasticc_to_parquet.py
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from catalog_functions.plasticc_transformer import PLAsTiCCTransformer

# Only use train files to match datasets train_only config
input_dir = Path("data/MultimodalUniverse/v1/plasticc/data/healpix=1378/")
input_files = sorted(input_dir.glob("train*.hdf5"))
output_file = "data/plasticc_hp1378_transformed.parquet"

print(f"Found {len(input_files)} train files: {[f.name for f in input_files]}")
print("Transforming HDF5 to Arrow table...")
transformer = PLAsTiCCTransformer()

# Process each file and concatenate
tables = []
for f in input_files:
    table = transformer.transform_from_hdf5_file(str(f))
    tables.append(table)
table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]

print(f"\nTable shape: {table.num_rows} rows, {table.num_columns} columns")
print(f"\nSchema:\n{table.schema}")

pq.write_table(table, output_file)
print(f"\nWrote output to {output_file}")

