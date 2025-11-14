import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from datasets import load_from_disk

# Load both tables
transformed_table = pq.read_table("data/transformed_table_class.parquet")
datasets_dataset = load_from_disk("data/MultimodalUniverse/v1/sdss_with_coordinates")
datasets_table = datasets_dataset.data.table  # Get the underlying Arrow table

print(f"Transformed table: {transformed_table.num_rows} rows, {transformed_table.num_columns} columns")
print(f"Datasets table: {datasets_table.num_rows} rows, {datasets_table.num_columns} columns")

# Check same number of rows
assert transformed_table.num_rows == datasets_table.num_rows, \
    f"Row count mismatch: {transformed_table.num_rows} vs {datasets_table.num_rows}"

# Check same columns (order doesn't matter)
transformed_cols = set(transformed_table.column_names)
datasets_cols = set(datasets_table.column_names)
assert transformed_cols == datasets_cols, \
    f"Column mismatch: {transformed_cols.symmetric_difference(datasets_cols)}"

# Find a sortable column (exclude list and struct types)
sort_column = None
for col_name in transformed_table.column_names:
    col_type = transformed_table.schema.field(col_name).type
    # Check if type is sortable (not list, struct, or other complex types)
    if not pa.types.is_nested(col_type):
        sort_column = col_name
        break

if sort_column is None:
    raise ValueError("No sortable column found in table")

print(f"\nSorting by column: {sort_column}")
transformed_sorted = transformed_table.sort_by(sort_column)
datasets_sorted = datasets_table.sort_by(sort_column)

# Compare each column
print("\nComparing columns...")
for col_name in transformed_table.column_names:
    print(f"  Checking {col_name}...", end=" ")
    transformed_col = transformed_sorted[col_name]
    datasets_col = datasets_sorted[col_name]

    # Use pyarrow's equal function which handles nulls and nested types
    if not transformed_col.equals(datasets_col):
        print(f"MISMATCH!")
        print(f"    First difference at row: {pc.index(pc.not_equal(transformed_col, datasets_col), True).as_py()}")
    else:
        print("OK")

print("\n✓ Tables are identical (possibly reordered)")
