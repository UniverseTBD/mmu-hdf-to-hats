import pyarrow as pa
import pyarrow.parquet as pq
from datasets import load_from_disk
import argparse
from pathlib import Path
import numpy as np


def flatten_struct_columns(table):
    """Flatten nested struct columns to top-level columns."""
    new_columns = {}

    for col_name in table.column_names:
        col = table[col_name]
        col_type = table.schema.field(col_name).type

        if pa.types.is_struct(col_type):
            # combine_chunks() + field(i) is zero-copy, unlike to_pylist()
            struct_col = col.combine_chunks()
            for i, field in enumerate(col_type):
                if field.name not in new_columns:
                    new_columns[field.name] = struct_col.field(i)
        else:
            new_columns[col_name] = col

    return pa.table(new_columns)


def load_table(file_path):
    """Load a PyArrow table from either a parquet file or datasets directory."""
    path = Path(file_path)

    if path.is_file() and path.suffix == ".parquet":
        return pq.read_table(file_path)

    if path.is_dir():
        dataset = load_from_disk(file_path)
        return dataset.data.table

    raise ValueError(f"Unsupported file type or format: {file_path}")


def compare_tables(table1, table2, label1="Table 1", label2="Table 2"):
    """Compare two PyArrow tables and report all differences."""
    issues = []

    print(f"\n{'='*70}")
    print(f"COMPARISON SUMMARY")
    print(f"{'='*70}")
    print(f"{label1}: {table1.num_rows} rows, {table1.num_columns} columns")
    print(f"{label2}: {table2.num_rows} rows, {table2.num_columns} columns")

    # Check row counts
    if table1.num_rows != table2.num_rows:
        issues.append({
            "type": "row_count",
            "message": f"Row count mismatch: {label1} has {table1.num_rows} rows, {label2} has {table2.num_rows} rows"
        })

    # Check columns
    cols1 = set(table1.column_names)
    cols2 = set(table2.column_names)

    cols_only_in_1 = cols1 - cols2
    cols_only_in_2 = cols2 - cols1
    common_cols = cols1 & cols2

    if cols_only_in_1:
        issues.append({
            "type": "columns",
            "message": f"{label1} has additional columns: {sorted(cols_only_in_1)}"
        })

    if cols_only_in_2:
        issues.append({
            "type": "columns",
            "message": f"{label2} has additional columns: {sorted(cols_only_in_2)}"
        })

    # Compare common columns (only if both tables have rows)
    if common_cols and table1.num_rows > 0 and table2.num_rows > 0:
        # Find a sortable column for comparison - prefer object_id for stability
        sort_column = None
        preferred_sort_cols = ["object_id", "source_id", "id"]
        for col_name in preferred_sort_cols:
            if col_name in common_cols:
                sort_column = col_name
                break
        if sort_column is None:
            for col_name in sorted(common_cols):
                col_type = table1.schema.field(col_name).type
                if not pa.types.is_nested(col_type):
                    sort_column = col_name
                    break

        if sort_column is None:
            issues.append({
                "type": "sorting",
                "message": "No sortable column found in common columns - cannot compare row-by-row"
            })
        else:
            print(f"\nSorting by column: {sort_column}")
            table1_sorted = table1.sort_by(sort_column)
            table2_sorted = table2.sort_by(sort_column)

            print(f"\nComparing {len(common_cols)} common columns...")
            for col_name in sorted(common_cols):
                print(f"  Checking {col_name}...", end=" ")
                col1 = table1_sorted[col_name]
                col2 = table2_sorted[col_name]

                col_type = table1_sorted.schema.field(col_name).type
                # Structs are flattened, but list columns remain nested
                if pa.types.is_nested(col_type):
                    columns_equal = col1.combine_chunks().to_pylist() == col2.combine_chunks().to_pylist()
                elif pa.types.is_floating(col_type):
                    columns_equal = np.allclose(col1.to_numpy(), col2.to_numpy(), rtol=1e-5, atol=1e-8, equal_nan=True)
                else:
                    columns_equal = col1.equals(col2)

                if columns_equal:
                    print("OK")
                else:
                    print("MISMATCH")
                    issues.append({
                        "type": "column_values",
                        "message": f"Column '{col_name}' has differences"
                    })

    # Print final report
    print(f"\n{'='*70}")
    print(f"FINAL REPORT")
    print(f"{'='*70}")

    if not issues:
        print("✓ Tables are identical (possibly reordered)")
        return True
    else:
        print(f"✗ Found {len(issues)} difference(s):\n")

        # Group issues by type
        for idx, issue in enumerate(issues, 1):
            print(f"{idx}. [{issue['type'].upper()}] {issue['message']}")

        print(f"\n{'='*70}")
        return False


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Compare two PyArrow tables from parquet files or datasets directories",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Compare a transformed parquet with a datasets directory
  python compare.py data/transformed.parquet data/datasets_output

  # Compare two parquet files
  python compare.py output1.parquet output2.parquet

  # Compare two datasets directories
  python compare.py data/dataset1 data/dataset2
        """,
    )
    parser.add_argument(
        "file1", type=str, help="First file (parquet) or directory (datasets)"
    )
    parser.add_argument(
        "file2", type=str, help="Second file (parquet) or directory (datasets)"
    )
    args = parser.parse_args()

    # Load both tables
    print(f"Loading first table from: {args.file1}")
    table1 = load_table(args.file1)

    print(f"Loading second table from: {args.file2}")
    table2 = load_table(args.file2)

    # Flatten struct columns for comparison
    print("Flattening struct columns...")
    table1 = flatten_struct_columns(table1)
    table2 = flatten_struct_columns(table2)

    # Compare tables and show full report
    compare_tables(table1, table2, label1=args.file1, label2=args.file2)


if __name__ == "__main__":
    main()
