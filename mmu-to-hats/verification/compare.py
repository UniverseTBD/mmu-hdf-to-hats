import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from datasets import load_from_disk
import argparse
import sys
from pathlib import Path


def load_table(file_path):
    """Load a PyArrow table from either a parquet file or datasets directory."""
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File or directory not found: {file_path}")

    # Try loading as parquet file
    if path.is_file() and path.suffix == ".parquet":
        return pq.read_table(file_path)

    # Try loading as datasets directory
    if path.is_dir():
        try:
            dataset = load_from_disk(file_path)
            return dataset.data.table  # Get the underlying Arrow table
        except Exception as e:
            raise ValueError(f"Could not load {file_path} as datasets directory: {e}")

    raise ValueError(f"Unsupported file type or format: {file_path}")


def compare_tables(table1, table2, label1="Table 1", label2="Table 2"):
    """Compare two PyArrow tables and report differences."""
    print(f"{label1}: {table1.num_rows} rows, {table1.num_columns} columns")
    print(f"{label2}: {table2.num_rows} rows, {table2.num_columns} columns")

    # Check same number of rows
    assert table1.num_rows == table2.num_rows, (
        f"Row count mismatch: {table1.num_rows} vs {table2.num_rows}"
    )

    # Check same columns (order doesn't matter)
    cols1 = set(table1.column_names)
    cols2 = set(table2.column_names)
    assert cols1 == cols2, f"Column mismatch: {cols1.symmetric_difference(cols2)}"

    # Find a sortable column (exclude list and struct types)
    sort_column = None
    for col_name in table1.column_names:
        col_type = table1.schema.field(col_name).type
        # Check if type is sortable (not list, struct, or other complex types)
        if not pa.types.is_nested(col_type):
            sort_column = col_name
            break

    if sort_column is None:
        raise ValueError("No sortable column found in table")

    print(f"\nSorting by column: {sort_column}")
    table1_sorted = table1.sort_by(sort_column)
    table2_sorted = table2.sort_by(sort_column)

    # Compare each column
    print("\nComparing columns...")
    all_match = True
    for col_name in table1.column_names:
        print(f"  Checking {col_name}...", end=" ")
        col1 = table1_sorted[col_name]
        col2 = table2_sorted[col_name]

        # Use pyarrow's equal function which handles nulls and nested types
        if not col1.equals(col2):
            print(f"MISMATCH!")
            print(
                f"    First difference at row: {pc.index(pc.not_equal(col1, col2), True).as_py()}"
            )
            all_match = False
        else:
            print("OK")

    if all_match:
        print("\n✓ Tables are identical (possibly reordered)")
    else:
        print("\n✗ Tables have differences")
        sys.exit(1)


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

    # Compare tables
    compare_tables(table1, table2, label1="File 1", label2="File 2")


if __name__ == "__main__":
    main()
