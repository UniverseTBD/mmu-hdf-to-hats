import pyarrow as pa
import pyarrow.parquet as pq
from datasets import load_from_disk
import click
from pathlib import Path
import numpy as np


def get_all_field_names(schema, prefix=""):
    """Recursively extract all field names from a schema, including nested struct fields.

    Returns a set of field names like: {'col1', 'struct_col.field1', 'struct_col.field2'}
    """
    field_names = set()

    for field in schema:
        field_path = f"{prefix}{field.name}" if prefix else field.name
        field_names.add(field_path)

        # If this field is a struct, recurse into its fields
        if pa.types.is_struct(field.type):
            nested_names = get_all_field_names(field.type, prefix=f"{field_path}.")
            field_names.update(nested_names)

    return field_names


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
                    new_columns[f"{col_name}.{field.name}"] = struct_col.field(i)
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


def columns_equal_or_samples(
    arr1: np.ndarray, arr2: np.ndarray
) -> tuple[bool, list[tuple[int, any, any]]]:
    columns_equal = np.allclose(
        arr1,
        arr2,
        rtol=1e-5,
        atol=1e-8,
        equal_nan=True,
    )
    if not columns_equal:
        # Find mismatched indices
        mask = ~np.isclose(arr1, arr2, rtol=1e-5, atol=1e-8, equal_nan=True)
        mismatch_indices = np.where(mask)[0][:3]
        sample_data = [
            {"index": i, "left": arr1[i], "right": arr2[i]} for i in mismatch_indices
        ]
        return False, sample_data
    return True, []


def compare_tables(
    table1, table2, label1="Table 1", label2="Table 2", mismatch_number=3
):
    """Compare two PyArrow tables and report all differences."""
    # general comparison report
    issues = []
    sample_data = []
    # we'll ignore the column types in the schema comparison, since datasets can make some optimizations, e.g.
    # list<item: extension<datasets.features.features.Array2DExtensionType<Array2DExtensionType>>>
    fields1 = get_all_field_names(table1.schema)
    fields2 = get_all_field_names(table2.schema)

    fields_only_in1 = fields1 - fields2
    fields_only_in2 = fields2 - fields1

    if fields_only_in1 or fields_only_in2:
        differences = []
        if fields_only_in1:
            differences.append(
                f"Fields in {label1} but not in {label2}: {sorted(fields_only_in1)}"
            )
        if fields_only_in2:
            differences.append(
                f"Fields in {label2} but not in {label1}: {sorted(fields_only_in2)}"
            )
        issues.append(
            {
                "type": "schema",
                "column": None,
                "message": "\n".join(differences),
                "samples": [],
            }
        )
    table1 = flatten_struct_columns(table1)
    table2 = flatten_struct_columns(table2)

    print(f"\n{'=' * 70}")
    print(f"COMPARISON SUMMARY")
    print(f"{'=' * 70}")
    print(f"{label1}: {table1.num_rows} rows, {table1.num_columns} columns")
    print(f"{label2}: {table2.num_rows} rows, {table2.num_columns} columns")

    # Check row counts
    if table1.num_rows != table2.num_rows:
        issues.append(
            {
                "type": "row_count",
                "column": None,
                "message": f"Row count mismatch: {label1} has {table1.num_rows} rows, {label2} has {table2.num_rows} rows",
            }
        )

    # Check columns
    cols1 = set(table1.column_names)
    cols2 = set(table2.column_names)

    cols_only_in_1 = cols1 - cols2
    cols_only_in_2 = cols2 - cols1
    common_cols = cols1 & cols2

    if cols_only_in_1:
        issues.append(
            {
                "type": "columns",
                "column": None,
                "message": f"{label1} has additional columns: {sorted(cols_only_in_1)}",
            }
        )

    if cols_only_in_2:
        issues.append(
            {
                "type": "columns",
                "column": None,
                "message": f"{label2} has additional columns: {sorted(cols_only_in_2)}",
            }
        )

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
            issues.append(
                {
                    "type": "sorting",
                    "column": None,
                    "message": "No sortable column found in common columns - cannot compare row-by-row",
                }
            )
        else:
            print(f"\nSorting by column: {sort_column}")
            table1_sorted = table1.sort_by(sort_column)
            table2_sorted = table2.sort_by(sort_column)

            print(f"\nComparing {len(common_cols)} common columns...")
            for col_name in sorted(common_cols):
                col1 = table1_sorted[col_name]
                col2 = table2_sorted[col_name]

                col_type = table1_sorted.schema.field(col_name).type
                # Structs are flattened, but list columns remain nested
                if pa.types.is_nested(col_type):
                    list1 = col1.combine_chunks().to_pylist()
                    list2 = col2.combine_chunks().to_pylist()
                    columns_equal = list1 == list2
                    if not columns_equal:
                        if (
                            isinstance(list1[0], list)
                            and isinstance(list1[0][0], float)
                            and isinstance(list2[0], list)
                            and isinstance(list2[0][0], float)
                        ):
                            arr1 = np.array(list1)
                            arr2 = np.array(list2)
                            columns_equal, sample_data = columns_equal_or_samples(
                                arr1, arr2
                            )
                        else:
                            # Find mismatched indices
                            mismatch_indices = [
                                i
                                for i in range(min(len(list1), len(list2)))
                                if list1[i] != list2[i]
                            ]
                            sample_data = [
                                {"index": i, "left": list1[i], "right": list2[i]}
                                for i in mismatch_indices[:3]
                            ]
                elif pa.types.is_floating(col_type):
                    arr1 = col1.to_numpy()
                    arr2 = col2.to_numpy()
                    columns_equal, sample_data = columns_equal_or_samples(arr1, arr2)
                else:
                    columns_equal = col1.equals(col2)
                    if not columns_equal:
                        # Find mismatched indices
                        arr1 = col1.to_pylist()
                        arr2 = col2.to_pylist()
                        mismatch_indices = [
                            i
                            for i in range(min(len(arr1), len(arr2)))
                            if arr1[i] != arr2[i]
                        ]
                        sample_data = [
                            {"index": i, "left": arr1[i], "right": arr2[i]}
                            for i in mismatch_indices[:mismatch_number]
                        ]

                if not columns_equal:
                    issues.append(
                        {
                            "type": "column_values",
                            "message": f"Column '{col_name}' has differences",
                            "column": col_name,
                            "samples": sample_data,
                        }
                    )
    return issues


@click.command()
@click.argument("file1", type=click.Path(exists=True))
@click.argument("file2", type=click.Path(exists=True))
@click.option("--allowed-mismatch-columns", type=str, default="")
def main(file1, file2, allowed_mismatch_columns):
    """Compare two PyArrow tables from parquet files or datasets directories.

    Examples:

      # Compare a transformed parquet with a datasets directory
      python compare.py data/transformed.parquet data/datasets_output

      # Compare two parquet files
      python compare.py output1.parquet output2.parquet

      # Compare two datasets directories
      python compare.py data/dataset1 data/dataset2
    """
    # Load both tables
    click.echo(f"Loading first table from: {file1}")
    table1 = load_table(file1)

    click.echo(f"Loading second table from: {file2}")
    table2 = load_table(file2)

    # Flatten struct columns for comparison
    click.echo("Flattening struct columns...")

    # Compare tables and show full report
    issues = compare_tables(table1, table2, label1=file1, label2=file2)

    # Print final report
    print(f"\n{'=' * 70}")
    print(f"FINAL REPORT")
    print(f"{'=' * 70}")

    for idx, issue in enumerate(issues, 1):
        msg = f"{idx}. [{issue['type'].upper()}] {issue['message']}"
        if "samples" in issue and issue["samples"]:
            msg += f" (showing {len(issue['samples'])} sample(s))"
            for sample in issue["samples"]:
                msg += f"\n    - Index {sample['index']}: Left = {sample['left']}, Right = {sample['right']}"
        print(msg)
    issues_leading_to_failure = [
        issue
        for issue in issues
        if issue["column"] not in allowed_mismatch_columns.split(",")
    ]
    if len(issues_leading_to_failure) > 0:
        exit(1)
    exit(0)


if __name__ == "__main__":
    main()
