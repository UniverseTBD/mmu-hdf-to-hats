import pyarrow as pa
import pyarrow.parquet as pq
from datasets import load_from_disk
import warnings
import click
from pathlib import Path
import numpy as np
from typing import TypedDict


class ComparisonIssue(TypedDict):
    type: str
    message: str
    column: str | None
    samples: list | None
    table: str | None


def normalize_nested_value(value):
    """Normalize nested values for comparison across Arrow/datasets representations."""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, list):
        return [normalize_nested_value(item) for item in value]
    if isinstance(value, dict):
        return {key: normalize_nested_value(val) for key, val in value.items()}
    return value


def values_equal(left, right):
    left = normalize_nested_value(left)
    right = normalize_nested_value(right)

    if isinstance(left, float) and isinstance(right, float):
        if np.isnan(left) and np.isnan(right):
            return True
        return np.isclose(left, right, rtol=1e-5, atol=1e-8, equal_nan=True)

    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            values_equal(l_item, r_item) for l_item, r_item in zip(left, right)
        )

    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(
            values_equal(left[key], right[key]) for key in left
        )

    return left == right


def arrays_equal_by_value(arr1, arr2):
    """Compare Arrow arrays by value, ignoring representation-only differences."""
    type1 = arr1.type
    type2 = arr2.type

    if hasattr(type1, "storage_type"):
        arr1 = arr1.cast(type1.storage_type)
        type1 = arr1.type
    if hasattr(type2, "storage_type"):
        arr2 = arr2.cast(type2.storage_type)
        type2 = arr2.type

    if arr1.equals(arr2):
        return True

    if pa.types.is_integer(type1) and pa.types.is_integer(type2):
        return np.array_equal(arr1.cast(pa.int64()).to_numpy(), arr2.cast(pa.int64()).to_numpy())

    if pa.types.is_floating(type1) and pa.types.is_floating(type2):
        return np.allclose(arr1.to_numpy(), arr2.to_numpy(), rtol=1e-5, atol=1e-8, equal_nan=True)

    if pa.types.is_binary(type1) or pa.types.is_string(type1) or pa.types.is_binary(type2) or pa.types.is_string(type2):
        return [normalize_nested_value(v) for v in arr1.to_pylist()] == [normalize_nested_value(v) for v in arr2.to_pylist()]

    if pa.types.is_nested(type1) and pa.types.is_nested(type2):
        return values_equal(arr1.to_pylist(), arr2.to_pylist())

    return values_equal(arr1.to_pylist(), arr2.to_pylist())


def normalize_coordinate_columns(table, table_name):
    """Rename RA->ra and DEC->dec if present, for case-insensitive comparison."""
    names = table.column_names
    for old, new in [("RA", "ra"), ("DEC", "dec")]:
        if old in names and new not in names:
            warnings.warn(
                f"Renaming column '{old}' to '{new}' in {table_name} for comparison"
            )
            idx = names.index(old)
            field = table.schema.field(idx).with_name(new)
            table = table.set_column(idx, field, table.column(idx))
            names = table.column_names
    return table


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


def check_for_col(table, col_name, table_name) -> list[ComparisonIssue]:
    col = next(
        (col for col in table.column_names if col.lower() == col_name.lower()), None
    )
    if col is None:
        return [
            {
                "type": "missing_column",
                "message": f"Column '{col_name}' not found",
                "column": col_name,
                "samples": None,
                "table": table_name,
            }
        ]
    col_type = table.schema.field(col).type
    # check if type is double, not float32
    if pa.types.is_float64(col_type):
        return []
    return [
        {
            "type": "column_type_mismatch",
            "message": f"Column '{col}' is of type {col_type}, expected float64",
            "column": col,
            "samples": None,
            "table": table_name,
        }
    ]


# Check for columns 'ra' and 'dec' case insensitive and make sure they are of type double
def check_for_coordinate_cols(table, table_name) -> list[ComparisonIssue]:
    ra_issues = check_for_col(table, "ra", table_name)
    dec_issues = check_for_col(table, "dec", table_name)
    return ra_issues + dec_issues


def truncate_long_arrays(obj, max_items=5):
    """
    Recursively truncate long lists/arrays in a nested structure for display.

    Args:
        obj: Any Python object (list, dict, primitive, etc.)
        max_items: Maximum number of items to show before truncating

    Returns:
        Truncated version of the object
    """
    if isinstance(obj, list):
        if len(obj) > max_items:
            # Truncate and add ellipsis marker
            truncated = [
                truncate_long_arrays(item, max_items) for item in obj[:max_items]
            ]
            truncated.append("...")
            return truncated
        else:
            # Recursively process all items
            return [truncate_long_arrays(item, max_items) for item in obj]
    elif isinstance(obj, dict):
        # Recursively process dict values
        return {k: truncate_long_arrays(v, max_items) for k, v in obj.items()}
    else:
        # Primitive value, return as-is
        return obj


def build_mismatch_samples(left_values, right_values, mismatch_number=3):
    mismatch_indices = [
        i
        for i in range(min(len(left_values), len(right_values)))
        if not values_equal(left_values[i], right_values[i])
    ]
    return [
        {
            "index": i,
            "left": truncate_long_arrays(normalize_nested_value(left_values[i])),
            "right": truncate_long_arrays(normalize_nested_value(right_values[i])),
        }
        for i in mismatch_indices[:mismatch_number]
    ]


def compare_nested_list_column(col1, col2, col_name, col_type):
    """
    Compare nested list columns by normalized semantic value.

    Returns:
        dict: {
            'field_name': (columns_equal: bool, sample_data: list),
            ...
        }
        If the column has no struct fields, returns a single key '' with the overall comparison.
    """
    left_rows = normalize_nested_value(col1.to_pylist())
    right_rows = normalize_nested_value(col2.to_pylist())

    if values_equal(left_rows, right_rows):
        return {"": (True, [])}

    if not pa.types.is_list(col_type):
        return {"": (False, build_mismatch_samples(left_rows, right_rows))}

    value_type = col_type.value_type
    if hasattr(value_type, "storage_type"):
        value_type = value_type.storage_type

    if not pa.types.is_struct(value_type):
        return {"": (False, build_mismatch_samples(left_rows, right_rows))}

    field_results = {}
    for field in value_type:
        field_name = field.name
        field_left = [
            [item.get(field_name) for item in row] if row is not None else None
            for row in left_rows
        ]
        field_right = [
            [item.get(field_name) for item in row] if row is not None else None
            for row in right_rows
        ]

        if values_equal(field_left, field_right):
            field_results[field_name] = (True, [])
        else:
            field_results[field_name] = (
                False,
                build_mismatch_samples(field_left, field_right),
            )

    return field_results


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
    datasets_table,
    rewritten_table,
    label1="Table 1",
    label2="Table 2",
    mismatch_number=3,
):
    """Compare two PyArrow tables and report all differences."""
    # general comparison report
    issues = []
    sample_data = []

    datasets_table = normalize_coordinate_columns(datasets_table, label1)
    rewritten_table = normalize_coordinate_columns(rewritten_table, label2)
    # we'll ignore the column types in the schema comparison, since datasets can make some optimizations, e.g.
    # list<item: extension<datasets.features.features.Array2DExtensionType<Array2DExtensionType>>>
    fields1 = get_all_field_names(datasets_table.schema)
    fields2 = get_all_field_names(rewritten_table.schema)

    fields_only_in1 = fields1 - fields2
    fields_only_in2 = fields2 - fields1 - {"ra", "dec"}

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

    datasets_table = flatten_struct_columns(datasets_table)
    rewritten_table = flatten_struct_columns(rewritten_table)

    print(f"\n{'=' * 70}")
    print(f"COMPARISON SUMMARY")
    print(f"{'=' * 70}")
    print(
        f"{label1}: {datasets_table.num_rows} rows, {datasets_table.num_columns} columns"
    )
    print(
        f"{label2}: {rewritten_table.num_rows} rows, {rewritten_table.num_columns} columns"
    )

    # Check row counts
    if datasets_table.num_rows != rewritten_table.num_rows:
        issues.append(
            {
                "type": "row_count",
                "column": None,
                "message": f"Row count mismatch: {label1} has {datasets_table.num_rows} rows, {label2} has {rewritten_table.num_rows} rows",
            }
        )
        # we cannot really compare more if row counts differ
        return issues
    # we only check for ra/dec columns in the rewritten table to ensure are of correct type
    issues += check_for_coordinate_cols(rewritten_table, label2)

    # Check columns
    cols1 = set(datasets_table.column_names)
    cols2 = set(rewritten_table.column_names)

    cols_only_in_1 = cols1 - cols2
    cols_only_in_2 = cols2 - cols1 - {"ra", "dec"}
    common_cols = cols1 & cols2

    if cols_only_in_1:
        issues.append(
            {
                "type": "columns",
                "column": None,
                "message": f"{label1} has additional columns: {sorted(cols_only_in_1)}",
                "table": label1,
            }
        )

    if cols_only_in_2:
        issues.append(
            {
                "type": "columns",
                "column": None,
                "message": f"{label2} has additional columns: {sorted(cols_only_in_2)}",
                "table": label2,
            }
        )

    # Compare common columns (only if both tables have rows)
    if common_cols and datasets_table.num_rows > 0 and rewritten_table.num_rows > 0:
        # Find a sortable column for comparison - prefer object_id for stability
        sort_column = None
        preferred_sort_cols = ["object_id", "source_id", "id"]
        for col_name in preferred_sort_cols:
            if col_name in common_cols:
                sort_column = col_name
                break
        if sort_column is None:
            for col_name in sorted(common_cols):
                col_type = datasets_table.schema.field(col_name).type
                if not pa.types.is_nested(col_type):
                    sort_column = col_name
                    break

        if sort_column is None:
            issues.append(
                {
                    "type": "sorting",
                    "column": None,
                    "message": "No sortable column found in common columns - cannot compare row-by-row",
                    "table": None,
                }
            )
        else:
            print(f"\nSorting by column: {sort_column}")
            datasets_table_sorted = datasets_table.sort_by(sort_column)
            rewritten_table_sorted = rewritten_table.sort_by(sort_column)

            print(f"\nComparing {len(common_cols)} common columns...")
            for col_name in sorted(common_cols):
                col1 = datasets_table_sorted[col_name]
                col2 = rewritten_table_sorted[col_name]

                col_type = datasets_table_sorted.schema.field(col_name).type
                # Structs are flattened, but list columns remain nested
                if pa.types.is_nested(col_type) and pa.types.is_list(col_type):
                    # Use PyArrow-native comparison for nested list columns
                    field_results = compare_nested_list_column(
                        col1, col2, col_name, col_type
                    )

                    # Report each mismatched field separately
                    for field_name, (
                        field_equal,
                        field_samples,
                    ) in field_results.items():
                        if not field_equal:
                            # Create full field path (e.g., "lightcurve.group")
                            full_field_name = (
                                f"{col_name}.{field_name}" if field_name else col_name
                            )

                            issues.append(
                                {
                                    "type": "column_values",
                                    "message": f"Column '{full_field_name}' has differences",
                                    "column": full_field_name,
                                    "samples": field_samples,
                                    "table": None,
                                }
                            )

                    # Skip the rest of the comparison logic for this column
                    continue
                elif pa.types.is_nested(col_type):
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
                                {
                                    "index": i,
                                    "left": truncate_long_arrays(list1[i]),
                                    "right": truncate_long_arrays(list2[i]),
                                }
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
                            {
                                "index": i,
                                "left": truncate_long_arrays(arr1[i]),
                                "right": truncate_long_arrays(arr2[i]),
                            }
                            for i in mismatch_indices[:mismatch_number]
                        ]

                if not columns_equal and sample_data:
                    issues.append(
                        {
                            "type": "column_values",
                            "message": f"Column '{col_name}' has differences",
                            "column": col_name,
                            "samples": sample_data,
                            "table": None,
                        }
                    )
    return issues


@click.command()
@click.option("--datasets-file", type=click.Path(exists=True))
@click.option("--rewritten-file", type=click.Path(exists=True))
@click.option("--allowed-mismatch-columns", type=str, default="")
def main(datasets_file, rewritten_file, allowed_mismatch_columns):
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
    click.echo(f"Loading first table from: {datasets_file}")
    datasets_table = load_table(datasets_file)

    click.echo(f"Loading second table from: {rewritten_file}")
    rewritten_table = load_table(rewritten_file)

    # Flatten struct columns for comparison
    click.echo("Flattening struct columns...")

    # Compare tables and show full report
    issues = compare_tables(
        datasets_table, rewritten_table, label1=datasets_file, label2=rewritten_file
    )

    # Print final report
    print(f"\n{'=' * 70}")
    print(f"FINAL REPORT")
    print(f"{'=' * 70}")

    for idx, issue in enumerate(issues, 1):
        msg = f"{idx}. [{issue['type'].upper()}] {issue['message']}"
        if "samples" in issue and issue["samples"]:
            msg += f" (showing {len(issue['samples'])} sample(s))"
            for sample in issue["samples"]:
                # Handle different sample formats
                if "error" in sample:
                    # TYPE_MISMATCH or other error format
                    msg += f"\n    - {sample.get('message', sample.get('error', 'Unknown error'))}"
                    if "left_type" in sample and "right_type" in sample:
                        msg += f"\n      Left type:  {sample['left_type']}"
                        msg += f"\n      Right type: {sample['right_type']}"
                elif "index" in sample:
                    # Standard sample format with index
                    msg += f"\n    - Index {sample['index']}: Left = {sample['left']}, Right = {sample['right']}"
                else:
                    # Fallback for other formats
                    msg += f"\n    - {sample}"
        print(msg)
    issues_leading_to_failure = [
        issue
        for issue in issues
        if issue["column"] not in allowed_mismatch_columns.split(",")
    ]
    if len(issues_leading_to_failure) > 0:
        for issue in issues_leading_to_failure:
            if issue["column"] in allowed_mismatch_columns.split(","):
                continue
            print(f"\n✗ Comparison failed due to issue: {issue['message']}")
        exit(1)
    exit(0)


if __name__ == "__main__":
    main()
