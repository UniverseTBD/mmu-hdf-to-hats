import pyarrow as pa
import pyarrow.compute as pc
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


def compare_nested_list_column(col1, col2, col_name, col_type):
    """
    Compare nested list columns using PyArrow native operations.

    Returns:
        dict: {
            'field_name': (columns_equal: bool, sample_data: list),
            ...
        }
        If the column has no struct fields, returns a single key '' with the overall comparison.
    """
    # if col_name == "image.array":
    #     breakpoint()
    # If not a list or not a struct, do simple comparison
    if not pa.types.is_list(col_type):
        if col1.equals(col2):
            return {"": (True, [])}

        list1 = col1[:5].to_pylist()
        list2 = col2[:5].to_pylist()
        mismatch_indices = [
            i for i in range(min(len(list1), len(list2))) if list1[i] != list2[i]
        ]
        sample_data = [
            {
                "index": i,
                "left": truncate_long_arrays(list1[i]),
                "right": truncate_long_arrays(list2[i]),
            }
            for i in mismatch_indices[:3]
        ]
        return {"": (False, sample_data)}

    value_type = col_type.value_type

    if not pa.types.is_struct(value_type):
        # Non-struct list, do simple comparison
        # Handle extension types by casting to storage type
        col1_compare = col1
        col2_compare = col2

        # Check if the element type (value_type) is an extension type
        col1_value_type = col_type.value_type
        col2_value_type = col2.type.value_type

        # Check for type mismatch (one extension, one not)
        has_ext1 = hasattr(col1_value_type, "storage_type")
        has_ext2 = hasattr(col2_value_type, "storage_type")

        if has_ext1 != has_ext2:
            # Type mismatch: one is extension type, other is not
            return {"": (False, [{
                "error": "TYPE_MISMATCH",
                "left_type": str(col1_value_type),
                "right_type": str(col2_value_type),
                "message": f"Extension type mismatch: left={col1_value_type}, right={col2_value_type}"
            }])}

        # If both or neither have extension types, try to compare
        if has_ext1:
            # Cast list<extension> to list<storage>
            storage_type1 = pa.list_(col1_value_type.storage_type)
            col1_compare = col1.cast(storage_type1)
        if has_ext2:
            storage_type2 = pa.list_(col2_value_type.storage_type)
            col2_compare = col2.cast(storage_type2)

        # Try direct equality first
        if col1_compare.equals(col2_compare):
            return {"": (True, [])}

        # For numeric types, use NaN-aware comparison
        # Check if the value type is a numeric type that might contain NaN
        is_float_type = pa.types.is_floating(col1_value_type) or pa.types.is_floating(col2_value_type)

        if is_float_type:
            # Convert to numpy for NaN-aware comparison
            try:
                import numpy as np
                # Flatten both columns to 1D arrays for comparison
                arr1_flat = pc.list_flatten(col1_compare).to_numpy()
                arr2_flat = pc.list_flatten(col2_compare).to_numpy()

                if np.allclose(arr1_flat, arr2_flat, rtol=1e-5, atol=1e-8, equal_nan=True):
                    return {"": (True, [])}
            except Exception:
                pass  # Fall through to Python list comparison

        list1 = col1[:5].to_pylist()
        list2 = col2[:5].to_pylist()
        mismatch_indices = [
            i for i in range(min(len(list1), len(list2))) if list1[i] != list2[i]
        ]
        sample_data = [
            {
                "index": i,
                "left": truncate_long_arrays(list1[i]),
                "right": truncate_long_arrays(list2[i]),
            }
            for i in mismatch_indices[:3]
        ]
        return {"": (False, sample_data)}

    # Compare each struct field separately using PyArrow native ops
    field_results = {}

    for field in value_type:
        field_name = field.name

        try:
            # Extract field from nested struct using PyArrow compute
            flattened1 = pc.list_flatten(col1)
            flattened2 = pc.list_flatten(col2)

            field1 = pc.struct_field(flattened1, field_name)
            field2 = pc.struct_field(flattened2, field_name)

            # Handle extension types by casting to storage type for comparison
            type1 = field1.type
            type2 = field2.type

            # Check if either is an extension type
            if hasattr(type1, "storage_type") or hasattr(type2, "storage_type"):
                # Cast both to storage type to compare underlying values
                if hasattr(type1, "storage_type"):
                    field1 = field1.cast(type1.storage_type)
                if hasattr(type2, "storage_type"):
                    field2 = field2.cast(type2.storage_type)

            if field1.equals(field2):
                field_results[field_name] = (True, [])
            else:
                # Convert only first 5 rows to Python for samples
                # Need to extract this field from each row
                col1_slice = col1[:5]
                col2_slice = col2[:5]

                flattened1_slice = pc.list_flatten(col1_slice)
                flattened2_slice = pc.list_flatten(col2_slice)

                field1_slice = pc.struct_field(flattened1_slice, field_name).to_pylist()
                field2_slice = pc.struct_field(flattened2_slice, field_name).to_pylist()

                # Find mismatches
                mismatch_indices = [
                    i
                    for i in range(min(len(field1_slice), len(field2_slice)))
                    if field1_slice[i] != field2_slice[i]
                ]

                sample_data = [
                    {
                        "index": i,
                        "left": truncate_long_arrays(field1_slice[i]),
                        "right": truncate_long_arrays(field2_slice[i]),
                    }
                    for i in mismatch_indices[:3]
                ]

                field_results[field_name] = (False, sample_data)

        except Exception as e:
            # If we can't extract the field, mark as mismatch
            field_results[field_name] = (False, [{"error": str(e)}])

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
