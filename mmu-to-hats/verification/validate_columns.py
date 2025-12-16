"""
Validate HDF5/H5 file columns against expected catalog schema.

This script loads an HDF5 file and checks if its columns match the expected
columns defined in catalog_expected_columns.py.
"""

import click
import h5py
import sys
from pathlib import Path

# Add parent directory to path to import catalog_expected_columns
sys.path.insert(0, str(Path(__file__).parent.parent))
from catalog_expected_columns import CATALOG_EXPECTED_COLUMNS


def get_dataset_structure(hdf5_file):
    """
    Extract the structure of datasets from an HDF5 file.

    Returns a dictionary mapping object_id to its column structure.
    """
    with h5py.File(hdf5_file, "r") as f:
        # Try to get structure from first object
        if len(f.keys()) == 0:
            return None

        # Get the first key (could be an object_id or a dataset name)
        first_key = list(f.keys())[0]

        # Check if this is a group (like in manga) or direct datasets
        if isinstance(f[first_key], h5py.Group):
            # It's a group, extract structure from it
            return extract_group_structure(f[first_key])
        else:
            # It's a flat structure with datasets at the root
            return extract_flat_structure(f)


def extract_group_structure(group):
    """Extract column structure from an HDF5 group."""
    columns = []

    for key in group.keys():
        item = group[key]

        if isinstance(item, h5py.Dataset):
            # Check if it's a compound/structured dataset
            if item.dtype.names:
                # Compound dtype - like a sequence with named fields
                columns.append({key: list(item.dtype.names)})
            elif len(item.shape) > 1 or (len(item.shape) == 1 and item.shape[0] > 1):
                # Array/sequence - just note it exists
                columns.append({key: ["array"]})
            else:
                # Simple scalar value
                columns.append(key)
        elif isinstance(item, h5py.Group):
            # Nested group
            nested = extract_group_structure(item)
            if nested:
                columns.append({key: nested})

    return columns


def extract_flat_structure(hdf5_file):
    """Extract column structure from flat HDF5 file (datasets at root)."""
    columns = []

    for key in hdf5_file.keys():
        item = hdf5_file[key]

        if isinstance(item, h5py.Dataset):
            # Check the dtype and shape
            if item.dtype.names:
                # Compound dtype
                columns.append({key: list(item.dtype.names)})
            else:
                # Simple dataset
                columns.append(key)

    return columns


def normalize_column_list(columns):
    """
    Normalize a column list to a set of strings for easier comparison.
    Returns (flat_columns, nested_columns) where:
    - flat_columns is a set of top-level column names
    - nested_columns is a dict mapping nested column names to their sub-columns
    """
    flat_columns = set()
    nested_columns = {}

    for item in columns:
        if isinstance(item, dict):
            # Nested structure
            for key, value in item.items():
                nested_columns[key] = set(value) if isinstance(value, list) else value
        else:
            # Flat column
            flat_columns.add(item)

    return flat_columns, nested_columns


def compare_columns(actual, expected, catalog_name):
    """
    Compare actual columns against expected columns.

    Returns (missing, extra, nested_issues) where:
    - missing: columns in expected but not in actual
    - extra: columns in actual but not in expected
    - nested_issues: list of issues with nested columns
    """
    actual_flat, actual_nested = normalize_column_list(actual)
    expected_flat, expected_nested = normalize_column_list(expected)

    # Compare flat columns
    missing_flat = expected_flat - actual_flat
    extra_flat = actual_flat - expected_flat

    # Compare nested columns
    nested_issues = []

    # Check for missing nested columns
    for nested_name in expected_nested:
        if nested_name not in actual_nested:
            nested_issues.append(f"Missing nested column: {nested_name}")
        else:
            # Compare sub-columns
            expected_sub = expected_nested[nested_name]
            actual_sub = actual_nested[nested_name]

            if isinstance(expected_sub, set) and isinstance(actual_sub, set):
                missing_sub = expected_sub - actual_sub
                extra_sub = actual_sub - expected_sub

                if missing_sub:
                    nested_issues.append(
                        f"Nested column '{nested_name}' missing sub-columns: {missing_sub}"
                    )
                if extra_sub:
                    nested_issues.append(
                        f"Nested column '{nested_name}' has extra sub-columns: {extra_sub}"
                    )

    # Check for extra nested columns
    for nested_name in actual_nested:
        if nested_name not in expected_nested:
            nested_issues.append(f"Extra nested column: {nested_name}")

    return missing_flat, extra_flat, nested_issues


@click.command()
@click.argument("file_path", type=click.Path(exists=True))
@click.option(
    "--catalog",
    type=click.Choice(list(CATALOG_EXPECTED_COLUMNS.keys()), case_sensitive=False),
    required=True,
    help="Catalog name to validate against",
)
def main(file_path, catalog):
    """
    Validate HDF5/H5 file columns against expected catalog schema.

    Examples:

        # Validate a YSE file
        python validate_columns.py data/yse/healpix=0/file.hdf5 --catalog yse

        # Validate a GAIA file in strict mode
        python validate_columns.py data/gaia/healpix=0/file.hdf5 --catalog gaia --strict
    """
    click.echo(f"Validating: {file_path}")
    click.echo(f"Catalog: {catalog}")
    click.echo()

    # Get expected columns
    expected = CATALOG_EXPECTED_COLUMNS.get(catalog)
    if expected is None:
        click.echo(
            f"Error: No expected columns defined for catalog '{catalog}'", err=True
        )
        sys.exit(1)

    # Load actual structure from file
    try:
        actual = get_dataset_structure(file_path)
        if actual is None:
            click.echo("Error: File is empty or has no datasets", err=True)
            sys.exit(1)
    except Exception as e:
        click.echo(f"Error reading file: {e}", err=True)
        sys.exit(1)

    # Compare
    missing_flat, extra_flat, nested_issues = compare_columns(actual, expected, catalog)

    # Report results
    has_issues = False

    if missing_flat:
        has_issues = True
        click.echo("❌ Missing expected columns:")
        for col in sorted(missing_flat):
            click.echo(f"  - {col}")
        click.echo()

    if extra_flat:
        has_issues = True
        click.echo("⚠️  Extra columns (not in expected schema):")
        for col in sorted(extra_flat):
            click.echo(f"  + {col}")
        click.echo()

    if nested_issues:
        has_issues = True
        click.echo("⚠️  Nested column issues:")
        for issue in nested_issues:
            click.echo(f"  ! {issue}")
        click.echo()

    if not has_issues:
        click.echo("✅ All columns match expected schema!")
    else:
        click.echo("Summary:")
        click.echo(f"  Missing columns: {len(missing_flat)}")
        click.echo(f"  Extra columns: {len(extra_flat)}")
        click.echo(f"  Nested issues: {len(nested_issues)}")
    sys.exit(0)


if __name__ == "__main__":
    main()
