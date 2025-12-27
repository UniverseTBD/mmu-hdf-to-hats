from verification.compare import compare_tables
import pyarrow as pa
import numpy as np


def test_compare_basic():
    table1 = pa.table(
        {
            "a": pa.array([1, 2, 3]),
            "ra": pa.array([10.0, 20.0, 30.0]),
            "dec": pa.array([-10.0, -20.0, -30.0]),
        }
    )
    table2 = pa.table(
        {
            "a": pa.array([1, 2, 4]),
            "ra": pa.array([10.0, 20.0, 30.0]),
            "dec": pa.array([-10.0, -20.0, -30.0]),
        }
    )
    issues = compare_tables(table1, table2, label1="Table 1", label2="Table 2")
    assert len(issues) == 1
    assert issues[0]["samples"][0]["left"] == 3
    assert issues[0]["samples"][0]["right"] == 4


def test_compare_basic_float32():
    table1 = pa.table(
        {
            "a": pa.array([1, 2, 3]),
            "ra": pa.array([10.0, 20.0, 30.0]),
            "dec": pa.array([-10.0, -20.0, -30.0]).cast(pa.float32()),
        }
    )
    table2 = pa.table(
        {
            "a": pa.array([1, 2, 4]),
            "ra": pa.array([10.0, 20.0, 30.0]),
            "dec": pa.array([-10.0, -20.0, -30.0]).cast(pa.float32()),
        }
    )
    issues = compare_tables(table1, table2, label1="Table 1", label2="Table 2")
    assert len(issues) == 2
    assert issues[0]["type"] == "column_type_mismatch"
    assert issues[0]["message"] == "Column 'dec' is of type float, expected float64"
    assert issues[1]["samples"][0]["left"] == 3
    assert issues[1]["samples"][0]["right"] == 4
    assert issues[1]["type"] == "column_values"
    assert issues[1]["message"] == "Column 'a' has differences"
    assert (
        issues[0]["message"]
        == "Column 'dec' is of type float, expected float64"
    )


def test_compare_multiple_columns():
    table1 = pa.table({"a": pa.array([1, 2, 3]), "b": pa.array([4, 5, 6]), "ra": pa.array([10.0, 20.0, 30.0]), "dec": pa.array([-10.0, -20.0, -30.0])})
    table2 = pa.table({"a": pa.array([1, 2, 4]), "b": pa.array([4, 5, 6]), "ra": pa.array([10.0, 20.0, 30.0]).cast(pa.float64()), "dec": pa.array([-10.0, -20.0, -30.0]).cast(pa.float64())})
    issues = compare_tables(table1, table2, label1="Table 1", label2="Table 2")
    assert len(issues) == 1

def test_compare_multiple_columns_nans():
    table1 = pa.table({"a": pa.array([1, 2, 3]), "b": pa.array([4, 5, np.nan]), "ra": pa.array([10.0, 20.0, 30.0]), "dec": pa.array([-10.0, -20.0, -30.0])})
    table2 = pa.table({"a": pa.array([1, 2, 3]), "b": pa.array([4, 5, np.nan]), "ra": pa.array([10.0, 20.0, 30.0]).cast(pa.float64()), "dec": pa.array([-10.0, -20.0, -30.0]).cast(pa.float64())})
    issues = compare_tables(table1, table2, label1="Table 1", label2="Table 2")
    assert len(issues) == 0

def test_compare_multiple_columns_floating():
    table1 = pa.table({"a": pa.array([1.0, 2.0, 3.0]), "b": pa.array([4.0, 5.0, 6.0]), "ra": pa.array([10.0, 20.0, 30.0]), "dec": pa.array([-10.0, -20.0, -30.0])})
    table2 = pa.table({"a": pa.array([1.0, 2.0, 4.0]), "b": pa.array([4.0, 5.0, 6.0]), "ra": pa.array([10.0, 20.0, 30.0]).cast(pa.float64()), "dec": pa.array([-10.0, -20.0, -30.0]).cast(pa.float64())})
    issues = compare_tables(table1, table2, label1="Table 1", label2="Table 2")
    assert len(issues) == 1


def test_compare_no_sort_column():
    table1 = pa.table(
        {
            "lightcurve": pa.array(
                [
                    {"time": [1.0, 2.0], "flux": [10.0, 20.0], "flux_err": [0.1, 0.2]},
                    {"time": [3.0, 4.0], "flux": [30.0, 40.0], "flux_err": [0.3, 0.4]},
                ]
            ),
            # "ra": pa.array([10.0, 20.0]),
            # "dec": pa.array([-10.0, -20.0]),
        }
    )
    table2 = pa.table(
        {
            "lightcurve": pa.array(
                [
                    {"time": [1.0, 2.0], "flux": [10.0, 25.0], "flux_err": [0.1, 0.2]},
                    {"time": [3.0, 4.0], "flux": [30.0, 40.0], "flux_err": [0.3, 0.4]},
                ]
            ),
            # "ra": pa.array([10.0, 20.0]).cast(pa.float64()),
            # "dec": pa.array([-10.0, -20.0]).cast(pa.float64()),
        }
    )
    issues = compare_tables(table1, table2, label1="Table 1", label2="Table 2")
    assert len(issues) == 3  # 2 issues for ra, dec only for table 2 + 1 sorting issue
    assert issues[-1]["type"] == "sorting"


def test_preferred_sort_column():
    table1 = pa.table(
        {"object_id": pa.array(["1", "2", "3"]), "b": pa.array([4.0, 5.0, 6.0])}
    )
    table2 = pa.table(
        {"object_id": pa.array(["1", "2", "4"]), "b": pa.array([4.0, 5.0, 6.0])}
    )
    issues = compare_tables(table1, table2, label1="Table 1", label2="Table 2")
    assert len(issues) == 3  # 1 issue for object_id mismatch + 2 issues for missing ra, dec for table2


def test_col_only_in_one_table():
    table1 = pa.table({"a": pa.array([1, 2, 3]), "b": pa.array([4, 5, 6])})
    table2 = pa.table({"a": pa.array([1, 2, 3]), "c": pa.array([7, 8, 9])})
    issues = compare_tables(table1, table2, label1="Table 1", label2="Table 2")
    assert len(issues) == 5
    assert issues[0] == {'type': 'schema', 'column': None, 'message': "Fields in Table 1 but not in Table 2: ['b']\nFields in Table 2 but not in Table 1: ['c']", 'samples': []}
    assert issues[1] == {'type': 'missing_column', 'message': "Column 'ra' not found", 'column': 'ra', 'samples': None, 'table': 'Table 2'}
    assert issues[2] == {'type': 'missing_column', 'message': "Column 'dec' not found", 'column': 'dec', 'samples': None, 'table': 'Table 2'}
    assert issues[3] == {'type': 'columns', 'column': None, 'message': "Table 1 has additional columns: ['b']", 'table': 'Table 1'}
    assert issues[4] == {'type': 'columns', 'column': None, 'message': "Table 2 has additional columns: ['c']", 'table': 'Table 2'}


def test_compare_unequal_rows():
    table1 = pa.table({"a": pa.array([1, 2, 3]), "ra": pa.array([10.0, 20.0, 30.0]), "dec": pa.array([-10.0, -20.0, -30.0])})
    table2 = pa.table({"a": pa.array([1, 2, 3, 4]), "ra": pa.array([10.0, 20.0, 30.0, 40.0]), "dec": pa.array([-10.0, -20.0, -30.0, -40.0])})
    label1 = "t1"
    label2 = "t2"
    issues = compare_tables(table1, table2, label1="t1", label2="t2")
    assert len(issues) == 1
    assert issues[-1]["type"] == "row_count"
    assert (
        issues[-1]["message"]
        == f"Row count mismatch: {label1} has {table1.num_rows} rows, {label2} has {table2.num_rows} rows"
    )


def test_compare_nested_mismatch():
    table1 = pa.table(
        {
            # needed since we sort! This is an assumption that there is always at least one non-nested column to sort on
            "index": pa.array([0, 1]),
            "lightcurve": pa.array(
                [
                    {"time": [1.0, 2.0], "flux": [10.0, 20.0], "flux_err": [0.1, 0.2]},
                    {"time": [3.0, 4.0], "flux": [30.0, 40.0], "flux_err": [0.3, 0.4]},
                ]
            ),
        }
    )
    table2 = pa.table(
        {
            "index": pa.array([0, 1]),
            "lightcurve": pa.array(
                [
                    {"time": [1.0, 2.0], "flux": [10.0, 25.0], "flux_err": [0.1, 0.2]},
                    {"time": [3.0, 4.0], "flux": [30.0, 40.0], "flux_err": [0.3, 0.4]},
                ]
            ),
        }
    )
    issues = compare_tables(table1, table2, label1="Table 1", label2="Table 2")
    assert len(issues) == 3  # 4 issues for missing ra, dec and 1 for flux mismatch


def test_compare_nested_passing():
    table1 = pa.table(
        {
            "index": pa.array([0, 1]),
            "lightcurve": pa.array(
                [
                    {"time": [1.0, 2.0], "flux": [10.0, 25.0], "flux_err": [0.1, 0.2]},
                    {"time": [3.0, 4.0], "flux": [30.0, 40.0], "flux_err": [0.3, 0.4]},
                ]
            ),
            "ra": pa.array([10.0, 20.0]),
            "dec": pa.array([-10.0, -20.0]),
        }
    )
    table2 = pa.table(
        {
            "index": pa.array([0, 1]),
            "lightcurve": pa.array(
                [
                    {"time": [1.0, 2.0], "flux": [10.0, 25.0], "flux_err": [0.1, 0.2]},
                    {"time": [3.0, 4.0], "flux": [30.0, 40.0], "flux_err": [0.3, 0.4]},
                ]
            ),
            "ra": pa.array([10.0, 20.0]),
            "dec": pa.array([-10.0, -20.0]),
        }
    )
    issues = compare_tables(table1, table2, label1="Table 1", label2="Table 2")
    assert len(issues) == 0


def test_compare_nested_vs_unnested():
    nested_col = pa.table(
        {
            "index": pa.array([0, 1]),
            "lightcurve": pa.array(
                [
                    {"time": [1.0, 2.0], "flux": [10.0, 25.0], "flux_err": [0.1, 0.2]},
                    {"time": [3.0, 4.0], "flux": [30.0, 40.0], "flux_err": [0.3, 0.4]},
                ]
            )
        }
    )
    unnested_col = pa.table(
        {
            "index": pa.array([0, 1]),
            "time": pa.array([[1.0, 2.0], [3.0, 4.0]], type=pa.list_(pa.float32())),
            "flux": pa.array([[10.0, 25.0], [30.0, 40.0]], type=pa.list_(pa.float32())),
            "flux_err": pa.array([[0.1, 0.2], [0.3, 0.4]], type=pa.list_(pa.float32())),
        }
    )
    issues = compare_tables(nested_col, unnested_col, label1="Table 1", label2="Table 2")
    assert len(issues) == 5
    assert issues[0] == {'type': 'schema', 'column': None, 'message': "Fields in Table 1 but not in Table 2: ['lightcurve', 'lightcurve.flux', 'lightcurve.flux_err', 'lightcurve.time']\nFields in Table 2 but not in Table 1: ['flux', 'flux_err', 'time']", 'samples': []}
    assert issues[1] == {'type': 'missing_column', 'message': "Column 'ra' not found", 'column': 'ra', 'samples': None, 'table': 'Table 2'}
    assert issues[2] == {'type': 'missing_column', 'message': "Column 'dec' not found", 'column': 'dec', 'samples': None, 'table': 'Table 2'}
    assert issues[3] == {'type': 'columns', 'column': None, 'message': "Table 1 has additional columns: ['lightcurve.flux', 'lightcurve.flux_err', 'lightcurve.time']", 'table': 'Table 1'}
    assert issues[4] == {'type': 'columns', 'column': None, 'message': "Table 2 has additional columns: ['flux', 'flux_err', 'time']", 'table': 'Table 2'}
