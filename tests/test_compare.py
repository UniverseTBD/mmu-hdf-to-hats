from verification.compare import compare_tables
import pyarrow as pa


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
            "dec": pa.array([-10.0, -20.0, -30.0]),
        }
    )
    issues = compare_tables(table1, table2, label1="Table 1", label2="Table 2")
    assert len(issues) == 2
    assert issues[1]["samples"][0]["left"] == 3
    assert issues[1]["samples"][0]["right"] == 4
    assert issues[0]["type"] == "column_type_mismatch"
    assert (
        issues[0]["message"]
        == "Column 'dec' is of type float, expected float64"
    )


def test_compare_multiple_columns():
    table1 = pa.table({"a": pa.array([1, 2, 3]), "b": pa.array([4, 5, 6]), "ra": pa.array([10.0, 20.0, 30.0]), "dec": pa.array([-10.0, -20.0, -30.0])})
    table2 = pa.table({"a": pa.array([1, 2, 4]), "b": pa.array([4, 5, 6]), "ra": pa.array([10.0, 20.0, 30.0]), "dec": pa.array([-10.0, -20.0, -30.0])})
    issues = compare_tables(table1, table2, label1="Table 1", label2="Table 2")
    assert len(issues) == 1


def test_compare_multiple_columns_floating():
    table1 = pa.table({"a": pa.array([1.0, 2.0, 3.0]), "b": pa.array([4.0, 5.0, 6.0]), "ra": pa.array([10.0, 20.0, 30.0]), "dec": pa.array([-10.0, -20.0, -30.0])})
    table2 = pa.table({"a": pa.array([1.0, 2.0, 4.0]), "b": pa.array([4.0, 5.0, 6.0]), "ra": pa.array([10.0, 20.0, 30.0]), "dec": pa.array([-10.0, -20.0, -30.0])})
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
        }
    )
    issues = compare_tables(table1, table2, label1="Table 1", label2="Table 2")
    assert len(issues) == 5  # 4 issues for ra, dec for each table + 1 sorting issue
    assert issues[-1]["type"] == "sorting"


def test_preferred_sort_column():
    table1 = pa.table(
        {"object_id": pa.array(["1", "2", "3"]), "b": pa.array([4.0, 5.0, 6.0])}
    )
    table2 = pa.table(
        {"object_id": pa.array(["1", "2", "4"]), "b": pa.array([4.0, 5.0, 6.0])}
    )
    issues = compare_tables(table1, table2, label1="Table 1", label2="Table 2")
    assert len(issues) == 5  # 1 issue for object_id mismatch + 4 issues for missing ra, dec per table


def test_col_only_in_one_table():
    table1 = pa.table({"a": pa.array([1, 2, 3]), "b": pa.array([4, 5, 6])})
    table2 = pa.table({"a": pa.array([1, 2, 3]), "c": pa.array([7, 8, 9])})
    issues = compare_tables(table1, table2, label1="Table 1", label2="Table 2")
    assert len(issues) == 6  # one for missing 'b' and one for missing 'c'
    assert issues[-2]["type"] == "columns"
    assert issues[-1]["type"] == "columns"


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
    assert len(issues) == 5  # 4 issues for missing ra, dec and 1 for flux mismatch


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
