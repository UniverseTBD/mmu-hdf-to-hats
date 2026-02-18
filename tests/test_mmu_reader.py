from main import MMUReader
from catalog_functions.csp_transformer import CSPTransformer
from catalog_functions.cfa_transformer import CFATransformer


def test_mmu_reader_cfa():
    mmu = MMUReader(chunk_mb=128, transform_klass=CFATransformer)
    tables = [t for t in mmu.read("tests/data/cfa/SN2007bc.hdf5")]

    # Single-object file should produce exactly one table chunk
    assert len(tables) == 1
    table = tables[0]

    # Should have exactly 1 row (one supernova per file)
    assert table.num_rows == 1

    # Check schema has the expected columns
    column_names = set(table.column_names)
    assert column_names == {'dec', 'ra', 'obj_type', 'object_id', 'lightcurve'}
    # Verify object_id value
    assert table.column("object_id")[0].as_py() == "SN2007bc"


def test_mmu_reader_csp():
    mmu = MMUReader(chunk_mb=128, transform_klass=CSPTransformer)
    tables = [t for t in mmu.read("tests/data/csp/example_SN2004dt.hdf5")]

    # Single-object file should produce exactly one table chunk
    assert len(tables) == 1
    table = tables[0]

    # Should have exactly 1 row (one supernova per file)
    assert table.num_rows == 1

    # Check schema has the expected columns
    column_names = set(table.column_names)
    assert "object_id" in column_names
    assert "lightcurve" in column_names
    assert "ra" in column_names
    assert "dec" in column_names
    assert "redshift" in column_names
    assert "spec_class" in column_names

    # Verify object_id value
    assert table.column("object_id")[0].as_py() == "SN2004dt"

    # Verify coordinates
    assert abs(table.column("ra")[0].as_py() - 30.553207) < 1e-4
    assert abs(table.column("dec")[0].as_py() - (-0.097639)) < 1e-4

    # Verify spec_class
    assert table.column("spec_class")[0].as_py() == "SN Ia"

    # Verify lightcurve struct has the right fields
    lc = table.column("lightcurve")[0].as_py()
    assert set(lc.keys()) == {"band", "time", "mag", "mag_err"}

    # 12 bands x 46 time points = 552 flattened entries
    assert len(lc["band"]) == 12 * 46
    assert len(lc["time"]) == 12 * 46
    assert len(lc["mag"]) == 12 * 46
    assert len(lc["mag_err"]) == 12 * 46

    # First band should be 'B' (repeated 46 times)
    assert lc["band"][0] == "B"
    assert lc["band"][45] == "B"
    # Second band should be 'H'
    assert lc["band"][46] == "H"


def test_mmu_reader_ra_dec_only():
    """When read_columns=['ra', 'dec'], should return a simple table with coordinates."""
    mmu = MMUReader(chunk_mb=128, transform_klass=CSPTransformer)
    tables = [t for t in mmu.read("tests/data/csp/example_SN2004dt.hdf5", read_columns=["ra", "dec"])]

    assert len(tables) == 1
    table = tables[0]

    assert table.num_rows == 1
    assert set(table.column_names) == {"ra", "dec"}
    assert abs(table.column("ra")[0].as_py() - 30.553207) < 1e-4
    assert abs(table.column("dec")[0].as_py() - (-0.097639)) < 1e-4
