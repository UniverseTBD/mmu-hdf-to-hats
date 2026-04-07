from __future__ import annotations

import h5py
import pyarrow as pa

from catalog_functions.cfa_transformer import CFATransformer
from catalog_functions.manga_transformer import MangaGroupReader
from catalog_functions.utils import BaseTransformer
from main import MMUReader


class DummyGroupedTransformer(BaseTransformer):
    def create_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("object_id", pa.string()),
                pa.field("ra", pa.float64()),
                pa.field("dec", pa.float64()),
            ]
        )

    def dataset_to_table(self, data) -> pa.Table:
        rows = []
        for group_name in data.keys():
            group = data[group_name]
            object_id = group["object_id"][()]
            if isinstance(object_id, bytes):
                object_id = object_id.decode("utf-8")
            rows.append(
                {
                    "object_id": str(object_id),
                    "ra": float(group["ra"][()]),
                    "dec": float(group["dec"][()]),
                }
            )
        return pa.Table.from_pylist(rows, schema=self.create_schema())


def create_grouped_hdf5(path):
    with h5py.File(path, "w") as h5_file:
        group = h5_file.create_group("8726-1901")
        group.create_dataset("object_id", data="8726-1901")
        group.create_dataset("ra", data=229.525725)
        group.create_dataset("dec", data=42.745853)

        group = h5_file.create_group("8323-12704")
        group.create_dataset("object_id", data="8323-12704")
        group.create_dataset("ra", data=157.112625)
        group.create_dataset("dec", data=25.523472)


def test_manga_group_reader_ra_dec_only(tmp_path):
    input_path = tmp_path / "manga.hdf5"
    create_grouped_hdf5(input_path)

    reader = MangaGroupReader(chunk_mb=128, transformer=DummyGroupedTransformer())
    tables = list(reader.read(input_path, read_columns=["ra", "dec"]))

    assert len(tables) == 1
    table = tables[0]
    assert table.num_rows == 2
    assert table.column_names == ["ra", "dec"]
    assert sorted(zip(table.column("ra").to_pylist(), table.column("dec").to_pylist())) == sorted(
        [(229.525725, 42.745853), (157.112625, 25.523472)]
    )


def test_manga_group_reader_full_read(tmp_path):
    input_path = tmp_path / "manga.hdf5"
    create_grouped_hdf5(input_path)

    reader = MangaGroupReader(chunk_mb=128, transformer=DummyGroupedTransformer())
    tables = list(reader.read(input_path))

    assert len(tables) == 1
    table = tables[0]
    assert table.num_rows == 2
    assert sorted(table.column("object_id").to_pylist()) == ["8323-12704", "8726-1901"]


def test_manga_group_reader_chunks_by_group(tmp_path):
    input_path = tmp_path / "manga.hdf5"
    create_grouped_hdf5(input_path)

    reader = MangaGroupReader(chunk_mb=1e-6, transformer=DummyGroupedTransformer())
    tables = list(reader.read(input_path, read_columns=["ra", "dec"]))

    assert len(tables) == 2
    assert [table.num_rows for table in tables] == [1, 1]
