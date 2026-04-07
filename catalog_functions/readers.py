from __future__ import annotations

import math
from typing import TYPE_CHECKING

import h5py
import numpy as np
import pyarrow as pa
from hats_import.catalog.file_readers import InputReader
from upath import UPath

from catalog_functions.utils import np_to_pyarrow_array

if TYPE_CHECKING:
    from catalog_functions.utils import BaseTransformer


def decode_if_needed(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


class MMUReader(InputReader):
    """Default reader for flat MMU HDF5 files with top-level datasets."""

    def __init__(
        self,
        chunk_mb: float,
        transformer: "BaseTransformer" | None = None,
        transform_klass=None,
    ):
        super().__init__()
        self.chunk_bytes = chunk_mb * 1024 * 1024
        if transformer is not None and transform_klass is not None:
            raise ValueError("Pass either transformer or transform_klass, not both")
        if transformer is None and transform_klass is None:
            raise ValueError("transformer or transform_klass is required")
        self.transformer = transformer if transformer is not None else transform_klass()

    def _get_h5_column(self, h5_file, col_name):
        """Get column from HDF5 file, checking both cases for ra/dec."""
        if col_name in h5_file:
            return col_name
        if col_name.lower() in ("ra", "dec"):
            upper_name = col_name.upper()
            if upper_name in h5_file:
                return upper_name
        raise KeyError(
            f"Column '{col_name}' not found in HDF5 file. "
            f"Available columns: {list(h5_file.keys())}"
        )

    def _num_chunks(self, upath, h5_file: h5py.File, columns: list[str] | None) -> int:
        if columns is None:
            size = upath.stat().st_size
        else:
            size = sum(
                h5_file[self._get_h5_column(h5_file, col)].nbytes for col in columns
            )
        return max(1, int(math.ceil(size / self.chunk_bytes)))

    def read(self, input_file: str, read_columns: list[str] | None = None):
        upath = UPath(input_file)
        cols_scalar = {}
        with upath.open("rb") as fh, h5py.File(fh) as h5_file:
            num_chunks = self._num_chunks(upath, h5_file, read_columns)
            if read_columns is None:
                read_columns = list(h5_file)
            first_col_name = self._get_h5_column(h5_file, read_columns[0])
            shape = h5_file[first_col_name].shape
            if shape == ():
                cols_scalar[read_columns[0]] = True
                n_rows = 1
            else:
                cols_scalar[read_columns[0]] = False
                n_rows = shape[0]
            for col in read_columns[1:]:
                col_name = self._get_h5_column(h5_file, col)
                shape = h5_file[col_name].shape
                if shape == ():
                    cols_scalar[col] = True
                else:
                    cols_scalar[col] = False
            chunk_size = max(1, n_rows // num_chunks)
            for i in range(0, n_rows, chunk_size):
                if {col.lower() for col in read_columns} == {"ra", "dec"}:
                    if cols_scalar[first_col_name]:
                        data = {
                            col: np_to_pyarrow_array(
                                np.array([h5_file[self._get_h5_column(h5_file, col)][()]])
                            )
                            for col in read_columns
                        }
                    else:
                        data = {
                            col: np_to_pyarrow_array(
                                h5_file[self._get_h5_column(h5_file, col)][i : i + chunk_size]
                            )
                            for col in read_columns
                        }
                    yield pa.table(data)
                    continue

                if cols_scalar[first_col_name]:
                    data = h5_file
                else:
                    data = {}
                    for col in read_columns:
                        if cols_scalar[col]:
                            data[col] = h5_file[col][()]
                        else:
                            data[col] = h5_file[col][i : i + chunk_size]
                yield self.transformer.dataset_to_table(data)


class MangaGroupReader(InputReader):
    """Reader for MaNGA HDF5 files with one object group per top-level key."""

    def __init__(self, chunk_mb: float, transformer: "BaseTransformer"):
        super().__init__()
        self.chunk_bytes = chunk_mb * 1024 * 1024
        self.transformer = transformer

    @staticmethod
    def _group_value(group: h5py.Group, column: str):
        candidates = (column, column.upper())
        for candidate in candidates:
            if candidate in group:
                return decode_if_needed(group[candidate][()])
        raise KeyError(
            f"Column '{column}' not found in group '{group.name}'. "
            f"Available columns: {list(group.keys())}"
        )

    def _group_name_chunks(self, upath: UPath, h5_file: h5py.File) -> list[list[str]]:
        group_names = list(h5_file.keys())
        if not group_names:
            return []

        num_chunks = max(1, int(math.ceil(upath.stat().st_size / self.chunk_bytes)))
        chunk_size = max(1, int(math.ceil(len(group_names) / num_chunks)))
        return [
            group_names[start : start + chunk_size]
            for start in range(0, len(group_names), chunk_size)
        ]

    def read(self, input_file: str, read_columns: list[str] | None = None):
        upath = UPath(input_file)
        with upath.open("rb") as fh, h5py.File(fh) as h5_file:
            for group_names in self._group_name_chunks(upath, h5_file):
                if read_columns is not None:
                    scalar_rows: dict[str, list] = {column: [] for column in read_columns}
                    for group_name in group_names:
                        group = h5_file[group_name]
                        for column in read_columns:
                            scalar_rows[column].append(self._group_value(group, column))
                    yield pa.table(scalar_rows)
                    continue

                grouped_data = {group_name: h5_file[group_name] for group_name in group_names}
                yield self.transformer.dataset_to_table(grouped_data)
