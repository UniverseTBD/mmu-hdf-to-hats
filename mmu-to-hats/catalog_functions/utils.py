from abc import ABC, abstractmethod
from pathlib import Path
from typing import Union, List

import h5py
import numpy as np
import pyarrow as pa
from upath import UPath


def np_to_pyarrow_array(array: np.ndarray) -> pa.Array:
    """Massively copy-pasted from hats_import.catalog.file_reader.fits._np_to_pyarrow_array
    https://github.com/astronomy-commons/hats-import/blob/e9c7b647dae309ced9f9ce2916692c2aecde2612/src/hats_import/catalog/file_readers/fits.py#L9
    """
    values = pa.array(array.reshape(-1))
    # "Base" type
    if array.ndim == 1:
        return values
    if array.ndim > 2:
        raise ValueError("Only 1D and 2D arrays are supported")
    n_lists, length = array.shape
    offsets = np.arange(0, (n_lists + 1) * length, length, dtype=np.int32)
    return pa.ListArray.from_arrays(values=values, offsets=offsets)


class BaseTransformer(ABC):
    """Transforms catalog HDF5 files to PyArrow tables with proper schema."""

    @abstractmethod
    def create_schema(self) -> pa.Schema:
        """Create the output PyArrow schema."""
        pass

    @abstractmethod
    def dataset_to_table(
        self, data: Union[dict[str, h5py.Dataset], h5py.File]
    ) -> pa.Table:
        """Transform HDF5 data (dict of arrays or h5py.File) to PyArrow table."""
        pass

    def transform_from_hdf5_file(self, hdf5_file_path: Union[str, Path, "UPath"]):
        with h5py.File(hdf5_file_path, "r") as data:
            return self.dataset_to_table(data)

    def _check_if_directory(self, path: Union[str, Path, "UPath"]) -> bool:
        p = UPath(path)
        return p.is_dir()

    def transform_from_hdf5(
        self, hdf5_file_path: List[Union[str, Path, "UPath"]] | Union[str, Path, "UPath"]
    ) -> pa.Table:
        """Transform HDF5 file to PyArrow table."""
        if self._check_if_directory(hdf5_file_path):
            # list all files in the dir
            suffixes = {".h5", ".hdf5"}
            hdf5_file_path = sorted(p for p in UPath(hdf5_file_path).glob("*.h*5") if p.suffix in suffixes)
        if isinstance(hdf5_file_path, (str, Path, UPath)):
            return self.transform_from_hdf5_file(hdf5_file_path)
        elif isinstance(hdf5_file_path, list):
            tables = []
            for file_path in hdf5_file_path:
                table = self.transform_from_hdf5_file(file_path)
                tables.append(table)
            return pa.concat_tables(tables)
        else:
            raise ValueError("Invalid type for hdf5_file_path:", type(hdf5_file_path))
