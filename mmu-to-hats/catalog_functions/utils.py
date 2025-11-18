from abc import ABC, abstractmethod
from pathlib import Path
from typing import Union

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

    def transform_from_hdf5(
        self, hdf5_file_path: Union[str, Path, "UPath"]
    ) -> pa.Table:
        """Transform HDF5 file to PyArrow table."""
        with h5py.File(hdf5_file_path, "r") as data:
            return self.dataset_to_table(data)
