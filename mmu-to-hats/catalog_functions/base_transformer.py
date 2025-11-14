import h5py
import pyarrow as pa
import numpy as np
from pathlib import Path
from typing import Union
from catalog_functions.utils import np_to_pyarrow_array
from abc import ABC, abstractmethod

from upath import UPath


class BaseTransformer(ABC):
    """Transforms catalog HDF5 files to PyArrow tables with proper schema."""

    @abstractmethod
    def create_schema(self) -> pa.Schema:
        """Create the output PyArrow schema."""
        pass

    @abstractmethod
    def dataset_to_table(self, data: Union[dict[str, h5py.Dataset], h5py.File]) -> pa.Table:
        """Transform HDF5 data (dict of arrays or h5py.File) to PyArrow table."""
        pass

    def transform_from_hdf5(self, hdf5_file_path: Union[str, Path, "UPath"]) -> pa.Table:
        """Transform HDF5 file to PyArrow table."""
        with h5py.File(hdf5_file_path, "r") as data:
            return self.dataset_to_table(data)
