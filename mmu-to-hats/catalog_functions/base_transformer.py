import h5py
import pyarrow as pa
import numpy as np
from catalog_functions.utils import np_to_pyarrow_array
from abc import ABC, abstractmethod



class BaseTransformer(ABC):
    """Transforms catalog HDF5 files to PyArrow tables with proper schema."""

    @abstractmethod
    @classmethod
    def create_schema(cls) -> pa.schema:
        """Create the output PyArrow schema."""
        pass

    @abstractmethod
    @classmethod
    def dataset_to_table(cls, data: dict[str, h5py.Dataset] | "File") -> pa.Table:
        pass

    @classmethod
    def transform_from_hdf5(cls, hdf5_file_path: str | "Upath" | "pathlib.Path") -> pa.Table:
        with h5py.File(hdf5_file_path, "r") as data:
            return cls.dataset_to_table(data)
