"""Standalone utilities for verification scripts - compatible with numpy<2."""
import numpy as np
import h5py
from astropy.table import Table, vstack
from typing import List
from functools import partial
from multiprocessing import Pool


def _file_to_catalog(filename: str, keys: List[str]):
    with h5py.File(filename, "r") as data:
        table_data = {}
        for k in keys:
            value = data[k][()]  # Get the value
            # Convert bytes dtype to unicode for astropy compatibility
            if hasattr(value, "dtype") and value.dtype.kind == "S":
                # Wrap scalar in array and convert bytes to unicode
                table_data[k] = np.atleast_1d(value).astype("U")
            else:
                table_data[k] = np.atleast_1d(value)
        return Table(table_data)


def get_catalog(
    dset,
    keys: List[str] = ["object_id", "ra", "dec", "healpix"],
    split: str = "train",
    num_proc: int = 1,
):
    """Return the catalog of a given Multimodal Universe parent sample.

    Args:
        dset (GeneratorBasedBuilder): An Multimodal Universe dataset builder.
        keys (List[str], optional): List of column names to include in the catalog.
        split (str, optional): The split of the dataset to retrieve the catalog from.
        num_proc (int, optional): Number of processes to use for parallel processing.

    Returns:
        astropy.table.Table: The catalog of the parent sample.
    """
    if not dset.config.data_files:
        raise ValueError(
            f"At least one data file must be specified, but got data_files={dset.config.data_files}"
        )
    catalogs = []
    if num_proc > 1:
        with Pool(num_proc) as pool:
            catalogs = pool.map(
                partial(_file_to_catalog, keys=keys), dset.config.data_files[split]
            )
    else:
        for filename in dset.config.data_files[split]:
            catalogs.append(_file_to_catalog(filename, keys=keys))
    return vstack(catalogs)
