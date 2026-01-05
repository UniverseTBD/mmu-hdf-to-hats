"""
YSETransformer: Clean class-based transformation from HDF5 to PyArrow tables.
"""

import pyarrow as pa
import numpy as np
from catalog_functions.utils import BaseTransformer


class YSETransformer(BaseTransformer):
    """Transforms YSE DR1 HDF5 files to PyArrow tables with proper schema."""

    # Feature definitions from yse.py
    STR_FEATURES = ["obj_type"]

    FLOAT_FEATURES = ["redshift", "host_log_mass"]
    DOUBLE_FEATURES = ["ra", "dec"]

    def create_schema(self):
        """Create the output PyArrow schema."""
        fields = []

        # Lightcurve struct with nested sequences
        lightcurve_struct = pa.struct(
            [
                pa.field("band", pa.list_(pa.string())),
                pa.field("time", pa.list_(pa.float32())),
                pa.field("flux", pa.list_(pa.float32())),
                pa.field("flux_err", pa.list_(pa.float32())),
            ]
        )
        fields.append(pa.field("lightcurve", lightcurve_struct))

        # Add all float features
        for f in self.FLOAT_FEATURES:
            fields.append(pa.field(f, pa.float32()))

        for f in self.DOUBLE_FEATURES:
            fields.append(pa.field(f, pa.float64()))

        # Add all string features
        for f in self.STR_FEATURES:
            fields.append(pa.field(f, pa.string()))

        # Object ID
        fields.append(pa.field("object_id", pa.string()))

        return pa.schema(fields)

    def dataset_to_table(self, data):
        """
        Convert HDF5 dataset to PyArrow table.

        Args:
            data: HDF5 file or dict of datasets

        Returns:
            pa.Table: Transformed Arrow table
        """
        # Dictionary to hold all columns
        columns = {}

        # 1. Create lightcurve struct column
        # Lightcurve data: band, time, flux, flux_err
        object_id = data["object_id"][()]
        if isinstance(object_id, bytes):
            object_id = object_id.decode("utf-8")

        bands_str = data["bands"][()]
        if isinstance(bands_str, bytes):
            bands_str = bands_str.decode("utf-8")
        band_names = bands_str.split(",")

        idxs = np.arange(0, data["flux"].shape[0])
        band_idxs = idxs.repeat(data["flux"].shape[-1]).reshape(len(band_names), -1)

        # Convert band indices to band names and flatten all arrays
        band_array = np.asarray(
            [
                band_names[band_number]
                for band_number in band_idxs.flatten().astype("int32")
            ],
            dtype="str",
        )
        time_array = data["time"][:].flatten().astype("float32")
        flux_array = data["flux"][:].flatten().astype("float32")
        flux_err_array = np.asarray(data["flux_err"]).flatten().astype("float32")

        # 4. Create flat list columns (one list per row containing all observations)
        columns["lightcurve"] = pa.StructArray.from_arrays(
            [
                pa.array([band_array], type=pa.list_(pa.string())),
                pa.array([time_array], type=pa.list_(pa.float32())),
                pa.array([flux_array], type=pa.list_(pa.float32())),
                pa.array([flux_err_array], type=pa.list_(pa.float32())),
            ],
            names=["band", "time", "flux", "flux_err"],
        )

        # 2. Add float and double features
        for f in self.FLOAT_FEATURES:
            columns[f] = pa.array([np.float32(data[f][()])])

        for f in self.DOUBLE_FEATURES:
            columns[f] = pa.array([np.float64(data[f][()])])

        # 3. Add string features
        for f in self.STR_FEATURES:
            value = data[f][()]
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            columns[f] = pa.array([value])

        # 4. Add object_id
        columns["object_id"] = pa.array([object_id])
        # Create table with schema
        schema = self.create_schema()
        table = pa.table(columns, schema=schema)

        return table
