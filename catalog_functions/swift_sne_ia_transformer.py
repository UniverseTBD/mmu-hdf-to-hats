"""
SwiftSNeIaTransformer: Clean class-based transformation from HDF5 to PyArrow tables.
"""

import pyarrow as pa
import numpy as np
from catalog_functions.utils import BaseTransformer


class SwiftSNeIaTransformer(BaseTransformer):
    """Transforms Swift SNE Ia HDF5 files to PyArrow tables with proper schema."""

    # Feature definitions from swift_sne_ia.py
    STR_FEATURES = ["obj_type"]

    FLOAT_FEATURES = ["redshift", "host_log_mass", "ra", "dec"]

    def create_schema(self):
        """Create the output PyArrow schema."""
        fields = []

        # Lightcurve data as flat list columns
        fields.append(pa.field("band", pa.list_(pa.string())))
        fields.append(pa.field("time", pa.list_(pa.float32())))
        fields.append(pa.field("flux", pa.list_(pa.float32())))
        fields.append(pa.field("flux_err", pa.list_(pa.float32())))

        # Add all float features
        for f in self.FLOAT_FEATURES:
            fields.append(pa.field(f, pa.float32()))

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

        # 1. Extract object_id
        object_id = data["object_id"][()]
        if isinstance(object_id, bytes):
            object_id = object_id.decode("utf-8")

        # 2. Parse bands (comma-separated string like 'B,N,U,V,W,X')
        bands_str = data["bands"][()]
        if isinstance(bands_str, bytes):
            bands_str = bands_str.decode("utf-8")
        band_names = bands_str.split(",")

        # 3. Flatten lightcurve data following the original HuggingFace implementation
        # Create band indices that map each position to a band
        idxs = np.arange(0, data["flux"].shape[0])
        band_idxs = idxs.repeat(data["flux"].shape[-1]).reshape(len(band_names), -1)

        # Convert band indices to band names and flatten all arrays
        band_array = np.asarray(
            [
                band_names[band_number]
                for band_number in band_idxs.flatten().astype("int32")
            ]
        ).astype("str")
        time_array = np.asarray(data["time"]).flatten().astype("float32")
        flux_array = np.asarray(data["flux"]).flatten().astype("float32")
        flux_err_array = np.asarray(data["flux_err"]).flatten().astype("float32")

        # 4. Create flat list columns (one list per row containing all observations)
        columns["band"] = pa.array([band_array.tolist()], type=pa.list_(pa.string()))
        columns["time"] = pa.array([time_array.tolist()], type=pa.list_(pa.float32()))
        columns["flux"] = pa.array([flux_array.tolist()], type=pa.list_(pa.float32()))
        columns["flux_err"] = pa.array([flux_err_array.tolist()], type=pa.list_(pa.float32()))

        # 5. Add float features
        for f in self.FLOAT_FEATURES:
            columns[f] = pa.array([np.float32(data[f][()])])

        # 6. Add string features
        for f in self.STR_FEATURES:
            value = data[f][()]
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            columns[f] = pa.array([value])

        # 7. Add object_id
        columns["object_id"] = pa.array([object_id])

        # Create table with schema
        schema = self.create_schema()
        table = pa.table(columns, schema=schema)

        return table
