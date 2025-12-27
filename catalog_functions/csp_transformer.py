"""
CSPTransformer: Clean class-based transformation from HDF5 to PyArrow tables.
"""

import pyarrow as pa
import numpy as np
from catalog_functions.utils import BaseTransformer


class CSPTransformer(BaseTransformer):
    """Transforms CSP-I DR3 HDF5 files to PyArrow tables with proper schema."""

    # Feature definitions from csp.py
    STR_FEATURES = [
        "spec_class",
    ]

    FLOAT_FEATURES = [
        "ra",
        "dec",
        "redshift",
    ]

    def create_schema(self):
        """Create the output PyArrow schema."""
        fields = []

        # Lightcurve struct with nested sequences (using mag instead of flux)
        lightcurve_struct = pa.struct(
            [
                pa.field("band", pa.list_(pa.string())),
                pa.field("time", pa.list_(pa.float32())),
                pa.field("mag", pa.list_(pa.float32())),
                pa.field("mag_err", pa.list_(pa.float32())),
            ]
        )
        fields.append(pa.field("lightcurve", lightcurve_struct))

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

        # 1. Create lightcurve struct column
        # CSP stores data per-object, we need to handle the flattening
        # mag shape: [n_bands, n_times]
        mag_data = data["mag"][:]
        mag_err_data = data["mag_err"][:]
        time_data = data["time"][:]
        bands_data = [
            b.decode("utf-8") if isinstance(b, bytes) else str(b)
            for b in data["bands"][:]
        ]

        # Create band indices for flattening
        n_bands = mag_data.shape[0]
        n_times = mag_data.shape[1]

        band_idxs = np.arange(n_bands).repeat(n_times).reshape(n_bands, -1)

        # Flatten and create lists
        bands_list = [
            bands_data[band_idx] for band_idx in band_idxs.flatten().astype(int)
        ]
        time_list = time_data.flatten().tolist()
        mag_list = mag_data.flatten().tolist()
        mag_err_list = mag_err_data.flatten().tolist()

        # Since CSP stores one object per file, wrap in list
        lightcurve_arrays = [
            pa.array([bands_list], type=pa.list_(pa.string())),
            pa.array([time_list], type=pa.list_(pa.float32())),
            pa.array([mag_list], type=pa.list_(pa.float32())),
            pa.array([mag_err_list], type=pa.list_(pa.float32())),
        ]

        columns["lightcurve"] = pa.StructArray.from_arrays(
            lightcurve_arrays, names=["band", "time", "mag", "mag_err"]
        )

        # 2. Add float features (scalars in CSP)
        for f in self.FLOAT_FEATURES:
            columns[f] = pa.array([float(data[f][:])], type=pa.float32())

        # 3. Add string features (scalars in CSP)
        for f in self.STR_FEATURES:
            val = data[f][()]
            if isinstance(val, bytes):
                val = val.decode("utf-8")
            columns[f] = pa.array([str(val)])

        # 4. Add object_id (scalar in CSP)
        oid = data["object_id"][()]
        if isinstance(oid, bytes):
            oid = oid.decode("utf-8")
        columns["object_id"] = pa.array([str(oid)])

        # Create table with schema
        schema = self.create_schema()
        table = pa.table(columns, schema=schema)

        return table
