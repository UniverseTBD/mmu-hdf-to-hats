"""
VIPERSTransformer: Clean class-based transformation from HDF5 to PyArrow tables.
"""

import pyarrow as pa
import numpy as np
from catalog_functions.utils import np_to_pyarrow_array, BaseTransformer


class VIPERSTransformer(BaseTransformer):
    """Transforms VIPERS HDF5 files to PyArrow tables with proper schema."""

    # Feature group definitions from vipers.py
    FLOAT_FEATURES = ["REDSHIFT", "REDFLAG", "EXPTIME", "NORM", "MAG"]

    def create_schema(self):
        """Create the output PyArrow schema."""
        fields = []

        # Spectrum struct with nested arrays
        spectrum_struct = pa.struct(
            [
                pa.field("flux", pa.list_(pa.float32())),
                pa.field("ivar", pa.list_(pa.float32())),
                pa.field("lambda", pa.list_(pa.float32())),
                pa.field("mask", pa.list_(pa.float32())),
            ]
        )
        fields.append(pa.field("spectrum", spectrum_struct))

        # Add all float features
        for f in self.FLOAT_FEATURES:
            fields.append(pa.field(f, pa.float32()))

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

        # 1. Create spectrum struct column
        # Note: VIPERS uses spectrum_flux * 1e17 for normalization
        # and 1/(spectrum_noise * 1e34) for ivar
        spectrum_flux = data["spectrum_flux"][:] * 1e17
        spectrum_noise = data["spectrum_noise"][:]
        spectrum_ivar = 1.0 / (spectrum_noise * 1e34)
        spectrum_lambda = data["spectrum_wave"][:]
        spectrum_mask = data["spectrum_mask"][:]

        spectrum_arrays = [
            np_to_pyarrow_array(spectrum_flux.astype(np.float32)),
            np_to_pyarrow_array(spectrum_ivar.astype(np.float32)),
            np_to_pyarrow_array(spectrum_lambda.astype(np.float32)),
            np_to_pyarrow_array(spectrum_mask.astype(np.float32)),
        ]

        columns["spectrum"] = pa.StructArray.from_arrays(
            spectrum_arrays, names=["flux", "ivar", "lambda", "mask"]
        )

        # 2. Add float features
        for f in self.FLOAT_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float32))

        # 3. Add object_id
        columns["object_id"] = pa.array([str(oid) for oid in data["object_id"][:]])

        # Create table with schema
        schema = self.create_schema()
        table = pa.table(columns, schema=schema)

        return table
