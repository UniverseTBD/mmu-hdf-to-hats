"""
DESITransformer: Clean class-based transformation from HDF5 to PyArrow tables.
"""

import pyarrow as pa
import numpy as np
from catalog_functions.utils import np_to_pyarrow_array, BaseTransformer


class DESITransformer(BaseTransformer):
    """Transforms DESI HDF5 files to PyArrow tables with proper schema."""

    # Feature group definitions from desi.py
    BOOL_FEATURES = ["ZWARN"]

    FLOAT_FEATURES = [
        "Z",
        "ZERR",
        "EBV",
        "FLUX_G",
        "FLUX_R",
        "FLUX_Z",
        "FLUX_IVAR_G",
        "FLUX_IVAR_R",
        "FLUX_IVAR_Z",
        "FIBERFLUX_G",
        "FIBERFLUX_R",
        "FIBERFLUX_Z",
        "FIBERTOTFLUX_G",
        "FIBERTOTFLUX_R",
        "FIBERTOTFLUX_Z",
    ]

    DOUBLE_FEATURES = [
        "ra",
        "dec",
    ]

    def create_schema(self):
        """Create the output PyArrow schema."""
        fields = []

        # Spectrum struct with nested arrays
        spectrum_struct = pa.struct(
            [
                pa.field("flux", pa.list_(pa.float32())),
                pa.field("ivar", pa.list_(pa.float32())),
                pa.field("lsf_sigma", pa.list_(pa.float32())),
                pa.field("lambda", pa.list_(pa.float32())),
                pa.field("mask", pa.list_(pa.bool_())),
            ]
        )
        fields.append(pa.field("spectrum", spectrum_struct))

        # Add all float features
        for f in self.FLOAT_FEATURES:
            fields.append(pa.field(f, pa.float32()))

        # Add all double features
        for f in self.DOUBLE_FEATURES:
            fields.append(pa.field(f, pa.float64()))

        # Add all boolean features
        for f in self.BOOL_FEATURES:
            fields.append(pa.field(f, pa.bool_()))

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
        spectrum_flux = data["spectrum_flux"][:]
        spectrum_ivar = data["spectrum_ivar"][:]
        spectrum_lsf_sigma = data["spectrum_lsf_sigma"][:]
        spectrum_lambda = data["spectrum_lambda"][:]
        spectrum_mask = data["spectrum_mask"][:]

        spectrum_arrays = [
            np_to_pyarrow_array(spectrum_flux),
            np_to_pyarrow_array(spectrum_ivar),
            np_to_pyarrow_array(spectrum_lsf_sigma),
            np_to_pyarrow_array(spectrum_lambda),
            np_to_pyarrow_array(spectrum_mask),
        ]

        columns["spectrum"] = pa.StructArray.from_arrays(
            spectrum_arrays, names=["flux", "ivar", "lsf_sigma", "lambda", "mask"]
        )

        # 2. Add float features
        for f in self.FLOAT_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float32))

        # 3. Add double features
        for f in self.DOUBLE_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float64))

        # 4. Add boolean features
        for f in self.BOOL_FEATURES:
            columns[f] = pa.array(data[f][:].astype(bool))

        # 5. Add object_id
        columns["object_id"] = pa.array([str(oid) for oid in data["object_id"][:]])

        # Create table with schema
        schema = self.create_schema()
        table = pa.table(columns, schema=schema)

        return table
