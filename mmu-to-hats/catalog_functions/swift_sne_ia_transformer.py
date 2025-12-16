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

    FLOAT_FEATURES = ["redshift", "host_log_mass"]

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

        # 1. Extract scalar values
        object_id = data["object_id"][()]
        if isinstance(object_id, bytes):
            object_id = object_id.decode('utf-8')

        obj_type = data["obj_type"][()]
        if isinstance(obj_type, bytes):
            obj_type = obj_type.decode('utf-8')

        # 2. Parse bands (comma-separated string like 'B,N,U,V,W,X')
        bands_str = data["bands"][()]
        if isinstance(bands_str, bytes):
            bands_str = bands_str.decode('utf-8')
        band_names = bands_str.split(',')

        # 3. Get lightcurve arrays (shape: n_bands × n_timepoints)
        time_arr = data["time"][:].astype(np.float32)  # Shape: (6, 29)
        flux_arr = data["flux"][:].astype(np.float32)
        flux_err_arr = data["flux_err"][:].astype(np.float32)

        # 4. Flatten lightcurve data: filter out invalid points (marked with -99 or 0)
        band_list = []
        time_list = []
        flux_list = []
        flux_err_list = []

        for band_idx, band_name in enumerate(band_names):
            times = time_arr[band_idx]
            fluxes = flux_arr[band_idx]
            flux_errs = flux_err_arr[band_idx]

            # Filter out invalid points (time=-99 or flux=0)
            valid_mask = (times > 0) & (fluxes != 0)

            for t, f, fe in zip(times[valid_mask], fluxes[valid_mask], flux_errs[valid_mask]):
                band_list.append(band_name)
                time_list.append(float(t))
                flux_list.append(float(f))
                flux_err_list.append(float(fe))

        # 5. Create lightcurve struct column
        lightcurve_arrays = [
            pa.array([band_list], type=pa.list_(pa.string())),
            pa.array([time_list], type=pa.list_(pa.float32())),
            pa.array([flux_list], type=pa.list_(pa.float32())),
            pa.array([flux_err_list], type=pa.list_(pa.float32())),
        ]

        columns["lightcurve"] = pa.StructArray.from_arrays(
            lightcurve_arrays, names=["band", "time", "flux", "flux_err"]
        )

        # 6. Add float features
        for f in self.FLOAT_FEATURES:
            columns[f] = pa.array([data[f][()].astype(np.float32)])

        # 7. Add string features
        columns["obj_type"] = pa.array([obj_type])

        # 8. Add object_id
        columns["object_id"] = pa.array([object_id])

        # Create table with schema
        schema = self.create_schema()
        table = pa.table(columns, schema=schema)

        return table
