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

        # 1. Create lightcurve struct column
        # Lightcurve data: band, time, flux, flux_err
        lightcurve_data = data["lightcurve"][:]  # Shape varies by implementation
        n_objects = len(data["object_id"][:])

        band_lists = []
        time_lists = []
        flux_lists = []
        flux_err_lists = []

        for i in range(n_objects):
            lc = lightcurve_data[i]
            bands = [
                b.decode("utf-8") if isinstance(b, bytes) else str(b)
                for b in lc["band"]
            ]
            times = lc["time"].astype(np.float32).tolist()
            fluxes = lc["flux"].astype(np.float32).tolist()
            flux_errs = lc["flux_err"].astype(np.float32).tolist()

            band_lists.append(bands)
            time_lists.append(times)
            flux_lists.append(fluxes)
            flux_err_lists.append(flux_errs)

        lightcurve_arrays = [
            pa.array(band_lists, type=pa.list_(pa.string())),
            pa.array(time_lists, type=pa.list_(pa.float32())),
            pa.array(flux_lists, type=pa.list_(pa.float32())),
            pa.array(flux_err_lists, type=pa.list_(pa.float32())),
        ]

        columns["lightcurve"] = pa.StructArray.from_arrays(
            lightcurve_arrays, names=["band", "time", "flux", "flux_err"]
        )

        # 2. Add float features
        for f in self.FLOAT_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float32))

        # 3. Add string features
        for f in self.STR_FEATURES:
            columns[f] = pa.array(
                [
                    str(x.decode("utf-8") if isinstance(x, bytes) else x)
                    for x in data[f][:]
                ]
            )

        # 4. Add object_id
        columns["object_id"] = pa.array(
            [
                str(oid.decode("utf-8") if isinstance(oid, bytes) else oid)
                for oid in data["object_id"][:]
            ]
        )

        # Create table with schema
        schema = self.create_schema()
        table = pa.table(columns, schema=schema)

        return table
