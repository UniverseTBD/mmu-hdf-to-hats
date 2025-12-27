"""
SNLSTransformer: Clean class-based transformation from HDF5 to PyArrow tables.
"""

import pyarrow as pa
import numpy as np
from catalog_functions.utils import BaseTransformer


class SNLSTransformer(BaseTransformer):
    """Transforms SNLS HDF5 files to PyArrow tables with proper schema."""

    # Feature definitions from snls.py
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

        # Add ra, dec, healpix
        fields.append(pa.field("ra", pa.float64()))
        fields.append(pa.field("dec", pa.float64()))
        fields.append(pa.field("healpix", pa.int64()))

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
        # SNLS stores flux and time as 2D arrays (n_bands x n_times)
        # We need to flatten them and create band labels
        flux_data = data["flux"][:]  # Shape: (n_bands, n_times)
        flux_err_data = data["flux_err"][:]  # Shape: (n_bands, n_times)
        time_data = data["time"][:]  # Shape: (n_bands, n_times)
        bands_str = data["bands"][()].decode("utf-8")
        bands = bands_str.split(",")

        # Flatten the data
        # Create indices for bands (repeat for each time point)
        idxs = np.arange(0, flux_data.shape[0])
        band_idxs = idxs.repeat(flux_data.shape[-1]).reshape(
            len(bands), -1
        )

        # Build lightcurve as lists
        band_list = np.asarray([
            bands[band_number]
            for band_number in band_idxs.flatten().astype("int32")
        ]).astype("str").tolist()

        time_list = time_data.flatten().astype("float32").tolist()
        flux_list = flux_data.flatten().astype("float32").tolist()
        flux_err_list = flux_err_data.flatten().astype("float32").tolist()

        lightcurve_arrays = [
            pa.array([band_list], type=pa.list_(pa.string())),
            pa.array([time_list], type=pa.list_(pa.float32())),
            pa.array([flux_list], type=pa.list_(pa.float32())),
            pa.array([flux_err_list], type=pa.list_(pa.float32())),
        ]

        columns["lightcurve"] = pa.StructArray.from_arrays(
            lightcurve_arrays, names=["band", "time", "flux", "flux_err"]
        )

        # 2. Add float features (these are scalars)
        for f in self.FLOAT_FEATURES:
            val = data[f][()]
            columns[f] = pa.array([np.float32(val)])

        # 3. Add string features (these are scalars)
        for f in self.STR_FEATURES:
            val = data[f][()]
            val_str = val.decode("utf-8") if isinstance(val, bytes) else str(val)
            columns[f] = pa.array([val_str])

        # 4. Add object_id (scalar)
        oid = data["object_id"][()]
        oid_str = oid.decode("utf-8") if isinstance(oid, bytes) else str(oid)
        columns["object_id"] = pa.array([oid_str])

        # 5. Add ra, dec, healpix
        columns["ra"] = pa.array([np.float64(data["ra"][()])])
        columns["dec"] = pa.array([np.float64(data["dec"][()])])
        columns["healpix"] = pa.array([np.int64(data["healpix"][()])])

        # Create table with schema
        schema = self.create_schema()
        table = pa.table(columns, schema=schema)

        return table
