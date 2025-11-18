"""
TESSTransformer: Clean class-based transformation from HDF5 to PyArrow tables.
"""
import pyarrow as pa
import numpy as np
from catalog_functions.base_transformer import BaseTransformer


class TESSTransformer(BaseTransformer):
    """Transforms TESS HDF5 files to PyArrow tables with proper schema."""

    # Feature definitions from tess.py
    FLOAT_FEATURES = ["RA", "DEC"]

    def create_schema(self):
        """Create the output PyArrow schema."""
        fields = []

        # Lightcurve struct with nested sequences
        lightcurve_struct = pa.struct([
            pa.field("time", pa.list_(pa.float32())),
            pa.field("flux", pa.list_(pa.float32())),
            pa.field("flux_err", pa.list_(pa.float32())),
        ])
        fields.append(pa.field("lightcurve", lightcurve_struct))

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

        # 1. Create lightcurve struct column
        time_data = data["time"][:]
        flux_data = data["flux"][:]
        flux_err_data = data["flux_err"][:]

        # Convert to lists for each object
        time_lists = [t.tolist() for t in time_data]
        flux_lists = [f.tolist() for f in flux_data]
        flux_err_lists = [fe.tolist() for fe in flux_err_data]

        lightcurve_arrays = [
            pa.array(time_lists, type=pa.list_(pa.float32())),
            pa.array(flux_lists, type=pa.list_(pa.float32())),
            pa.array(flux_err_lists, type=pa.list_(pa.float32())),
        ]

        columns["lightcurve"] = pa.StructArray.from_arrays(
            lightcurve_arrays,
            names=["time", "flux", "flux_err"]
        )

        # 2. Add float features
        for f in self.FLOAT_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float32))

        # 3. Add object_id
        columns["object_id"] = pa.array(
            [str(oid) for oid in data["object_id"][:]]
        )

        # Create table with schema
        schema = self.create_schema()
        table = pa.table(columns, schema=schema)

        return table
