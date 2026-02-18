"""
CFATransformer: Clean class-based transformation from HDF5 to PyArrow tables.
"""

import pyarrow as pa
import numpy as np
from catalog_functions.utils import BaseTransformer

def convert_scalar_col_if_bytes(col):
    if col.dtype.kind == "S" and col.shape == ():
        val = col
    else:
        val = col[()]
    return val

class CFATransformer(BaseTransformer):
    """Transforms CFA Supernova HDF5 files to PyArrow tables with proper schema."""

    # Feature definitions from cfa.py
    STR_FEATURES = ["obj_type"]
    DOUBLE_FEATURES = ["ra", "dec"]

    def create_schema(self):
        """Create the output PyArrow schema."""
        fields = []

        # Lightcurve struct with nested sequences
        lightcurve_struct = pa.struct(
            [
                pa.field("band", pa.list_(pa.string())),
                pa.field("time", pa.list_(pa.float32())),
                pa.field("mag", pa.list_(pa.float32())),
                pa.field("mag_err", pa.list_(pa.float32())),
            ]
        )
        fields.append(pa.field("lightcurve", lightcurve_struct))

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

        Each CFA HDF5 file contains a single supernova with:
        - bands: shape (n_bands,) byte strings
        - time: shape (n_bands, n_obs) float32
        - mag: shape (n_bands, n_obs) float32
        - mag_err: shape (n_bands, n_obs) float32

        Args:
            data: HDF5 file or dict of datasets

        Returns:
            pa.Table: Transformed Arrow table
        """
        columns = {}

        # 1. Extract object_id
        object_id = convert_scalar_col_if_bytes(data["object_id"])
        if isinstance(object_id, bytes):
            object_id = object_id.decode("utf-8")

        # 2. Parse bands
        band_names = [b.decode("utf-8") if isinstance(b, bytes) else str(b) for b in data["bands"][()]]

        # 3. Flatten lightcurve data following the original cfa.py _generate_examples
        idxs = np.arange(0, data["mag"].shape[0])
        band_idxs = idxs.repeat(data["mag"].shape[-1]).reshape(len(band_names), -1)

        band_array = np.asarray(
            [band_names[band_number] for band_number in band_idxs.flatten().astype("int32")]
        ).astype("str")
        time_array = np.asarray(data["time"]).flatten().astype("float32")
        mag_array = np.asarray(data["mag"]).flatten().astype("float32")
        mag_err_array = np.asarray(data["mag_err"]).flatten().astype("float32")

        # 4. Create struct column (single row per file)
        columns["lightcurve"] = pa.StructArray.from_arrays(
            [
                pa.array([band_array], type=pa.list_(pa.string())),
                pa.array([time_array], type=pa.list_(pa.float32())),
                pa.array([mag_array], type=pa.list_(pa.float32())),
                pa.array([mag_err_array], type=pa.list_(pa.float32())),
            ],
            names=["band", "time", "mag", "mag_err"],
        )

        # 5. Add ra/dec
        for f in self.DOUBLE_FEATURES:
            columns[f] = pa.array([np.float64(data[f][()])])

        # 6. Add string features
        for f in self.STR_FEATURES:
            try:
                value = convert_scalar_col_if_bytes(data[f][()])
                if isinstance(value, bytes):
                    value = value.decode("utf-8")
                columns[f] = pa.array([value])
            except:
                breakpoint()
                value = convert_scalar_col_if_bytes(data[f][()])
                if isinstance(value, bytes):
                    value = value.decode("utf-8")
                columns[f] = pa.array([value])

        # 7. Add object_id
        breakpoint()
        columns["object_id"] = pa.array([object_id])

        # Create table with schema
        schema = self.create_schema()
        table = pa.table(columns, schema=schema)

        return table
