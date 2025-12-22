"""
GZ10Transformer: Clean class-based transformation from HDF5 to PyArrow tables.
"""

import pyarrow as pa
import numpy as np
from catalog_functions.utils import BaseTransformer


class GZ10Transformer(BaseTransformer):
    """Transforms GZ10 HDF5 files to PyArrow tables with proper schema."""

    # Feature definitions from gz10.py
    INT32_FEATURES = [
        "gz10_label",
    ]

    FLOAT_FEATURES = [
        "redshift",
    ]

    def create_schema(self):
        """Create the output PyArrow schema."""
        fields = []

        # Add int32 features
        for f in self.INT32_FEATURES:
            fields.append(pa.field(f, pa.int32()))

        # Add float features
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

        # 1. Add int32 features (gz10_label comes from "ans" in HDF5)
        columns["gz10_label"] = pa.array(data["ans"][:].astype(np.int32))

        # 2. Add float features
        for f in self.FLOAT_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float32))

        # 3. Add object_id
        columns["object_id"] = pa.array([str(oid) for oid in data["object_id"][:]])

        # Create table with schema
        schema = self.create_schema()
        table = pa.table(columns, schema=schema)

        return table
