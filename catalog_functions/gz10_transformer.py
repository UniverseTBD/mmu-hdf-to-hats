"""
GZ10Transformer: Clean class-based transformation from HDF5 to PyArrow tables.
"""

import pyarrow as pa
import numpy as np
from catalog_functions.utils import BaseTransformer
from PIL import Image
import io


class GZ10Transformer(BaseTransformer):
    """Transforms GZ10 HDF5 files to PyArrow tables with proper schema."""

    # Feature definitions from gz10.py
    INT32_FEATURES = [
        "gz10_label",
    ]

    FLOAT_FEATURES = [
        "redshift",
        "rgb_pixel_scale",
    ]

    DOUBLE_FEATURES = ["ra", "dec"]

    IMAGE_SIZE = 256

    def create_schema(self):
        """Create the output PyArrow schema."""
        fields = []

        # Add RGB image as struct with bytes and path (matching datasets Image feature)
        rgb_image_struct = pa.struct([
            pa.field("bytes", pa.binary()),
            pa.field("path", pa.string()),
        ])
        fields.append(pa.field("rgb_image", rgb_image_struct))

        # Add int32 features
        for f in self.INT32_FEATURES:
            fields.append(pa.field(f, pa.int32()))

        # Add float features
        for f in self.FLOAT_FEATURES:
            fields.append(pa.field(f, pa.float32()))

        for f in self.DOUBLE_FEATURES:
            fields.append(pa.field(f, pa.float64()))

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

        # 1. Convert RGB images to PNG bytes (matching datasets Image feature)
        images_data = data["images"][:]  # Shape: (n_objects, 256, 256, 3)
        n_objects = images_data.shape[0]

        rgb_bytes = []
        rgb_paths = []
        for i in range(n_objects):
            # Convert numpy array to PIL Image and encode as PNG
            img_array = images_data[i]  # (256, 256, 3)
            pil_img = Image.fromarray(img_array, mode='RGB')

            # Encode to PNG bytes
            buf = io.BytesIO()
            pil_img.save(buf, format='PNG')
            png_bytes = buf.getvalue()

            rgb_bytes.append(png_bytes)
            rgb_paths.append(None)  # No file path for embedded images

        # Create struct array for rgb_image
        columns["rgb_image"] = pa.StructArray.from_arrays(
            [pa.array(rgb_bytes, type=pa.binary()),
             pa.array(rgb_paths, type=pa.string())],
            names=["bytes", "path"]
        )

        # 2. Add int32 features (gz10_label comes from "ans" in HDF5)
        columns["gz10_label"] = pa.array(data["ans"][:].astype(np.int32))

        # 3. Add float features (including rgb_pixel_scale from "pxscale" in HDF5)
        columns["redshift"] = pa.array(data["redshift"][:].astype(np.float32))
        columns["rgb_pixel_scale"] = pa.array(data["pxscale"][:].astype(np.float32))

        for f in self.DOUBLE_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float64))

        # 3. Add object_id
        columns["object_id"] = pa.array([str(oid) for oid in data["object_id"][:]])

        # Create table with schema
        schema = self.create_schema()
        table = pa.table(columns, schema=schema)

        return table
