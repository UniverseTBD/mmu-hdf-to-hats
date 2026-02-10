"""
SSLLegacySurveyTransformer: Clean class-based transformation from HDF5 to PyArrow tables.
"""

import pyarrow as pa
import numpy as np
from catalog_functions.utils import BaseTransformer
from datasets.features.features import Array2DExtensionType


class SSLLegacySurveyTransformer(BaseTransformer):
    """Transforms SSL LegacySurvey HDF5 files to PyArrow tables with proper schema."""

    # Feature group definitions from ssl_legacysurvey.py
    FLOAT_FEATURES = [
        "ebv",
        "flux_g",
        "flux_r",
        "flux_z",
        "fiberflux_g",
        "fiberflux_r",
        "fiberflux_z",
        "psfdepth_g",
        "psfdepth_r",
        "psfdepth_z",
        "z_spec",
    ]
    DOUBLE_FEATURES = ["ra", "dec"]

    IMAGE_SIZE = 152
    BANDS = ["DES-G", "DES-R", "DES-Z"]

    def create_schema(self):
        """Create the output PyArrow schema."""
        fields = []

        array_2d_float = Array2DExtensionType(shape=(self.IMAGE_SIZE, self.IMAGE_SIZE), dtype='float32')

        # Image struct-of-lists (matching datasets Sequence behavior)
        image_struct = pa.struct(
            [
                pa.field("band", pa.list_(pa.string())),
                pa.field("flux", pa.list_(array_2d_float)),
                pa.field("psf_fwhm", pa.list_(pa.float32())),
                pa.field("scale", pa.list_(pa.float32())),
            ]
        )
        fields.append(pa.field("image", image_struct))

        # Add all float features
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
        columns = {}
        n_objects = len(data["object_id"][:])

        # 1. Create image struct-of-lists column
        image_array = data["image_array"][:]
        image_band = data["image_band"][:]
        image_psf_fwhm = data["image_psf_fwhm"][:]
        image_scale = data["image_scale"][:]

        # Decode band names (same for all objects)
        band_names_decoded = []
        for j in range(len(self.BANDS)):
            band = image_band[0][j]
            if isinstance(band, bytes):
                band = band.decode("utf-8")
            band_names_decoded.append(band)

        band_arrays = pa.array([band_names_decoded] * n_objects, type=pa.list_(pa.string()))

        # Build flux arrays with Array2D extension type (following HSC pattern)
        array_2d_float = Array2DExtensionType(shape=(self.IMAGE_SIZE, self.IMAGE_SIZE), dtype='float32')

        flux_data = []
        for i in range(n_objects):
            obj_bands = []
            for j in range(len(self.BANDS)):
                obj_bands.append([row for row in image_array[i, j]])
            flux_data.append(obj_bands)

        storage_type = pa.list_(array_2d_float.storage_type)
        storage_array = pa.array(flux_data, type=storage_type)
        target_type = pa.list_(array_2d_float)
        flux_arrays = storage_array.cast(target_type)

        # Scalar lists
        psf_fwhm_arrays = pa.array([row for row in image_psf_fwhm.astype(np.float32)], type=pa.list_(pa.float32()))
        scale_arrays = pa.array([row for row in image_scale.astype(np.float32)], type=pa.list_(pa.float32()))

        columns["image"] = pa.StructArray.from_arrays(
            [band_arrays, flux_arrays, psf_fwhm_arrays, scale_arrays],
            names=["band", "flux", "psf_fwhm", "scale"],
        )

        # 2. Add float features
        for f in self.FLOAT_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float32))

        # 3. Add ra/dec as float64
        for f in self.DOUBLE_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float64))

        # 4. Add object_id (int64 in HDF5, convert to string)
        columns["object_id"] = pa.array([str(oid) for oid in data["object_id"][:]])

        # Create table with schema
        schema = self.create_schema()
        table = pa.table(columns, schema=schema)

        return table
