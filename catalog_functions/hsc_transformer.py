"""
HSCTransformer: Clean class-based transformation from HDF5 to PyArrow tables.
"""

import pyarrow as pa
import numpy as np
from catalog_functions.utils import BaseTransformer


class HSCTransformer(BaseTransformer):
    """Transforms HSC HDF5 files to PyArrow tables with proper schema."""

    # Feature group definitions from hsc.py
    FLOAT_FEATURES = [
        "a_g",
        "a_r",
        "a_i",
        "a_z",
        "a_y",
        "g_extendedness_value",
        "r_extendedness_value",
        "i_extendedness_value",
        "z_extendedness_value",
        "y_extendedness_value",
        "g_cmodel_mag",
        "g_cmodel_magerr",
        "r_cmodel_mag",
        "r_cmodel_magerr",
        "i_cmodel_mag",
        "i_cmodel_magerr",
        "z_cmodel_mag",
        "z_cmodel_magerr",
        "y_cmodel_mag",
        "y_cmodel_magerr",
        "g_sdssshape_psf_shape11",
        "g_sdssshape_psf_shape22",
        "g_sdssshape_psf_shape12",
        "r_sdssshape_psf_shape11",
        "r_sdssshape_psf_shape22",
        "r_sdssshape_psf_shape12",
        "i_sdssshape_psf_shape11",
        "i_sdssshape_psf_shape22",
        "i_sdssshape_psf_shape12",
        "z_sdssshape_psf_shape11",
        "z_sdssshape_psf_shape22",
        "z_sdssshape_psf_shape12",
        "y_sdssshape_psf_shape11",
        "y_sdssshape_psf_shape22",
        "y_sdssshape_psf_shape12",
        "g_sdssshape_shape11",
        "g_sdssshape_shape22",
        "g_sdssshape_shape12",
        "r_sdssshape_shape11",
        "r_sdssshape_shape22",
        "r_sdssshape_shape12",
        "i_sdssshape_shape11",
        "i_sdssshape_shape22",
        "i_sdssshape_shape12",
        "z_sdssshape_shape11",
        "z_sdssshape_shape22",
        "z_sdssshape_shape12",
        "y_sdssshape_shape11",
        "y_sdssshape_shape22",
        "y_sdssshape_shape12",
    ]

    DOUBLE_FEATURES = ["ra", "dec"]

    IMAGE_SIZE = 160
    BANDS = ["G", "R", "I", "Z", "Y"]

    def create_schema(self):
        """Create the output PyArrow schema."""
        fields = []

        # Image struct with lists of band data (matching datasets library output)
        image_struct = pa.struct(
            [
                pa.field("band", pa.list_(pa.string())),
                pa.field("flux", pa.list_(pa.list_(pa.list_(pa.float32())))),  # List of 2D arrays
                pa.field("ivar", pa.list_(pa.list_(pa.list_(pa.float32())))),  # List of 2D arrays
                pa.field("mask", pa.list_(pa.list_(pa.list_(pa.bool_())))),  # List of 2D arrays
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
        # Dictionary to hold all columns
        columns = {}
        n_objects = len(data["object_id"][:])

        # 1. Create image sequence column
        # image_array shape: [n_objects, n_bands, 160, 160]
        image_array = data["image_array"][:]
        image_ivar = data["image_ivar"][:]
        image_mask = data["image_mask"][:]
        image_band = data["image_band"][:]
        image_psf_fwhm = data["image_psf_fwhm"][:]
        image_scale = data["image_scale"][:]

        # Create struct of lists (matching datasets library output structure)
        # Decode band names once
        band_names_decoded = []
        for j in range(len(self.BANDS)):
            band = image_band[0][j]
            if isinstance(band, bytes):
                band = band.decode("utf-8")
            band_names_decoded.append(band)

        # Build nested PyArrow arrays directly from numpy without intermediate copies
        # For bands: same list repeated for each object
        band_arrays = pa.array([band_names_decoded] * n_objects, type=pa.list_(pa.string()))

        # For 2D image data (flux, ivar, mask): reshape and convert efficiently
        # Shape: (n_objects, n_bands, 160, 160) -> need list[list[list[float]]]
        # Convert only the 2D arrays to list-of-rows (keeping rows as numpy arrays)
        def build_nested_image_array(img_data, pa_type):
            """Build nested list array from 4D numpy array.

            Converts 2D arrays (160x160) to list of numpy 1D arrays (160 rows).
            This avoids converting individual pixels to Python scalars.
            PyArrow will then use the numpy buffer protocol for the row data.
            """
            result = []
            for i in range(n_objects):
                obj_bands = []
                for j in range(len(self.BANDS)):
                    # Split 2D array into list of row arrays (no pixel-level copy)
                    obj_bands.append([row for row in img_data[i, j]])
                result.append(obj_bands)
            return pa.array(result, type=pa.list_(pa.list_(pa.list_(pa_type))))

        flux_arrays = build_nested_image_array(image_array, pa.float32())
        ivar_arrays = build_nested_image_array(image_ivar, pa.float32())
        mask_arrays = build_nested_image_array(image_mask, pa.bool_())

        # For scalar lists (psf_fwhm, scale): each row is a list of 5 values
        # Convert 2D array (n_objects, 5) to list of 1D arrays
        psf_fwhm_arrays = pa.array([row for row in image_psf_fwhm.astype(np.float32)], type=pa.list_(pa.float32()))
        scale_arrays = pa.array([row for row in image_scale.astype(np.float32)], type=pa.list_(pa.float32()))

        columns["image"] = pa.StructArray.from_arrays(
            [band_arrays, flux_arrays, ivar_arrays, mask_arrays, psf_fwhm_arrays, scale_arrays],
            names=["band", "flux", "ivar", "mask", "psf_fwhm", "scale"],
        )

        # 2. Add float features
        for f in self.FLOAT_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float32))

        for f in self.DOUBLE_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float64))

        # 3. Add object_id
        columns["object_id"] = pa.array([str(oid) for oid in data["object_id"][:]])

        # Create table with schema
        schema = self.create_schema()
        table = pa.table(columns, schema=schema)

        return table
