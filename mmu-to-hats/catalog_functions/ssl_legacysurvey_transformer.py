"""
SSLLegacySurveyTransformer: Clean class-based transformation from HDF5 to PyArrow tables.
"""

import pyarrow as pa
import numpy as np
from catalog_functions.utils import BaseTransformer


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

    IMAGE_SIZE = 152
    BANDS = ["DES-G", "DES-R", "DES-Z"]

    def create_schema(self):
        """Create the output PyArrow schema."""
        fields = []

        # Image sequence with band, flux, psf_fwhm, scale
        image_struct = pa.struct(
            [
                pa.field("band", pa.string()),
                pa.field("flux", pa.list_(pa.list_(pa.float32()))),  # 2D array
                pa.field("psf_fwhm", pa.float32()),
                pa.field("scale", pa.float32()),
            ]
        )
        fields.append(pa.field("image", pa.list_(image_struct)))

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
        n_objects = len(data["object_id"][:])

        # 1. Create image sequence column
        # image_array shape: [n_objects, n_bands, 152, 152]
        image_array = data["image_array"][:]
        image_band = data["image_band"][:]
        image_psf_fwhm = data["image_psf_fwhm"][:]
        image_scale = data["image_scale"][:]

        image_lists = []
        for i in range(n_objects):
            images_for_object = []
            for j, band_name in enumerate(self.BANDS):
                # Get band name from data
                band = image_band[i][j]
                if isinstance(band, bytes):
                    band = band.decode("utf-8")

                # Convert 2D flux array to list of lists
                flux_2d = image_array[i][j]
                flux_list = [row.tolist() for row in flux_2d]

                images_for_object.append(
                    {
                        "band": band,
                        "flux": flux_list,
                        "psf_fwhm": float(image_psf_fwhm[i][j]),
                        "scale": float(image_scale[i][j]),
                    }
                )
            image_lists.append(images_for_object)

        # Create struct arrays for images
        band_arrays = []
        flux_arrays = []
        psf_fwhm_arrays = []
        scale_arrays = []

        for obj_images in image_lists:
            band_arrays.append([img["band"] for img in obj_images])
            flux_arrays.append([img["flux"] for img in obj_images])
            psf_fwhm_arrays.append([img["psf_fwhm"] for img in obj_images])
            scale_arrays.append([img["scale"] for img in obj_images])

        image_structs = pa.StructArray.from_arrays(
            [
                pa.array(band_arrays, type=pa.list_(pa.string())),
                pa.array(flux_arrays, type=pa.list_(pa.list_(pa.list_(pa.float32())))),
                pa.array(psf_fwhm_arrays, type=pa.list_(pa.float32())),
                pa.array(scale_arrays, type=pa.list_(pa.float32())),
            ],
            names=["band", "flux", "psf_fwhm", "scale"],
        )

        columns["image"] = image_structs

        # 2. Add float features
        for f in self.FLOAT_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float32))

        # 3. Add object_id
        columns["object_id"] = pa.array([str(oid) for oid in data["object_id"][:]])

        # Create table with schema
        schema = self.create_schema()
        table = pa.table(columns, schema=schema)

        return table
