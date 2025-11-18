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

    IMAGE_SIZE = 160
    BANDS = ["G", "R", "I", "Z", "Y"]

    def create_schema(self):
        """Create the output PyArrow schema."""
        fields = []

        # Image sequence with band, flux, ivar, mask, psf_fwhm, scale
        image_struct = pa.struct(
            [
                pa.field("band", pa.string()),
                pa.field("flux", pa.list_(pa.list_(pa.float32()))),  # 2D array
                pa.field("ivar", pa.list_(pa.list_(pa.float32()))),  # 2D array
                pa.field("mask", pa.list_(pa.list_(pa.bool_()))),  # 2D array
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
        # image_array shape: [n_objects, n_bands, 160, 160]
        image_array = data["image_array"][:]
        image_ivar = data["image_ivar"][:]
        image_mask = data["image_mask"][:]
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

                # Convert 2D arrays to lists of lists
                flux_2d = image_array[i][j]
                flux_list = [row.tolist() for row in flux_2d]

                ivar_2d = image_ivar[i][j]
                ivar_list = [row.tolist() for row in ivar_2d]

                mask_2d = image_mask[i][j]
                mask_list = [row.tolist() for row in mask_2d]

                images_for_object.append(
                    {
                        "band": band,
                        "flux": flux_list,
                        "ivar": ivar_list,
                        "mask": mask_list,
                        "psf_fwhm": float(image_psf_fwhm[i][j]),
                        "scale": float(image_scale[i][j]),
                    }
                )
            image_lists.append(images_for_object)

        # Create struct arrays for images
        band_arrays = []
        flux_arrays = []
        ivar_arrays = []
        mask_arrays = []
        psf_fwhm_arrays = []
        scale_arrays = []

        for obj_images in image_lists:
            band_arrays.append([img["band"] for img in obj_images])
            flux_arrays.append([img["flux"] for img in obj_images])
            ivar_arrays.append([img["ivar"] for img in obj_images])
            mask_arrays.append([img["mask"] for img in obj_images])
            psf_fwhm_arrays.append([img["psf_fwhm"] for img in obj_images])
            scale_arrays.append([img["scale"] for img in obj_images])

        image_structs = pa.StructArray.from_arrays(
            [
                pa.array(band_arrays, type=pa.list_(pa.string())),
                pa.array(flux_arrays, type=pa.list_(pa.list_(pa.list_(pa.float32())))),
                pa.array(ivar_arrays, type=pa.list_(pa.list_(pa.list_(pa.float32())))),
                pa.array(mask_arrays, type=pa.list_(pa.list_(pa.list_(pa.bool_())))),
                pa.array(psf_fwhm_arrays, type=pa.list_(pa.float32())),
                pa.array(scale_arrays, type=pa.list_(pa.float32())),
            ],
            names=["band", "flux", "ivar", "mask", "psf_fwhm", "scale"],
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
