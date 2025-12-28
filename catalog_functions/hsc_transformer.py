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
        # For each object, collect all bands into lists
        image_data_per_object = []
        for i in range(n_objects):
            bands_list = []
            flux_list = []
            ivar_list = []
            mask_list = []
            psf_fwhm_list = []
            scale_list = []

            for j in range(len(self.BANDS)):
                # Get band name from data
                band = image_band[i][j]
                if isinstance(band, bytes):
                    band = band.decode("utf-8")
                bands_list.append(band)

                # Convert 2D arrays to lists of lists
                flux_2d = image_array[i][j]
                flux_list.append([row.tolist() for row in flux_2d])

                ivar_2d = image_ivar[i][j]
                ivar_list.append([row.tolist() for row in ivar_2d])

                mask_2d = image_mask[i][j]
                mask_list.append([row.tolist() for row in mask_2d])

                psf_fwhm_list.append(float(image_psf_fwhm[i][j]))
                scale_list.append(float(image_scale[i][j]))

            image_data_per_object.append({
                "band": bands_list,
                "flux": flux_list,
                "ivar": ivar_list,
                "mask": mask_list,
                "psf_fwhm": psf_fwhm_list,
                "scale": scale_list,
            })

        # Create struct arrays for each object
        band_arrays = pa.array([obj["band"] for obj in image_data_per_object], type=pa.list_(pa.string()))
        flux_arrays = pa.array([obj["flux"] for obj in image_data_per_object], type=pa.list_(pa.list_(pa.list_(pa.float32()))))
        ivar_arrays = pa.array([obj["ivar"] for obj in image_data_per_object], type=pa.list_(pa.list_(pa.list_(pa.float32()))))
        mask_arrays = pa.array([obj["mask"] for obj in image_data_per_object], type=pa.list_(pa.list_(pa.list_(pa.bool_()))))
        psf_fwhm_arrays = pa.array([obj["psf_fwhm"] for obj in image_data_per_object], type=pa.list_(pa.float32()))
        scale_arrays = pa.array([obj["scale"] for obj in image_data_per_object], type=pa.list_(pa.float32()))

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
