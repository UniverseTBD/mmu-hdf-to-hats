"""
JWSTTransformer: Clean class-based transformation from HDF5 to PyArrow tables.
"""
import pyarrow as pa
import numpy as np
from catalog_functions.base_transformer import BaseTransformer


class JWSTTransformer(BaseTransformer):
    """Transforms JWST HDF5 files to PyArrow tables with proper schema."""

    # Feature group definitions from jwst.py
    FLOAT_FEATURES = [
        'mag_auto', 'flux_radius', 'flux_auto', 'fluxerr_auto',
        'cxx_image', 'cyy_image', 'cxy_image'
    ]

    # Default image size (can vary by config)
    IMAGE_SIZE = 96

    def create_schema(self):
        """Create the output PyArrow schema."""
        fields = []

        # Image sequence with band, flux, ivar, mask, psf_fwhm, scale
        image_struct = pa.struct([
            pa.field("band", pa.string()),
            pa.field("flux", pa.list_(pa.list_(pa.float32()))),  # 2D array
            pa.field("ivar", pa.list_(pa.list_(pa.float32()))),  # 2D array
            pa.field("mask", pa.list_(pa.list_(pa.bool_()))),    # 2D array
            pa.field("psf_fwhm", pa.float32()),
            pa.field("scale", pa.float32()),
        ])
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
        # image_flux shape: [n_objects, n_bands, image_size, image_size]
        image_flux = data["image_flux"][:]
        image_ivar = data["image_ivar"][:]
        image_mask = data["image_mask"][:]
        image_band = data["image_band"][:]
        image_psf_fwhm = data["image_psf_fwhm"][:]
        image_scale = data["image_scale"][:]

        image_lists = []
        for i in range(n_objects):
            images_for_object = []
            n_bands = len(image_band[i])
            for j in range(n_bands):
                # Get band name from data
                band = image_band[i][j]
                if isinstance(band, bytes):
                    band = band.decode('utf-8')

                # Convert 2D arrays to lists of lists
                flux_2d = image_flux[i][j]
                flux_list = [row.tolist() for row in flux_2d]

                ivar_2d = image_ivar[i][j]
                ivar_list = [row.tolist() for row in ivar_2d]

                mask_2d = image_mask[i][j].astype(bool)
                mask_list = [row.tolist() for row in mask_2d]

                images_for_object.append({
                    'band': band,
                    'flux': flux_list,
                    'ivar': ivar_list,
                    'mask': mask_list,
                    'psf_fwhm': float(image_psf_fwhm[i][j]),
                    'scale': float(image_scale[i][j])
                })
            image_lists.append(images_for_object)

        # Create struct arrays for images
        band_arrays = []
        flux_arrays = []
        ivar_arrays = []
        mask_arrays = []
        psf_fwhm_arrays = []
        scale_arrays = []

        for obj_images in image_lists:
            band_arrays.append([img['band'] for img in obj_images])
            flux_arrays.append([img['flux'] for img in obj_images])
            ivar_arrays.append([img['ivar'] for img in obj_images])
            mask_arrays.append([img['mask'] for img in obj_images])
            psf_fwhm_arrays.append([img['psf_fwhm'] for img in obj_images])
            scale_arrays.append([img['scale'] for img in obj_images])

        image_structs = pa.StructArray.from_arrays([
            pa.array(band_arrays, type=pa.list_(pa.string())),
            pa.array(flux_arrays, type=pa.list_(pa.list_(pa.list_(pa.float32())))),
            pa.array(ivar_arrays, type=pa.list_(pa.list_(pa.list_(pa.float32())))),
            pa.array(mask_arrays, type=pa.list_(pa.list_(pa.list_(pa.bool_())))),
            pa.array(psf_fwhm_arrays, type=pa.list_(pa.float32())),
            pa.array(scale_arrays, type=pa.list_(pa.float32())),
        ], names=["band", "flux", "ivar", "mask", "psf_fwhm", "scale"])

        columns["image"] = image_structs

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
