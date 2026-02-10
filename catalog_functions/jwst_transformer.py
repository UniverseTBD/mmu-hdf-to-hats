"""
JWSTTransformer: Clean class-based transformation from HDF5 to PyArrow tables.
"""

import pyarrow as pa
import numpy as np
from datasets.features.features import Array2DExtensionType
from catalog_functions.utils import BaseTransformer


class JWSTTransformer(BaseTransformer):
    """Transforms JWST HDF5 files to PyArrow tables with proper schema."""

    # Feature group definitions from jwst.py
    FLOAT_FEATURES = [
        "mag_auto",
        "flux_radius",
        "flux_auto",
        "fluxerr_auto",
        "cxx_image",
        "cyy_image",
        "cxy_image",
    ]

    # Default image size (can vary by config)
    IMAGE_SIZE = 96

    def create_schema(self):
        """Create the output PyArrow schema."""
        fields = []

        # Image as a struct with lists using Array2DExtensionType for 2D arrays
        # This matches the datasets format: struct<band: list<string>, flux: list<Array2D>, ...>
        arr2d_float = Array2DExtensionType((self.IMAGE_SIZE, self.IMAGE_SIZE), 'float32')
        arr2d_bool = Array2DExtensionType((self.IMAGE_SIZE, self.IMAGE_SIZE), 'bool')
        
        image_struct = pa.struct(
            [
                pa.field("band", pa.list_(pa.string())),
                pa.field("flux", pa.list_(arr2d_float)),  # list of 2D arrays
                pa.field("ivar", pa.list_(arr2d_float)),  # list of 2D arrays
                pa.field("mask", pa.list_(arr2d_bool)),  # list of 2D arrays
                pa.field("psf_fwhm", pa.list_(pa.float32())),
                pa.field("scale", pa.list_(pa.float32())),
            ]
        )
        fields.append(pa.field("image", image_struct))

        # Add all float features
        for f in self.FLOAT_FEATURES:
            fields.append(pa.field(f, pa.float32()))

        # Object ID
        fields.append(pa.field("object_id", pa.string()))
        
        # Coordinates (required for HATS)
        fields.append(pa.field("ra", pa.float64()))
        fields.append(pa.field("dec", pa.float64()))

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

        # Build struct with lists for each field (not list of structs)
        # Structure: struct<band: list<str>, flux: list<2D>, ivar: list<2D>, mask: list<2D>, psf_fwhm: list<float>, scale: list<float>>
        band_lists = []
        flux_lists = []
        ivar_lists = []
        mask_lists = []
        psf_fwhm_lists = []
        scale_lists = []

        for i in range(n_objects):
            n_bands = len(image_band[i])
            bands = []
            fluxes = []
            ivars = []
            masks = []
            psf_fwhms = []
            scales = []

            for j in range(n_bands):
                # Get band name from data
                band = image_band[i][j]
                if isinstance(band, bytes):
                    band = band.decode("utf-8")
                bands.append(band)

                # Convert 2D arrays to lists of lists
                flux_2d = image_flux[i][j]
                fluxes.append([row.tolist() for row in flux_2d])

                ivar_2d = image_ivar[i][j]
                ivars.append([row.tolist() for row in ivar_2d])

                mask_2d = image_mask[i][j].astype(bool)
                masks.append([row.tolist() for row in mask_2d])

                psf_fwhms.append(float(image_psf_fwhm[i][j]))
                scales.append(float(image_scale[i][j]))

            band_lists.append(bands)
            flux_lists.append(fluxes)
            ivar_lists.append(ivars)
            mask_lists.append(masks)
            psf_fwhm_lists.append(psf_fwhms)
            scale_lists.append(scales)

        # Create struct array with lists using Array2DExtensionType
        arr2d_float = Array2DExtensionType((self.IMAGE_SIZE, self.IMAGE_SIZE), 'float32')
        arr2d_bool = Array2DExtensionType((self.IMAGE_SIZE, self.IMAGE_SIZE), 'bool')
        
        image_struct_type = pa.struct(
            [
                pa.field("band", pa.list_(pa.string())),
                pa.field("flux", pa.list_(arr2d_float)),
                pa.field("ivar", pa.list_(arr2d_float)),
                pa.field("mask", pa.list_(arr2d_bool)),
                pa.field("psf_fwhm", pa.list_(pa.float32())),
                pa.field("scale", pa.list_(pa.float32())),
            ]
        )

        # Create arrays with storage types first, then cast to extension types
        # Storage type for Array2DExtensionType is list<list<dtype>>
        storage_type_float = pa.list_(pa.list_(pa.float32()))
        storage_type_bool = pa.list_(pa.list_(pa.bool_()))
        
        # Create with storage types
        flux_storage = pa.array(flux_lists, type=pa.list_(storage_type_float))
        ivar_storage = pa.array(ivar_lists, type=pa.list_(storage_type_float))
        mask_storage = pa.array(mask_lists, type=pa.list_(storage_type_bool))
        
        # Cast to extension types
        flux_ext = flux_storage.cast(pa.list_(arr2d_float))
        ivar_ext = ivar_storage.cast(pa.list_(arr2d_float))
        mask_ext = mask_storage.cast(pa.list_(arr2d_bool))

        image_structs = pa.StructArray.from_arrays(
            [
                pa.array(band_lists, type=pa.list_(pa.string())),
                flux_ext,
                ivar_ext,
                mask_ext,
                pa.array(psf_fwhm_lists, type=pa.list_(pa.float32())),
                pa.array(scale_lists, type=pa.list_(pa.float32())),
            ],
            names=["band", "flux", "ivar", "mask", "psf_fwhm", "scale"],
        )

        columns["image"] = image_structs

        # 2. Add float features
        for f in self.FLOAT_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float32))

        # 3. Add object_id
        columns["object_id"] = pa.array([str(oid) for oid in data["object_id"][:]])

        # 4. Add coordinates (ra, dec) - required for HATS
        # Convert to native byte order (HDF5 data big-endian)
        ra_data = data["ra"][:].astype(np.float64)
        dec_data = data["dec"][:].astype(np.float64)
        columns["ra"] = pa.array(ra_data, type=pa.float64())
        columns["dec"] = pa.array(dec_data, type=pa.float64())

        # Create table with schema
        schema = self.create_schema()
        table = pa.table(columns, schema=schema)

        return table

