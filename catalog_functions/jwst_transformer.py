"""
JWSTTransformer: Clean class-based transformation from HDF5 to PyArrow tables.
"""

import pyarrow as pa
import numpy as np
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

    def create_schema(self):
        """Create the output PyArrow schema."""
        fields = []

        image_struct = pa.struct(
            [
                pa.field("band", pa.list_(pa.string())),
                pa.field("flux", pa.list_(pa.list_(pa.list_(pa.float32())))),
                pa.field("ivar", pa.list_(pa.list_(pa.list_(pa.float32())))),
                pa.field("mask", pa.list_(pa.list_(pa.list_(pa.bool_())))),
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

        image_struct_type = pa.struct(
            [
                pa.field("band", pa.list_(pa.string())),
                pa.field("flux", pa.list_(pa.list_(pa.list_(pa.float32())))),
                pa.field("ivar", pa.list_(pa.list_(pa.list_(pa.float32())))),
                pa.field("mask", pa.list_(pa.list_(pa.list_(pa.bool_())))),
                pa.field("psf_fwhm", pa.list_(pa.float32())),
                pa.field("scale", pa.list_(pa.float32())),
            ]
        )

        image_structs = []
        for i in range(n_objects):
            image_structs.append({
                "band": band_lists[i],
                "flux": flux_lists[i],
                "ivar": ivar_lists[i],
                "mask": mask_lists[i],
                "psf_fwhm": psf_fwhm_lists[i],
                "scale": scale_lists[i],
            })
        columns["image"] = pa.array(image_structs, type=image_struct_type)

        # 2. Add float features (convert from big-endian HDF5 to little-endian)
        for f in self.FLOAT_FEATURES:
            columns[f] = pa.array(data[f][:].astype('<f4'))

        # 3. Add object_id
        columns["object_id"] = pa.array([str(oid) for oid in data["object_id"][:]])

        # 4. Add coordinates (ra, dec) - required for HATS
        # Convert from big-endian HDF5 to little-endian
        columns["ra"] = pa.array(data["ra"][:].astype('<f8'), type=pa.float64())
        columns["dec"] = pa.array(data["dec"][:].astype('<f8'), type=pa.float64())

        # Create table with schema
        schema = self.create_schema()
        table = pa.table(columns, schema=schema)

        return table

