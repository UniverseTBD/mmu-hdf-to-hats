"""
LegacySurveyTransformer: Clean class-based transformation from HDF5 to PyArrow tables.
"""
import pyarrow as pa
import numpy as np
from catalog_functions.base_transformer import BaseTransformer


class LegacySurveyTransformer(BaseTransformer):
    """Transforms Legacy Survey HDF5 files to PyArrow tables with proper schema."""

    # Feature group definitions from legacysurvey.py
    FLOAT_FEATURES = [
        "EBV", "FLUX_G", "FLUX_R", "FLUX_I", "FLUX_Z",
        "FLUX_W1", "FLUX_W2", "FLUX_W3", "FLUX_W4",
        "SHAPE_R", "SHAPE_E1", "SHAPE_E2",
    ]

    CATALOG_FEATURES = [
        "FLUX_G", "FLUX_R", "FLUX_I", "FLUX_Z",
        "TYPE", "SHAPE_R", "SHAPE_E1", "SHAPE_E2", "X", "Y",
    ]

    IMAGE_SIZE = 160
    BANDS = ['DES-G', 'DES-R', 'DES-I', 'DES-Z']

    def create_schema(self):
        """Create the output PyArrow schema."""
        fields = []

        # Image sequence with band, flux, mask, ivar, psf_fwhm, scale
        image_struct = pa.struct([
            pa.field("band", pa.string()),
            pa.field("flux", pa.list_(pa.list_(pa.float32()))),  # 2D array
            pa.field("mask", pa.list_(pa.list_(pa.bool_()))),    # 2D array
            pa.field("ivar", pa.list_(pa.list_(pa.float32()))),  # 2D array
            pa.field("psf_fwhm", pa.float32()),
            pa.field("scale", pa.float32()),
        ])
        fields.append(pa.field("image", pa.list_(image_struct)))

        # Blobmodel image (2D array)
        fields.append(pa.field("blobmodel", pa.list_(pa.list_(pa.uint8()))))

        # RGB image (3D array: height x width x 3)
        fields.append(pa.field("rgb", pa.list_(pa.list_(pa.list_(pa.uint8())))))

        # Object mask (2D array)
        fields.append(pa.field("object_mask", pa.list_(pa.list_(pa.uint8()))))

        # Catalog (list of objects with features)
        catalog_struct = pa.struct([
            pa.field("FLUX_G", pa.float32()),
            pa.field("FLUX_R", pa.float32()),
            pa.field("FLUX_I", pa.float32()),
            pa.field("FLUX_Z", pa.float32()),
            pa.field("TYPE", pa.string()),
            pa.field("SHAPE_R", pa.float32()),
            pa.field("SHAPE_E1", pa.float32()),
            pa.field("SHAPE_E2", pa.float32()),
            pa.field("X", pa.float32()),
            pa.field("Y", pa.float32()),
        ])
        fields.append(pa.field("catalog", pa.list_(catalog_struct)))

        # Add all float features
        for f in self.FLOAT_FEATURES:
            fields.append(pa.field(f, pa.float32()))

        # TYPE (string feature)
        fields.append(pa.field("TYPE", pa.string()))

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
        image_array = data["image_array"][:]
        image_mask = data["image_mask"][:]
        image_ivar = data["image_ivar"][:]
        image_band = data["image_band"][:]
        image_psf_fwhm = data["image_psf_fwhm"][:]
        image_scale = data["image_scale"][:]

        image_lists = []
        for i in range(n_objects):
            images_for_object = []
            for j in range(len(self.BANDS)):
                band = image_band[i][j]
                if isinstance(band, bytes):
                    band = band.decode('utf-8')

                flux_2d = image_array[i][j]
                flux_list = [row.tolist() for row in flux_2d]

                mask_2d = image_mask[i]
                mask_list = [row.tolist() for row in mask_2d]

                ivar_2d = image_ivar[i][j]
                ivar_list = [row.tolist() for row in ivar_2d]

                images_for_object.append({
                    'band': band,
                    'flux': flux_list,
                    'mask': mask_list,
                    'ivar': ivar_list,
                    'psf_fwhm': float(image_psf_fwhm[i][j]),
                    'scale': float(image_scale[i][j])
                })
            image_lists.append(images_for_object)

        # Create struct arrays
        band_arrays = []
        flux_arrays = []
        mask_arrays = []
        ivar_arrays = []
        psf_fwhm_arrays = []
        scale_arrays = []

        for obj_images in image_lists:
            band_arrays.append([img['band'] for img in obj_images])
            flux_arrays.append([img['flux'] for img in obj_images])
            mask_arrays.append([img['mask'] for img in obj_images])
            ivar_arrays.append([img['ivar'] for img in obj_images])
            psf_fwhm_arrays.append([img['psf_fwhm'] for img in obj_images])
            scale_arrays.append([img['scale'] for img in obj_images])

        columns["image"] = pa.StructArray.from_arrays([
            pa.array(band_arrays, type=pa.list_(pa.string())),
            pa.array(flux_arrays, type=pa.list_(pa.list_(pa.list_(pa.float32())))),
            pa.array(mask_arrays, type=pa.list_(pa.list_(pa.list_(pa.bool_())))),
            pa.array(ivar_arrays, type=pa.list_(pa.list_(pa.list_(pa.float32())))),
            pa.array(psf_fwhm_arrays, type=pa.list_(pa.float32())),
            pa.array(scale_arrays, type=pa.list_(pa.float32())),
        ], names=["band", "flux", "mask", "ivar", "psf_fwhm", "scale"])

        # 2. Add blobmodel (2D uint8 array)
        blobmodel_data = data["blobmodel"][:]
        blobmodel_lists = [[row.tolist() for row in img] for img in blobmodel_data]
        columns["blobmodel"] = pa.array(blobmodel_lists, type=pa.list_(pa.list_(pa.uint8())))

        # 3. Add RGB image (3D uint8 array)
        rgb_data = data["image_rgb"][:]
        rgb_lists = [[[pixel.tolist() for pixel in row] for row in img] for img in rgb_data]
        columns["rgb"] = pa.array(rgb_lists, type=pa.list_(pa.list_(pa.list_(pa.uint8()))))

        # 4. Add object mask (2D uint8 array)
        object_mask_data = data["object_mask"][:]
        object_mask_lists = [[row.tolist() for row in img] for img in object_mask_data]
        columns["object_mask"] = pa.array(object_mask_lists, type=pa.list_(pa.list_(pa.uint8())))

        # 5. Add catalog (list of objects)
        catalog_lists = []
        for i in range(n_objects):
            catalog_for_object = []
            n_catalog_objects = len(data[f"catalog_FLUX_G"][i])
            for j in range(n_catalog_objects):
                cat_type = data[f"catalog_TYPE"][i][j]
                if isinstance(cat_type, bytes):
                    cat_type = cat_type.decode('utf-8')

                catalog_for_object.append({
                    "FLUX_G": float(data[f"catalog_FLUX_G"][i][j]),
                    "FLUX_R": float(data[f"catalog_FLUX_R"][i][j]),
                    "FLUX_I": float(data[f"catalog_FLUX_I"][i][j]),
                    "FLUX_Z": float(data[f"catalog_FLUX_Z"][i][j]),
                    "TYPE": cat_type,
                    "SHAPE_R": float(data[f"catalog_SHAPE_R"][i][j]),
                    "SHAPE_E1": float(data[f"catalog_SHAPE_E1"][i][j]),
                    "SHAPE_E2": float(data[f"catalog_SHAPE_E2"][i][j]),
                    "X": float(data[f"catalog_X"][i][j]),
                    "Y": float(data[f"catalog_Y"][i][j]),
                })
            catalog_lists.append(catalog_for_object)

        # Convert catalog to PyArrow
        catalog_arrays = {
            "FLUX_G": [], "FLUX_R": [], "FLUX_I": [], "FLUX_Z": [],
            "TYPE": [], "SHAPE_R": [], "SHAPE_E1": [], "SHAPE_E2": [],
            "X": [], "Y": []
        }
        for cat_obj in catalog_lists:
            for key in catalog_arrays.keys():
                catalog_arrays[key].append([obj[key] for obj in cat_obj])

        catalog_struct = pa.StructArray.from_arrays([
            pa.array(catalog_arrays["FLUX_G"], type=pa.list_(pa.float32())),
            pa.array(catalog_arrays["FLUX_R"], type=pa.list_(pa.float32())),
            pa.array(catalog_arrays["FLUX_I"], type=pa.list_(pa.float32())),
            pa.array(catalog_arrays["FLUX_Z"], type=pa.list_(pa.float32())),
            pa.array(catalog_arrays["TYPE"], type=pa.list_(pa.string())),
            pa.array(catalog_arrays["SHAPE_R"], type=pa.list_(pa.float32())),
            pa.array(catalog_arrays["SHAPE_E1"], type=pa.list_(pa.float32())),
            pa.array(catalog_arrays["SHAPE_E2"], type=pa.list_(pa.float32())),
            pa.array(catalog_arrays["X"], type=pa.list_(pa.float32())),
            pa.array(catalog_arrays["Y"], type=pa.list_(pa.float32())),
        ], names=["FLUX_G", "FLUX_R", "FLUX_I", "FLUX_Z", "TYPE", "SHAPE_R", "SHAPE_E1", "SHAPE_E2", "X", "Y"])

        columns["catalog"] = catalog_struct

        # 6. Add float features
        for f in self.FLOAT_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float32))

        # 7. Add TYPE (string)
        type_data = data["TYPE"][:]
        columns["TYPE"] = pa.array([t.decode('utf-8') if isinstance(t, bytes) else str(t) for t in type_data])

        # 8. Add object_id
        columns["object_id"] = pa.array([str(oid) for oid in data["object_id"][:]])

        # Create table with schema
        schema = self.create_schema()
        table = pa.table(columns, schema=schema)

        return table
