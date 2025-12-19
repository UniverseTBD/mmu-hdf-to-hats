"""
LegacySurveyTransformer: Transforms HDF5 to PyArrow tables matching datasets library format.
"""

import pyarrow as pa
import numpy as np
from PIL import Image
import io
from catalog_functions.utils import BaseTransformer


class LegacySurveyTransformer(BaseTransformer):
    """Transforms Legacy Survey HDF5 files to PyArrow tables matching datasets format."""

    # Feature group definitions from legacysurvey.py
    FLOAT_FEATURES = [
        "EBV",
        "FLUX_G",
        "FLUX_R",
        "FLUX_I",
        "FLUX_Z",
        "FLUX_W1",
        "FLUX_W2",
        "FLUX_W3",
        "FLUX_W4",
        "SHAPE_R",
        "SHAPE_E1",
        "SHAPE_E2",
    ]

    CATALOG_FEATURES = [
        "FLUX_G",
        "FLUX_R",
        "FLUX_I",
        "FLUX_Z",
        "TYPE",
        "SHAPE_R",
        "SHAPE_E1",
        "SHAPE_E2",
        "X",
        "Y",
    ]

    IMAGE_SIZE = 160
    BANDS = ["DES-G", "DES-R", "DES-I", "DES-Z"]

    def create_schema(self) -> pa.Schema:
        """Create the output PyArrow schema matching datasets format."""
        fields = []

        # Image struct with flattened lists (matching datasets Sequence behavior)
        # {'band': List(Value('string')), 'flux': List(Array2D(...)), ...}
        image_struct = pa.struct([
            pa.field("band", pa.list_(pa.string())),
            pa.field("flux", pa.list_(pa.list_(pa.list_(pa.float32())))),  # List of 2D arrays
            pa.field("mask", pa.list_(pa.list_(pa.list_(pa.bool_())))),
            pa.field("ivar", pa.list_(pa.list_(pa.list_(pa.float32())))),
            pa.field("psf_fwhm", pa.list_(pa.float32())),
            pa.field("scale", pa.list_(pa.float32())),
        ])
        fields.append(pa.field("image", image_struct))

        # Image types - datasets stores as {'bytes': binary, 'path': string}
        image_type = pa.struct([
            pa.field("bytes", pa.binary()),
            pa.field("path", pa.string()),
        ])
        fields.append(pa.field("blobmodel", image_type))
        fields.append(pa.field("rgb", image_type))
        fields.append(pa.field("object_mask", image_type))

        # Catalog struct with flattened lists
        # Note: TYPE becomes float32 in the catalog (matching the schema you showed)
        catalog_struct = pa.struct([
            pa.field("FLUX_G", pa.list_(pa.float32())),
            pa.field("FLUX_R", pa.list_(pa.float32())),
            pa.field("FLUX_I", pa.list_(pa.float32())),
            pa.field("FLUX_Z", pa.list_(pa.float32())),
            pa.field("TYPE", pa.list_(pa.float32())),  # datasets converts to float32
            pa.field("SHAPE_R", pa.list_(pa.float32())),
            pa.field("SHAPE_E1", pa.list_(pa.float32())),
            pa.field("SHAPE_E2", pa.list_(pa.float32())),
            pa.field("X", pa.list_(pa.float32())),
            pa.field("Y", pa.list_(pa.float32())),
        ])
        fields.append(pa.field("catalog", catalog_struct))

        # Add all float features
        for f in self.FLOAT_FEATURES:
            fields.append(pa.field(f, pa.float32()))

        # Object ID
        fields.append(pa.field("object_id", pa.string()))

        # Coordinates (added by the processing step)
        fields.append(pa.field("ra", pa.float64()))
        fields.append(pa.field("dec", pa.float64()))

        return pa.schema(fields)

    def _array_to_image_bytes(self, array: np.ndarray) -> bytes:
        """Convert numpy array to PNG bytes (matching datasets Image behavior)."""
        # Ensure uint8
        if array.dtype != np.uint8:
            array = array.astype(np.uint8)
        
        # Create PIL Image
        if array.ndim == 2:
            img = Image.fromarray(array, mode='L')
        elif array.ndim == 3:
            if array.shape[2] == 3:
                img = Image.fromarray(array, mode='RGB')
            elif array.shape[2] == 4:
                img = Image.fromarray(array, mode='RGBA')
            else:
                raise ValueError(f"Unexpected array shape: {array.shape}")
        else:
            raise ValueError(f"Unexpected array dimensions: {array.ndim}")
        
        # Convert to PNG bytes
        buffer = io.BytesIO()
        img.save(buffer, format='PNG')
        return buffer.getvalue()

    def dataset_to_table(self, data) -> pa.Table:
        """
        Convert HDF5 dataset to PyArrow table matching datasets library format.

        Args:
            data: HDF5 file or dict of datasets

        Returns:
            pa.Table: Transformed Arrow table
        """
        columns = {}
        n_objects = len(data["object_id"][:])

        # 1. Create image column (flattened lists within struct)
        image_array = data["image_array"][:]
        image_mask = data["image_mask"][:]
        image_ivar = data["image_ivar"][:]
        image_band = data["image_band"][:]
        image_psf_fwhm = data["image_psf_fwhm"][:]
        image_scale = data["image_scale"][:]

        # Build flattened lists for each field
        band_lists = []
        flux_lists = []
        mask_lists = []
        ivar_lists = []
        psf_fwhm_lists = []
        scale_lists = []

        for i in range(n_objects):
            # Each object has a list of bands
            bands = []
            fluxes = []
            ivars = []
            psf_fwhms = []
            scales = []

            for j in range(len(self.BANDS)):
                band = image_band[i][j]
                if isinstance(band, bytes):
                    band = band.decode("utf-8")
                bands.append(band)

                # 2D arrays as list of lists
                flux_2d = image_array[i][j]
                fluxes.append([row.tolist() for row in flux_2d])

                ivar_2d = image_ivar[i][j]
                ivars.append([row.tolist() for row in ivar_2d])

                psf_fwhms.append(float(image_psf_fwhm[i][j]))
                scales.append(float(image_scale[i][j]))

            # Mask is shared across bands (2D array)
            mask_2d = image_mask[i]
            masks = [[row.tolist() for row in mask_2d]] * len(self.BANDS)

            band_lists.append(bands)
            flux_lists.append(fluxes)
            mask_lists.append(masks)
            ivar_lists.append(ivars)
            psf_fwhm_lists.append(psf_fwhms)
            scale_lists.append(scales)

        # Create struct array for image
        image_struct_type = pa.struct([
            pa.field("band", pa.list_(pa.string())),
            pa.field("flux", pa.list_(pa.list_(pa.list_(pa.float32())))),
            pa.field("mask", pa.list_(pa.list_(pa.list_(pa.bool_())))),
            pa.field("ivar", pa.list_(pa.list_(pa.list_(pa.float32())))),
            pa.field("psf_fwhm", pa.list_(pa.float32())),
            pa.field("scale", pa.list_(pa.float32())),
        ])

        image_structs = []
        for i in range(n_objects):
            image_structs.append({
                "band": band_lists[i],
                "flux": flux_lists[i],
                "mask": mask_lists[i],
                "ivar": ivar_lists[i],
                "psf_fwhm": psf_fwhm_lists[i],
                "scale": scale_lists[i],
            })
        columns["image"] = pa.array(image_structs, type=image_struct_type)

        # 2. Add blobmodel, rgb, object_mask as Image type (bytes + path struct)
        image_type = pa.struct([
            pa.field("bytes", pa.binary()),
            pa.field("path", pa.string()),
        ])

        blobmodel_data = data["blobmodel"][:]
        blobmodel_structs = []
        for img in blobmodel_data:
            blobmodel_structs.append({
                "bytes": self._array_to_image_bytes(img),
                "path": None,
            })
        columns["blobmodel"] = pa.array(blobmodel_structs, type=image_type)

        rgb_data = data["image_rgb"][:]
        rgb_structs = []
        for img in rgb_data:
            rgb_structs.append({
                "bytes": self._array_to_image_bytes(img),
                "path": None,
            })
        columns["rgb"] = pa.array(rgb_structs, type=image_type)

        object_mask_data = data["object_mask"][:]
        object_mask_structs = []
        for img in object_mask_data:
            object_mask_structs.append({
                "bytes": self._array_to_image_bytes(img),
                "path": None,
            })
        columns["object_mask"] = pa.array(object_mask_structs, type=image_type)

        # 3. Add catalog (struct with flattened lists)
        catalog_struct_type = pa.struct([
            pa.field("FLUX_G", pa.list_(pa.float32())),
            pa.field("FLUX_R", pa.list_(pa.float32())),
            pa.field("FLUX_I", pa.list_(pa.float32())),
            pa.field("FLUX_Z", pa.list_(pa.float32())),
            pa.field("TYPE", pa.list_(pa.float32())),
            pa.field("SHAPE_R", pa.list_(pa.float32())),
            pa.field("SHAPE_E1", pa.list_(pa.float32())),
            pa.field("SHAPE_E2", pa.list_(pa.float32())),
            pa.field("X", pa.list_(pa.float32())),
            pa.field("Y", pa.list_(pa.float32())),
        ])

        catalog_structs = []
        for i in range(n_objects):
            n_catalog_objects = len(data["catalog_FLUX_G"][i])
            
            # Build lists for this object's catalog
            cat_entry = {
                "FLUX_G": [float(data["catalog_FLUX_G"][i][j]) for j in range(n_catalog_objects)],
                "FLUX_R": [float(data["catalog_FLUX_R"][i][j]) for j in range(n_catalog_objects)],
                "FLUX_I": [float(data["catalog_FLUX_I"][i][j]) for j in range(n_catalog_objects)],
                "FLUX_Z": [float(data["catalog_FLUX_Z"][i][j]) for j in range(n_catalog_objects)],
                "SHAPE_R": [float(data["catalog_SHAPE_R"][i][j]) for j in range(n_catalog_objects)],
                "SHAPE_E1": [float(data["catalog_SHAPE_E1"][i][j]) for j in range(n_catalog_objects)],
                "SHAPE_E2": [float(data["catalog_SHAPE_E2"][i][j]) for j in range(n_catalog_objects)],
                "X": [float(data["catalog_X"][i][j]) for j in range(n_catalog_objects)],
                "Y": [float(data["catalog_Y"][i][j]) for j in range(n_catalog_objects)],
            }
            
            # TYPE needs special handling - convert to float
            type_values = []
            for j in range(n_catalog_objects):
                cat_type = data["catalog_TYPE"][i][j]
                if isinstance(cat_type, bytes):
                    # Convert string type to numeric
                    cat_type = cat_type.decode("utf-8")
                else:
                    type_values.append(float(cat_type))
            cat_entry["TYPE"] = type_values
            
            catalog_structs.append(cat_entry)

        columns["catalog"] = pa.array(catalog_structs, type=catalog_struct_type)

        # 4. Add float features
        for f in self.FLOAT_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float32))

        # 5. Add object_id (skip TYPE at top level - it's only in catalog now based on schema)
        columns["object_id"] = pa.array([str(oid) for oid in data["object_id"][:]])

        # 6. Add ra/dec from the data
        columns["ra"] = pa.array(data["ra"][:].astype(np.float64))
        columns["dec"] = pa.array(data["dec"][:].astype(np.float64))

        # Create table with schema
        schema = self.create_schema()
        table = pa.table(columns, schema=schema)

        return table
