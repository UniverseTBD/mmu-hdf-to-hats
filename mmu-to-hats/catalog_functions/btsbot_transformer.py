"""
BTSbotTransformer: Clean class-based transformation from HDF5 to PyArrow tables.
"""

import pyarrow as pa
import numpy as np
from catalog_functions.utils import BaseTransformer


class BTSbotTransformer(BaseTransformer):
    """Transforms BTSbot HDF5 files to PyArrow tables with proper schema."""

    # Feature group definitions from btsbot.py
    FLOAT_FEATURES = [
        "jd",
        "diffmaglim",
        "magpsf",
        "sigmapsf",
        "chipsf",
        "magap",
        "sigmagap",
        "distnr",
        "magnr",
        "chinr",
        "sharpnr",
        "sky",
        "magdiff",
        "fwhm",
        "classtar",
        "mindtoedge",
        "seeratio",
        "magapbig",
        "sigmagapbig",
        "sgmag1",
        "srmag1",
        "simag1",
        "szmag1",
        "sgscore1",
        "distpsnr1",
        "jdstarthist",
        "scorr",
        "sgmag2",
        "srmag2",
        "simag2",
        "szmag2",
        "sgscore2",
        "distpsnr2",
        "sgmag3",
        "srmag3",
        "simag3",
        "szmag3",
        "sgscore3",
        "distpsnr3",
        "jdstartref",
        "dsnrms",
        "ssnrms",
        "magzpsci",
        "magzpsciunc",
        "magzpscirms",
        "clrcoeff",
        "clrcounc",
        "neargaia",
        "neargaiabright",
        "maggaia",
        "maggaiabright",
        "exptime",
        "drb",
        "acai_h",
        "acai_v",
        "acai_o",
        "acai_n",
        "acai_b",
        "new_drb",
        "peakmag",
        "maxmag",
        "peakmag_so_far",
        "maxmag_so_far",
        "age",
        "days_since_peak",
        "days_to_peak",
    ]

    INT_FEATURES = [
        "label",
        "fid",
        "programid",
        "field",
        "nneg",
        "nbad",
        "ndethist",
        "ncovhist",
        "nmtchps",
        "nnotdet",
        "N",
    ]

    BOOL_FEATURES = [
        "isdiffpos",
        "is_SN",
        "near_threshold",
        "is_rise",
    ]

    STRING_FEATURES = [
        "OBJECT_ID_",
        "source_set",
        "split",
    ]

    IMAGE_SIZE = 63
    VIEWS = ["science", "reference", "difference"]

    def create_schema(self):
        """Create the output PyArrow schema."""
        fields = []

        # Image sequence with band, view, array, scale
        image_struct = pa.struct(
            [
                pa.field("band", pa.string()),
                pa.field("view", pa.string()),
                pa.field("array", pa.list_(pa.list_(pa.float32()))),  # 2D array
                pa.field("scale", pa.float32()),
            ]
        )
        fields.append(pa.field("image", pa.list_(image_struct)))

        # Add all float features
        for f in self.FLOAT_FEATURES:
            fields.append(pa.field(f, pa.float32()))

        # Add all int features (note: object_id is int64 in BTSbot)
        for f in self.INT_FEATURES:
            fields.append(pa.field(f, pa.int64()))

        # Add all boolean features
        for f in self.BOOL_FEATURES:
            fields.append(pa.field(f, pa.bool_()))

        # Add all string features
        for f in self.STRING_FEATURES:
            fields.append(pa.field(f, pa.string()))

        # Object ID (int64 in BTSbot)
        fields.append(pa.field("object_id", pa.int64()))

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
        # image_triplet shape: [n_objects, 63, 63, 3] for 3 views
        image_triplet = data["image_triplet"][:]
        band_data = data["band"][:]
        scale_data = data["image_scale"][:]

        image_lists = []
        for i in range(n_objects):
            images_for_object = []
            for j, view in enumerate(self.VIEWS):
                img_array = image_triplet[i, :, :, j]
                # Convert 2D array to list of lists
                img_list = [row.tolist() for row in img_array]

                band = band_data[i]
                if isinstance(band, bytes):
                    band = band.decode("utf-8")

                images_for_object.append(
                    {
                        "band": band,
                        "view": view,
                        "array": img_list,
                        "scale": float(scale_data[i]),
                    }
                )
            image_lists.append(images_for_object)

        # Create struct arrays for images
        band_arrays = []
        view_arrays = []
        array_arrays = []
        scale_arrays = []

        for obj_images in image_lists:
            band_arrays.append([img["band"] for img in obj_images])
            view_arrays.append([img["view"] for img in obj_images])
            array_arrays.append([img["array"] for img in obj_images])
            scale_arrays.append([img["scale"] for img in obj_images])

        image_structs = pa.StructArray.from_arrays(
            [
                pa.array(band_arrays, type=pa.list_(pa.string())),
                pa.array(view_arrays, type=pa.list_(pa.string())),
                pa.array(array_arrays, type=pa.list_(pa.list_(pa.list_(pa.float32())))),
                pa.array(scale_arrays, type=pa.list_(pa.float32())),
            ],
            names=["band", "view", "array", "scale"],
        )

        columns["image"] = image_structs

        # 2. Add float features
        for f in self.FLOAT_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float32))

        # 3. Add int features
        for f in self.INT_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.int64))

        # 4. Add boolean features
        for f in self.BOOL_FEATURES:
            columns[f] = pa.array(data[f][:].astype(bool))

        # 5. Add string features
        for f in self.STRING_FEATURES:
            str_data = data[f][:]
            columns[f] = pa.array(
                [
                    s.decode("utf-8") if isinstance(s, bytes) else str(s)
                    for s in str_data
                ]
            )

        # 6. Add object_id (int64 in BTSbot)
        columns["object_id"] = pa.array(data["object_id"][:].astype(np.int64))

        # Create table with schema
        schema = self.create_schema()
        table = pa.table(columns, schema=schema)

        return table
