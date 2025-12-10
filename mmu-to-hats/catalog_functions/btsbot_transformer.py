"""BTSbot HDF5 to PyArrow transformer."""

import pyarrow as pa
import numpy as np
from catalog_functions.utils import BaseTransformer


class BTSbotTransformer(BaseTransformer):
    """Transforms BTSbot HDF5 files to PyArrow tables."""
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
        "healpix",
    ]

    DOUBLE_FEATURES = [
        "ra",
        "dec",
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

    VIEWS = ["science", "reference", "difference"]

    def create_schema(self):
        fields = []

        # Datasets library's Sequence produces struct-of-lists, not list-of-structs
        image_struct = pa.struct(
            [
                pa.field("band", pa.list_(pa.string())),
                pa.field("view", pa.list_(pa.string())),
                pa.field("array", pa.list_(pa.list_(pa.list_(pa.float32())))),  # list of 2D arrays
                pa.field("scale", pa.list_(pa.float32())),
            ]
        )
        fields.append(pa.field("image", image_struct))

        for f in self.FLOAT_FEATURES:
            fields.append(pa.field(f, pa.float32()))
        for f in self.DOUBLE_FEATURES:
            fields.append(pa.field(f, pa.float64()))
        for f in self.INT_FEATURES:
            fields.append(pa.field(f, pa.int64()))
        for f in self.BOOL_FEATURES:
            fields.append(pa.field(f, pa.bool_()))
        for f in self.STRING_FEATURES:
            fields.append(pa.field(f, pa.string()))
        fields.append(pa.field("object_id", pa.int64()))

        return pa.schema(fields)

    def dataset_to_table(self, data):
        columns = {}
        n_objects = len(data["object_id"][:])

        # Build image struct-of-lists (matches datasets library Sequence output)
        image_triplet = data["image_triplet"][:]  # shape: [n_objects, 63, 63, 3]
        band_data = data["band"][:]
        scale_data = data["image_scale"][:]

        band_arrays = []
        view_arrays = []
        array_arrays = []
        scale_arrays = []

        for i in range(n_objects):
            band = band_data[i].decode("utf-8") if isinstance(band_data[i], bytes) else band_data[i]
            scale = float(scale_data[i])

            band_arrays.append([band] * len(self.VIEWS))
            view_arrays.append(list(self.VIEWS))
            array_arrays.append([
                [row.tolist() for row in image_triplet[i, :, :, j]]
                for j in range(len(self.VIEWS))
            ])
            scale_arrays.append([scale] * len(self.VIEWS))

        columns["image"] = pa.StructArray.from_arrays(
            [
                pa.array(band_arrays, type=pa.list_(pa.string())),
                pa.array(view_arrays, type=pa.list_(pa.string())),
                pa.array(array_arrays, type=pa.list_(pa.list_(pa.list_(pa.float32())))),
                pa.array(scale_arrays, type=pa.list_(pa.float32())),
            ],
            names=["band", "view", "array", "scale"],
        )

        for f in self.FLOAT_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float32))
        for f in self.DOUBLE_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float64))
        for f in self.INT_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.int64))
        for f in self.BOOL_FEATURES:
            columns[f] = pa.array(data[f][:].astype(bool))
        for f in self.STRING_FEATURES:
            str_data = data[f][:]
            columns[f] = pa.array([
                s.decode("utf-8") if isinstance(s, bytes) else str(s)
                for s in str_data
            ])
        columns["object_id"] = pa.array(data["object_id"][:].astype(np.int64))

        return pa.table(columns, schema=self.create_schema())
