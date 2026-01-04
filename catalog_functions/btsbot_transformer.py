"""BTSbot HDF5 to PyArrow transformer."""

import pyarrow as pa
import numpy as np
from catalog_functions.utils import BaseTransformer
from datasets.features.features import Array2DExtensionType


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
    _image_size = 63

    def create_schema(self):
        fields = []

        # Image data as struct of lists (datasets Sequence creates this structure)
        # array field is a list of Array2D extension types (one 63x63 array per view)
        array_2d_type = Array2DExtensionType(shape=(self._image_size, self._image_size), dtype='float32')

        fields.append(
            pa.field(
                "image",
                pa.struct(
                    [
                        pa.field("band", pa.list_(pa.string())),
                        pa.field("view", pa.list_(pa.string())),
                        pa.field("array", pa.list_(array_2d_type)),  # list of Array2D extension types
                        pa.field("scale", pa.list_(pa.float32())),
                    ]
                ),
            )
        )

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

        # 1. Create image struct column (struct of lists, not list of structs)
        # image_triplet shape: [n_objects, 63, 63, 3] for 3 views
        image_triplet = data["image_triplet"][:]
        band_data = (
            np.char.decode(data["band"][:], encoding="utf-8")
            if isinstance(data["band"][0], bytes)
            else data["band"][:]
        )
        scale_data = data["image_scale"][:]

        band_arrays = [
            [band_data[obj_idx] for _ in range(len(self.VIEWS))]
            for obj_idx in range(n_objects)
        ]
        view_arrays = [self.VIEWS for _ in range(n_objects)]

        # Create array data in storage format (list of list of lists for each 2D array)
        array_arrays = [
            [[row.tolist() for row in image_triplet[i, :, :, j]] for j in range(3)]
            for i in range(n_objects)
        ]

        scale_arrays = [
            [float(scale_data[i]) for _j in range(3)] for i in range(n_objects)
        ]

        # Create Array2D extension type
        array_2d_type = Array2DExtensionType(shape=(self._image_size, self._image_size), dtype='float32')

        # Build array field using storage type structure
        # Storage format: list<list<list<float>>> where each inner list<list<float>> is one Array2D
        # We create the storage array and cast it to list<Array2D>
        storage_type = pa.list_(array_2d_type.storage_type)  # list<list<list<float>>>
        target_type = pa.list_(array_2d_type)  # list<Array2D>

        # Create array with storage type
        storage_array = pa.array(array_arrays, type=storage_type)

        # Cast to extension type (this wraps the storage with extension metadata)
        array_ext_array = storage_array.cast(target_type)

        columns["image"] = pa.StructArray.from_arrays(
            [
                pa.array(band_arrays, type=pa.list_(pa.string())),
                pa.array(view_arrays, type=pa.list_(pa.string())),
                array_ext_array,
                pa.array(scale_arrays, type=pa.list_(pa.float32())),
            ],
            names=["band", "view", "array", "scale"],
        )

        # 2. Add float features
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
            columns[f] = pa.array(
                [
                    s.decode("utf-8") if isinstance(s, bytes) else str(s)
                    for s in str_data
                ]
            )
        columns["object_id"] = pa.array(data["object_id"][:].astype(np.int64))

        return pa.table(columns, schema=self.create_schema())
