"""
MaNGATransformer: Clean class-based transformation from HDF5 to PyArrow tables.
"""

import math
import h5py
import numpy as np
import pyarrow as pa
from hats_import.catalog.file_readers import InputReader
from upath import UPath
from catalog_functions.utils import BaseTransformer


def decode_if_needed(value):
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


class MangaGroupReader(InputReader):
    """Reader for MaNGA HDF5 files with one object group per top-level key."""

    def __init__(self, chunk_mb: float, transformer):
        super().__init__()
        self.chunk_bytes = chunk_mb * 1024 * 1024
        self.transformer = transformer

    @staticmethod
    def _group_value(group: h5py.Group, column: str):
        candidates = (column, column.upper())
        for candidate in candidates:
            if candidate in group:
                return decode_if_needed(group[candidate][()])
        raise KeyError(
            f"Column '{column}' not found in group '{group.name}'. "
            f"Available columns: {list(group.keys())}"
        )

    def _group_name_chunks(self, upath: UPath, h5_file: h5py.File) -> list[list[str]]:
        group_names = list(h5_file.keys())
        if not group_names:
            return []

        num_chunks = max(1, int(math.ceil(upath.stat().st_size / self.chunk_bytes)))
        chunk_size = max(1, int(math.ceil(len(group_names) / num_chunks)))
        return [
            group_names[start : start + chunk_size]
            for start in range(0, len(group_names), chunk_size)
        ]

    def read(self, input_file: str, read_columns: list[str] | None = None):
        upath = UPath(input_file)
        with upath.open("rb") as fh, h5py.File(fh) as h5_file:
            for group_names in self._group_name_chunks(upath, h5_file):
                if read_columns is not None:
                    scalar_rows: dict[str, list] = {column: [] for column in read_columns}
                    for group_name in group_names:
                        group = h5_file[group_name]
                        for column in read_columns:
                            scalar_rows[column].append(self._group_value(group, column))
                    yield pa.table(scalar_rows)
                    continue

                grouped_data = {group_name: h5_file[group_name] for group_name in group_names}
                yield self.transformer.dataset_to_table(grouped_data)


class MaNGATransformer(BaseTransformer):
    """Transforms MaNGA HDF5 files to PyArrow tables with proper schema."""

    # Constants from manga.py
    IMAGE_SIZE = 96
    IMAGE_FILTERS = ["G", "R", "I", "Z"]
    SPECTRUM_SIZE = 4563
    SPAXELS_PER_ROW = IMAGE_SIZE * IMAGE_SIZE  # upstream pads to 96x96 = 9216
    DOUBLE_FEATURES = ["ra", "dec"]

    def create_schema(self):
        """Create the output PyArrow schema."""
        fields = []

        # Metadata
        fields.append(pa.field("z", pa.float64()))
        fields.append(pa.field("spaxel_size", pa.float64()))
        fields.append(pa.field("spaxel_size_units", pa.string()))

        # Spaxels - list of structs with spectrum data
        # Note: flux, ivar, mask, lsf, lambda are 2D arrays (shape: 1 x spectrum_size)
        # to match datasets Array2D structure
        spectrum_2d = pa.list_(pa.list_(pa.float32(), self.SPECTRUM_SIZE), 1)
        spectrum_2d_int = pa.list_(pa.list_(pa.int32(), self.SPECTRUM_SIZE), 1)
        spaxel_struct = pa.struct(
            [
                pa.field("flux", spectrum_2d),
                pa.field("ivar", spectrum_2d),
                pa.field("mask", spectrum_2d_int),
                pa.field("lsf", spectrum_2d),
                pa.field("lambda", spectrum_2d),
                pa.field("x", pa.int64()),
                pa.field("y", pa.int64()),
                pa.field("spaxel_idx", pa.int64()),
                pa.field("flux_units", pa.string()),
                pa.field("lambda_units", pa.string()),
                pa.field("skycoo_x", pa.float32()),
                pa.field("skycoo_y", pa.float32()),
                pa.field("ellcoo_r", pa.float32()),
                pa.field("ellcoo_rre", pa.float32()),
                pa.field("ellcoo_rkpc", pa.float32()),
                pa.field("ellcoo_theta", pa.float32()),
                pa.field("skycoo_units", pa.string()),
                pa.field("ellcoo_r_units", pa.string()),
                pa.field("ellcoo_rre_units", pa.string()),
                pa.field("ellcoo_rkpc_units", pa.string()),
                pa.field("ellcoo_theta_units", pa.string()),
            ]
        )
        fields.append(pa.field("spaxels", pa.list_(spaxel_struct, self.SPAXELS_PER_ROW)))

        # Images - list of reconstructed griz images
        image_2d = pa.list_(pa.list_(pa.float32(), self.IMAGE_SIZE), self.IMAGE_SIZE)
        image_struct = pa.struct(
            [
                pa.field("filter", pa.string()),
                pa.field("flux", image_2d),
                pa.field("flux_units", pa.string()),
                pa.field("psf", image_2d),
                pa.field("psf_units", pa.string()),
                pa.field("scale", pa.float32()),
                pa.field("scale_units", pa.string()),
            ]
        )
        fields.append(pa.field("images", pa.list_(image_struct, len(self.IMAGE_FILTERS))))

        for f in self.DOUBLE_FEATURES:
            fields.append(pa.field(f, pa.float64()))
        # Maps - list of DAP analysis maps
        map_2d = pa.list_(pa.list_(pa.float32(), self.IMAGE_SIZE), self.IMAGE_SIZE)
        map_struct = pa.struct(
            [
                pa.field("group", pa.string()),
                pa.field("label", pa.string()),
                pa.field("flux", map_2d),
                pa.field("ivar", map_2d),
                pa.field("mask", map_2d),
                pa.field("array_units", pa.string()),
            ]
        )
        fields.append(pa.field("maps", pa.list_(map_struct)))

        # Object ID
        fields.append(pa.field("object_id", pa.string()))

        return pa.schema(fields)

    @staticmethod
    def _decode_bytes_array(values):
        if h5py.check_string_dtype(values.dtype) is not None:
            return values.astype(str)
        if values.dtype.kind == "S":
            return np.char.decode(values, "utf-8")
        return values

    @staticmethod
    def _ndarray_to_fixed_size_list(array: np.ndarray) -> pa.Array:
        """Convert an N-D array to nested FixedSizeListArrays without Python lists."""
        values = pa.array(array.reshape(-1))
        for size in reversed(array.shape[1:]):
            values = pa.FixedSizeListArray.from_arrays(values, size)
        return values

    @staticmethod
    def _wrap_variable_list(values: pa.Array) -> pa.Array:
        offsets = pa.array([0, len(values)], type=pa.int32())
        return pa.ListArray.from_arrays(offsets, values)

    @staticmethod
    def _wrap_fixed_size_list(values: pa.Array, list_size: int) -> pa.Array:
        return pa.FixedSizeListArray.from_arrays(values, list_size)

    def _group_to_table(self, grp: h5py.Group) -> pa.Table:
        schema = self.create_schema()

        spaxels = grp["spaxels"][:]
        images = grp["images"][:]
        maps = grp["maps"][:]

        spaxel_struct_type = schema.field("spaxels").type.value_type
        image_struct_type = schema.field("images").type.value_type
        map_struct_type = schema.field("maps").type.value_type

        spaxel_struct = pa.StructArray.from_arrays(
            [
                self._ndarray_to_fixed_size_list(spaxels["flux"][:, np.newaxis, :]),
                self._ndarray_to_fixed_size_list(spaxels["ivar"][:, np.newaxis, :]),
                self._ndarray_to_fixed_size_list(spaxels["mask"][:, np.newaxis, :]),
                self._ndarray_to_fixed_size_list(spaxels["lsf_sigma"][:, np.newaxis, :]),
                self._ndarray_to_fixed_size_list(spaxels["lambda"][:, np.newaxis, :]),
                pa.array(spaxels["x"], type=pa.int64()),
                pa.array(spaxels["y"], type=pa.int64()),
                pa.array(spaxels["spaxel_idx"], type=pa.int64()),
                pa.array(self._decode_bytes_array(spaxels["flux_units"]), type=pa.string()),
                pa.array(self._decode_bytes_array(spaxels["lambda_units"]), type=pa.string()),
                pa.array(spaxels["skycoo_x"], type=pa.float32()),
                pa.array(spaxels["skycoo_y"], type=pa.float32()),
                pa.array(spaxels["ellcoo_r"], type=pa.float32()),
                pa.array(spaxels["ellcoo_rre"], type=pa.float32()),
                pa.array(spaxels["ellcoo_rkpc"], type=pa.float32()),
                pa.array(spaxels["ellcoo_theta"], type=pa.float32()),
                pa.array(self._decode_bytes_array(spaxels["skycoo_units"]), type=pa.string()),
                pa.array(self._decode_bytes_array(spaxels["ellcoo_r_units"]), type=pa.string()),
                pa.array(self._decode_bytes_array(spaxels["ellcoo_rre_units"]), type=pa.string()),
                pa.array(self._decode_bytes_array(spaxels["ellcoo_rkpc_units"]), type=pa.string()),
                pa.array(self._decode_bytes_array(spaxels["ellcoo_theta_units"]), type=pa.string()),
            ],
            fields=list(spaxel_struct_type),
        )

        image_struct = pa.StructArray.from_arrays(
            [
                pa.array(self._decode_bytes_array(images["image_band"]), type=pa.string()),
                self._ndarray_to_fixed_size_list(images["image_array"]),
                pa.array(self._decode_bytes_array(images["image_array_units"]), type=pa.string()),
                self._ndarray_to_fixed_size_list(images["image_psf"]),
                pa.array(self._decode_bytes_array(images["image_psf_units"]), type=pa.string()),
                pa.array(images["image_scale"], type=pa.float32()),
                pa.array(self._decode_bytes_array(images["image_scale_units"]), type=pa.string()),
            ],
            fields=list(image_struct_type),
        )

        map_struct = pa.StructArray.from_arrays(
            [
                pa.array(self._decode_bytes_array(maps["group"]), type=pa.string()),
                pa.array(self._decode_bytes_array(maps["label"]), type=pa.string()),
                self._ndarray_to_fixed_size_list(maps["array"]),
                self._ndarray_to_fixed_size_list(maps["ivar"]),
                self._ndarray_to_fixed_size_list(maps["mask"]),
                pa.array(self._decode_bytes_array(maps["array_units"]), type=pa.string()),
            ],
            fields=list(map_struct_type),
        )

        arrays = [
            pa.array([float(grp["z"][()])], type=pa.float64()),
            pa.array([float(grp["spaxel_size"][()])], type=pa.float64()),
            pa.array([decode_if_needed(grp["spaxel_size_unit"][()])], type=pa.string()),
            self._wrap_fixed_size_list(spaxel_struct, self.SPAXELS_PER_ROW),
            self._wrap_fixed_size_list(image_struct, len(self.IMAGE_FILTERS)),
            pa.array([float(grp["ra"][()])], type=pa.float64()),
            pa.array([float(grp["dec"][()])], type=pa.float64()),
            self._wrap_variable_list(map_struct),
            pa.array([decode_if_needed(grp["object_id"][()])], type=pa.string()),
        ]

        return pa.Table.from_arrays(arrays, schema=schema)

    def dataset_to_table(self, data):
        """
        Convert HDF5 dataset to PyArrow table.

        Args:
            data: HDF5 file or dict of datasets (MaNGA has hierarchical groups)

        Returns:
            pa.Table: Transformed Arrow table
        """
        tables = [self._group_to_table(data[key]) for key in data.keys()]
        if not tables:
            return self.create_schema().empty_table()
        if len(tables) == 1:
            return tables[0]
        return pa.concat_tables(tables)
