"""
LegacySurveyTransformer: Optimized transformation from HDF5 to PyArrow tables.
"""

import io
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pyarrow as pa
from PIL import Image

from catalog_functions.utils import BaseTransformer

PNG_THREADS = 4


def _encode_png(array: np.ndarray) -> bytes:
    """Encode a numpy array as PNG bytes. PIL releases the GIL during zlib compression."""
    buf = io.BytesIO()
    Image.fromarray(array).save(buf, format="PNG")
    return buf.getvalue()


def _arr_to_nested_list_array(arr: np.ndarray, leaf_type: pa.DataType) -> pa.Array:
    """Convert an N-D numpy array to a nested pa.ListArray without Python loops.

    Shape (d0, d1, ..., dn) → list<list<...<leaf_type>...>> of length d0.
    Uses ListArray.from_arrays with precomputed offsets — zero Python objects created.
    """
    if arr.dtype.byteorder == ">":
        arr = arr.byteswap().view(arr.dtype.newbyteorder("<"))
    result = pa.array(arr.reshape(-1), type=leaf_type)
    for size in reversed(arr.shape[1:]):
        n_lists = len(result) // size
        offsets = np.arange(n_lists + 1, dtype=np.int32) * size
        result = pa.ListArray.from_arrays(offsets, result)
    return result


def _make_image_struct(byte_list: list) -> pa.StructArray:
    """Build a HuggingFace-compatible {bytes, path} image struct array."""
    return pa.StructArray.from_arrays(
        [
            pa.array(byte_list, type=pa.binary()),
            pa.nulls(len(byte_list), type=pa.string()),
        ],
        names=["bytes", "path"],
    )


class LegacySurveyTransformer(BaseTransformer):
    """Transforms Legacy Survey HDF5 files to PyArrow tables matching datasets format."""

    FLOAT_FEATURES = [
        "EBV",
        "FLUX_G", "FLUX_R", "FLUX_I", "FLUX_Z",
        "FLUX_W1", "FLUX_W2", "FLUX_W3", "FLUX_W4",
        "SHAPE_R", "SHAPE_E1", "SHAPE_E2",
    ]

    CATALOG_FEATURES = [
        "FLUX_G", "FLUX_R", "FLUX_I", "FLUX_Z",
        "TYPE", "SHAPE_R", "SHAPE_E1", "SHAPE_E2", "X", "Y",
    ]

    DOUBLE_FEATURES = ["ra", "dec"]
    IMAGE_SIZE = 160
    BANDS = ["DES-G", "DES-R", "DES-I", "DES-Z"]

    def create_schema(self) -> pa.Schema:
        image_type = pa.struct([
            pa.field("bytes", pa.binary()),
            pa.field("path", pa.string()),
        ])
        image_struct = pa.struct([
            pa.field("band", pa.list_(pa.string())),
            pa.field("flux", pa.list_(pa.list_(pa.list_(pa.float32())))),
            pa.field("mask", pa.list_(pa.list_(pa.list_(pa.bool_())))),
            pa.field("ivar", pa.list_(pa.list_(pa.list_(pa.float32())))),
            pa.field("psf_fwhm", pa.list_(pa.float32())),
            pa.field("scale", pa.list_(pa.float32())),
        ])
        catalog_struct = pa.struct([
            pa.field(f, pa.list_(pa.float32())) for f in self.CATALOG_FEATURES
        ])
        return pa.schema([
            pa.field("image", image_struct),
            pa.field("blobmodel", image_type),
            pa.field("rgb", image_type),
            pa.field("object_mask", image_type),
            pa.field("catalog", catalog_struct),
            *[pa.field(f, pa.float32()) for f in self.FLOAT_FEATURES],
            pa.field("object_id", pa.string()),
            pa.field("ra", pa.float64()),
            pa.field("dec", pa.float64()),
        ])

    def dataset_to_table(self, data) -> pa.Table:
        n = len(data["object_id"][:])

        # 1. Parallel PNG encoding for the 3 image types (blobmodel, rgb, object_mask).
        #    All 3*n tasks submitted together; PIL releases GIL during zlib compression.
        blobmodel_arrs = data["blobmodel"][:]           # (N, 160, 160, 3) uint8
        rgb_arrs       = data["image_rgb"][:]           # (N, 160, 160, 3) uint8
        omask_arrs     = data["object_mask"][:]         # (N, 160, 160) uint8

        all_arrs = [*blobmodel_arrs, *rgb_arrs, *omask_arrs]
        with ThreadPoolExecutor(max_workers=PNG_THREADS) as ex:
            all_bytes = list(ex.map(_encode_png, all_arrs))

        blobmodel_bytes = all_bytes[:n]
        rgb_bytes       = all_bytes[n:2 * n]
        omask_bytes     = all_bytes[2 * n:]

        # 2. Vectorised image struct — no Python loops, no intermediate Python objects.
        image_array  = data["image_array"][:]           # (N, B, H, W) float32
        image_ivar   = data["image_ivar"][:]            # (N, B, H, W) float32
        image_mask   = data["image_mask"][:]            # (N, H, W) bool
        image_band   = data["image_band"][:]            # (N, B) bytes
        psf_fwhm     = data["image_psf_fwhm"][:].astype(np.float32)  # (N, B)
        scale        = data["image_scale"][:].astype(np.float32)     # (N, B)

        band_names  = [
            b.decode("utf-8") if isinstance(b, bytes) else b
            for b in image_band[0]
        ]
        n_bands     = len(band_names)
        band_arr    = pa.ListArray.from_arrays(
            np.arange(n + 1, dtype=np.int32) * n_bands,
            pa.array(band_names * n, type=pa.string()),
        )

        # Replicate the 2-D mask once per band: (N, H, W) → (N, B, H, W).
        mask_tiled = np.ascontiguousarray(
            np.tile(image_mask[:, np.newaxis, :, :], (1, n_bands, 1, 1))
        )

        image_struct_arr = pa.StructArray.from_arrays(
            [
                band_arr,
                _arr_to_nested_list_array(image_array,  pa.float32()),
                _arr_to_nested_list_array(mask_tiled,   pa.bool_()),
                _arr_to_nested_list_array(image_ivar,   pa.float32()),
                _arr_to_nested_list_array(psf_fwhm,     pa.float32()),
                _arr_to_nested_list_array(scale,        pa.float32()),
            ],
            names=["band", "flux", "mask", "ivar", "psf_fwhm", "scale"],
        )

        # 3. Vectorised catalog struct — (N, 20) arrays → list<float32> per field.
        catalog_arr = pa.StructArray.from_arrays(
            [
                _arr_to_nested_list_array(
                    data[f"catalog_{feat}"][:].astype(np.float32), pa.float32()
                )
                for feat in self.CATALOG_FEATURES
            ],
            names=self.CATALOG_FEATURES,
        )

        # 4. Assemble final table.
        columns = {
            "image":       image_struct_arr,
            "blobmodel":   _make_image_struct(blobmodel_bytes),
            "rgb":         _make_image_struct(rgb_bytes),
            "object_mask": _make_image_struct(omask_bytes),
            "catalog":     catalog_arr,
            **{f: pa.array(data[f][:].astype(np.float32)) for f in self.FLOAT_FEATURES},
            "object_id":   pa.array(
                np.char.decode(data["object_id"][:], "utf-8"), type=pa.string()
            ),
            "ra":  pa.array(data["ra"][:].astype(np.float64)),
            "dec": pa.array(data["dec"][:].astype(np.float64)),
        }

        return pa.table(columns, schema=self.create_schema())
