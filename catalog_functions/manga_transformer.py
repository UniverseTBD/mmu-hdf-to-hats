"""
MaNGATransformer: Clean class-based transformation from HDF5 to PyArrow tables.
"""

import pyarrow as pa
import numpy as np
from catalog_functions.utils import BaseTransformer


class MaNGATransformer(BaseTransformer):
    """Transforms MaNGA HDF5 files to PyArrow tables with proper schema."""

    # Constants from manga.py
    IMAGE_SIZE = 96
    IMAGE_FILTERS = ["G", "R", "I", "Z"]
    SPECTRUM_SIZE = 4563

    def create_schema(self):
        """Create the output PyArrow schema."""
        fields = []

        # Metadata
        fields.append(pa.field("z", pa.float32()))
        fields.append(pa.field("spaxel_size", pa.float32()))
        fields.append(pa.field("spaxel_size_units", pa.string()))

        # Spaxels - list of structs with spectrum data
        spaxel_struct = pa.struct(
            [
                pa.field("flux", pa.list_(pa.float32())),
                pa.field("ivar", pa.list_(pa.float32())),
                pa.field("mask", pa.list_(pa.int64())),
                pa.field("lsf", pa.list_(pa.float32())),
                pa.field("lambda", pa.list_(pa.float32())),
                pa.field("x", pa.int8()),
                pa.field("y", pa.int8()),
                pa.field("spaxel_idx", pa.int16()),
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
        fields.append(pa.field("spaxels", pa.list_(spaxel_struct)))

        # Images - list of reconstructed griz images
        image_struct = pa.struct(
            [
                pa.field("filter", pa.string()),
                pa.field("flux", pa.list_(pa.list_(pa.float32()))),  # 2D array
                pa.field("flux_units", pa.string()),
                pa.field("psf", pa.list_(pa.list_(pa.float32()))),  # 2D array
                pa.field("psf_units", pa.string()),
                pa.field("scale", pa.float32()),
                pa.field("scale_units", pa.string()),
            ]
        )
        fields.append(pa.field("images", pa.list_(image_struct)))

        # Maps - list of DAP analysis maps
        map_struct = pa.struct(
            [
                pa.field("group", pa.string()),
                pa.field("label", pa.string()),
                pa.field("flux", pa.list_(pa.list_(pa.float32()))),  # 2D array
                pa.field("ivar", pa.list_(pa.list_(pa.float32()))),  # 2D array
                pa.field("mask", pa.list_(pa.list_(pa.float32()))),  # 2D array
                pa.field("array_units", pa.string()),
            ]
        )
        fields.append(pa.field("maps", pa.list_(map_struct)))

        # Object ID
        fields.append(pa.field("object_id", pa.string()))

        return pa.schema(fields)

    def dataset_to_table(self, data):
        """
        Convert HDF5 dataset to PyArrow table.

        Args:
            data: HDF5 file or dict of datasets (MaNGA has hierarchical groups)

        Returns:
            pa.Table: Transformed Arrow table
        """
        # MaNGA stores data in groups, one per galaxy
        # We'll process each group and create table rows

        all_rows = []

        # Iterate over groups in HDF5 file
        for key in data.keys():
            grp = data[key]

            row = {}

            # 1. Add metadata
            row["z"] = float(grp["z"][()])
            row["spaxel_size"] = float(grp["spaxel_size"][()])
            spaxel_size_unit = grp["spaxel_size_unit"][()]
            if isinstance(spaxel_size_unit, bytes):
                spaxel_size_unit = spaxel_size_unit.decode("utf-8")
            row["spaxel_size_units"] = str(spaxel_size_unit)

            # 2. Process spaxels
            spaxels_list = []
            for spaxel_data in grp["spaxels"][:]:
                # Reshape 1D arrays if needed (flux, ivar, mask, lsf, lambda)
                flux = spaxel_data[0]
                if flux.ndim == 1:
                    flux = flux.reshape(1, -1)[0]

                ivar = spaxel_data[1]
                if ivar.ndim == 1:
                    ivar = ivar.reshape(1, -1)[0]

                mask = spaxel_data[2]
                if mask.ndim == 1:
                    mask = mask.reshape(1, -1)[0]

                lsf = spaxel_data[3]
                if lsf.ndim == 1:
                    lsf = lsf.reshape(1, -1)[0]

                lmbda = spaxel_data[4]
                if lmbda.ndim == 1:
                    lmbda = lmbda.reshape(1, -1)[0]

                flux_units = spaxel_data[8]
                if isinstance(flux_units, bytes):
                    flux_units = flux_units.decode("utf-8")

                lambda_units = spaxel_data[9]
                if isinstance(lambda_units, bytes):
                    lambda_units = lambda_units.decode("utf-8")

                skycoo_units = spaxel_data[16]
                if isinstance(skycoo_units, bytes):
                    skycoo_units = skycoo_units.decode("utf-8")

                ellcoo_r_units = spaxel_data[17]
                if isinstance(ellcoo_r_units, bytes):
                    ellcoo_r_units = ellcoo_r_units.decode("utf-8")

                ellcoo_rre_units = spaxel_data[18]
                if isinstance(ellcoo_rre_units, bytes):
                    ellcoo_rre_units = ellcoo_rre_units.decode("utf-8")

                ellcoo_rkpc_units = spaxel_data[19]
                if isinstance(ellcoo_rkpc_units, bytes):
                    ellcoo_rkpc_units = ellcoo_rkpc_units.decode("utf-8")

                ellcoo_theta_units = spaxel_data[20]
                if isinstance(ellcoo_theta_units, bytes):
                    ellcoo_theta_units = ellcoo_theta_units.decode("utf-8")

                spaxel = {
                    "flux": flux.tolist(),
                    "ivar": ivar.tolist(),
                    "mask": mask.astype(np.int64).tolist(),
                    "lsf": lsf.tolist(),
                    "lambda": lmbda.tolist(),
                    "x": int(spaxel_data[5]),
                    "y": int(spaxel_data[6]),
                    "spaxel_idx": int(spaxel_data[7]),
                    "flux_units": str(flux_units),
                    "lambda_units": str(lambda_units),
                    "skycoo_x": float(spaxel_data[10]),
                    "skycoo_y": float(spaxel_data[11]),
                    "ellcoo_r": float(spaxel_data[12]),
                    "ellcoo_rre": float(spaxel_data[13]),
                    "ellcoo_rkpc": float(spaxel_data[14]),
                    "ellcoo_theta": float(spaxel_data[15]),
                    "skycoo_units": str(skycoo_units),
                    "ellcoo_r_units": str(ellcoo_r_units),
                    "ellcoo_rre_units": str(ellcoo_rre_units),
                    "ellcoo_rkpc_units": str(ellcoo_rkpc_units),
                    "ellcoo_theta_units": str(ellcoo_theta_units),
                }
                spaxels_list.append(spaxel)

            row["spaxels"] = spaxels_list

            # 3. Process images
            images_list = []
            for img_data in grp["images"][:]:
                filt = img_data[0]
                if isinstance(filt, bytes):
                    filt = filt.decode("utf-8")

                flux_units = img_data[2]
                if isinstance(flux_units, bytes):
                    flux_units = flux_units.decode("utf-8")

                psf_units = img_data[4]
                if isinstance(psf_units, bytes):
                    psf_units = psf_units.decode("utf-8")

                scale_units = img_data[6]
                if isinstance(scale_units, bytes):
                    scale_units = scale_units.decode("utf-8")

                image = {
                    "filter": str(filt),
                    "flux": [row.tolist() for row in img_data[1]],
                    "flux_units": str(flux_units),
                    "psf": [row.tolist() for row in img_data[3]],
                    "psf_units": str(psf_units),
                    "scale": float(img_data[5]),
                    "scale_units": str(scale_units),
                }
                images_list.append(image)

            row["images"] = images_list

            # 4. Process maps
            maps_list = []
            for map_data in grp["maps"][:]:
                group = map_data[0]
                if isinstance(group, bytes):
                    group = group.decode("utf-8")

                label = map_data[1]
                if isinstance(label, bytes):
                    label = label.decode("utf-8")

                flux_units = map_data[5]
                if isinstance(flux_units, bytes):
                    flux_units = flux_units.decode("utf-8")

                map_item = {
                    "group": str(group),
                    "label": str(label),
                    "flux": [row.tolist() for row in map_data[2]],
                    "ivar": [row.tolist() for row in map_data[3]],
                    "mask": [row.tolist() for row in map_data[4]],
                    "array_units": str(flux_units),
                }
                maps_list.append(map_item)

            row["maps"] = maps_list

            # 5. Add object_id
            oid = grp["object_id"][()]
            if isinstance(oid, bytes):
                oid = oid.decode("utf-8")
            row["object_id"] = str(oid)

            all_rows.append(row)

        # Convert to PyArrow table
        schema = self.create_schema()
        table = pa.Table.from_pylist(all_rows, schema=schema)

        return table
