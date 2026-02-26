"""
ChandraTransformer: Clean class-based transformation from HDF5 to PyArrow tables.
"""

import pyarrow as pa
import numpy as np
from catalog_functions.utils import BaseTransformer


class ChandraTransformer(BaseTransformer):
    """Transforms Chandra Source Catalog HDF5 files to PyArrow tables with proper schema."""

    # Feature definitions from chandra.py
    FLOAT_FEATURES = [
        "flux_aper_b",
        "flux_bb_aper_b",
        "flux_significance_b",
        "hard_hm",
        "hard_hs",
        "hard_ms",
        "var_index_b",
        "var_prob_b",
    ]
    DOUBLE_FEATURES = ["ra", "dec"]

    def create_schema(self):
        """Create the output PyArrow schema."""
        fields = []

        # Spectrum struct-of-lists (matching datasets Sequence behavior)
        spectrum_struct = pa.struct(
            [
                pa.field("ene_center_bin", pa.list_(pa.float32())),
                pa.field("ene_high_bin", pa.list_(pa.float32())),
                pa.field("ene_low_bin", pa.list_(pa.float32())),
                pa.field("flux", pa.list_(pa.float32())),
                pa.field("flux_error", pa.list_(pa.float32())),
            ]
        )
        fields.append(pa.field("spectrum", spectrum_struct))

        # Add all float features
        for f in self.FLOAT_FEATURES:
            fields.append(pa.field(f, pa.float32()))

        # Name and object_id
        fields.append(pa.field("name", pa.string()))
        fields.append(pa.field("object_id", pa.string()))

        for f in self.DOUBLE_FEATURES:
            fields.append(pa.field(f, pa.float64()))

        return pa.schema(fields)

    def dataset_to_table(self, data):
        """
        Convert HDF5 dataset to PyArrow table.

        Each Chandra HDF5 file contains multiple objects with variable-length spectra.

        Args:
            data: HDF5 file or dict of datasets

        Returns:
            pa.Table: Transformed Arrow table
        """
        columns = {}
        n_objects = len(data["object_id"][:])

        # 1. Create spectrum struct-of-lists column
        # Spectrum arrays are variable-length (dtype=object in HDF5)
        spectrum_ene = data["spectrum_ene"]
        spectrum_ene_hi = data["spectrum_ene_hi"]
        spectrum_ene_lo = data["spectrum_ene_lo"]
        spectrum_flux = data["spectrum_flux"]
        spectrum_flux_err = data["spectrum_flux_err"]

        ene_center_lists = []
        ene_high_lists = []
        ene_low_lists = []
        flux_lists = []
        flux_error_lists = []

        for i in range(n_objects):
            ene_center_lists.append(np.asarray(spectrum_ene[i]).astype(np.float32).tolist())
            ene_high_lists.append(np.asarray(spectrum_ene_hi[i]).astype(np.float32).tolist())
            ene_low_lists.append(np.asarray(spectrum_ene_lo[i]).astype(np.float32).tolist())
            flux_lists.append(np.asarray(spectrum_flux[i]).astype(np.float32).tolist())
            flux_error_lists.append(np.asarray(spectrum_flux_err[i]).astype(np.float32).tolist())

        columns["spectrum"] = pa.StructArray.from_arrays(
            [
                pa.array(ene_center_lists, type=pa.list_(pa.float32())),
                pa.array(ene_high_lists, type=pa.list_(pa.float32())),
                pa.array(ene_low_lists, type=pa.list_(pa.float32())),
                pa.array(flux_lists, type=pa.list_(pa.float32())),
                pa.array(flux_error_lists, type=pa.list_(pa.float32())),
            ],
            names=["ene_center_bin", "ene_high_bin", "ene_low_bin", "flux", "flux_error"],
        )

        # 2. Add float features
        for f in self.FLOAT_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float32))

        # 3. Add name
        name_data = data["name"][:]
        columns["name"] = pa.array([
            n.decode("utf-8") if isinstance(n, bytes) else str(n)
            for n in name_data
        ])

        # 4. Add object_id (int64 in HDF5, convert to string)
        columns["object_id"] = pa.array([str(oid) for oid in data["object_id"][:]])

        # 5. Add ra/dec as float64
        for f in self.DOUBLE_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float64))

        # Create table with schema
        schema = self.create_schema()
        table = pa.table(columns, schema=schema)

        return table
