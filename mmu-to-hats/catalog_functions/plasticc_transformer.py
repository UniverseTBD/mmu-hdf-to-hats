"""
PLAsTiCCTransformer: Clean class-based transformation from HDF5 to PyArrow tables.
"""

import pyarrow as pa
import numpy as np
from catalog_functions.utils import np_to_pyarrow_array, BaseTransformer


class PLAsTiCCTransformer(BaseTransformer):
    """Transforms PLAsTiCC HDF5 files to PyArrow tables with proper schema."""

    # Feature group definitions from plasticc.py
    FLOAT_FEATURES = [
        "hostgal_photoz",
        "hostgal_specz",
        "redshift",
    ]

    STR_FEATURES = [
        "obj_type",
    ]

    BANDS = ["u", "g", "r", "i", "z", "Y"]

    def create_schema(self):
        """Create the output PyArrow schema."""
        fields = []

        # Lightcurve struct with nested sequences
        lightcurve_struct = pa.struct(
            [
                pa.field("band", pa.list_(pa.string())),
                pa.field("time", pa.list_(pa.float32())),
                pa.field("flux", pa.list_(pa.float32())),
                pa.field("flux_err", pa.list_(pa.float32())),
            ]
        )
        fields.append(pa.field("lightcurve", lightcurve_struct))

        # Add all float features
        for f in self.FLOAT_FEATURES:
            fields.append(pa.field(f, pa.float32()))

        # Add all string features
        for f in self.STR_FEATURES:
            fields.append(pa.field(f, pa.string()))

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

        # Class mapping for obj_type
        CLASS_MAPPING = {
            90: "SNIa",
            67: "SNIa-91bg",
            52: "SNIax",
            42: "SNII",
            62: "SNIbc",
            95: "SLSN-I",
            15: "TDE",
            64: "KN",
            88: "AGN",
            92: "RRL",
            65: "M-dwarf",
            16: "EB",
            53: "Mira",
            6: "MicroLens-Single",
            99: "extra",
            991: "MicroLens-Binary",
            992: "ILOT",
            993: "CaRT",
            994: "PISN",
            995: "MicroLens-String",
        }

        # 1. Create lightcurve struct column
        # lightcurve shape is [n_objects, n_bands, 3, seq_len]
        lightcurve = data["lightcurve"][:]
        n_objects = lightcurve.shape[0]
        n_bands = lightcurve.shape[1]
        seq_len = lightcurve.shape[3]

        # Process lightcurves for each object
        band_lists = []
        time_lists = []
        flux_lists = []
        flux_err_lists = []

        for i in range(n_objects):
            lc = lightcurve[i]  # Shape: [n_bands, 3, seq_len]
            band_arr = (
                np.array([np.ones(seq_len) * band for band in range(n_bands)])
                .flatten()
                .astype("int")
            )
            bands = np.array(self.BANDS)[band_arr]
            times = lc[:, 0].flatten()
            fluxes = lc[:, 1].flatten()
            flux_errs = lc[:, 2].flatten()

            band_lists.append(bands.tolist())
            time_lists.append(times.tolist())
            flux_lists.append(fluxes.tolist())
            flux_err_lists.append(flux_errs.tolist())

        lightcurve_arrays = [
            pa.array(band_lists, type=pa.list_(pa.string())),
            pa.array(time_lists, type=pa.list_(pa.float32())),
            pa.array(flux_lists, type=pa.list_(pa.float32())),
            pa.array(flux_err_lists, type=pa.list_(pa.float32())),
        ]

        columns["lightcurve"] = pa.StructArray.from_arrays(
            lightcurve_arrays, names=["band", "time", "flux", "flux_err"]
        )

        # 2. Add float features
        for f in self.FLOAT_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float32))

        # 3. Add string features
        for f in self.STR_FEATURES:
            if f == "obj_type":
                columns[f] = pa.array([CLASS_MAPPING[int(x)] for x in data[f][:]])
            else:
                columns[f] = pa.array([str(x) for x in data[f][:]])

        # 4. Add object_id
        columns["object_id"] = pa.array([str(oid) for oid in data["object_id"][:]])

        # Create table with schema
        schema = self.create_schema()
        table = pa.table(columns, schema=schema)

        return table
