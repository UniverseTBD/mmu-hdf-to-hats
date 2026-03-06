"""
SDSSTransformer: Clean class-based transformation from HDF5 to PyArrow tables.
"""

import pyarrow as pa
import numpy as np
from catalog_functions.utils import np_to_pyarrow_array, BaseTransformer


class SDSSTransformer(BaseTransformer):
    """Transforms SDSS HDF5 files to PyArrow tables with proper schema."""

    # Feature group definitions
    FLOAT_FEATURES = [
        "VDISP",
        "VDISP_ERR",
        "Z",
        "Z_ERR",
    ]

    DOUBLE_FEATURES = [
        "ra",
        "dec",
    ]

    INT64_FEATURES = [
        "healpix",
    ]

    BOOL_FEATURES = ["ZWARNING"]

    FLUX_FEATURES = [
        "SPECTROFLUX",
        "SPECTROFLUX_IVAR",
        "SPECTROSYNFLUX",
        "SPECTROSYNFLUX_IVAR",
    ]

    FLUX_FILTERS = ["U", "G", "R", "I", "Z"]

    def create_schema(self):
        """Create the output PyArrow schema."""
        fields = []

        # Spectrum struct with nested arrays
        spectrum_struct = pa.struct(
            [
                pa.field("flux", pa.list_(pa.float32())),
                pa.field("ivar", pa.list_(pa.float32())),
                pa.field("lsf_sigma", pa.list_(pa.float32())),
                pa.field("lambda", pa.list_(pa.float32())),
                pa.field("mask", pa.list_(pa.bool_())),
            ]
        )
        fields.append(pa.field("spectrum", spectrum_struct))

        # Add all feature types
        for f in self.FLOAT_FEATURES:
            fields.append(pa.field(f, pa.float32()))

        for f in self.DOUBLE_FEATURES:
            fields.append(pa.field(f, pa.float64()))

        for f in self.INT64_FEATURES:
            fields.append(pa.field(f, pa.int64()))

        for f in self.BOOL_FEATURES:
            fields.append(pa.field(f, pa.bool_()))

        # Flux features split by filter
        for f in self.FLUX_FEATURES:
            for b in self.FLUX_FILTERS:
                fields.append(pa.field(f"{f}_{b}", pa.float32()))

        # Object ID
        fields.append(pa.field("object_id", pa.string()))

        return pa.schema(fields)

    def dataset_to_table(self, data):
        """
        Convert HDF5 dataset to PyArrow table.

        Args:
            hdf5_file_path: Path to HDF5 file

        Returns:
            pa.Table: Transformed Arrow table
        """
        # Dictionary to hold all columns
        columns = {}

        # 1. Create spectrum struct column
        spectrum_flux = data["spectrum_flux"][:]
        spectrum_ivar = data["spectrum_ivar"][:]
        spectrum_lsf_sigma = data["spectrum_lsf_sigma"][:]
        spectrum_lambda = data["spectrum_lambda"][:]
        spectrum_mask = data["spectrum_mask"][:]

        spectrum_arrays = [
            np_to_pyarrow_array(spectrum_flux),
            np_to_pyarrow_array(spectrum_ivar),
            np_to_pyarrow_array(spectrum_lsf_sigma),
            np_to_pyarrow_array(spectrum_lambda),
            np_to_pyarrow_array(spectrum_mask),
        ]

        columns["spectrum"] = pa.StructArray.from_arrays(
            spectrum_arrays, names=["flux", "ivar", "lsf_sigma", "lambda", "mask"]
        )

        # 2. Add float features
        for f in self.FLOAT_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float32))

        # 3. Add double features
        for f in self.DOUBLE_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float64))

        # 4. Add int64 features
        for f in self.INT64_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.int64))

        # 5. Add boolean features
        for f in self.BOOL_FEATURES:
            columns[f] = pa.array(data[f][:].astype(bool))

        # 6. Split flux features by filter (vectorized)
        for f in self.FLUX_FEATURES:
            flux_data = data[f][:]  # Shape: [n_objects, 5]
            for n, b in enumerate(self.FLUX_FILTERS):
                # Extract column n for all objects
                columns[f"{f}_{b}"] = pa.array(flux_data[:, n].astype(np.float32))

        # 7. Add object_id
        columns["object_id"] = pa.array([str(oid) for oid in data["object_id"][:]])

        # Create table with schema
        schema = self.create_schema()
        table = pa.table(columns, schema=schema)

        return table


# just here for testing purposes, can be used to another file to separate prod and test code
if __name__ == "__main__":
    import pyarrow.parquet as pq

    # Example usage
    input_file = "data/MultimodalUniverse/v1/sdss/sdss/healpix=583/001-of-001.hdf5"
    output_file = "data/transformed_table_class.parquet"

    print("Transforming HDF5 to Arrow table...")
    transformer = SDSSTransformer()
    table = transformer.transform_from_hdf5(input_file)

    print(f"\nTable shape: {table.num_rows} rows, {table.num_columns} columns")
    print(f"\nSchema:\n{table.schema}")

    # Write to parquet
    pq.write_table(table, output_file)
    print(f"\nWrote output to {output_file}")
