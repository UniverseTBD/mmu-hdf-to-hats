"""
DESIPROVABGSTransformer: Clean class-based transformation from HDF5 to PyArrow tables.
"""
import pyarrow as pa
import numpy as np
from catalog_functions.utils import np_to_pyarrow_array
from catalog_functions.base_transformer import BaseTransformer


class DESIPROVABGSTransformer(BaseTransformer):
    """Transforms DESI PROVABGS HDF5 files to PyArrow tables with proper schema."""

    # Feature group definitions from desi_provabgs.py
    FLOAT_FEATURES = [
        'Z_HP', 'Z_MW', 'TAGE_MW', 'AVG_SFR', 'ZERR', 'TSNR2_BGS',
        'MAG_G', 'MAG_R', 'MAG_Z', 'MAG_W1', 'FIBMAG_R', 'HPIX_64',
        'PROVABGS_Z_MAX', 'SCHLEGEL_COLOR', 'PROVABGS_W_ZFAIL',
        'PROVABGS_W_FIBASSIGN',
    ]

    BOOL_FEATURES = [
        'IS_BGS_BRIGHT',
        'IS_BGS_FAINT',
    ]

    def create_schema(self):
        """Create the output PyArrow schema."""
        fields = []

        # Coordinates
        fields.append(pa.field("ra", pa.float32()))
        fields.append(pa.field("dec", pa.float32()))

        # PROVABGS MCMC posterior samples (100 samples x 13 parameters)
        fields.append(pa.field("PROVABGS_MCMC", pa.list_(pa.list_(pa.float32()))))

        # PROVABGS best-fit parameters (13 parameters)
        fields.append(pa.field("PROVABGS_THETA_BF", pa.list_(pa.float32())))

        # Log stellar mass
        fields.append(pa.field("LOG_MSTAR", pa.float32()))

        # Add all float features
        for f in self.FLOAT_FEATURES:
            fields.append(pa.field(f, pa.float32()))

        # Add all boolean features
        for f in self.BOOL_FEATURES:
            fields.append(pa.field(f, pa.bool_()))

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

        # 1. Add coordinates
        columns["ra"] = pa.array(data["ra"][:].astype(np.float32))
        columns["dec"] = pa.array(data["dec"][:].astype(np.float32))

        # 2. Add PROVABGS MCMC samples (shape: [n_objects, 100, 13])
        mcmc_data = data["PROVABGS_MCMC"][:]
        # Convert to list of lists of lists
        mcmc_lists = [[row.tolist() for row in obj_mcmc] for obj_mcmc in mcmc_data]
        columns["PROVABGS_MCMC"] = pa.array(mcmc_lists, type=pa.list_(pa.list_(pa.float32())))

        # 3. Add PROVABGS best-fit parameters (shape: [n_objects, 13])
        theta_bf_data = data["PROVABGS_THETA_BF"][:]
        theta_bf_lists = [row.tolist() for row in theta_bf_data]
        columns["PROVABGS_THETA_BF"] = pa.array(theta_bf_lists, type=pa.list_(pa.float32()))

        # 4. Add log stellar mass
        columns["LOG_MSTAR"] = pa.array(data["PROVABGS_LOGMSTAR_BF"][:].astype(np.float32))

        # 5. Add float features
        for f in self.FLOAT_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float32))

        # 6. Add boolean features
        for f in self.BOOL_FEATURES:
            columns[f] = pa.array(data[f][:].astype(bool))

        # 7. Add object_id
        columns["object_id"] = pa.array(
            [str(oid) for oid in data["object_id"][:]]
        )

        # Create table with schema
        schema = self.create_schema()
        table = pa.table(columns, schema=schema)

        return table
