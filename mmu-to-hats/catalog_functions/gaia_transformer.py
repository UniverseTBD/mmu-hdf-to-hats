"""
GaiaTransformer: Clean class-based transformation from HDF5 to PyArrow tables.
"""

import pyarrow as pa
import numpy as np
from catalog_functions.utils import np_to_pyarrow_array, BaseTransformer


class GaiaTransformer(BaseTransformer):
    """Transforms Gaia HDF5 files to PyArrow tables with proper schema."""

    # Feature group definitions from gaia.py
    SPECTRUM_FEATURES = [
        "coeff",
        "coeff_error",
    ]

    PHOTOMETRY_FEATURES = [
        "phot_g_mean_mag",
        "phot_g_mean_flux",
        "phot_g_mean_flux_error",
        "phot_bp_mean_mag",
        "phot_bp_mean_flux",
        "phot_bp_mean_flux_error",
        "phot_rp_mean_mag",
        "phot_rp_mean_flux",
        "phot_rp_mean_flux_error",
        "phot_bp_rp_excess_factor",
        "bp_rp",
        "bp_g",
        "g_rp",
    ]

    ASTROMETRY_FEATURES = [
        "ra",
        "ra_error",
        "dec",
        "dec_error",
        "parallax",
        "parallax_error",
        "pmra",
        "pmra_error",
        "pmdec",
        "pmdec_error",
        "ra_dec_corr",
        "ra_parallax_corr",
        "ra_pmra_corr",
        "ra_pmdec_corr",
        "dec_parallax_corr",
        "dec_pmra_corr",
        "dec_pmdec_corr",
        "parallax_pmra_corr",
        "parallax_pmdec_corr",
        "pmra_pmdec_corr",
    ]

    RV_FEATURES = [
        "radial_velocity",
        "radial_velocity_error",
        "rv_template_fe_h",
        "rv_template_logg",
        "rv_template_teff",
    ]

    GSPPHOT_FEATURES = [
        "ag_gspphot",
        "ag_gspphot_lower",
        "ag_gspphot_upper",
        "azero_gspphot",
        "azero_gspphot_lower",
        "azero_gspphot_upper",
        "distance_gspphot",
        "distance_gspphot_lower",
        "distance_gspphot_upper",
        "ebpminrp_gspphot",
        "ebpminrp_gspphot_lower",
        "ebpminrp_gspphot_upper",
        "logg_gspphot",
        "logg_gspphot_lower",
        "logg_gspphot_upper",
        "mh_gspphot",
        "mh_gspphot_lower",
        "mh_gspphot_upper",
        "teff_gspphot",
        "teff_gspphot_lower",
        "teff_gspphot_upper",
    ]

    FLAG_FEATURES = ["ruwe"]

    CORRECTION_FEATURES = [
        "ecl_lat",
        "ecl_lon",
        "nu_eff_used_in_astrometry",
        "pseudocolour",
        "astrometric_params_solved",
        "rv_template_teff",
        "grvs_mag",
    ]

    INT64_FEATURES = [
        "healpix",
    ]

    def create_schema(self):
        """Create the output PyArrow schema."""
        fields = []

        # Spectral coefficients as list features
        for f in self.SPECTRUM_FEATURES:
            fields.append(pa.field(f, pa.list_(pa.float32())))

        # All photometry features as float32
        for f in self.PHOTOMETRY_FEATURES:
            fields.append(pa.field(f, pa.float32()))

        # All astrometry features as float32
        for f in self.ASTROMETRY_FEATURES:
            fields.append(pa.field(f, pa.float32()))

        # Radial velocity features as float32
        for f in self.RV_FEATURES:
            fields.append(pa.field(f, pa.float32()))

        # GSPPHOT features as float32
        for f in self.GSPPHOT_FEATURES:
            fields.append(pa.field(f, pa.float32()))

        # Flag features as float32
        for f in self.FLAG_FEATURES:
            fields.append(pa.field(f, pa.float32()))

        # Correction features as float32
        for f in self.CORRECTION_FEATURES:
            fields.append(pa.field(f, pa.float32()))

        # Int64 features
        for f in self.INT64_FEATURES:
            fields.append(pa.field(f, pa.int64()))

        # Object ID
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

        # 1. Add spectral coefficient arrays
        for f in self.SPECTRUM_FEATURES:
            columns[f] = np_to_pyarrow_array(data[f][:])

        # 2. Add photometry features
        for f in self.PHOTOMETRY_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float32))

        # 3. Add astrometry features
        for f in self.ASTROMETRY_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float32))

        # 4. Add radial velocity features
        for f in self.RV_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float32))

        # 5. Add GSPPHOT features
        for f in self.GSPPHOT_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float32))

        # 6. Add flag features
        for f in self.FLAG_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float32))

        # 7. Add correction features
        for f in self.CORRECTION_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.float32))

        # 8. Add int64 features
        for f in self.INT64_FEATURES:
            columns[f] = pa.array(data[f][:].astype(np.int64))

        # 9. Add source_id as object_id
        columns["object_id"] = pa.array(data["source_id"][:].astype(np.int64))

        # Create table with schema
        schema = self.create_schema()
        table = pa.table(columns, schema=schema)

        return table
