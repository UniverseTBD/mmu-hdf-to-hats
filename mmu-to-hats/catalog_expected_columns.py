"""
Expected columns for each catalog in MMU-to-HATS.

This dictionary maps catalog names to their expected column names.
- Simple strings represent top-level columns (or flattened struct fields)
- Dicts represent nested structures (sequences/lists) with their sub-columns

Generated from catalog_download_scripts/*.py
"""

CATALOG_EXPECTED_COLUMNS = {
    "btsbot": [
        "object_id",
        "OBJECT_ID_", "source_set", "split",
        "label", "fid", "programid", "field", "nneg", "nbad", "ndethist",
        "ncovhist", "nmtchps", "nnotdet", "N",
        "jd", "diffmaglim", "magpsf", "sigmapsf", "chipsf", "magap",
        "sigmagap", "distnr", "magnr", "chinr", "sharpnr", "sky",
        "magdiff", "fwhm", "classtar", "mindtoedge", "seeratio",
        "magapbig", "sigmagapbig", "sgmag1", "srmag1", "simag1",
        "szmag1", "sgscore1", "distpsnr1", "jdstarthist", "scorr",
        "sgmag2", "srmag2", "simag2", "szmag2", "sgscore2", "distpsnr2",
        "sgmag3", "srmag3", "simag3", "szmag3", "sgscore3", "distpsnr3",
        "jdstartref", "dsnrms", "ssnrms", "magzpsci", "magzpsciunc",
        "magzpscirms", "clrcoeff", "clrcounc", "neargaia",
        "neargaiabright", "maggaia", "maggaiabright", "exptime", "drb",
        "acai_h", "acai_v", "acai_o", "acai_n", "acai_b", "new_drb",
        "peakmag", "maxmag", "peakmag_so_far", "maxmag_so_far", "age",
        "days_since_peak", "days_to_peak",
        "isdiffpos", "is_SN", "near_threshold", "is_rise",
        {"image": ["band", "view", "array", "scale"]},
    ],

    "cfa": [
        "object_id", "obj_type",
        {"lightcurve": ["band", "time", "mag", "mag_err"]},
    ],

    "csp": [
        "object_id", "spec_class",
        "ra", "dec", "redshift",
        {"lightcurve": ["band", "mag", "mag_err", "time"]},
    ],

    "des_y3_sne_ia": [
        "object_id", "obj_type",
        "redshift", "host_log_mass",
        {"lightcurve": ["band", "flux", "flux_err", "time"]},
    ],

    "desi": [
        "object_id",
        "Z", "ZERR", "EBV",
        "FLUX_G", "FLUX_R", "FLUX_Z",
        "FLUX_IVAR_G", "FLUX_IVAR_R", "FLUX_IVAR_Z",
        "FIBERFLUX_G", "FIBERFLUX_R", "FIBERFLUX_Z",
        "FIBERTOTFLUX_G", "FIBERTOTFLUX_R", "FIBERTOTFLUX_Z",
        "ZWARN",
        {"spectrum": ["flux", "ivar", "lsf_sigma", "lambda", "mask"]},
    ],

    "desi_provabgs": [
        "object_id", "ra", "dec",
        "LOG_MSTAR", "Z_HP", "Z_MW", "TAGE_MW", "AVG_SFR", "ZERR",
        "TSNR2_BGS", "MAG_G", "MAG_R", "MAG_Z", "MAG_W1",
        "FIBMAG_R", "HPIX_64", "PROVABGS_Z_MAX", "SCHLEGEL_COLOR",
        "PROVABGS_W_ZFAIL", "PROVABGS_W_FIBASSIGN",
        "IS_BGS_BRIGHT", "IS_BGS_FAINT",
        {"PROVABGS_MCMC": ["array_2d"]},
        {"PROVABGS_THETA_BF": ["values"]},
    ],

    "foundation": [
        "object_id", "obj_type",
        "redshift", "host_log_mass",
        {"lightcurve": ["band", "flux", "flux_err", "time"]},
    ],

    "gaia": [
        "object_id", "healpix", "ra", "dec",
        {"spectral_coefficients": ["coeff", "coeff_error"]},
        # Flattened from photometry struct
        "phot_g_mean_mag", "phot_g_mean_flux", "phot_g_mean_flux_error",
        "phot_bp_mean_mag", "phot_bp_mean_flux", "phot_bp_mean_flux_error",
        "phot_rp_mean_mag", "phot_rp_mean_flux", "phot_rp_mean_flux_error",
        "phot_bp_rp_excess_factor", "bp_rp", "bp_g", "g_rp",
        # Flattened from astrometry struct
        "ra_error", "dec_error", "parallax", "parallax_error",
        "pmra", "pmra_error", "pmdec", "pmdec_error",
        "ra_dec_corr", "ra_parallax_corr", "ra_pmra_corr", "ra_pmdec_corr",
        "dec_parallax_corr", "dec_pmra_corr", "dec_pmdec_corr",
        "parallax_pmra_corr", "parallax_pmdec_corr", "pmra_pmdec_corr",
        # Flattened from radial_velocity struct
        "radial_velocity", "radial_velocity_error", "rv_template_fe_h",
        "rv_template_logg", "rv_template_teff",
        # Flattened from gspphot struct
        "ag_gspphot", "ag_gspphot_lower", "ag_gspphot_upper",
        "azero_gspphot", "azero_gspphot_lower", "azero_gspphot_upper",
        "distance_gspphot", "distance_gspphot_lower", "distance_gspphot_upper",
        "ebpminrp_gspphot", "ebpminrp_gspphot_lower", "ebpminrp_gspphot_upper",
        "logg_gspphot", "logg_gspphot_lower", "logg_gspphot_upper",
        "mh_gspphot", "mh_gspphot_lower", "mh_gspphot_upper",
        "teff_gspphot", "teff_gspphot_lower", "teff_gspphot_upper",
        # Flattened from flags struct
        "ruwe",
        # Flattened from corrections struct
        "ecl_lat", "ecl_lon", "nu_eff_used_in_astrometry",
        "pseudocolour", "astrometric_params_solved", "grvs_mag",
    ],

    "gz10": [
        "object_id", "gz10_label", "redshift",
        # Optional (gz10_rgb_images config): "rgb_image", "rgb_pixel_scale"
    ],

    "hsc": [
        "object_id",
        "a_g", "a_r", "a_i", "a_z", "a_y",
        "g_extendedness_value", "r_extendedness_value", "i_extendedness_value",
        "z_extendedness_value", "y_extendedness_value",
        "g_cmodel_mag", "g_cmodel_magerr", "r_cmodel_mag", "r_cmodel_magerr",
        "i_cmodel_mag", "i_cmodel_magerr", "z_cmodel_mag", "z_cmodel_magerr",
        "y_cmodel_mag", "y_cmodel_magerr",
        "g_sdssshape_psf_shape11", "g_sdssshape_psf_shape22", "g_sdssshape_psf_shape12",
        "r_sdssshape_psf_shape11", "r_sdssshape_psf_shape22", "r_sdssshape_psf_shape12",
        "i_sdssshape_psf_shape11", "i_sdssshape_psf_shape22", "i_sdssshape_psf_shape12",
        "z_sdssshape_psf_shape11", "z_sdssshape_psf_shape22", "z_sdssshape_psf_shape12",
        "y_sdssshape_psf_shape11", "y_sdssshape_psf_shape22", "y_sdssshape_psf_shape12",
        "g_sdssshape_shape11", "g_sdssshape_shape22", "g_sdssshape_shape12",
        "r_sdssshape_shape11", "r_sdssshape_shape22", "r_sdssshape_shape12",
        "i_sdssshape_shape11", "i_sdssshape_shape22", "i_sdssshape_shape12",
        "z_sdssshape_shape11", "z_sdssshape_shape22", "z_sdssshape_shape12",
        "y_sdssshape_shape11", "y_sdssshape_shape22", "y_sdssshape_shape12",
        {"image": ["band", "flux", "ivar", "mask", "psf_fwhm", "scale"]},
    ],

    "jwst": [
        "object_id",
        "mag_auto", "flux_radius", "flux_auto", "fluxerr_auto",
        "cxx_image", "cyy_image", "cxy_image",
        {"image": ["band", "flux", "ivar", "mask", "psf_fwhm", "scale"]},
    ],

    "legacysurvey": [
        "object_id", "TYPE",
        "EBV", "FLUX_G", "FLUX_R", "FLUX_I", "FLUX_Z",
        "FLUX_W1", "FLUX_W2", "FLUX_W3", "FLUX_W4",
        "SHAPE_R", "SHAPE_E1", "SHAPE_E2",
        {"image": ["band", "flux", "mask", "ivar", "psf_fwhm", "scale"]},
        {"catalog": ["FLUX_G", "FLUX_R", "FLUX_I", "FLUX_Z", "TYPE", "SHAPE_R", "SHAPE_E1", "SHAPE_E2", "X", "Y"]},
        {"blobmodel": ["array"]},
        {"rgb": ["array"]},
        {"object_mask": ["array"]},
    ],

    "manga": [
        "object_id", "ra", "dec", "healpix", "z",
        "spaxel_size", "spaxel_size_units",
        {"spaxels": [
            "flux", "ivar", "mask", "lsf", "lambda", "x", "y", "spaxel_idx",
            "flux_units", "lambda_units", "skycoo_x", "skycoo_y",
            "ellcoo_r", "ellcoo_rre", "ellcoo_rkpc", "ellcoo_theta",
            "skycoo_units", "ellcoo_r_units", "ellcoo_rre_units",
            "ellcoo_rkpc_units", "ellcoo_theta_units"
        ]},
        {"images": ["filter", "flux", "flux_units", "psf", "psf_units", "scale", "scale_units"]},
        {"maps": ["group", "label", "flux", "ivar", "mask", "array_units"]},
    ],

    "plasticc": [
        "object_id", "obj_type",
        "hostgal_photoz", "hostgal_specz", "redshift",
        {"lightcurve": ["band", "flux", "flux_err", "time"]},
    ],

    "ps1_sne_ia": [
        "object_id", "obj_type",
        "redshift", "host_log_mass",
        {"lightcurve": ["band", "flux", "flux_err", "time"]},
    ],

    "snls": [
        "object_id", "obj_type",
        "redshift", "host_log_mass",
        {"lightcurve": ["band", "flux", "flux_err", "time"]},
    ],

    "ssl_legacysurvey": [
        "object_id",
        "ebv", "flux_g", "flux_r", "flux_z",
        "fiberflux_g", "fiberflux_r", "fiberflux_z",
        "psfdepth_g", "psfdepth_r", "psfdepth_z", "z_spec",
        {"image": ["band", "flux", "psf_fwhm", "scale"]},
    ],

    "swift_sne_ia": [
        "object_id", "obj_type",
        "redshift", "host_log_mass",
        {"lightcurve": ["band", "flux", "flux_err", "time"]},
    ],

    "tess": [
        "object_id", "RA", "DEC",
        {"lightcurve": ["time", "flux", "flux_err"]},
    ],

    "vipers": [
        "object_id",
        "REDSHIFT", "REDFLAG", "EXPTIME", "NORM", "MAG",
        {"spectrum": ["flux", "ivar", "lambda", "mask"]},
    ],

    "yse": [
        "object_id", "obj_type",
        "redshift", "host_log_mass",
        {"lightcurve": ["band", "flux", "flux_err", "time"]},
    ],
}
