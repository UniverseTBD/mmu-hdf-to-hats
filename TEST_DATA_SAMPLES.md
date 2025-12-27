# Test Data Samples for Catalog Transformers

This document lists small healpix samples (<100MB) for each catalog to use for testing and verification.

## Data Repository Base URL
https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/

## Small Healpix Samples by Catalog

### Spectroscopic Catalogs

**SDSS** (CONFIRMED - 23MB)
- healpix=583
- URL: https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/sdss/sdss/healpix=583/
- Size: ~23 MB
- Status: ✓ Confirmed by user

**GAIA** (CONFIRMED - 16MB)
- healpix=1631
- URL: https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/gaia/gaia/healpix=1631/
- Size: ~16 MB
- Status: ✓ Verified transformer matches datasets output

**DESI**
- healpix=626
- URL: https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/desi/edr_sv3/healpix=626/
- Status: ✓ Verified accessible
- Notes: needed to transform the object_id to int, since the catalog stores them as integers, see the function `match_desi_catalog_object_ids`!

**VIPERS**
- healpix=0
- URL: https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/vipers/vipers_w1/healpix=0/
- Status: ✓ Verified accessible

**DESI_PROVABGS** (MCMC posteriors)
- healpix=0
- URL: https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/desi_provabgs/datafiles/healpix=0/
- Status: ✓ Verified accessible

### Photometric/Galaxy Morphology Catalogs

**GZ10** (Galaxy Zoo)
- healpix=0
- URL: https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/gz10/datafiles/healpix=0/
- Status: ✓ Verified accessible

### Time Series / Lightcurve Catalogs

**PLAsTiCC** (Simulated transients)
- healpix=0
- URL: https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/plasticc/data/healpix=0/
- Status: ✓ Verified accessible

**TESS** (Exoplanet lightcurves)
- healpix=0
- URL: https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/tess/spoc/healpix=0/
- Status: ✓ Verified accessible

### Supernova Catalogs

**Foundation**
- healpix=0
- URL: https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/foundation/foundation_dr1/healpix=0/
- Status: ✓ Verified accessible

**SNLS** (Supernova Legacy Survey)
- healpix=0
- URL: https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/snls/data/healpix=0/
- Status: ✓ Verified accessible

**PS1_SNE_IA** (Pan-STARRS1 Type Ia)
- healpix=0
- URL: https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/ps1_sne_ia/ps1_sne_ia/healpix=0/
- Status: ✓ Verified accessible

**YSE** (Young Supernova Experiment)
- healpix=0
- URL: https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/yse/yse_dr1/healpix=0/
- Status: ✓ Verified accessible

**Swift_SNE_IA** (Swift Type Ia)
- healpix=0
- URL: https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/swift_sne_ia/data/healpix=0/
- Status: ✓ Verified accessible

**CFA** (CFA Supernova)
- healpix=0
- URL: https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/cfa/cfa3/healpix=0/
- Status: ✓ Verified accessible

**DES_Y3_SNE_IA** (DES Year 3 Type Ia)
- healpix=0
- URL: https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/des_y3_sne_ia/des_y3_sne_ia/healpix=0/
- Status: ✓ Verified accessible

**CSP** (Carnegie Supernova Project)
- healpix=0
- URL: https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/csp/csp/healpix=0/
- Status: ✓ Verified accessible

### Alert/Transient Catalogs

**BTSbot** (Bright Transient Survey)
- healpix=0
- URL: https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/btsbot/data/healpix=0/
- Status: ✓ Verified accessible

### Imaging Catalogs

**SSL_LegacySurvey** (Self-supervised learning)
- healpix=0
- URL: https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/ssl_legacysurvey/north/healpix=0/
- Status: ✓ Verified accessible
- Note: 152x152 pixel images, may be larger

**HSC** (Hyper Suprime-Cam)
- healpix=0
- URL: https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/hsc/pdr3_dud_22.5/healpix=0/
- Status: ✓ Verified accessible
- Note: 160x160 pixel images, may be larger

**LegacySurvey** (DESI Legacy Survey)
- healpix=0
- URL: https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/legacysurvey/dr10_south_21/healpix=0/
- Status: ✓ Verified accessible
- Note: Complex catalog with images, RGB, masks - may be larger

**JWST** (James Webb Space Telescope)
- healpix=0
- URL: https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/jwst/primer-cosmos/healpix=0/
- Status: ✓ Verified accessible
- Note: Deep field imaging, may be larger

### IFU Datacubes

**MaNGA** (SDSS-IV MaNGA IFU)
- healpix=0
- URL: https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/manga/manga/healpix=0/
- Status: ✓ Verified accessible
- Note: Full IFU datacubes with spectra, images, maps - likely larger

## Notes

1. All healpix=0 directories have been verified as accessible (HTTP 200 response)
2. Actual file sizes could not be determined without downloading due to access restrictions on directory listings
3. The SDSS healpix=583 at 23MB is confirmed working as a reference point
4. For catalogs noted as "may be larger", alternative healpix values may be needed if >100MB

## Alternative Healpix Values (if needed)

If any catalog's healpix=0 exceeds 100MB, try these alternatives in order:
- sdss: healpix=583 (confirmed 23MB)
- Others: Try sequential values 1, 2, 3, 4, 5, 10, 50, 100, 500, etc.

## Download Instructions

To download a specific healpix for testing:

```bash
# Example for SDSS healpix=583
wget -r -np -nH --cut-dirs=1 -R "index.html*" -q \
  https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/sdss/sdss/healpix=583/

# Generic pattern:
wget -r -np -nH --cut-dirs=1 -R "index.html*" -q \
  https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/{catalog}/{subcatalog}/healpix={N}/
```

## Verification Workflow

For each catalog:
1. Download the specified healpix
2. Process using the catalog's download script (datasets library)
3. Transform using the corresponding transformer class
4. Compare the two outputs using the generalized compare.py script
5. Document any discrepancies or issues
