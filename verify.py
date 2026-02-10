import subprocess
import click


catalogs = [
    "sdss",
    "btsbot",
    "cfa",
    "csp",
    "des_y3_sne_ia",
    "desi",
    "desi_provabgs",
    "foundation",
    "gaia",
    "gui",
    "gz10",
    "hsc",
    "jwst",
    "legacysurvey",
    "manga",
    "plasticc",
    "ps1_sne_ia",
    "snls",
    "ssl_legacysurvey",
    "start",
    "swift_sne_ia",
    "tess",
    "vipers",
    "yse",
]

catalog_data = {
    "btsbot": {
        "original-mmu": "data/MultimodalUniverse/v1/btsbot_with_coordinates/",
        "rewritten": "data/btsbot_hp0313_transformed.parquet",
        "allowed-mismatch-columns": "image.band",
    },
    "desi": {
        "original-mmu": "data/MultimodalUniverse/v1/desi_with_coordinates/",
        "rewritten": "data/desi_hp626_transformed.parquet",
    },
    "desi_provabgs": {
        "original-mmu": "data/MultimodalUniverse/v1/desi_provabgs_with_coordinates/",
        "rewritten": "data/desi_provabgs_hp669_transformed.parquet",
    },
    "des_y3_sne_ia": {
        "original-mmu": "data/MultimodalUniverse/v1/des_y3_sne_ia_with_coordinates/",
        "rewritten": "data/des_y3_sne_ia_hp1105_transformed.parquet",
    },
    "foundation": {
        "original-mmu": "data/MultimodalUniverse/v1/foundation_with_coordinates/",
        "rewritten": "data/foundation_hp1628_transformed.parquet",
    },
    "gaia": {
        "original-mmu": "data/MultimodalUniverse/v1/gaia_with_coordinates/",
        "rewritten": "data/gaia_hp1631_transformed.parquet",
    },
    "legacysurvey": {
        "original-mmu": "data/MultimodalUniverse/v1/legacysurvey_with_coordinates/",
        "rewritten": "data/legacysurvey_hp1981_transformed.parquet",
    },
    "gz10": {
        "original-mmu": "data/MultimodalUniverse/v1/gz10_with_coordinates/",
        "rewritten": "data/gz10_hp513_transformed.parquet",
    },
    "hsc": {
        "original-mmu": "data/MultimodalUniverse/v1/hsc_with_coordinates/",
        "rewritten": "data/hsc_hp1106_transformed.parquet",
    },
    "sdss": {
        "original-mmu": "data/MultimodalUniverse/v1/sdss_with_coordinates/",
        "rewritten": "data/sdss_hp583_transformed.parquet",
    },
    "snls": {
        "original-mmu": "data/MultimodalUniverse/v1/snls_with_coordinates/",
        "rewritten": "data/snls_hp0714_transformed.parquet",
    },
    "ps1_sne_ia": {
        "original-mmu": "data/MultimodalUniverse/v1/ps1_sne_ia_with_coordinates/",
        "rewritten": "data/ps1_sne_ia_hp1105_transformed.parquet",
    },
    "tess": {
        "original-mmu": "data/MultimodalUniverse/v1/tess_with_coordinates/",
        "rewritten": "data/tess_hp2201_transformed.parquet",
    },
    "vipers": {
        "original-mmu": "data/MultimodalUniverse/v1/vipers_with_coordinates/",
        "rewritten": "data/vipers_hp1107_transformed.parquet",
    },
    "manga": {
        "original-mmu": "data/MultimodalUniverse/v1/manga_with_coordinates/",
        "rewritten": "data/manga_hp385_transformed.parquet",
        "allowed-mismatch-columns": "images.filter,images.flux_units,images.psf_units,images.scale_units,maps.group,maps.label,spaxels.flux_units,spaxels.lambda_units,spaxels.skycoo_units,spaxels.ellcoo_r_units,spaxels.ellcoo_rre_units,spaxels.ellcoo_rkpc_units,spaxels.ellcoo_theta_units,maps.array_units",
    },
    "swift_sne_ia": {
        "original-mmu": "data/MultimodalUniverse/v1/swift_sne_ia_with_coordinates/",
        "rewritten": "data/swift_sne_ia_hp2158_transformed.parquet",
    },
    "yse": {
        "original-mmu": "data/MultimodalUniverse/v1/yse_with_coordinates/",
        "rewritten": "data/yse_hp584_transformed.parquet",
    },
    "jwst": {
        "original-mmu": "data/MultimodalUniverse/v1/jwst_with_coordinates/",
        "rewritten": "data/jwst_hp2245_transformed.parquet",
    },
    "plasticc": {
        "original-mmu": "data/MultimodalUniverse/v1/plasticc_with_coordinates/",
        "rewritten": "data/plasticc_hp1378_transformed.parquet",
    },
    "cfa": {
        "original-mmu": "data/MultimodalUniverse/v1/cfa_with_coordinates/",
        "rewritten": "data/cfa_hp0146_transformed.parquet",
    },
    "csp": {
        "original-mmu": "data/MultimodalUniverse/v1/csp_with_coordinates/",
        "rewritten": "data/csp_hp1113_transformed.parquet",
    },
    "ssl_legacysurvey": {
        "original-mmu": "data/MultimodalUniverse/v1/ssl_legacysurvey_with_coordinates/",
        "rewritten": "data/ssl_legacysurvey_hp125_transformed.parquet",
    },
}


@click.command()
@click.argument("catalog_name", type=click.Choice(catalogs))
def run_single_catalog(catalog_name: str):
    """Run verification pipeline for a catalog."""
    # Step 0: Download data + script file
    click.echo(f"Step 0: Downloading data and script for {catalog_name}...")
    download_command = f"./verification/download_{catalog_name}.sh"
    result = subprocess.run(
        download_command,
        shell=True,
        capture_output=True,
        text=True,
    )
    click.echo(result.stdout)
    if result.returncode != 0:
        click.echo("Error in download step:", err=True)
        click.echo(result.stderr, err=True)
        exit(1)
    # Step 1: Load via external script
    click.echo(f"Step 1: Loading {catalog_name} via datasets...")
    load_via_extern_script_command = f"uv run --with-requirements=verification/requirements.in python verification/process_{catalog_name}_using_datasets.py"
    result = subprocess.run(
        load_via_extern_script_command,
        shell=True,
        capture_output=True,
        text=True,
    )
    click.echo(result.stdout)
    if result.returncode != 0:
        click.echo("Error in loading step:", err=True)
        click.echo(result.stderr, err=True)
        exit(1)

    # Step 2: Transform to parquet
    click.echo(f"\nStep 2: Transforming {catalog_name} to parquet...")
    transform_to_parquet_command = (
        f"python -m transform_scripts.transform_{catalog_name}_to_parquet"
    )
    result = subprocess.run(
        transform_to_parquet_command,
        shell=True,
        capture_output=True,
        text=True,
    )
    click.echo(result.stdout)
    if result.returncode != 0:
        click.echo("Error in transform step:", err=True)
        click.echo(result.stderr, err=True)
        exit(1)

    # Step 3: Compare files
    click.echo(f"\nStep 3: Comparing {catalog_name} files...")
    compare_command = f"python verification/compare.py --datasets-file {catalog_data[catalog_name]['original-mmu']} --rewritten-file {catalog_data[catalog_name]['rewritten']}"
    if "allowed-mismatch-columns" in catalog_data[catalog_name]:
        compare_command += f" --allowed-mismatch-columns {catalog_data[catalog_name]['allowed-mismatch-columns']}"
    result = subprocess.run(
        compare_command,
        shell=True,
        capture_output=True,
        text=True,
    )
    click.echo(result.stdout)
    if result.returncode != 0:
        click.echo("Error in comparison step:", err=True)
        click.echo(result.stderr, err=True)
        exit(1)

    click.echo(f"\n✓ Verification complete for {catalog_name}")
    return


if __name__ == "__main__":
    run_single_catalog()
