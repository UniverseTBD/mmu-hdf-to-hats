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
        "original-mmu": "data/btsbot_hp0313_transformed.parquet",
        "rewritten": "data/MultimodalUniverse/v1/btsbot_with_coordinates/",
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
    if result.returncode != 0:
        click.echo("Error in download step:", err=True)
        click.echo(result.stderr, err=True)
        return result.returncode
    click.echo(result.stdout)
    # Step 1: Load via external script
    click.echo(f"Step 1: Loading {catalog_name} via datasets...")
    load_via_extern_script_command = f"uv run --with-requirements=verification/requirements.in python verification/process_{catalog_name}_using_datasets.py"
    result = subprocess.run(
        load_via_extern_script_command,
        shell=True,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        click.echo("Error in loading step:", err=True)
        click.echo(result.stderr, err=True)
        return result.returncode
    click.echo(result.stdout)

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
    if result.returncode != 0:
        click.echo("Error in transform step:", err=True)
        click.echo(result.stderr, err=True)
        return result.returncode
    click.echo(result.stdout)

    # Step 3: Compare files
    click.echo(f"\nStep 3: Comparing {catalog_name} files...")
    compare_command = f"python verification/compare.py {catalog_data[catalog_name]['original-mmu']} {catalog_data[catalog_name]['rewritten']}"
    result = subprocess.run(
        compare_command,
        shell=True,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        click.echo("Error in comparison step:", err=True)
        click.echo(result.stderr, err=True)
        return result.returncode
    click.echo(result.stdout)

    click.echo(f"\n✓ Verification complete for {catalog_name}")
    return


if __name__ == "__main__":
    run_single_catalog()
