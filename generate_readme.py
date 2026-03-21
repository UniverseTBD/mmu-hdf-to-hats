"""Generate a HuggingFace Dataset Card README for a HATS collection hosted on HuggingFace."""

import argparse
import re
from pathlib import Path
from urllib.parse import urlparse

import hats
import yaml
from hats.io.summary_file import write_collection_summary_file


def get_size_category(total_rows: int) -> str:
    """Map a row count to a HuggingFace size_categories value."""
    thresholds = [
        (1_000, "n<1K"),
        (10_000, "1K<n<10K"),
        (100_000, "10K<n<100K"),
        (1_000_000, "100K<n<1M"),
        (10_000_000, "1M<n<10M"),
        (100_000_000, "10M<n<100M"),
        (1_000_000_000, "100M<n<1B"),
        (10_000_000_000, "1B<n<10B"),
    ]
    for limit, label in thresholds:
        if total_rows < limit:
            return label
    return "n>10B"


def parse_hf_url(url: str) -> tuple[str, str]:
    """Extract org and repo from a HuggingFace dataset URL.

    Args:
        url: e.g. "https://huggingface.co/datasets/LSDB/mmu_sdss_sdss"

    Returns:
        Tuple of (org, repo)
    """
    parsed = urlparse(url)
    parts = parsed.path.strip("/").split("/")
    # Expected: ["datasets", "ORG", "REPO"]
    if len(parts) < 3 or parts[0] != "datasets":
        raise ValueError(
            f"Expected a HuggingFace dataset URL like "
            f"https://huggingface.co/datasets/ORG/REPO, got: {url}"
        )
    return parts[1], parts[2]


def main():
    parser = argparse.ArgumentParser(
        description="Generate a HuggingFace Dataset Card README for a HATS collection."
    )
    parser.add_argument(
        "url",
        help="HuggingFace dataset URL, e.g. https://huggingface.co/datasets/LSDB/mmu_sdss_sdss",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default="./readmes/",
        help="Local directory for generated README.md (default: ./)",
    )
    parser.add_argument(
        "-n",
        "--name",
        default=None,
        help="Human-readable dataset name (default: derived from repo name)",
    )
    parser.add_argument(
        "-d",
        "--description",
        default=None,
        help="Description text (default: auto-generated)",
    )
    parser.add_argument(
        "--filename",
        default=None,
        help="Output filename (default: README.md)",
    )
    parser.add_argument(
        "--license",
        default="cc-by-4.0",
        help="SPDX license identifier (default: cc-by-4.0)",
    )
    parser.add_argument(
        "--pretty-name",
        default=None,
        help="Pretty name for HF metadata (default: derived from --name)",
    )
    parser.add_argument(
        "--size-category",
        default=None,
        help="HF size category, e.g. '100K<n<1M', '1M<n<10M'",
    )
    parser.add_argument(
        "--task-categories",
        nargs="+",
        default=None,
        help="HF task categories, e.g. 'tabular-regression'",
    )
    args = parser.parse_args()

    org, repo = parse_hf_url(args.url)
    collection_path = f"hf://datasets/{org}/{repo}"

    name = args.name if args.name else repo
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    kwargs = {
        "collection_path": collection_path,
        "fmt": "markdown",
        "output_dir": output_dir,
        "name": name,
        "uri": args.url,
        "huggingface_metadata": True,
    }
    if args.description:
        kwargs["description"] = args.description
    if args.filename:
        kwargs["filename"] = args.filename

    output_path = write_collection_summary_file(**kwargs)

    # Auto-detect size category from catalog metadata if not provided
    size_category = args.size_category
    if not size_category:
        try:
            catalog = hats.read_hats(collection_path)
            # Collections return a CatalogCollection; get the main catalog
            if hasattr(catalog, "main_catalog"):
                catalog = catalog.main_catalog
            total_rows = catalog.catalog_info.total_rows
            if total_rows is not None:
                size_category = get_size_category(total_rows)
        except Exception:
            pass  # Fall back to no size category

    # Post-process to inject additional HF metadata into YAML frontmatter
    extra_fields = []
    extra_fields.append(f"license: {args.license}")
    pretty_name = args.pretty_name if args.pretty_name else name
    extra_fields.append(f"pretty_name: {pretty_name}")
    if size_category:
        extra_fields.append(f"size_categories:\n- {size_category}")
    if args.task_categories:
        lines = "\n".join(f"- {t}" for t in args.task_categories)
        extra_fields.append(f"task_categories:\n{lines}")

    readme_text = output_path.read_text()

    # Parse the YAML frontmatter, strip margin catalog configs, and inject extra fields
    fm_match = re.search(r"\n?---\n(.*?)---\n", readme_text, re.DOTALL)
    if fm_match:
        frontmatter = yaml.safe_load(fm_match.group(1)) or {}

        # Remove margin catalog configs (keep only the default/main catalog)
        if "configs" in frontmatter:
            frontmatter["configs"] = [
                c for c in frontmatter["configs"] if c.get("config_name") == "default"
            ]

        # Inject extra metadata
        frontmatter["license"] = args.license
        frontmatter["pretty_name"] = args.pretty_name if args.pretty_name else name
        if size_category:
            frontmatter["size_categories"] = [size_category]
        if args.task_categories:
            frontmatter["task_categories"] = args.task_categories

        new_frontmatter = yaml.dump(frontmatter, default_flow_style=False, sort_keys=False)
        readme_text = readme_text[: fm_match.start()] + f"\n---\n{new_frontmatter}---\n" + readme_text[fm_match.end() :]

    # Append crossmatch example
    crossmatch_section = f"""
### Crossmatch with another catalog

HATS catalogs can be efficiently crossmatched using [LSDB](https://lsdb.io),
which leverages the HEALPix partitioning to avoid loading the full datasets into memory:

```python
import lsdb

{name} = lsdb.open_catalog("{args.url}")
other = lsdb.open_catalog("https://huggingface.co/datasets/<org>/<other_catalog>")

crossmatched = {name}.crossmatch(other, radius_arcsec=1.0)
print(crossmatched)
```

See the [LSDB documentation](https://docs.lsdb.io/) for more details on crossmatching and other operations.
"""
    readme_text += crossmatch_section

    output_path.write_text(readme_text)

    print(f"Generated dataset card: {output_path}")


if __name__ == "__main__":
    main()
