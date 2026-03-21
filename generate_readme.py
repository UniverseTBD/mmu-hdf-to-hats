"""Generate a HuggingFace Dataset Card README for a HATS collection hosted on HuggingFace."""

import argparse
from pathlib import Path
from urllib.parse import urlparse

from hats.io.summary_file import write_collection_summary_file


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
        default="./",
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
    print(f"Generated dataset card: {output_path}")


if __name__ == "__main__":
    main()
