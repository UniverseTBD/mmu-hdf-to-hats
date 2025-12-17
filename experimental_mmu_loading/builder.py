from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from datasets import ArrowBasedBuilder, BuilderConfig, DatasetInfo, Features, SplitGenerator, Split, SplitDict
from datasets.download.download_manager import DownloadManager
from datasets.arrow_writer import ArrowWriter
from datasets.utils.file_utils import is_remote_url
from huggingface_hub import HfFileSystem
from datasets.packaged_modules.parquet.parquet import Parquet, ParquetConfig
from datasets import config
import numpy as np
import re

# todo: maybe we can get around polars?!
import polars as pl

from astropy.table import Table, hstack
from astropy.coordinates import SkyCoord
from astropy import units as u


# todo: move to config
CROSS_MATCH_COLS = ['ra', 'dec', 'object_id', "healpix"]
FILE_PARTITION_PATTERN = r"(healpix=\d+)\/"

@dataclass
# class MMUConfig(BuilderConfig):
class MMUConfig(ParquetConfig):
    """Configuration for MMU datasets with crossmatching support.

    Attributes:
        name: The name of the configuration.
        version: The version of the configuration.
        data_dir: Path to the directory containing the source data.
        data_files: Path(s) to source data file(s).
        description: A human description of the configuration.
        matching_datasets: Dict mapping {name: dataset_path} for datasets to crossmatch with.
        matching_fn: Function(primary_index, other_indices, config) -> Dict[str, List[Tuple]].
        matching_config: Configuration dict passed to matching_fn (e.g., {"tolerance": 1.0}).
        index_partition: Name of the index partition directory (default: "_index").
        split_name: Name of the split directory (default: "train").
        batch_size: Batch size for reading parquet files.
        columns: Specific columns to load (None means all).
        features: Dataset features schema.
    """

    split_name: str = "train"
    index_partition: str = "_index"
    left_dataset: str = "default_left"
    right_dataset: str = "default_right"
    batch_size: Optional[int] = None
    columns: Optional[List[str]] = None
    features: Optional[Features] = None

    # name: str = "default"
    # version: Optional[Union[utils.Version, str]] = utils.Version("0.0.0")
    # data_dir: Optional[str] = None
    # data_files: Optional[Union[DataFilesDict, DataFilesPatternsDict]] = None
    # description: Optional[str] = None

    def __post_init__(self):
        super().__post_init__()

class MMUDatasetBuilder(Parquet):
    """Custom DatasetBuilder for Multimodal Universe datasets.

    Implements efficient crossmatching by:
    1. Loading _index partition first
    2. Applying crossmatch function to filter partitions
    3. Downloading only relevant data partitions
    """

    BUILDER_CONFIG_CLASS = MMUConfig

    def __init__(self, *args, **kwargs):
        """Initialize builder and prepare for index-aware loading."""
        left_dataset = kwargs.get("left_dataset", "default_left")
        right_dataset = kwargs.get("right_dataset", "default_right")
        self.all_files = {"left": [], "right": []}
        cache_dir = Path(config.DEFAULT_HF_DATASETS_CACHE) / f"{left_dataset.replace('/', '_')}_{right_dataset.replace('/', '_')}"
        super().__init__(*args, **kwargs, cache_dir=cache_dir)

        self.left_name = self.config.left_dataset.split("/")[-1]
        self.right_name = self.config.right_dataset.split("/")[-1]
        self._relevant_partitions: Optional[List[Tuple[int, int]]] = None
        self.hf_fs = HfFileSystem()

    def _download_and_prepare(self, dl_manager, verification_mode, **prepare_split_kwargs):
        """Downloads and prepares dataset following the datasets library pattern.

        This method:
        1. Downloads and crossmatches index tables
        2. Downloads and joins matched data
        3. Writes partitioned parquet files to train split
        4. Updates dataset info with split information
        """
        # Step 1: Download indices and perform crossmatching
        index_tables = self._download_and_load_crossmatching_cols(dl_manager)
        matched_catalog = self.crossmatch_index_tables(*index_tables)

        # Step 2: Write matched data to partitioned structure
        split_info = self._write_matched_data_to_train_split(matched_catalog)

        # Step 3: Update info object with splits
        split_dict = SplitDict(dataset_name=self.dataset_name)
        split_dict.add(split_info)
        self.info.splits = split_dict
        self.info.dataset_size = split_info.num_bytes
        self.info.download_size = dl_manager.downloaded_size

    def _write_matched_data_to_train_split(self, matched_catalog):
        """Write matched catalog data to train split with partition structure.

        Structure: cache_dir/train/<partition-name>/data.parquet

        Returns:
            SplitInfo with statistics about the written data
        """
        from datasets import SplitInfo, Split

        # Prepare output directory: cache_dir/train/
        train_dir = Path(self._output_dir) / "train"
        train_dir.mkdir(parents=True, exist_ok=True)

        # Group matched catalog by healpix partition
        partitions = {}
        for group in matched_catalog.groups:
            healpix = group['healpix'][0]
            partition_name = f"healpix={healpix}"
            partitions[partition_name] = group

        total_num_examples = 0
        total_num_bytes = 0

        # Process each partition
        for partition_name, partition_catalog in partitions.items():
            # Create partition directory
            partition_dir = train_dir / partition_name
            partition_dir.mkdir(parents=True, exist_ok=True)

            # Get files for this partition
            left_grouped = partition_catalog.group_by(self.left_name + "_file")
            left_files = {group[self.left_name + "_file"][0]: group[self.left_name + "_object_id"].tolist()
                          for group in left_grouped.groups}
            right_grouped = partition_catalog.group_by(self.right_name + "_file")
            right_files = {group[self.right_name + "_file"][0]: group[self.right_name + "_object_id"].tolist()
                           for group in right_grouped.groups}

            # Download and join data
            # we don't need to yield here, we can just get the full tables
            lt_iter = self._download_files_single_partition(left_files)
            rt_iter = self._download_files_single_partition(right_files)

            # todo: can be cast directly to polars
            mc_objects = pa.table(partition_catalog[[self.left_name + "_object_id",
                                                    self.right_name + "_object_id"]].to_pandas())

            partition_tables = []
            # todo: this can only work if the files are correctly grouped by partition!!! And lt_iter and rt_iter have the same length -> change lt_iter and rt_iter to tables
            for left_table, right_table in zip(lt_iter, rt_iter):
                # Join left with matched catalog
                print("Joining tables for partition:", partition_name, "left rows:", left_table.num_rows, "right rows:", right_table.num_rows)
                lm = pl.from_arrow(left_table).join(
                    pl.from_arrow(mc_objects),
                    left_on="object_id",
                    right_on=self.left_name + "_object_id",
                    how="inner"
                )
                # Join with right table
                lmr = lm.join(
                    pl.from_arrow(right_table),
                    left_on=self.right_name + "_object_id",
                    right_on="object_id",
                    how="inner"
                )
                partition_tables.append(lmr.to_arrow())

            # Concatenate all tables for this partition
            if partition_tables:
                partition_table = pa.concat_tables(partition_tables)

                # Write to parquet file
                output_file = partition_dir / "data.parquet"
                pq.write_table(partition_table, output_file)

                total_num_examples += partition_table.num_rows
                total_num_bytes += output_file.stat().st_size

        # Create and return split info
        split_info = SplitInfo(
            name="train",
            num_examples=total_num_examples,
            num_bytes=total_num_bytes
        )

        return split_info

    def _download_files_single_partition(self, files):
        """Download and yield tables for files in a single partition.

        Args:
            files: Dict mapping file paths to lists of object IDs

        Yields:
            PyArrow tables for each file
        """
        partition_tables = []
        for file, obj_ids in files.items():
            table = self._process_file(file, obj_ids)
            partition_tables.append(table)
        yield pa.concat_tables(partition_tables)

    def _build_single_dataset(
        self,
        split,
        in_memory,
        **dataset_kwargs,
    ):
        """Override to load from custom partition structure.

        Reads parquet files from train/<partition>/data.parquet instead of
        the standard arrow file format.
        """
        from datasets import Dataset, concatenate_datasets

        # Get the train directory
        train_dir = Path(self._output_dir) / "train"

        if not train_dir.exists():
            raise FileNotFoundError(f"Train directory not found: {train_dir}")

        # Find all partition directories
        partition_dirs = [d for d in train_dir.iterdir() if d.is_dir() and d.name.startswith("healpix=")]

        if not partition_dirs:
            raise FileNotFoundError(f"No partition directories found in {train_dir}")

        # Load datasets from each partition
        datasets = []
        for partition_dir in sorted(partition_dirs):
            parquet_file = partition_dir / "data.parquet"
            if parquet_file.exists():
                # Load the parquet file as a Dataset
                table = pq.read_table(parquet_file)
                ds = Dataset(pa.table(table))
                datasets.append(ds)

        if not datasets:
            raise FileNotFoundError(f"No data.parquet files found in partition directories")

        # Concatenate all partition datasets
        if len(datasets) == 1:
            return datasets[0]
        else:
            return concatenate_datasets(datasets)

    def _process_file(self, file_name: str, object_ids) -> pa.Table:
        print("Downloading and filtering file:", file_name, "for", len(object_ids), "object IDs")
        table = pq.read_table(
            self.hf_fs.open(file_name, "rb"),
            filters=pc.field("object_id").isin(pa.array(object_ids))
        )
        return table

    def crossmatch_index_tables(self, left, right,
                                # todo: change back to one
                                matching_radius : float = 1., 
                                ):
        left = Table.from_pandas(left.to_pandas())
        right = Table.from_pandas(right.to_pandas())

        left['sc'] = SkyCoord(left['ra'], 
                              left['dec'], unit='deg')
        
        right['sc'] = SkyCoord(right['ra'],
                               right['dec'], unit='deg')
        cat_left = left
        cat_right = right
        # Cross match the catalogs and restricting them to matches
        idx, sep2d, _ = cat_left['sc'].match_to_catalog_sky(cat_right['sc'])
        mask = sep2d < matching_radius*u.arcsec
        cat_left = cat_left[mask]
        cat_right = cat_right[idx[mask]]
        assert len(cat_left) == len(cat_right), "There was an error in the cross-matching."
        print("Initial number of matches: ", len(cat_left))
        matched_catalog = hstack([cat_left, cat_right], 
                                 table_names=[self.left_name, self.right_name],
                                 uniq_col_name='{table_name}_{col_name}')
        # todo: why do we do this? This is not strictly necessary for our implementation, we could simply include these!!
        # Remove objects that were matched between the two catalogs but fall under different healpix indices
        mask = matched_catalog[f'{self.left_name}_healpix'] == matched_catalog[f'{self.right_name}_healpix']
        matched_catalog = matched_catalog[mask]
        print("Number of matches lost at healpix region borders: ", len(cat_left) - len(matched_catalog))
        print("Final size of cross-matched catalog: ", len(matched_catalog))

        # Adding default columns to respect format
        matched_catalog['object_id'] = matched_catalog[self.left_name+'_object_id']
        matched_catalog['ra'] = 0.5*(matched_catalog[self.left_name+'_ra'] +
                                     matched_catalog[self.right_name+'_ra'])
        matched_catalog['dec'] = 0.5*(matched_catalog[self.left_name+'_dec'] +
                                     matched_catalog[self.right_name+'_dec'])
        
        # Check that all matches have the same healpix index
        assert np.all(matched_catalog[self.left_name+'_healpix'] == matched_catalog[self.right_name+'_healpix']), "There was an error in the cross-matching."
        matched_catalog['healpix'] = matched_catalog[self.left_name+'_healpix']
        matched_catalog = matched_catalog.group_by(['healpix'])
        return matched_catalog

    def _download_and_load_crossmatching_cols(self, dl_manager: DownloadManager) -> List[pa.Table]:
        """Download and load the _index partition into memory.

        Args:
            dl_manager: DownloadManager for handling downloads

        Returns:
            PyArrow Table containing the index data
        """
        files_left, files_right = self._get_file_urls()

        if len(files_left) == 0 or len(files_right) == 0:
            raise ValueError(f"No index files found in '{self.config.index_partition}' partition")

        # Download index files
        hf_fs = HfFileSystem()

        # Load all index files into a single table
        left_tables: list[pa.Table] = []
        right_tables: list[pa.Table] = []
        for f_left in files_left:
            table_left: pa.Table = pq.read_table(
                                  hf_fs.open(f_left, "rb"),
                                  columns=CROSS_MATCH_COLS
                                  )
            table_left = table_left.append_column("file", pa.array([f_left] * table_left.num_rows))
            left_tables.append(table_left)
        for f_right in files_right:
            table_right: pa.Table = pq.read_table(
                                  hf_fs.open(f_right, "rb"),
                                  columns=CROSS_MATCH_COLS
            )
            table_right = table_right.append_column("file", pa.array([f_right] * table_right.num_rows))
            right_tables.append(table_right)

        return pa.concat_tables(left_tables), pa.concat_tables(right_tables)

    def _get_file_urls(self) -> tuple[list[str]]:
        """Get URLs/paths for all files in the _index partition.

        Uses HfFileSystem to list files in the repository and filter
        for those in the index partition.

        Returns:
            List of HF URLs (e.g., ["hf://datasets/org/dataset/train/_index/file.parquet"])
        """
        self.all_files = self._list_repository_files()

        # Filter for index partition files (under split_name/_index/)
        all_files_flat = self.all_files[self.left_name] + self.all_files[self.right_name]
        partitions_left = set()
        partitions_right = set()
        for f in self.all_files[self.left_name]:
            if (match := re.search(FILE_PARTITION_PATTERN, f)):
                partitions_left.add(match.group(1))
        for f in self.all_files[self.right_name]:
            if (match := re.search(FILE_PARTITION_PATTERN, f)):
                partitions_right.add(match.group(1))
        common_partitions = partitions_left.intersection(partitions_right)
        # todo: remove this hack, only to limit test data to process
        # common_partitions = [k for k in common_partitions if "1174" not in k]

        files_left = [f"datasets/{f}" for f in self.all_files[self.left_name] if any(part in f for part in common_partitions) and f.endswith(".parquet")] 
        files_right = [f"datasets/{f}" for f in self.all_files[self.right_name] if any(part in f for part in common_partitions) and f.endswith(".parquet")] 

        return files_left, files_right

    def _list_repo_files_single_ds(self, dataset_name):
        if (Path(config.DEFAULT_HF_DATASETS_CACHE) / dataset_name.replace("/", "_")).exists():
            base_path = Path(config.DEFAULT_HF_DATASETS_CACHE) / dataset_name.replace("/", "_")
            files = []
            for file_path in base_path.rglob("*"):
                if file_path.is_file():
                    files.append(str(file_path.relative_to(base_path)))
            return files
        else:
            repo_id = self._extract_repo_id_from_url(dataset_name)
            try:
                fs = HfFileSystem()
                files = fs.ls(f"datasets/{repo_id}", detail=False, recursive=True)
                # Strip the repo prefix
                files = [f.replace(f"datasets/", "") for f in files]
                return files
            except Exception as e:
                raise ValueError(f"Failed to list files in repository {repo_id}: {e}")


    def _list_repository_files(self) -> Dict[str, str]:
        """List all files in the dataset repository.

        Returns:
            List of file paths in the repository
        """
        files_left = self._list_repo_files_single_ds(self.config.left_dataset)
        files_right = self._list_repo_files_single_ds(self.config.right_dataset)
        return {self.left_name: files_left,
                self.right_name: files_right}

    def _extract_repo_id_from_url(self, url: str) -> str:
        """Extract repo_id from HuggingFace URL.

        Args:
            url: HF URL like "hf://datasets/org/dataset" or "https://huggingface.co/datasets/org/dataset"

        Returns:
            Repo ID like "org/dataset"
        """
        if url.startswith("hf://datasets/"):
            return url.replace("hf://datasets/", "").split("@")[0].rstrip("/")
        elif "huggingface.co/datasets/" in url:
            parts = url.split("huggingface.co/datasets/")[1]
            return parts.split("/")[0] + "/" + parts.split("/")[1]
        else:
            # Assume it's already a repo_id
            return url.rstrip("/")

    def _info(*args, **kwargs) -> DatasetInfo:
        """Return the dataset metadata and schema."""
        return DatasetInfo()
