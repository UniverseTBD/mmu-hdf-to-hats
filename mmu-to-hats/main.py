# for fast debugging run:
#  python ./main.py   --input=https://users.flatironinstitute.org/~polymathic/data/MultimodalUniverse/v1/sdss/sdss/healpix=583/   --output=./hats   --name=mmu_sdss_sdss   --tmp-dir=./tmp   --max-rows=8192
import argparse
import importlib
import logging
import math
from multiprocessing import cpu_count
from pkgutil import walk_packages

import h5py
import numpy as np
import pyarrow as pa
from dask.distributed import Client
from hats_import import CollectionArguments
from hats_import.catalog.file_readers import InputReader
from hats_import.pipeline import pipeline_with_client
from upath import UPath

import catalog_functions
from catalog_functions.utils import BaseTransformer

LOGGER = logging.getLogger(__name__)


def available_transformers():
    transformers = []
    for module_info in walk_packages(
        catalog_functions.__path__, catalog_functions.__name__ + "."
    ):
        *_, module_name = module_info.name.rsplit(".", maxsplit=1)
        if not module_name.endswith("_transformer"):
            continue
        name = module_name.removesuffix("_transformer")
        transformers.append(name)
    return transformers


def get_transformer(name):
    module = importlib.import_module(
        catalog_functions.__name__ + f".{name}_transformer"
    )
    classes = []
    for module_attr in dir(module):
        obj = getattr(module, module_attr)
        if (
            isinstance(obj, type)
            and issubclass(obj, BaseTransformer)
            and obj != BaseTransformer
        ):
            classes.append(obj)
    if len(classes) == 0:
        raise ValueError("No transformers found")
    if len(classes) >= 2:
        raise ValueError(f"More than one transformer found: {classes}")
    return classes[0]


def parse_args(argv):
    parser = argparse.ArgumentParser("Convert MMU dataset to HATS")
    parser.add_argument(
        "-c",
        "--transformer",
        required=True,
        type=str,
        help="one of the available catalog transformers",
        choices=available_transformers(),
    )
    parser.add_argument("-n", "--name", required=True, help="HATS catalog name")
    parser.add_argument(
        "-i", "--input", required=True, type=UPath, help="Input MMU dataset URI"
    )
    parser.add_argument(
        "-o",
        "--output",
        required=True,
        type=UPath,
        help="Output HATS URI, [NAME] directory will be created inside it",
    )
    parser.add_argument(
        "-r",
        "--max-rows",
        required=True,
        type=int,
        help="Max number of rows per HATS partition",
    )
    parser.add_argument(
        "-t",
        "--tmp-dir",
        default=UPath("./tmp"),
        type=UPath,
        help="Temporary directory path",
    )
    parser.add_argument("--ra", default="ra", help="Right ascension column name")
    parser.add_argument("--dec", default="dec", help="Declination column name")
    parser.add_argument(
        "--first-n",
        default=None,
        type=int,
        help="First N files only, useful for debugging",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode (single worker, single thread, no separate processes)",
    )
    return parser.parse_args(argv)


def np_to_pyarrow_array(array: np.ndarray) -> pa.Array:
    """Massively copy-pasted from hats_import.catalog.file_reader.fits._np_to_pyarrow_array
    https://github.com/astronomy-commons/hats-import/blob/e9c7b647dae309ced9f9ce2916692c2aecde2612/src/hats_import/catalog/file_readers/fits.py#L9
    """
    values = pa.array(array.reshape(-1))
    # "Base" type
    if array.ndim == 1:
        return values
    if array.ndim > 2:
        raise ValueError("Only 1D and 2D arrays are supported")
    n_lists, length = array.shape
    offsets = np.arange(0, (n_lists + 1) * length, length, dtype=np.int32)
    return pa.ListArray.from_arrays(values=values, offsets=offsets)


class MMUReader(InputReader):
    def __init__(self, chunk_mb: float, transform_klass):
        super().__init__()
        self.chunk_bytes = chunk_mb * 1024 * 1024
        self.transform = transform_klass()

    def _num_chunks(
        self, file_obj, h5_file: h5py.File, columns: list[str] | None
    ) -> int:
        if columns is None:
            size = file_obj.size
        else:
            size = sum(h5_file[col].nbytes for col in columns)
        return max(1, int(math.ceil(size / self.chunk_bytes)))

    def read(self, input_file: str, read_columns: list[str] | None = None):
        with UPath(input_file).open("rb") as fh, h5py.File(fh) as h5_file:
            num_chunks = self._num_chunks(fh, h5_file, read_columns)
            if read_columns is None:
                read_columns = list(h5_file)
            n_rows = h5_file[read_columns[0]].shape[0]
            chunk_size = max(1, n_rows // num_chunks)
            for i in range(0, n_rows, chunk_size):
                if set(read_columns) == set(["ra", "dec"]):
                    data = {
                        col: np_to_pyarrow_array(h5_file[col][i : i + chunk_size])
                        for col in read_columns
                    }
                    table = pa.table(data)
                    yield table
                else:
                    data = {
                        col: h5_file[col][i : i + chunk_size] for col in read_columns
                    }
                    table = self.transform.dataset_to_table(data)
                    yield table


def input_file_list(path: UPath) -> list[str]:
    path_list = sorted(path.rglob("*.hdf5"))
    return [upath.path for upath in path_list]


def main(argv=None):
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    cmd_args = parse_args(argv)

    # https://github.com/astronomy-commons/hats-import/issues/624
    cmd_args.tmp_dir.mkdir(parents=True, exist_ok=True)

    transformer_klass = get_transformer(cmd_args.transformer)

    input_files = input_file_list(cmd_args.input)
    if cmd_args.first_n is not None:
        input_files = input_files[: cmd_args.first_n]

    import_args = (
        CollectionArguments(
            output_artifact_name=cmd_args.name,
            output_path=cmd_args.output,
            tmp_dir=cmd_args.tmp_dir,
        )
        .catalog(
            input_file_list=input_files,
            file_reader=MMUReader(chunk_mb=128, transform_klass=transformer_klass),
            ra_column=cmd_args.ra,
            dec_column=cmd_args.dec,
            pixel_threshold=cmd_args.max_rows,
            lowest_healpix_order=4,
        )
        .add_margin(margin_threshold=10.0, is_default=True)
    )

    # Choose Client configuration based on debug flag
    if cmd_args.debug:
        # Debug mode: use 1 worker and 1 thread for easier debugging with breakpoints
        client_kwargs = {"n_workers": 1, "threads_per_worker": 1, "processes": False}
        LOGGER.info(
            "Running in DEBUG mode (single worker, single thread, no separate processes)"
        )
    else:
        # Production mode: use multiple workers
        client_kwargs = {"n_workers": min(8, cpu_count()), "threads_per_worker": 1}
        LOGGER.info(
            f"Running in PRODUCTION mode ({client_kwargs['n_workers']} workers)"
        )

    with Client(**client_kwargs) as client:
        LOGGER.info(f"Dask dashboard: {client.dashboard_link}")
        pipeline_with_client(import_args, client)


if __name__ == "__main__":
    main()
