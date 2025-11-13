import h5py
import pyarrow.parquet as pq
import pyarrow as pa
import math
import numpy as np
from upath import UPath

input_file = "data/MultimodalUniverse/v1/sdss/sdss/healpix=583/001-of-001.hdf5"

def transform_sdss(data: pa.Table):
        spectrum_col = pa.StructArray.from_arrays(
              [
                  data["spectrum_flux"].combine_chunks(),
                  data["spectrum_ivar"].combine_chunks(),
                  data["spectrum_lsf_sigma"].combine_chunks(),
                  data["spectrum_lambda"].combine_chunks(),
                  data["spectrum_mask"].combine_chunks()
              ],
              names=["flux", "ivar", "lambda", "lsf_sigma", "mask"]
        )
        data = data.append_column("spectrum", spectrum_col)
        return data.drop_columns(["spectrum_flux",
                                  "spectrum_ivar",
                                  "spectrum_lsf_sigma",
                                  "spectrum_lambda",
                                  "spectrum_mask"
                                  ]
                                 )

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


with UPath(input_file).open("rb") as fh, h5py.File(fh) as h5_file:
    read_columns = list(h5_file)
    n_rows = h5_file[read_columns[0]].shape[0]
    data = {
        col: np_to_pyarrow_array(h5_file[col])
        for col in read_columns
    }
    table = pa.table(data)
    table_transformed = transform_sdss(table)
    table_transformed.to_parquet('data/transformed_table.parquet')
