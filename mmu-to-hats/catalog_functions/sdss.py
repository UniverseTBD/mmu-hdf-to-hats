import h5py
import numpy as np
import pyarrow as pa

def infer_schema_from_hdf5(f):
    """Infer PyArrow schema from HDF5 file."""
    fields = []

    for key in f.keys():
        if isinstance(f[key], h5py.Dataset):
            dataset = f[key]
            dtype = dataset.dtype
            shape = dataset.shape

            # Map numpy dtype to PyArrow type
            if dtype == np.float32:
                pa_type = pa.float32()
            elif dtype == np.float64:
                pa_type = pa.float64()
            elif dtype == np.int32:
                pa_type = pa.int32()
            elif dtype == np.int64:
                pa_type = pa.int64()
            else:
                pa_type = pa.from_numpy_dtype(dtype)

            # Handle multidimensional arrays as lists
            if len(shape) > 1:
                pa_type = pa.list_(pa_type)

            fields.append(pa.field(key, pa_type))

    return pa.schema(fields)

def transform_sdss(data: pa.Table):
    if set(data.column_names) == set(["ra", "dec"]):
        return data
    else:
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
    
#                    example = {
#                        "spectrum": {
#                            "flux": data["spectrum_flux"][i].reshape([-1,1]),
#                            "ivar": data["spectrum_ivar"][i].reshape([-1,1]),
#                            "lsf_sigma": data["spectrum_lsf_sigma"][i].reshape([-1,1]),
#                            "lambda": data["spectrum_lambda"][i].reshape([-1,1]),
#                            "mask": data["spectrum_mask"][i].reshape([-1,1]),
#                        }
#                    }
