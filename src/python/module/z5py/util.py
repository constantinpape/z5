from __future__ import print_function
from itertools import product
from concurrent import futures
from datetime import datetime
import numpy as np

from .file import File


# ND blocking generator
def blocking(shape, block_shape):
    """ Generator for nd blocking.
    """
    if len(shape) != len(block_shape):
        raise RuntimeError("Invalid number of dimensions.")
    ranges = [range(sha // bsha if sha % bsha == 0 else sha // bsha + 1)
              for sha, bsha in zip(shape, block_shape)]
    start_points = product(*ranges)
    for start_point in start_points:
        positions = [sp * bshape for sp, bshape in zip(start_point, block_shape)]
        yield tuple(slice(pos, min(pos + bsha, sha))
                    for pos, bsha, sha in zip(positions, block_shape, shape))


def rechunk(in_path,
            out_path,
            in_path_in_file,
            out_path_in_file,
            out_chunks,
            n_threads,
            out_blocks=None,
            dtype=None,
            use_zarr_format=None,
            **new_compression):
    """ Copy and rechunk a dataset.

    The input dataset will be copied to the output dataset chunk by chunk.
    Allows to change datatype, file format and compression as well.

    Args:
        in_path (str): path to the input file.
        out_path (str): path to the output file.
        in_path_in_file (str): name of input dataset.
        out_path_in_file (str): name of output dataset.
        out_chunks (tuple): chunks of the output dataset.
        n_threads (int): number of threads used for copying.
        out_blocks (tuple): blocks used for copying. Must be a multiple
            of ``out_chunks``, which are used by default (default: None)
        dtype (str): datatype of the output dataset, default does not change datatype (default: None).
        use_zarr_format (bool): file format of the output file,
            default does not change format (default: None).
        **new_compression: compression library and options for output dataset. If not given,
            the same compression as in the input is used.

    """
    f_in = File(in_path)
    # check if the file format was specified
    # if not, keep the format of the input file
    # otherwise set the file format
    is_zarr = f_in.is_zarr if use_zarr_format is None else use_zarr_format
    f_out = File(out_path, use_zarr_format=is_zarr)

    # if we don't have out-blocks explitictly given,
    # we iterate over the out chunks
    if out_blocks is None:
        out_blocks = out_chunks

    ds_in = f_in[in_path_in_file]
    # if no out dtype was specified, use the original dtype
    if dtype is None:
        dtype = ds_in.dtype

    shape = ds_in.shape
    compression_opts = new_compression if new_compression else ds_in.compression_options
    ds_out = f_out.create_dataset(out_path_in_file,
                                  dtype=dtype,
                                  shape=shape,
                                  chunks=out_chunks,
                                  **compression_opts)

    def write_single_chunk(roi):
        data_in = ds_in[roi].astype(dtype, copy=False)
        if np.sum(data_in) == 0:
            return
        ds_out[roi] = data_in

    with futures.ThreadPoolExecutor(max_workers=n_threads) as tp:
        tasks = [tp.submit(write_single_chunk, roi)
                 for roi in blocking(shape, out_blocks)]
        [t.result() for t in tasks]

    # copy attributes
    in_attrs = ds_in.attrs
    out_attrs = ds_out.attrs
    for key, val in in_attrs.items():
        out_attrs[key] = val


class Timer(object):
    def __init__(self):
        self.start_time = None
        self.stop_time = None

    @property
    def elapsed(self):
        try:
            return (self.stop_time - self.start_time).total_seconds()
        except TypeError as e:
            if "'NoneType'" in str(e):
                raise RuntimeError("{} either not started, or not stopped".format(self))

    def start(self):
        self.start_time = datetime.utcnow()

    def stop(self):
        self.stop_time = datetime.utcnow()
        return self.elapsed

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
