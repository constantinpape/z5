from __future__ import print_function
from itertools import product
from concurrent import futures
from contextlib import closing
from datetime import datetime
import numpy as np

from . import _z5py as z5_impl
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


def fetch_test_data_stent():
    from imageio import volread
    data_i16 = volread('imageio:stent.npz')
    return (data_i16 / data_i16.max() * 255).astype(np.uint8)


def fetch_test_data():
    try:
        from urllib.request import urlopen
    except ImportError:
        from urllib2 import urlopen

    try:
        from io import BytesIO as Buffer
    except ImportError:
        from StringIO import StringIO as Buffer

    import zipfile
    from imageio import volread

    im_url = "https://imagej.nih.gov/ij/images/t1-head-raw.zip"

    with closing(urlopen(im_url)) as response:
        if response.status != 200:
            raise RuntimeError("Test data could not be found at {}, status code {}".format(
                im_url, response.status
            ))
        zip_buffer = Buffer(response.read())

    with zipfile.ZipFile(zip_buffer) as zf:
        tif_buffer = Buffer(zf.read('JeffT1_le.tif'))
        return np.asarray(volread(tif_buffer, format='tif'), dtype=np.uint8)


def remove_trivial_chunks(dataset, n_threads,
                          remove_specific_value=None):
    """ Remove chunks that only contain a single value.

    The input dataset will be copied to the output dataset chunk by chunk.
    Allows to change datatype, file format and compression as well.

    Args:
        dataset (z5py.Dataset)
        n_threads (int): number of threads
        remove_specific_value (int or float): only remove chunks that contain (only) this specific value (default: None)

    """

    dtype = dataset.dtype
    function = eval('z5_impl.remove_trivial_chunks_%s' % dtype)
    remove_specific = remove_specific_value is not None
    value = remove_specific_value if remove_specific else 0
    function(dataset._impl, n_threads, remove_specific, value)


def remove_dataset(dataset, n_threads):
    """
    Remvoe dataset multi-threaded.
    """
    z5_impl.remove_dataset(dataset._impl, n_threads)


def unique(dataset, n_threads, return_counts=False):
    """ Find unique values in dataset.

    Args:
        dataset (z5py.Dataset)
        n_threads (int): number of threads
        return_counts (bool): return counts of unique values (default: False)

    """
    dtype = dataset.dtype
    if return_counts:
        function = eval('z5_impl.unique_with_counts_%s' % dtype)
    else:
        function = eval('z5_impl.unique_%s' % dtype)
    return function(dataset._impl, n_threads)
