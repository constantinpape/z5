from __future__ import print_function
import os
from itertools import product
from concurrent import futures
from contextlib import closing
from datetime import datetime
import numpy as np

from . import _z5py as z5_impl
from .file import File
from .group import Group
from .dataset import Dataset
from .shape_utils import normalize_slices


def product1d(inrange):
    for ii in inrange:
        yield ii


def blocking(shape, block_shape, roi=None, center_blocks_at_roi=False):
    """ Generator for nd blocking.

    Args:
        shape (tuple): nd shape
        block_shape (tuple): nd block shape
        roi (tuple[slice]): region of interest (default: None)
        center_blocks_at_roi (bool): if given a roi,
            whether to center the blocks being generated
            at the roi's origin (default: False)
    """
    assert len(shape) == len(block_shape), "Invalid number of dimensions."

    if roi is None:
        # compute the ranges for the full shape
        ranges = [range(sha // bsha if sha % bsha == 0 else sha // bsha + 1)
                  for sha, bsha in zip(shape, block_shape)]
        min_coords = [0] * len(shape)
        max_coords = shape
    else:
        # make sure that the roi is valid
        roi, _ = normalize_slices(roi, shape)
        ranges = [range(rr.start // bsha,
                        rr.stop // bsha if rr.stop % bsha == 0 else rr.stop // bsha + 1)
                  for rr, bsha in zip(roi, block_shape)]
        min_coords = [rr.start for rr in roi]
        max_coords = [rr.stop for rr in roi]

    need_shift = False
    if roi is not None and center_blocks_at_roi:
        shift = [rr.start % bsha for rr, bsha in zip(roi, block_shape)]
        need_shift = sum(shift) > 0

    # product raises memory error for too large ranges,
    # because input iterators are cast to tuple
    # TODO so far I have only seen this for 1d "open-ended" datasets
    # and hence just implemented a workaround for this case,
    # but it should be fairly easy to implement an nd version of product
    # without casting to tuple for our use case using the imglib loop trick, see also
    # https://stackoverflow.com/questions/8695422/why-do-i-get-a-memoryerror-with-itertools-product
    try:
        start_points = product(*ranges)
    except MemoryError:
        assert len(ranges) == 1
        start_points = product1d(ranges)

    for start_point in start_points:
        positions = [sp * bshape for sp, bshape in zip(start_point, block_shape)]
        if need_shift:
            positions = [pos + sh for pos, sh in zip(positions, shift)]
            if any(pos > maxc for pos, maxc in zip(positions, max_coords)):
                continue
        yield tuple(slice(max(pos, minc), min(pos + bsha, maxc))
                    for pos, bsha, minc, maxc in zip(positions, block_shape,
                                                     min_coords, max_coords))


def copy_dataset(in_path, out_path,
                 in_path_in_file, out_path_in_file,
                 n_threads, chunks=None,
                 block_shape=None, dtype=None,
                 use_zarr_format=None, roi=None,
                 fit_to_roi=False, **new_compression):
    """ Copy dataset, optionally change metadata.

    The input dataset will be copied to the output dataset chunk by chunk.
    Allows to change chunks, datatype, file format and compression.
    Can also just copy a roi.

    Args:
        in_path (str): path to the input file.
        out_path (str): path to the output file.
        in_path_in_file (str): name of input dataset.
        out_path_in_file (str): name of output dataset.
        n_threads (int): number of threads used for copying.
        chunks (tuple): chunks of the output dataset.
            By default same as input dataset's chunks. (default: None)
        block_shape (tuple): block shape used for copying. Must be a multiple
            of ``chunks``, which are used by default (default: None)
        dtype (str): datatype of the output dataset, default does not change datatype (default: None).
        use_zarr_format (bool): file format of the output file,
            default does not change format (default: None).
        roi (tuple[slice]): region of interest that will be copied. (default: None)
        fit_to_roi (bool): if given a roi, whether to set the shape of
            the output dataset to the roi's shape
            and align chunks with the roi's origin. (default: False)
        **new_compression: compression library and options for output dataset. If not given,
            the same compression as in the input is used.

    """
    f_in = File(in_path)
    # check if the file format was specified
    # if not, keep the format of the input file
    # otherwise set the file format
    is_zarr = f_in.is_zarr if use_zarr_format is None else use_zarr_format
    f_out = File(out_path, use_zarr_format=is_zarr)

    ds_in = f_in[in_path_in_file]

    # check if we can copy chunk by chunk
    if (chunks is None or chunks == ds_in.chunks) and (roi is None):
        copy_chunks = True
    else:
        copy_chunks = False

    # get dataset metadata from input dataset if defaults were given
    chunks = ds_in.chunks if chunks is None else chunks
    dtype = ds_in.dtype if dtype is None else dtype
    compression_opts = new_compression if new_compression else ds_in.compression_opts

    # if we don't have block-shape explitictly given, use chunk size
    # otherwise check that it's a multiple of chunks
    if block_shape is None:
        block_shape = chunks
    else:
        assert all(bs % ch == 0 for bs, ch in zip(block_shape, chunks)),\
            "block_shape must be a multiple of chunks"

    shape = ds_in.shape
    # we need to create the blocking here, before the shape is potentially altered
    # if fit_to_roi == True
    blocks = blocking(shape, block_shape, roi, fit_to_roi)
    if roi is not None:
        roi, _ = normalize_slices(roi, shape)
        if fit_to_roi:
            shape = tuple(rr.stop - rr.start for rr in roi)

    ds_out = f_out.require_dataset(out_path_in_file,
                                   dtype=dtype,
                                   shape=shape,
                                   chunks=chunks,
                                   **compression_opts)

    def write_single_block(bb):
        data_in = ds_in[bb].astype(dtype, copy=False)
        if np.sum(data_in) == 0:
            return
        if fit_to_roi and roi is not None:
            bb = tuple(slice(b.start - rr.start, b.stop - rr.start)
                       for b, rr in zip(bb, roi))
        ds_out[bb] = data_in

    def write_single_chunk(bb):
        chunk_id = tuple(b.start // ch for b, ch in zip(bb, chunks))
        chunk_in = ds_in.read_chunk(chunk_id)
        if chunk_in is None:
            return
        # check if this is a varlen chunk
        varlen = tuple(chunk_in.shape) != tuple(b.stop - b.start for b in bb)
        ds_out.write_chunk(chunk_id, chunk_in.astype(dtype, copy=False), varlen)

    write_single = write_single_chunk if copy_chunks else write_single_block

    with futures.ThreadPoolExecutor(max_workers=n_threads) as tp:
        tasks = [tp.submit(write_single, bb) for bb in blocks]
        [t.result() for t in tasks]

    # copy attributes
    in_attrs = ds_in.attrs
    out_attrs = ds_out.attrs
    for key, val in in_attrs.items():
        out_attrs[key] = val


# TODO we could allow to change formats from zarr to n5 and vice versa
def copy_group(in_path, out_path, in_path_in_file, out_path_in_file, n_threads):
    """ Copy group recursively.

    Copy the group recursively, using copy_dataset. Metadata of datasets that
    are copied cannot be changed and rois cannot be applied.

    Args:
        in_path (str): path to the input file.
        out_path (str): path to the output file.
        in_path_in_file (str): name of input group.
        out_path_in_file (str): name of output group.
        n_threads (int): number of threads used to copy datasets.
    """
    f_in = File(in_path)
    f_out = File(out_path)

    g_in = f_in[in_path_in_file]
    for key, obj in g_in.items():
        abs_in_key = os.path.join(in_path_in_file, key)
        abs_out_key = os.path.join(out_path_in_file, key)
        if isinstance(obj, Dataset):
            copy_dataset(in_path, out_path,
                         abs_in_key, abs_out_key, n_threads)
        else:
            assert isinstance(obj, Group)
            # copy group attributes
            g_out = f_out.require_group(abs_out_key)
            in_attrs = obj.attrs
            out_attrs = g_out.attrs
            for key, val in in_attrs.items():
                out_attrs[key] = val

            # copy group content
            copy_group(in_path, out_path,
                       abs_in_key, abs_out_key, n_threads)


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
    """ Remvoe dataset multi-threaded.
    """
    z5_impl.remove_dataset(dataset._impl, n_threads)


def remove_chunk(dataset, chunk_id):
    """ Remove a chunk
    """
    z5_impl.remove_chunk(dataset._impl, chunk_id)


def remove_chunks(dataset, bounding_box):
    """ Remove all chunks overlapping the bounding box
    """
    shape = dataset.shape
    chunks = dataset.chunks
    blocks = blocking(shape, chunks, roi=bounding_box)
    for block in blocks:
        chunk_id = tuple(b.start // ch for b, ch in zip(block, chunks))
        remove_chunk(dataset, chunk_id)


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
