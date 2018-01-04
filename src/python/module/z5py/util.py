from __future__ import print_function
from itertools import product
from concurrent import futures

from .file import File


# ND blocking generator
def blocking(shape, block_shape):
    assert len(shape) == len(block_shape)
    ranges = [range(sha // bsha if sha % bsha == 0 else sha // bsha + 1)
              for sha, bsha in zip(shape, block_shape)]
    start_points = product(*ranges)
    for start_point in start_points:
        positions = [sp * bshape for sp, bshape in zip(start_point, block_shape)]
        yield tuple(slice(pos, min(pos + bsha, sha))
                    for pos, bsha, sha in zip(positions, block_shape, shape))


# TODO zarr support
# rechunk a n5 dataset
# also supports new compression opts
def rechunk(in_path,
            out_path,
            in_path_in_file,
            out_path_in_file,
            out_chunks,
            n_threads,
            out_blocks=None,
            dtype=None,
            **new_compression):
    f_in = File(in_path, use_zarr_format=False)
    f_out = File(out_path, use_zarr_format=False)

    # if we don't have out-blocks explitictly given,
    # we iterate over the out chunks
    if out_blocks is None:
        out_blocks = out_chunks

    ds_in = f_in[in_path_in_file]
    # if no out dtype was specified, use the original dtype
    if dtype is None:
        dtype = ds_in.dtype

    shape = ds_in.shape
    compression_opts = ds_in.compression_options
    compression_opts.update(new_compression)
    ds_out = f_out.create_dataset(out_path_in_file,
                                  dtype=dtype,
                                  shape=shape,
                                  chunks=out_chunks,
                                  **compression_opts)

    def write_single_chunk(roi):
        ds_out[roi] = ds_in[roi].astype(dtype, copy=False)

    with futures.ThreadPoolExecutor(max_workers=n_threads) as tp:
        tasks = [tp.submit(write_single_chunk, roi)
                 for roi in blocking(shape, out_blocks)]
        [t.result() for t in tasks]

    # copy attributes
    in_attrs = ds_in.attrs
    out_attrs = ds_out.attrs
    for key, val in in_attrs.items():
        out_attrs[key] = val
