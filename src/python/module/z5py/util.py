from __future__ import print_function
import os
from concurrent import futures
from itertools import product
from random import shuffle

from .file import File


# TODO zarr support
# TODO allow changing dtype ?
# rechunk a n5 dataset
# also supports new compression opts
def rechunk(in_path,
            out_path,
            in_path_in_file,
            out_path_in_file,
            out_chunks,
            n_threads,
            **new_compression):
    f_in = File(in_path, use_zarr_format=False)
    f_out = File(out_path, use_zarr_format=False)

    ds_in = f_in[in_path_in_file]
    shape = ds_in.shape
    compression_opts = ds_in.compression_options
    compression_opts.update(new_compression)
    ds_out = f_out.create_dataset(out_path_in_file,
                                  dtype=ds_in.dtype,
                                  shape=shape,
                                  chunks=out_chunks,
                                  **compression_opts)
    chunks_per_dim = ds_out.chunks_per_dimension

    def write_single_chunk(chunk_ids):
        # print("Writing new chunk ", chunk_ids, "/", chunks_per_dim)
        starts = [chunk_id * chunk_shape
                  for chunk_id, chunk_shape in zip(chunk_ids, out_chunks)]
        stops = [min((chunk_id + 1) * chunk_shape, max_dim)
                 for chunk_id, chunk_shape, max_dim in zip(chunk_ids, out_chunks, shape)]
        bb = tuple(slice(start, stop) for start, stop in zip(starts, stops))
        ds_out[bb] = ds_in[bb]#.astype(out_dtype, copy=False)

    chunk_ranges = [range(n_dim) for n_dim in chunks_per_dim]
    all_chunk_ids = list(product(*chunk_ranges))
    # we randomize the ids to minimize overlapping requests in the in dataset
    # this should speed up I/O significantly
    shuffle(all_chunk_ids)
    with futures.ThreadPoolExecutor(max_workers=n_threads) as tp:
        tasks = [tp.submit(write_single_chunk, chunk_ids) for chunk_ids in all_chunk_ids]
        [t.result() for t in tasks]

    # copy attributes
    in_attrs = ds_in.attrs
    out_attrs = ds_out.attrs
    for key, val in in_attrs.items():
        out_attrs[key] = val
