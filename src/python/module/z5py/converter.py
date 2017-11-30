# helper functions to convert to and from different formats
# - hdf5
# TODO implement png, tiff
from __future__ import print_function
import os
from concurrent import futures
from itertools import product

try:
    import h5py
    with_h5 = True
except ImportError:
    print("h5py is not installed, conversions to h5 are not possible")
    with_h5 = False

from .file import File


if with_h5:

    def convert_n5_to_h5(in_path,
                         out_path,
                         in_path_in_file,
                         out_path_in_file,
                         out_chunks,
                         n_threads,
                         **h5_kwargs):
        assert False, "Not implemented yet"
        assert os.path.exists(in_path), in_path
        f_n5 = File(in_path, use_zarr_format=False)
        ds_n5 = f_n5[in_path_in_file]
        shape = ds_n5.shape
        # modify h5 arguments
        out_dtype = h5_kwargs.pop('dtype', ds_n5.dtype)

        with h5py.File(out_path, 'w') as f_h5:
            print("creating dataset in", out_path, "with name", out_path_in_file)
            ds_h5 = f_h5.create_dataset(out_path_in_file,
                                        dtype=out_dtype,
                                        shape=shape,
                                        chunks=out_chunks,
                                        **h5_kwargs)

            chunks_per_dim = ds_n5.chunks_per_dimension

            def convert_chunk(chunk_ids):
                print("Converting chunk ", chunk_ids, "/", chunks_per_dim)
                starts = [chunk_id * chunk_shape
                          for chunk_id, chunk_shape in zip(chunk_ids, out_chunks)]
                stops = [min((chunk_id + 1) * chunk_shape, max_dim)
                         for chunk_id, chunk_shape, max_dim in zip(chunk_ids, out_chunks, shape)]
                bb = tuple(slice(start, stop) for start, stop in zip(starts, stops))
                ds_h5[bb] = ds_n5[bb].astype(out_dtype, copy=False)

            chunk_ranges = [range(n_dim) for n_dim in chunks_per_dim]
            all_chunk_ids = product(*chunk_ranges)
            with futures.ThreadPoolExecutor(max_workers=n_threads) as tp:
                tasks = [tp.submit(convert_chunk, chunk_ids) for chunk_ids in all_chunk_ids]
                [t.result() for t in tasks]

            # copy attributes
            h5_attrs = ds_h5.attrs
            n5_attrs = ds_n5.attrs
            for key, val in n5_attrs.items():
                h5_attrs[key] = val


    def convert_h5_to_n5(in_path,
                         out_path,
                         in_path_in_file,
                         out_path_in_file,
                         out_chunks,
                         n_threads,
                         **n5_kwargs):
        assert os.path.exists(in_path), in_path

        f_n5 = File(out_path, use_zarr_format=False)
        with h5py.File(in_path, 'r') as f_h5:
            ds_h5 = f_h5[in_path_in_file]
            shape = ds_h5.shape

            # modify n5 arguments
            out_dtype = n5_kwargs.pop('dtype', ds_h5.dtype)
            if not 'compressor' in n5_kwargs:
                n5_kwargs['compressor'] = 'raw'
            ds_n5 = f_n5.create_dataset(out_path_in_file,
                                        dtype=out_dtype,
                                        shape=shape,
                                        chunks=out_chunks,
                                        **n5_kwargs)
            chunks_per_dim = ds_n5.chunks_per_dimension

            def convert_chunk(chunk_ids):
                # print("Converting chunk ", chunk_ids, "/", chunks_per_dim)
                starts = [chunk_id * chunk_shape
                          for chunk_id, chunk_shape in zip(chunk_ids, out_chunks)]
                stops = [min((chunk_id + 1) * chunk_shape, max_dim)
                         for chunk_id, chunk_shape, max_dim in zip(chunk_ids, out_chunks, shape)]
                bb = tuple(slice(start, stop) for start, stop in zip(starts, stops))
                ds_n5[bb] = ds_h5[bb].astype(out_dtype, copy=False)

            chunk_ranges = [range(n_dim) for n_dim in chunks_per_dim]
            all_chunk_ids = product(*chunk_ranges)
            with futures.ThreadPoolExecutor(max_workers=n_threads) as tp:
                tasks = [tp.submit(convert_chunk, chunk_ids) for chunk_ids in all_chunk_ids]
                [t.result() for t in tasks]

            # copy attributes
            h5_attrs = ds_h5.attrs
            n5_attrs = ds_n5.attrs
            for key, val in h5_attrs.items():
                n5_attrs[key] = val
