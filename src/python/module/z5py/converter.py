# helper functions to convert to and from different formats
# - hdf5
# TODO implement png, tiff, dvid
from __future__ import print_function
import os
from concurrent import futures
from itertools import product

try:
    import h5py
    WITH_H5 = True
except ImportError:
    print("h5py is not installed, conversions to h5 are not possible")
    WITH_H5 = False

from .file import File
from .util import blocking


if WITH_H5:

    def convert_n5_to_h5(in_path,
                         out_path,
                         in_path_in_file,
                         out_path_in_file,
                         out_chunks,
                         n_threads,
                         out_blocks=None,
                         **h5_kwargs):
        assert os.path.exists(in_path), in_path
        f_n5 = File(in_path, use_zarr_format=False)
        ds_n5 = f_n5[in_path_in_file]
        shape = ds_n5.shape
        # modify h5 arguments
        out_dtype = h5_kwargs.pop('dtype', ds_n5.dtype)
        if out_blocks is None:
            out_blocks = out_chunks

        with h5py.File(out_path) as f_h5:
            ds_h5 = f_h5.create_dataset(out_path_in_file,
                                        dtype=out_dtype,
                                        shape=shape,
                                        chunks=out_chunks,
                                        **h5_kwargs)

            def convert_chunk(bb):
                # print("Converting chunk ", chunk_ids, "/", chunks_per_dim)
                ds_h5[bb] = ds_n5[bb].astype(out_dtype, copy=False)

            with futures.ThreadPoolExecutor(max_workers=n_threads) as tp:
                tasks = [tp.submit(convert_chunk, bb)
                         for bb in blocking(shape, out_blocks)]
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
                         out_blocks=None,
                         **n5_kwargs):
        assert os.path.exists(in_path), in_path
        if out_blocks is None:
            out_blocks = out_chunks

        f_n5 = File(out_path, use_zarr_format=False)
        with h5py.File(in_path, 'r') as f_h5:
            ds_h5 = f_h5[in_path_in_file]
            shape = ds_h5.shape

            # modify n5 arguments
            out_dtype = n5_kwargs.pop('dtype', ds_h5.dtype)
            if 'compression' not in n5_kwargs:
                n5_kwargs['compression'] = 'raw'
            ds_n5 = f_n5.create_dataset(out_path_in_file,
                                        dtype=out_dtype,
                                        shape=shape,
                                        chunks=out_chunks,
                                        **n5_kwargs)

            def convert_chunk(bb):
                # print("Converting chunk ", chunk_ids, "/", chunks_per_dim)
                ds_n5[bb] = ds_h5[bb].astype(out_dtype, copy=False)

            with futures.ThreadPoolExecutor(max_workers=n_threads) as tp:
                tasks = [tp.submit(convert_chunk, bb)
                         for bb in blocking(shape, out_blocks)]
                [t.result() for t in tasks]

            # copy attributes
            h5_attrs = ds_h5.attrs
            n5_attrs = ds_n5.attrs
            for key, val in h5_attrs.items():
                n5_attrs[key] = val
