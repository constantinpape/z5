# helper functions to convert to and from different formats
# - hdf5
# TODO implement png, tiff, dvid
from __future__ import print_function
import os
from concurrent import futures

try:
    import h5py
    WITH_H5 = True
except ImportError:
    print("h5py is not installed, hdf5 converters not available")
    WITH_H5 = False

from .file import File
from .util import blocking


if WITH_H5:

    def convert_to_h5(in_path,
                      out_path,
                      in_path_in_file,
                      out_path_in_file,
                      out_chunks,
                      n_threads,
                      out_blocks=None,
                      **h5_kwargs):
        """ Convert n5 ot zarr dataset to hdf5 dataset.

        The chunks of the output dataset must be spcified.
        The dataset is converted to hdf5 in parallel over the chunks.
        Note that hdf5 does not support parallel write access, so more threads
        may not speed up the conversion.
        Datatype and compression can be specified, otherwise defaults will be used.

        Args:
            in_path (str): path to n5 or zarr file.
            out_path (str): path to output hdf5 file.
            in_path_in_file (str): name of input dataset.
            out_path_in_file (str): name of output dataset.
            out_chunks (tuple): chunks of output dataset.
            n_threads (int): number of threads used for converting.
            out_blocks (tuple): block size used for converting, must be multiple of ``out_chunks``.
                If None, the chunk size will be used (default: None).
            **h5_kwargs: keyword arguments for ``h5py`` dataset, e.g. datatype or compression.
        """
        if not os.path.exists(in_path):
            raise RuntimeError("Path %s does not exist" % in_path)
        f_z5 = File(in_path)
        ds_z5 = f_z5[in_path_in_file]
        shape = ds_z5.shape
        # modify h5 arguments
        out_dtype = h5_kwargs.pop('dtype', ds_z5.dtype)
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
                ds_h5[bb] = ds_z5[bb].astype(out_dtype, copy=False)

            with futures.ThreadPoolExecutor(max_workers=n_threads) as tp:
                tasks = [tp.submit(convert_chunk, bb)
                         for bb in blocking(shape, out_blocks)]
                [t.result() for t in tasks]

            # copy attributes
            h5_attrs = ds_h5.attrs
            z5_attrs = ds_z5.attrs
            for key, val in z5_attrs.items():
                h5_attrs[key] = val

    def convert_from_h5(in_path,
                        out_path,
                        in_path_in_file,
                        out_path_in_file,
                        out_chunks,
                        n_threads,
                        out_blocks=None,
                        use_zarr_format=None,
                        **z5_kwargs):
        """ Convert hdf5 dataset to n5 or zarr dataset.

        The chunks of the output dataset must be spcified.
        The dataset is converted in parallel over the chunks.
        Datatype and compression can be specified, otherwise defaults will be used.

        Args:
            in_path (str): path to hdf5 file.
            out_path (str): path to output zarr or n5 file.
            in_path_in_file (str): name of input dataset.
            out_path_in_file (str): name of output dataset.
            out_chunks (tuple): chunks of output dataset.
            n_threads (int): number of threads used for converting.
            out_blocks (tuple): block size used for converting, must be multiple of ``out_chunks``.
                If None, the chunk size will be used (default: None).
            use_zarr_format (bool): flag to indicate zarr format.
                If None, an attempt will be made to infer the format from the file extension,
                otherwise zarr will be used (default: None).
            **z5_kwargs: keyword arguments for ``z5py`` dataset, e.g. datatype or compression.
        """
        if not os.path.exists(in_path):
            raise RuntimeError("Path %s does not exist" % in_path)
        if out_blocks is None:
            out_blocks = out_chunks

        f_z5 = File(out_path, use_zarr_format=use_zarr_format)
        with h5py.File(in_path, 'r') as f_h5:
            ds_h5 = f_h5[in_path_in_file]
            shape = ds_h5.shape

            # modify z5 arguments
            out_dtype = z5_kwargs.pop('dtype', ds_h5.dtype)
            if 'compression' not in z5_kwargs:
                z5_kwargs['compression'] = 'raw'
            ds_z5 = f_z5.create_dataset(out_path_in_file,
                                        dtype=out_dtype,
                                        shape=shape,
                                        chunks=out_chunks,
                                        **z5_kwargs)

            def convert_chunk(bb):
                # print("Converting chunk ", chunk_ids, "/", chunks_per_dim)
                ds_z5[bb] = ds_h5[bb].astype(out_dtype, copy=False)

            with futures.ThreadPoolExecutor(max_workers=n_threads) as tp:
                tasks = [tp.submit(convert_chunk, bb)
                         for bb in blocking(shape, out_blocks)]
                [t.result() for t in tasks]

            # copy attributes
            h5_attrs = ds_h5.attrs
            z5_attrs = ds_z5.attrs
            for key, val in h5_attrs.items():
                z5_attrs[key] = val
