# helper functions to convert to and from different formats
# - hdf5, tiff
from __future__ import print_function
import os
from math import ceil
from concurrent import futures
import numpy as np

try:
    import h5py
    WITH_H5 = True
except ImportError:
    print("h5py is not installed, hdf5 converters not available")
    WITH_H5 = False

try:
    import imageio
    WITH_IMIO = True
except ImportError:
    print("imageio is not installed, tif converters not available")
    WITH_IMIO = False

from .file import File
from .util import blocking
from .shape_utils import normalize_slices

if WITH_H5:

    def convert_to_h5(in_path, out_path,
                      in_path_in_file, out_path_in_file,
                      n_threads, chunks=None,
                      block_shape=None, roi=None,
                      fit_to_roi=False, **h5_kwargs):
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
            n_threads (int): number of threads used for converting.
            chunks (tuple): chunks of output dataset.
                By default input datase's chunks are used. (default: None)
            block_shape (tuple): block shape used for converting, must be multiple of ``chunks``.
                If None, the chunk size will be used (default: None).
            roi (tuple[slice]): region of interest that will be copied. (default: None)
            fit_to_roi (bool): if given a roi, whether to set the shape of
                the output dataset to the roi's shape
                and align chunks with the roi's origin. (default: False)
            **h5_kwargs: keyword arguments for ``h5py`` dataset, e.g. datatype or compression.
        """
        if not os.path.exists(in_path):
            raise RuntimeError("Path %s does not exist" % in_path)
        f_z5 = File(in_path)
        ds_z5 = f_z5[in_path_in_file]
        shape = ds_z5.shape
        chunks = ds_z5.chunks if chunks is None else chunks
        # modify h5 arguments
        out_dtype = h5_kwargs.pop('dtype', ds_z5.dtype)
        if block_shape is None:
            block_shape = chunks
        else:
            assert all(bs % ch == 0 for bs, ch in zip(block_shape, chunks)),\
                "block_shape must be a multiple of chunks"

        # we need to create the blocking here, before the shape is potentially altered
        # if fit_to_roi == True
        blocks = blocking(shape, block_shape, roi, fit_to_roi)
        if roi is not None:
            roi = normalize_slices(roi, shape)
            if fit_to_roi:
                shape = tuple(rr.stop - rr.start for rr in roi)

        with h5py.File(out_path) as f_h5:
            ds_h5 = f_h5.create_dataset(out_path_in_file,
                                        dtype=out_dtype,
                                        shape=shape,
                                        chunks=chunks,
                                        **h5_kwargs)

            def convert_chunk(bb):
                # print("Converting chunk ", chunk_ids, "/", chunks_per_dim)
                # don't copy empty data
                chunk_data = ds_z5[bb].astype(out_dtype, copy=False)
                if chunk_data.sum() == 0:
                    return
                # if we have a roi and fit to it, we need to substract its start from
                # the current bounding box
                if fit_to_roi and roi is not None:
                    bb = tuple(slice(b.start - rr.start, b.stop - rr.start)
                               for b, rr in zip(bb, roi))
                ds_h5[bb] = chunk_data

            with futures.ThreadPoolExecutor(max_workers=n_threads) as tp:
                tasks = [tp.submit(convert_chunk, bb) for bb in blocks]
                [t.result() for t in tasks]

            # copy attributes
            h5_attrs = ds_h5.attrs
            z5_attrs = ds_z5.attrs
            for key, val in z5_attrs.items():
                h5_attrs[key] = val


    def convert_from_h5(in_path, out_path,
                        in_path_in_file, out_path_in_file,
                        n_threads, chunks=None,
                        block_shape=None, use_zarr_format=None,
                        roi=None, fit_to_roi=False,
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
            n_threads (int): number of threads used for converting.
            chunks (tuple): chunks of output dataset.
             By default input dataset's chunks are used. (default: None)
            block_shape (tuple): block shape used for converting, must be multiple of ``chunks``.
                If None, the chunk shape will be used (default: None).
            use_zarr_format (bool): flag to indicate zarr format.
                If None, an attempt will be made to infer the format from the file extension,
                otherwise zarr will be used (default: None).
            roi (tuple[slice]): region of interest that will be copied. (default: None)
            fit_to_roi (bool): if given a roi, whether to set the shape of
                the output dataset to the roi's shape
                and align chunks with the roi's origin. (default: False)
            **z5_kwargs: keyword arguments for ``z5py`` dataset, e.g. datatype or compression.
        """
        if not os.path.exists(in_path):
            raise RuntimeError("Path %s does not exist" % in_path)

        f_z5 = File(out_path, use_zarr_format=use_zarr_format)
        with h5py.File(in_path, 'r') as f_h5:
            ds_h5 = f_h5[in_path_in_file]
            shape = ds_h5.shape
            chunks = ds_h5.chunks if chunks is None else chunks

            if block_shape is None:
                block_shape = chunks
            else:
                assert all(bs % ch == 0 for bs, ch in zip(block_shape, chunks)),\
                    "block_shape must be a multiple of chunks"

            # we need to create the blocking here, before the shape is potentially altered
            # if fit_to_roi == True
            blocks = blocking(shape, block_shape, roi, fit_to_roi)
            if roi is not None:
                roi, to_squeeze = normalize_slices(roi, shape)
                if fit_to_roi:
                    shape = tuple(rr.stop - rr.start for rr in roi)

            # modify z5 arguments
            out_dtype = z5_kwargs.pop('dtype', ds_h5.dtype)
            if 'compression' not in z5_kwargs:
                z5_kwargs['compression'] = 'raw'
            ds_z5 = f_z5.create_dataset(out_path_in_file,
                                        dtype=out_dtype,
                                        shape=shape,
                                        chunks=chunks,
                                        **z5_kwargs)

            def convert_chunk(bb):
                chunk_data = ds_h5[bb].astype(out_dtype, copy=False)
                # if we have a roi and fit to it, we need to substract its start from
                # the current bounding box
                if fit_to_roi and roi is not None:
                    bb = tuple(slice(b.start - rr.start, b.stop - rr.start)
                               for b, rr in zip(bb, roi))
                ds_z5[bb] = chunk_data

            with futures.ThreadPoolExecutor(max_workers=n_threads) as tp:
                tasks = [tp.submit(convert_chunk, bb) for bb in blocks]
                [t.result() for t in tasks]

            # copy attributes
            h5_attrs = ds_h5.attrs
            z5_attrs = ds_z5.attrs
            for key, val in h5_attrs.items():
                # h5 attributes might come as ndarrays, which are not
                # json deserializable by default.
                # in this case, cast to list
                if isinstance(val, np.ndarray):
                    val = val.tolist()
                z5_attrs[key] = val


if WITH_IMIO:

    def convert_to_tif():
        raise NotImplementedError("Conversion to tif not implemented.")


    def is_int(string):
        try:
            int(string)
            return True
        except ValueError:
            return False


    # TODO use proper regex
    def default_index_parser(fname):
        # get rid of the postfix
        parsed = os.path.splitext(fname)[0]
        # try to convert to interger directly
        if is_int(parsed):
            return int(parsed)
        # try to split at '_'
        parsed = parsed.split('_')
        if len(parsed) == 1:
            # otherwise try to split at '-'
            parsed = parsed[0].split('-')
            if len(parsed) == 1 and not is_int(parsed[0]):
                raise ValueError("Cannot parse file with default parser")
        # convert the parsed strings to int
        parsed = [int(par) for par in parsed if is_int(par)]
        if len(parsed) == 0:
            raise ValueError("Cannot parse file with default parser")
        else:
            # we assume that the image-index is always the last number parsed
            return parsed[-1]


    def _read_tif_metadata(in_path, file_names=None):
        if file_names is None:
            pass  # TODO
        else:
            # TODO can we somehow read shape from the metadata ???
            # with imageio.get_reader(os.path.join(in_path, file_names[0])) as f:
            #     meta = f.get_meta_data()
            #     print(meta)
            d = imageio.imread(os.path.join(in_path, file_names[0]))
            # TODO check shape for all images once we can read from metadata
            shape, dtype = d.shape, d.dtype
            shape = (len(file_names),) + shape
        return shape, dtype


    # TODO
    # - implement for tif volumes / stacks / multi-page tifs
    # - fix behaviour for multi-channel images: now we would get (z, c, x, y)
    #   but we want (c, z, y x)
    def convert_from_tif(in_path, out_path,
                         out_path_in_file, chunks,
                         n_threads, use_zarr_format=None,
                         parser=None, preprocess=None,
                         **z5_kwargs):
        """ Convert tif stack or folder of tifs to n5 or zarr dataset.

        The chunks of the output dataset must be specified.
        The dataset is converted in parallel over the chunks.
        Datatype and compression can be specified, otherwise defaults will be used.

        Args:
            in_path (str): path to tif stack or folder of tifs.
            out_path (str): path to output zarr or n5 file.
            out_path_in_file (str): name of output dataset.
            chunks (tuple): chunks of output dataset.
            n_threads (int): number of threads used for converting.
            use_zarr_format (bool): flag to indicate zarr format.
                If None, an attempt will be made to infer the format from the file extension,
                otherwise zarr will be used (default: None).
            parser (callable): function to parse the image indices for tifs in a folder.
                If None, some default patterns are tried (default: None)
            process (callable): function to preprocess chunks before wrting to n5/zarr
                Must take np.ndarray and int as arguments. (default: None)
            **z5_kwargs: keyword arguments for ``z5py`` dataset, e.g. datatype or compression.
        """
        parser_ = default_index_parser if parser is None else parser
        if preprocess is not None:
            assert callable(preprocess)

        if os.path.isdir(in_path):
            file_names = [pp for pp in os.listdir(in_path)
                          if pp.split('.')[-1].lower() in ('tif', 'tiff')]
            indices = [parser_(pp) for pp in file_names]
            assert len(indices) == len(file_names)
            # TODO should we check that indices are consecutive and unique ?
            # order file names according to indices
            file_names = [fname for _, fname in sorted(zip(indices, file_names), key=lambda pair: pair[0])]
        elif os.path.isfile(in_path):
            file_names = None
            raise NotImplementedError("Single tiff conversion not implemented")
        else:
            raise RuntimeError("Path %s does not exist" % in_path)

        # get shape and dtype from metadata
        shape, dtype_ = _read_tif_metadata(in_path, file_names)
        print(shape, dtype_)

        # create the z5 file
        f_z5 = File(out_path, use_zarr_format=use_zarr_format)
        dtype = z5_kwargs.pop('dtype', dtype_)

        # create the dataset
        ds_z5 = f_z5.create_dataset(out_path_in_file, dtype=dtype,
                                    shape=shape, chunks=chunks, **z5_kwargs)

        # TODO implement for tif volume
        # write all chunks overlapping with z images
        def convert_block(i0, i1):
            cshape = (i1 - i0,) + shape[1:]
            slice_ = (slice(i0, i1),) + len(shape[1:]) * (slice(None),)
            chunk = np.zeros(cshape, dtype=dtype)
            for z, i in enumerate(range(i0, i1)):
                chunk[z] = imageio.imread(os.path.join(in_path, file_names[i]))\
                    if preprocess is None else preprocess(imageio.imread(os.path.join(in_path, file_names[i])), i)

            ds_z5[slice_] = chunk.astype(dtype, copy=False)

        n_blocks = int(ceil(shape[0] / chunks[0]))
        starts = [chunks[0] * i for i in range(n_blocks)]
        stops = [min(chunks[0] * (i + 1), shape[0]) for i in range(n_blocks)]

        with futures.ThreadPoolExecutor(max_workers=n_threads) as tp:
            tasks = [tp.submit(convert_block, i0, i1)
                     for i0, i1 in zip(starts, stops)]
            [t.result() for t in tasks]
