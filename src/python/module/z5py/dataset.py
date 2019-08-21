import os
import numbers
import json

import numpy as np

from . import _z5py
from .attribute_manager import AttributeManager
from .shape_utils import normalize_slices, rectify_shape, get_default_chunks


class Dataset:
    """ Dataset for access to data on disc.

    Should not be instantiated directly, but rather
    be created or opened via ``create_dataset``, ``require_dataset`` or
    the ``[]`` operator of File or Group.
    """

    _dtype_dict = {np.dtype('uint8'): 'uint8',
                   np.dtype('uint16'): 'uint16',
                   np.dtype('uint32'): 'uint32',
                   np.dtype('uint64'): 'uint64',
                   np.dtype('int8'): 'int8',
                   np.dtype('int16'): 'int16',
                   np.dtype('int32'): 'int32',
                   np.dtype('int64'): 'int64',
                   np.dtype('float32'): 'float32',
                   np.dtype('float64'): 'float64'}

    # TODO for now we hardcode all compressors
    # but we should instead check which ones are present
    # (similar to nifty WITH_CPLEX, etc.)

    #: Compression libraries supported by zarr format
    compressors_zarr = ['raw', 'blosc', 'zlib', 'bzip2', 'gzip']
    #: Default compression for zarr format
    zarr_default_compressor = 'blosc'

    #: Compression libraries supported by n5 format
    compressors_n5 = ['raw', 'gzip', 'bzip2', 'xz', 'lz4']
    #: Default compression for n5 format
    n5_default_compressor = 'gzip'

    def __init__(self, dset_impl, handle, n_threads=1):
        self._impl = dset_impl
        self._handle = handle
        self._attrs = AttributeManager(self._handle)
        self.n_threads = n_threads

    @staticmethod
    def _to_zarr_compression_options(compression, compression_options):
        if compression == 'blosc':
            default_opts = {'codec': 'lz4', 'clevel': 5, 'shuffle': 1}
        elif compression == 'zlib':
            default_opts = {'id': 'zlib', 'level': 5}
        elif compression == 'gzip':
            default_opts = {'id': 'gzip', 'level': 5}
        elif compression == 'bzip2':
            default_opts = {'level': 5}
        elif compression == 'raw':
            default_opts = {}
        else:
            raise RuntimeError("Compression %s is not supported in zarr format" % compression)

        # check for invalid options
        extra_args = set(compression_options) - set(default_opts)
        if extra_args:
            raise RuntimeError("Invalid options for %s compression: %s" % (compression, ' '.join(list(extra_args))))

        # return none for raw compression
        if not default_opts:
            return {}

        # update the default options
        default_opts.update(compression_options)
        return default_opts

    @staticmethod
    def _to_n5_compression_options(compression, compression_options):
        if compression == 'gzip':
            default_opts = {'level': 5}
        elif compression == 'bzip2':
            default_opts = {'level': 5}
        elif compression == 'raw':
            default_opts = {}
        elif compression == 'xz':
            default_opts = {'level': 6}
        elif compression == 'lz4':
            default_opts = {'level': 6}
        else:
            raise RuntimeError("Compression %s is not supported in n5 format" % compression)

        # check for invalid options
        extra_args = set(compression_options) - set(default_opts)
        if extra_args:
            raise RuntimeError("Invalid options for %s compression: %s" % (compression, ' '.join(list(extra_args))))

        # update the default options
        default_opts.update(compression_options)
        return default_opts

    # NOTE in contrast to h5py, we also check that the chunks match
    # this is crucial, because different chunks can lead to subsequent incorrect
    # code when relying on chunk-aligned access for parallel writing
    @classmethod
    def _require_dataset(cls, group, name,
                         shape, dtype,
                         chunks, n_threads, **kwargs):
        if group.has(name):

            if group.is_sub_group(name):
                raise TypeError("Incompatible object (Group) already exists")

            handle = group.get_dataset_handle(name)
            ds = cls(_z5py.open_dataset(group, name), handle, n_threads)
            if shape != ds.shape:
                raise TypeError("Shapes do not match (existing (%s) vs new (%s))" % (', '.join(map(str, ds.shape)),
                                                                                     ', '.join(map(str, shape))))
            if chunks is not None:
                if chunks != ds.chunks:
                    raise TypeError("Chunks do not match (existing (%s) vs new (%s))" % (', '.join(map(str, ds.chunks)),
                                                                                         ', '.join(map(str, chunks))))
            if dtype is not None:
                if np.dtype(dtype) != ds.dtype:
                    raise TypeError("Datatypes do not match (existing %s vs new %s)" % str(ds.dtype), str(dtype))
            return ds

        else:
            # pop all kwargs that are not compression options
            data = kwargs.pop('data', None)
            compression = kwargs.pop('compression', None)
            fillvalue = kwargs.pop('fillvalue', 0)
            return cls._create_dataset(group, name, shape, dtype, data=data,
                                       chunks=chunks, compression=compression,
                                       fillvalue=fillvalue, n_threads=n_threads,
                                       compression_options=kwargs)

    @classmethod
    def _create_dataset(cls, group, name,
                        shape=None, dtype=None,
                        data=None, chunks=None,
                        compression=None,
                        fillvalue=0, n_threads=1,
                        compression_options={}):

        # check shape, dtype and data
        have_data = data is not None
        if have_data:
            if shape is None:
                shape = data.shape
            elif shape != data.shape:
                raise ValueError("Shape is incompatible with data")
            if dtype is None:
                dtype = data.dtype
            # NOTE In contrast to h5py, we don't do automatic type conversion
            elif np.dtype(dtype) != data.dtype:
                raise ValueError("Datatype is incompatible with data")
        else:
            if shape is None:
                raise TypeError("One of shape or data must be specified")
            # NOTE In contrast to h5py (and numpy), we DO NOT have float64
            # as default data type, but require a data type if no data is given
            if dtype is None:
                raise TypeError("One of dtype or data must be specified")
        parsed_dtype = np.dtype(dtype)

        # get default chunks if necessary
        # NOTE in contrast to h5py, datasets are chunked
        # by default, with chunk size ~ 64**3
        if chunks is None:
            chunks = get_default_chunks(shape)
        # check chunks have the same len than the shape
        if len(chunks) != len(shape):
            raise RuntimeError("Chunks %s must have same length as shape %s" % (str(chunks),
                                                                                str(shape)))
        # limit chunks to shape
        chunks = tuple(min(ch, sh) for ch, sh in zip(chunks, shape))
        is_zarr = group.is_zarr()

        # check compression / get default compression
        # if no compression is given
        if compression is None:
            compression = cls.zarr_default_compressor if is_zarr else cls.n5_default_compressor
        else:
            valid_compression = compression in cls.compressors_zarr if is_zarr else\
                compression in cls.compressors_n5
            if not valid_compression:
                raise ValueError("Compression filter \"%s\" is unavailable" % compression)

        # get and check compression
        if is_zarr and compression not in cls.compressors_zarr:
            compression = cls.zarr_default_compressor
        elif not is_zarr and compression not in cls.compressors_n5:
            compression = cls.n5_default_compressor

        if parsed_dtype not in cls._dtype_dict:
            raise ValueError("Invalid data type {} for N5 dataset".format(repr(dtype)))

        # update the compression options
        if is_zarr:
            copts = cls._to_zarr_compression_options(compression, compression_options)
        else:
            copts = cls._to_n5_compression_options(compression, compression_options)

        # get the dataset and write data if necessary
        impl = _z5py.create_dataset(group, name, cls._dtype_dict[parsed_dtype],
                                    shape, chunks, compression, copts)
        handle = group.get_dataset_handle(name)
        ds = cls(impl, handle, n_threads)
        if have_data:
            ds[:] = data
        return ds

    @classmethod
    def _open_dataset(cls, group, name):
        ds = _z5py.open_dataset(group, name)
        handle = group.get_dataset_handle(name)
        return cls(ds, handle)

    @property
    def is_zarr(self):
        """ Flag to indicate zarr or n5 format of this dataset.
        """
        return self._impl.is_zarr

    @property
    def attrs(self):
        """ The ``AttributeManager`` of this dataset.
        """
        return self._attrs

    @property
    def shape(self):
        """ Shape of this dataset.
        """
        return tuple(self._impl.shape)

    @property
    def ndim(self):
        """ Number of dimensions of this dataset.
        """
        return self._impl.ndim

    @property
    def size(self):
        """ Size (total number of elements) of this dataset.
        """
        return self._impl.size

    @property
    def chunks(self):
        """ Chunks of this dataset.
        """
        return tuple(self._impl.chunks)

    @property
    def dtype(self):
        """ Datatype of this dataset.
        """
        return np.dtype(self._impl.dtype)

    @property
    def chunks_per_dimension(self):
        """ Number of chunks in each dimension of this dataset.
        """
        return self._impl.chunks_per_dimension

    @property
    def number_of_chunks(self):
        """ Total number of chunks of this dataset.
        """
        return self._impl.number_of_chunks

    @property
    def compression(self):
        opts = self.compression_opts
        compression = opts['compression']
        return compression if compression != 'raw' else None

    @property
    def compression_opts(self):
        """ Compression library options of this dataset.
        """
        return self._impl.compression_options

    def __len__(self):
        return self._impl.len

    def index_to_roi(self, index):
        """ Convert index to region of interest.

        Convert an index, which can be a slice or a tuple of slices / ellipsis to a
        region of interest. The roi consists of the region offset and the region shape.

        Args:
            index (slice or tuple): index into dataset.

        Returns:
            tuple: offset of the region of interest.
            tuple: shape of the region of interest.
            tuple: which dimensions should be squeezed out
        """
        normalized, to_squeeze = normalize_slices(index, self.shape)
        return (
            tuple(norm.start for norm in normalized),
            tuple(
                0 if norm.start is None else norm.stop - norm.start for norm in normalized
            ),
            to_squeeze
        )

    # most checks are done in c++
    def __getitem__(self, index):
        roi_begin, shape, to_squeeze = self.index_to_roi(index)
        out = np.empty(shape, dtype=self.dtype)
        if 0 not in shape:
            _z5py.read_subarray(self._impl,
                                out, roi_begin,
                                n_threads=self.n_threads)

        # todo: this probably has more copies than necessary
        if len(to_squeeze) == len(shape):
            return out.flatten()[0]
        elif to_squeeze:
            return out.squeeze(to_squeeze)
        else:
            return out

    # most checks are done in c++
    def __setitem__(self, index, item):
        roi_begin, shape, _ = self.index_to_roi(index)
        if 0 in shape:
            return

        # broadcast scalar
        if isinstance(item, (numbers.Number, np.number)):
            _z5py.write_scalar(self._impl, roi_begin,
                               list(shape), item,
                               str(self.dtype), self.n_threads)
            return

        try:
            item_arr = np.asarray(item, self.dtype, order='C')
        except ValueError as e:
            if any(s in str(e) for s in ('invalid literal for ', 'could not convert')):
                bad_dtype = np.asarray(item).dtype
                raise TypeError("No conversion path for dtype: " + repr(bad_dtype))
            else:
                raise
        except TypeError as e:
            if 'argument must be' in str(e):
                raise OSError("Can't prepare for writing data (no appropriate function for conversion path)")
            else:
                raise

        item_arr = rectify_shape(item_arr, shape)
        _z5py.write_subarray(self._impl,
                             item_arr,
                             roi_begin,
                             n_threads=self.n_threads)

    def read_direct(self, dest, source_sel=None, dest_sel=None):
        """ Wrapper to improve similarity to h5py. Reads from the dataset to ``dest``, using ``read_subarray``.

        Args:
            dest (array) destination object into which the read data is written to.
            dest_sel (slice array) selection of data to write to ``dest``. Defaults to the whole range of ``dest``.
            source_sel (slice array) selection in dataset to read from. Defaults to the whole range of the dataset.
        Spaces, defined by ``source_sel`` and ``dest_sel`` must be in the same size but dont need to have the same
        offset
        """
        if source_sel is None:
            source_sel = tuple(slice(0, sh) for sh in self.shape)
        if dest_sel is None:
            dest_sel = tuple(slice(0, sh) for sh in dest.shape)
        start = [s.start for s in source_sel]
        stop = [s.stop for s in source_sel]
        dest[dest_sel] = self.read_subarray(start, stop)

    def write_direct(self, source, source_sel=None, dest_sel=None):
        """ Wrapper to improve similarity to h5py. Writes to the dataset from ``source``, using ``write_subarray``.

        Args:
            source (array) source object from which the written data is obtained.
            source_sel (slice array) selection of data to write from ``source``. Defaults to the whole range of
            ``source``.
            dest_sel (slice array) selection in dataset to write to. Defaults to the whole range of the dataset.
        Spaces, defined by ``source_sel`` and ``dest_sel`` must be in the same size but dont need to have the same
        offset
        """
        if dest_sel is None:
            dest_sel = tuple(slice(0, sh) for sh in self.shape)
        if source_sel is None:
            source_sel = tuple(slice(0, sh) for sh in source.shape)
        start = [s.start for s in dest_sel]
        self.write_subarray(start, source[source_sel])

    # expose the impl write subarray functionality
    def write_subarray(self, start, data):
        """ Write subarray to dataset.

        ``data`` is written to region of interest, defined by ``start``
        and the shape of ``data``. The region of interest must be in
        bounds of the dataset and the datatype must agree with the dataset.

        Args:
            start (tuple): offset of the roi to write.
            data (np.ndarray): data to write; shape determines the roi shape.
        """
        _z5py.write_subarray(self._impl,
                             np.require(data, requirements='C'),
                             list(start),
                             n_threads=self.n_threads)

    # expose the impl read subarray functionality
    def read_subarray(self, start, stop):
        """ Read subarray from region of interest.

        Region of interest is defined by ``start`` and ``stop``
        and must be in bounds of the dataset.

        Args:
            start (tuple): start coordinates of the roi.
            stop (tuple): stop coordinates of the roi.

        Returns:
            np.ndarray
        """
        shape = tuple(sto - sta for sta, sto in zip(start, stop))
        out = np.empty(shape, dtype=self.dtype)
        _z5py.read_subarray(self._impl, out, start, n_threads=self.n_threads)
        return out

    def chunk_exists(self, chunk_indices):
        """ Check if chunk has data.

        Check for the given indices if the chunk has data.

        Args:
            chunk_indices (tuple): chunk indices.

        Returns:
            bool
        """
        return self._impl.chunkExists(chunk_indices)

    def write_chunk(self, chunk_indices, data, varlen=False):
        """ Write single chunk

        Args:
            chunk_indices (tuple): indices of the chunk to write to
            data (np.ndarray): data to be written
            varlen (bool): write this chunk in varlen mode; only supported in n5
                (default: False)
        """
        if self.is_zarr and varlen:
            raise RuntimeError("Varlength chunks are not supported in zarr")
        _z5py.write_chunk(self._impl, chunk_indices, data, varlen)

    def read_chunk(self, chunk_indices):
        """ Read single chunk

        Args:
            chunk_indices (tuple): indices of the chunk to write to
        Returns
            np.ndarray
        """
        chunk_reader = getattr(_z5py, 'read_chunk_%s' % self._impl.dtype)
        # FIXME this is super hacky, but for some reason returning none in pybind raises
        # an exception for integer types:
        # "TypeError: int() argument must be a string,
        # a bytes-like object or a number, not 'NoneType"
        # so for now we catch the exception
        # for float types, it doesn't, but returns an array with `nan`
        try:
            out = chunk_reader(self._impl, chunk_indices)
        except TypeError:
            return None
        return None if np.isnan(out).any() else out
