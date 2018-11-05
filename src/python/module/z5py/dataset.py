import os
import numbers
import json

import numpy as np

from ._z5py import open_dataset
from ._z5py import write_subarray, write_scalar, read_subarray, convert_array_to_format
from .attribute_manager import AttributeManager, get_json_encoder
from .shape_utils import slice_to_begin_shape, int_to_begin_shape, rectify_shape
from .shape_utils import get_default_chunks, is_group


class Dataset(object):
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

    _zarr_dtype_dict = {np.dtype('uint8'): '|u1',
                        np.dtype('uint16'): '<u2',
                        np.dtype('uint32'): '<u4',
                        np.dtype('uint64'): '<u8',
                        np.dtype('int8'): '|i1',
                        np.dtype('int16'): '<i2',
                        np.dtype('int32'): '<i4',
                        np.dtype('int64'): '<i8',
                        np.dtype('float32'): '<f4',
                        np.dtype('float64'): '<f8'}

    # TODO for now we hardcode all compressors
    # but we should instead check which ones are present
    # (similar to nifty WITH_CPLEX, etc.)

    #: Compression libraries supported by zarr format
    compressors_zarr = ['raw', 'blosc', 'zlib', 'bzip2']
    #: Default compression for zarr format
    zarr_default_compressor = 'blosc'

    # TODO support blosc in n5
    #: Compression libraries supported by n5 format
    compressors_n5 = ['raw', 'gzip', 'bzip2', 'xz', 'lz4']
    #: Default compression for n5 format
    n5_default_compressor = 'gzip'

    def __init__(self, path, dset_impl, n_threads=1):
        self._impl = dset_impl
        self._attrs = AttributeManager(path, self._impl.is_zarr)
        self.path = path
        self.n_threads = n_threads

    @staticmethod
    def _to_zarr_compression_options(compression, compression_options):
        if compression == 'blosc':
            default_opts = {'id': 'blosc', 'cname': 'lz4', 'clevel': 5, 'shuffle': 1}
        elif compression == 'zlib':
            default_opts = {'id': 'zlib', 'level': 5}
        elif compression == 'bzip2':
            default_opts = {'id': 'bz2', 'level': 5}
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
            return None

        # update the default options
        default_opts.update(compression_options)
        return default_opts

    def _read_zarr_compression_options(self):
        opts = {}
        with open(os.path.join(self.path, '.zarray'), 'r') as f:
            zarr_opts = json.load(f)
        if zarr_opts is None:
            opts['compression'] = 'raw'
        elif zarr_opts['id'] == 'blosc':
            opts['compression'] = 'blosc'
            opts['level'] = zarr_opts['clevel']
            opts['shuffle'] = zarr_opts['shuffle']
            opts['codec'] = zarr_opts['cname']
        elif zarr_opts['id'] == 'zlib':
            opts['compression'] = 'zlib'
            opts['level'] = zarr_opts['level']
        elif zarr_opts['id'] == 'bz2':
            opts['compression'] = 'bzip2'
            opts['level'] = zarr_opts['level']
        return opts

    @staticmethod
    def _to_n5_compression_options(compression, compression_options):
        if compression == 'gzip':
            default_opts = {'type': 'gzip', 'level': 5}
            level_key = None  # for several compressors, we need to change the parameter key
        elif compression == 'bzip2':
            default_opts = {'type': 'bzip2', 'level': 5}
            level_key = 'blockSize'
        elif compression == 'raw':
            default_opts = {'type': 'raw'}
            level_key = None
        elif compression == 'xz':
            default_opts = {'type': 'xz', 'level': 6}
            level_key = 'preset'
        elif compression == 'lz4':
            default_opts = {'type': 'lz4', 'level': 6}
            level_key = 'blockSize'
        # TODO blosc in n5
        # elif compression == 'blosc':
        #    default_otps = {'type': 'blosc', 'codec': 'lz4', 'level': 5, 'shuffle': 1}
        #    level_key = None
        else:
            raise RuntimeError("Compression %s is not supported in n5 format" % compression)

        # check for invalid options
        extra_args = set(compression_options) - set(default_opts)
        if extra_args:
            raise RuntimeError("Invalid options for %s compression: %s" % (compression, ' '.join(list(extra_args))))

        # update the default options
        default_opts.update(compression_options)

        # rectify the level option for compressions which have a key different than level
        if level_key is not None:
            default_opts[level_key] = default_opts.pop('level')

        return default_opts

    def _read_n5_compression_options(self):
        opts = {}
        with open(os.path.join(self.path, 'attributes.json'), 'r') as f:
            n5_opts = json.load(f)
        # old compression scheme
        if 'compressionType' in n5_opts:
            ctype = n5_opts['compressionType']
            new_compression = False
        # new compression scheme
        else:
            ctype = n5_opts['compression']['type']
            new_compression = True

        if ctype == 'raw':
            opts['compression'] = 'raw'
        elif ctype == 'gzip':
            opts['compression'] = 'gzip'
            opts['level'] = n5_opts['compression']['level'] if new_compression else 5
        elif ctype == 'bzip2':
            opts['compression'] = 'bzip2'
            opts['level'] = n5_opts['compression']['blockSize'] if new_compression else 5
        elif ctype == 'xz':
            opts['compression'] = 'xz'
            opts['level'] = n5_opts['compression']['preset'] if new_compression else 6
        elif ctype == 'lz4':
            opts['compression'] = 'lz4'
            opts['level'] = n5_opts['compression']['blockSize'] if new_compression else 6
        # TODO blosc in n5
        # elif n5_opts['id'] == 'blosc':
        #     opts['compression'] = 'blosc'
        #     opts['level'] = n5_opts['clevel']
        #     opts['shuffle'] = n5_opts['shuffle']
        #     opts['codec'] = n5_opts['cname']
        return opts

    @staticmethod
    def _create_dataset_zarr(path, dtype, shape, chunks,
                             compression, compression_options,
                             fill_value):
        os.makedirs(path)
        params = {'dtype': Dataset._zarr_dtype_dict[np.dtype(dtype)],
                  'shape': shape,
                  'chunks': chunks,
                  'fill_value': fill_value,
                  'zarr_format': 2,
                  'order': 'C',
                  'filters': None,
                  'compressor': Dataset._to_zarr_compression_options(compression,
                                                                     compression_options)}
        with open(os.path.join(path, '.zarray'), 'w') as f:
            json.dump(params, f, cls=get_json_encoder())

    @staticmethod
    def _create_dataset_n5(path, dtype, shape, chunks,
                           compression, compression_options):
        os.makedirs(path)
        params = {'dataType': Dataset._dtype_dict[np.dtype(dtype)],
                  'dimensions': shape[::-1],
                  'blockSize': chunks[::-1],
                  'compression': Dataset._to_n5_compression_options(compression,
                                                                    compression_options)}
        with open(os.path.join(path, 'attributes.json'), 'w') as f:
            json.dump(params, f, cls=get_json_encoder())

    # NOTE in contrast to h5py, we also check that the chunks match
    # this is crucial, because different chunks can lead to subsequent incorrect
    # code when relying on chunk-aligned access for parallel writing
    @classmethod
    def _require_dataset(cls, path,
                         shape, dtype,
                         chunks, n_threads,
                         is_zarr, mode, **kwargs):
        if os.path.exists(path):

            if is_group(path, is_zarr):
                raise TypeError("Incompatible object (Group) already exists")

            ds = cls(path, open_dataset(path, mode), n_threads)
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
            return cls._create_dataset(path, shape, dtype,
                                       chunks=chunks,
                                       n_threads=n_threads,
                                       is_zarr=is_zarr,
                                       mode=mode, **kwargs)

    @classmethod
    def _create_dataset(cls, path, shape, dtype,
                        data=None, chunks=None,
                        compression=None,
                        fillvalue=0, n_threads=1,
                        compression_options={},
                        is_zarr=True,
                        mode=None):
        # check if this dataset already exists
        if os.path.exists(path):
            raise RuntimeError("Cannot create dataset (name already exists)")

        # check shape, dtype and data
        have_data = data is not None
        if have_data:
            if shape is None:
                shape = data.shape
            elif shape != data.shape:
                raise ValueError("Shape Tuple is incompatible with data")
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
            raise RuntimeError("Chunks %s must have same legnth than shape %s" % (str(chunks),
                                                                                  str(shape)))
        # check chunks are not bigger than shape
        if any(ch > sh for ch, sh in zip(chunks, shape)):
            raise RuntimeError("Chunks %s must not be bigger smaller than shape %s" % (str(chunks),
                                                                                       str(shape)))

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

        if is_zarr:
            if parsed_dtype not in cls._zarr_dtype_dict:
                raise ValueError("Invalid data type {} for zarr dataset".format(dtype))
            cls._create_dataset_zarr(path, parsed_dtype, shape, chunks,
                                     compression, compression_options, fillvalue)
        else:
            if parsed_dtype not in cls._dtype_dict:
                raise ValueError("Invalid data type {} for N5 dataset".format(repr(dtype)))
            cls._create_dataset_n5(path, parsed_dtype, shape, chunks,
                                   compression, compression_options)

        # get the dataset and write data if necessary
        ds = cls(path, open_dataset(path, mode), n_threads)
        if have_data:
            ds[:] = data
        return ds

    @classmethod
    def _open_dataset(cls, path, mode):
        return cls(path, open_dataset(path, mode))

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
    def compression_options(self):
        """ Compression library options of this dataset.
        """
        return self._read_zarr_compression_options() if self._impl.is_zarr else \
            self._read_n5_compression_options()

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
        """
        type_msg = 'Advanced selection inappropriate. ' \
                   'Only numbers, slices (`:`), and ellipsis (`...`) are valid indices (or tuples thereof)'

        if isinstance(index, tuple):
            index_lst = list(index)
        elif isinstance(index, (numbers.Number, slice, type(Ellipsis))):
            index_lst = [index]
        else:
            raise TypeError(type_msg)

        if len([item for item in index_lst if item != Ellipsis]) > self.ndim:
            raise TypeError("Argument sequence too long")
        elif len(index_lst) < self.ndim and Ellipsis not in index_lst:
            index_lst.append(Ellipsis)

        start_shapes = []
        found_ellipsis = False
        for item in index_lst:
            d = len(start_shapes)
            if isinstance(item, slice):
                start_shapes.append(slice_to_begin_shape(item, self.shape[d]))
            elif isinstance(item, numbers.Number):
                start_shapes.append(int_to_begin_shape(int(item), self.shape[d]))
            elif isinstance(item, type(Ellipsis)):
                if found_ellipsis:
                    raise ValueError("Only one ellipsis may be used")
                found_ellipsis = True
                while len(start_shapes) + (len(index_lst) - d - 1) < self.ndim:
                    start_shapes.append((0, self.shape[len(start_shapes)]))
            else:
                raise TypeError(type_msg)

        roi_begin, roi_shape = zip(*start_shapes)
        return roi_begin, roi_shape

    # most checks are done in c++
    def __getitem__(self, index):
        # todo: support newaxis, integer/boolean arrays, striding
        roi_begin, shape = self.index_to_roi(index)
        out = np.empty(shape, dtype=self.dtype)
        if 0 in shape:
            return out
        read_subarray(self._impl,
                      out, roi_begin,
                      n_threads=self.n_threads)
        try:
            return out.item()
        except ValueError:
            return out

    # most checks are done in c++
    def __setitem__(self, index, item):
        roi_begin, shape = self.index_to_roi(index)
        if 0 in shape:
            return

        # broadcast scalar
        if isinstance(item, (numbers.Number, np.number)):
            write_scalar(self._impl, roi_begin,
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
        write_subarray(self._impl,
                       item_arr,
                       roi_begin,
                       n_threads=self.n_threads)

    def find_minimum_coordinates(self, dim):
        """ Find coordinates of chunk with smallest coordinate along dimension.

        Only considers chunks that contain data.

        Args:
            dim (int): query dimension.
        Returns:
            tuple: start coordinates of the chunk.
        """
        return tuple(self._impl.findMinimumCoordinates(dim))

    def find_maximum_coordinates(self, dim):
        """ Find coordinates of chunk with largest coordinate along dimension.

        Only considers chunks that contain data.

        Args:
            dim (int): query dimension.
        Returns:
            tuple: start coordinates of the chunk.
        """
        return self._impl.findMaximumCoordinates(dim)

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
        write_subarray(self._impl,
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
        read_subarray(self._impl, out, start, n_threads=self.n_threads)
        return out

    def array_to_format(self, array):
        """ Convert array to serialization.

        Convert an array to the (1d) binary data that would be serialized to disc
        for the format of the dataset.

        Args:
            array (np.ndarray): array to be converted to serialization.

        Returns:
            np.ndarray
        """
        if array.ndim != self.ndim:
            raise RuntimeError("Array needs to be of same dimension as dataset")
        if np.dtype(array.dtype) != np.dtype(self.dtype):
            raise RuntimeError("Array needs to have same dtype as dataset")
        return convert_array_to_format(self._impl, np.require(array, requirements='C'))

    def chunk_exists(self, chunk_indices):
        """ Check if chunk has data.

        Check for the given indices if the chunk has data.

        Args:
            chunk_indices (tuple): chunk indices.

        Returns:
            bool
        """
        return self._impl.chunkExists(chunk_indices)
