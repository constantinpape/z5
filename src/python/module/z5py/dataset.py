"""The :class:`Dataset` class: chunked, optionally compressed nd array storage.

A dataset wraps a single zarr / n5 array. Data is read and written with numpy-style
indexing (``ds[:]``, ``ds[z0:z1, y0:y1]``) or via the explicit
:meth:`Dataset.read_subarray` / :meth:`Dataset.write_subarray` and chunk-level
methods. Datasets are not created directly; use
:meth:`z5py.Group.create_dataset` / :meth:`z5py.Group.require_dataset` or the
``[]`` operator of a :class:`z5py.File` / :class:`z5py.Group`.
"""
import base64
import numbers
import json
import re

import numpy as np

from . import _z5py
from .attribute_manager import AttributeManager
from .shape_utils import normalize_slices, rectify_shape, get_default_chunks

AVAILABLE_COMPRESSORS = _z5py.get_available_codecs()

# numpy dtype of variable-length string datasets: numpy's StringDType (numpy >= 2,
# what zarr-python returns), else object arrays of str
STRING_DTYPE = (np.dtypes.StringDType() if hasattr(getattr(np, 'dtypes', None), 'StringDType')
                else np.dtype(object))


# variable-length data types, by their name in the C++ layer: the python item
# type, the numpy dtype of read arrays, and the numpy kinds of arrays that can be
# written (zarr-python returns object arrays of bytes for variable_length_bytes)
VLEN_KINDS = {
    'string': (str, STRING_DTYPE, ('U', 'T', 'O')),
    'variable_length_bytes': (bytes, np.dtype(object), ('S', 'O')),
}
# suffix of the _z5py IO functions for each kind
_VLEN_SUFFIX = {'string': 'string', 'variable_length_bytes': 'bytes'}

# data types the C++ layer stores as another numpy type of the same size: the
# IO functions of _z5py take and return arrays of the storage type, and z5py
# hands them views (a bool dataset holds the bytes 0 and 1, as numpy does, and
# a float16 dataset the half-precision bits)
_STORAGE_DTYPE = {np.dtype('bool'): np.dtype('uint8'), np.dtype('float16'): np.dtype('uint16')}


def _vlen_kind(dtype):
    """ The variable-length kind ``dtype`` asks for, or None.

    ``str``, numpy's StringDType and object give 'string'; ``bytes`` (``S0``) gives
    'variable_length_bytes'. Fixed-length ``U<n>`` / ``S<n>`` are not variable-length
    (zarr-python stores those as fixed-length strings).
    """
    if dtype is str:
        return 'string'
    try:
        parsed = np.dtype(dtype)
    except TypeError:
        return None
    if parsed.kind in ('T', 'O') or (parsed.kind == 'U' and parsed.itemsize == 0):
        return 'string'
    if parsed.kind == 'S' and parsed.itemsize == 0:
        return 'variable_length_bytes'
    return None


# the raw data types, by their name in the C++ layer; their numpy dtype is V<n>
RAW_KINDS = ('raw_bits', 'raw_bytes')


def _raw_dtype(dtype):
    """ The C++ name and numpy dtype of the raw data type ``dtype`` asks for, or None.

    A numpy ``V<n>`` dtype (``np.void``, without fields) gives zarr-python's raw bytes
    type (``raw_bytes`` in zarr v3, ``|V<n>`` in zarr v2), the string ``'r<bits>'``
    the zarr v3 core raw bits type (``r8``, ``r16``, ...). Both read as ``V<n>``.
    """
    if isinstance(dtype, str):
        match = re.fullmatch(r'r([0-9]+)', dtype)
        if match:
            bits = int(match.group(1))
            if bits == 0 or bits % 8:
                raise ValueError("Raw bits data type %r must have a positive multiple of 8 bits" % dtype)
            return dtype, np.dtype('V%i' % (bits // 8))
    try:
        parsed = np.dtype(dtype)
    except TypeError:
        return None
    if parsed.kind == 'V' and parsed.fields is None and parsed.subdtype is None and parsed.itemsize > 0:
        return 'V%i' % parsed.itemsize, parsed
    return None


def _navigate(file_obj, name):
    """ Navigate from a (root) file to the object with the given (absolute) name.

    Used for unpickling: re-opens the object by traversing the container one
    path component at a time, which reconstructs the proper parent chain.
    """
    obj = file_obj
    rel = name.strip('/')
    if rel:
        for part in rel.split('/'):
            obj = obj[part]
    return obj


def _unpickle_dataset(file_obj, name, n_threads):
    ds = _navigate(file_obj, name)
    ds.n_threads = n_threads
    return ds


# TODO lz4 compression is currently not compatible with zarr
# COMPRESSORS_ZARR = ('raw', 'blosc', 'zlib', 'bzip2', 'gzip', 'lz4')
COMPRESSORS_ZARR = ('raw', 'blosc', 'zlib', 'bzip2', 'gzip', 'zstd')
COMPRESSORS_N5 = ('raw', 'blosc', 'gzip', 'bzip2', 'xz', 'lz4', 'zstd')
# zarr v3 codecs ('raw' == the v3 'bytes' codec). 'raw', 'gzip', 'blosc' and
# 'zstd' are core codecs; 'zlib', 'bzip2', 'xz' and 'lz4' are written as the
# zarr-python extension codecs numcodecs.zlib / .bz2 / .lzma / .lz4.
COMPRESSORS_ZARR_V3 = ('raw', 'gzip', 'blosc', 'zstd', 'zlib', 'bzip2', 'xz', 'lz4')


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
                   np.dtype('float16'): 'float16',
                   np.dtype('float32'): 'float32',
                   np.dtype('float64'): 'float64',
                   np.dtype('bool'): 'bool',
                   np.dtype('complex64'): 'complex64',
                   np.dtype('complex128'): 'complex128'}

    # Compression libraries supported by zarr format
    compressors_zarr = tuple(comp for comp in COMPRESSORS_ZARR if AVAILABLE_COMPRESSORS[comp])
    # Default compression for zarr format
    zarr_default_compressor = 'blosc' if AVAILABLE_COMPRESSORS['blosc'] else 'raw'

    # Compression libraries supported by n5 format
    compressors_n5 = tuple(comp for comp in COMPRESSORS_N5 if AVAILABLE_COMPRESSORS[comp])
    # Default compression for n5 format
    n5_default_compressor = 'gzip' if AVAILABLE_COMPRESSORS['gzip'] else 'raw'

    # Compression codecs supported by the zarr v3 format
    compressors_zarr_v3 = tuple(comp for comp in COMPRESSORS_ZARR_V3 if AVAILABLE_COMPRESSORS[comp])
    # Default compression for zarr v3 format
    zarr_v3_default_compressor = 'blosc' if AVAILABLE_COMPRESSORS['blosc'] else 'raw'

    def __init__(self, dset_impl, handle, parent, name, n_threads=1):
        self._impl = dset_impl
        self._handle = handle
        self._attrs = AttributeManager(self._handle)
        self.n_threads = n_threads
        self._parent = parent
        self._name = name

    def __reduce__(self):
        # pickle by re-opening from the (picklable) root file; this avoids
        # pickling the underlying C++ objects, which are not constructible
        # outside of the factory functions.
        return (_unpickle_dataset, (self.file, self._name, self.n_threads))

    def __array__(self, dtype=None):
        """ Create a numpy array containing the whole dataset.

        NOTE: Datasets are not interchangeable with arrays!
        Every time this method is called the whole dataset is loaded into memory!
        """
        arr = self[...]
        if dtype is not None:
            arr = arr.astype(dtype, copy=False)
        return arr

    @staticmethod
    def _to_zarr_compression_options(compression, compression_options):
        if compression == 'blosc':
            default_opts = {'codec': 'lz4', 'clevel': 5, 'shuffle': 1, 'blocksize': 0}
        elif compression == 'zlib':
            default_opts = {'id': 'zlib', 'level': 5}
        elif compression == 'gzip':
            default_opts = {'id': 'gzip', 'level': 5}
        elif compression == 'bzip2':
            default_opts = {'level': 5}
        elif compression == 'lz4':
            default_opts = {'level': 6}
        elif compression == 'zstd':
            default_opts = {'level': 3}
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
    def _to_zarr_v3_compression_options(compression, compression_options):
        # the options below populate the runtime CompressionOptions; the C++
        # toJsonV3 turns them into the on-disk v3 codec configuration. NOTE: gzip
        # must set useZlib=False so the zlib compressor emits a gzip wrapper.
        if compression == 'blosc':
            default_opts = {'codec': 'lz4', 'level': 5, 'shuffle': 1, 'blocksize': 0}
        elif compression == 'gzip':
            default_opts = {'level': 5, 'useZlib': False}
        elif compression == 'zstd':
            default_opts = {'level': 3, 'checksum': False}
        elif compression == 'zlib':
            default_opts = {'level': 5, 'useZlib': True}
        elif compression == 'bzip2':
            default_opts = {'level': 5}
        elif compression == 'xz':
            # level is the lzma preset
            default_opts = {'level': 6}
        elif compression == 'lz4':
            # level is the lz4 acceleration (1 == default, higher == faster)
            default_opts = {'level': 1}
        elif compression == 'raw':
            default_opts = {}
        else:
            raise RuntimeError("Compression %s is not supported in zarr v3 format" % compression)

        # check for invalid options
        extra_args = set(compression_options) - set(default_opts)
        if extra_args:
            raise RuntimeError("Invalid options for %s compression: %s" % (compression, ' '.join(list(extra_args))))

        if not default_opts:
            return {}

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
        elif compression == 'blosc':
            default_opts = {'codec': 'lz4', 'clevel': 5, 'shuffle': 1, 'blocksize': 0, 'nthreads': 1}
        elif compression == 'zstd':
            default_opts = {'level': 3}
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
        ghandle = group._handle
        # normalize to tuples: the properties below are tuples, and a list-valued
        # shape / chunks argument must not fail the consistency check
        shape = tuple(shape)
        chunks = None if chunks is None else tuple(chunks)
        if ghandle.has(name):

            if ghandle.is_sub_group(name):
                raise TypeError("Incompatible object (Group) already exists")

            handle = ghandle.get_dataset_handle(name)
            ds = cls(_z5py.open_dataset(ghandle, name), handle, group,
                     group._name + '/' + name, n_threads)
            if shape != ds.shape:
                raise TypeError("Shapes do not match (existing (%s) vs new (%s))" % (', '.join(map(str, ds.shape)),
                                                                                     ', '.join(map(str, shape))))
            if chunks is not None:
                if chunks != ds.chunks:
                    raise TypeError("Chunks do not match (existing (%s) vs new (%s))" % (', '.join(map(str, ds.chunks)),
                                                                                         ', '.join(map(str, chunks))))
            if dtype is not None:
                if ds._vlen or _vlen_kind(dtype):
                    if ds._vlen != _vlen_kind(dtype):
                        raise TypeError("Datatypes do not match (existing %s vs new %s)"
                                        % (str(ds.dtype), str(dtype)))
                elif (_raw_dtype(dtype) or (None, np.dtype(dtype)))[1] != ds.dtype:
                    raise TypeError("Datatypes do not match (existing %s vs new %s)" % (str(ds.dtype), str(dtype)))
            return ds

        else:
            # the write permission is only needed if the dataset does not exist yet;
            # requiring an existing, consistent dataset works in read-only mode
            # (matching h5py)
            if not ghandle.mode().can_write():
                raise ValueError("Cannot create dataset with read-only permissions.")
            # pop all kwargs that are not compression options
            data = kwargs.pop('data', None)
            compression = kwargs.pop('compression', None)
            fillvalue = kwargs.pop('fillvalue', 0)
            dimension_separator = kwargs.pop('dimension_separator', '.')
            zarr_format = kwargs.pop('zarr_format', 2)
            shards = kwargs.pop('shards', None)
            chunk_key_encoding = kwargs.pop('chunk_key_encoding', 'default')
            dimension_names = kwargs.pop('dimension_names', None)
            transpose = kwargs.pop('transpose', None)
            crc32c = kwargs.pop('crc32c', False)
            return cls._create_dataset(group, name, shape, dtype, data=data,
                                       chunks=chunks, compression=compression,
                                       fillvalue=fillvalue, n_threads=n_threads,
                                       compression_options=kwargs, dimension_separator=dimension_separator,
                                       zarr_format=zarr_format, shards=shards,
                                       chunk_key_encoding=chunk_key_encoding,
                                       dimension_names=dimension_names,
                                       transpose=transpose, crc32c=crc32c)

    @staticmethod
    def _fill_value_as(fillvalue, dtype):
        """ ``fillvalue`` as a numpy scalar of ``dtype``.

        Raises:
            ValueError: if ``fillvalue`` can't be represented in ``dtype``.
        """
        try:
            value = np.asarray(fillvalue, dtype=dtype)
        except (ValueError, TypeError, OverflowError) as e:
            raise ValueError("Fill value %r is not valid for data type %s: %s" % (fillvalue, dtype, e))
        if value.ndim != 0:
            raise ValueError("Fill value %r must be a scalar" % (fillvalue,))
        return value[()]

    @classmethod
    def _fill_value_bits(cls, fillvalue, dtype):
        """ The fill value's bits: its bytes as ``dtype``, read as a little-endian
        unsigned integer. Passed to the C++ layer so that 64-bit integers are
        kept exactly (a float argument would round them).

        Raises:
            ValueError: if ``fillvalue`` can't be represented in ``dtype``.
        """
        value = np.asarray(cls._fill_value_as(fillvalue, dtype))
        return int(value.astype(value.dtype.newbyteorder('<')).view('<u%i' % value.itemsize))

    @classmethod
    def _create_dataset(cls, group, name,
                        shape=None, dtype=None,
                        data=None, chunks=None,
                        compression=None,
                        fillvalue=0, n_threads=1,
                        compression_options={},
                        dimension_separator=".",
                        zarr_format=2,
                        shards=None,
                        chunk_key_encoding="default",
                        dimension_names=None,
                        transpose=None,
                        crc32c=False):

        # check shape, dtype and data
        ghandle = group._handle
        have_data = data is not None
        if have_data:
            if shape is None:
                shape = data.shape
            elif tuple(shape) != tuple(data.shape):
                raise ValueError("Shape is incompatible with data")
            if dtype is None:
                dtype = data.dtype
            elif _vlen_kind(dtype):
                # any numpy text (or bytes) array can fill a string (or bytes) dataset
                if data.dtype.kind not in VLEN_KINDS[_vlen_kind(dtype)][2]:
                    raise ValueError("Datatype is incompatible with data")
            # NOTE In contrast to h5py, we don't do automatic type conversion
            elif (_raw_dtype(dtype) or (None, np.dtype(dtype)))[1] != data.dtype:
                raise ValueError("Datatype is incompatible with data")
        else:
            if shape is None:
                raise TypeError("One of shape or data must be specified")
            # NOTE In contrast to h5py (and numpy), we DO NOT have float64
            # as default data type, but require a data type if no data is given
            if dtype is None:
                raise TypeError("One of dtype or data must be specified")
        vlen = _vlen_kind(dtype)
        raw = None if vlen else _raw_dtype(dtype)
        parsed_dtype = VLEN_KINDS[vlen][1] if vlen else (raw[1] if raw else np.dtype(dtype))

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
        is_zarr = ghandle.is_zarr()

        if vlen:
            dtype_name = vlen
            item_type = VLEN_KINDS[vlen][0]
            if not is_zarr:
                raise ValueError("Variable-length datasets are only supported for zarr")
            if transpose is not None:
                raise ValueError("Transpose is not supported for variable-length datasets")
            # the numeric default 0 means the empty string / bytes
            if isinstance(fillvalue, numbers.Number) and fillvalue == 0:
                fillvalue = item_type()
            if not isinstance(fillvalue, item_type):
                raise ValueError("The fill value of a %s dataset must be %s, got %r"
                                 % (vlen, item_type.__name__, fillvalue))
            # bytes travel base64-encoded, the form the metadata stores them in
            fill_kwargs = ({'fill_value_string': fillvalue} if item_type is str else
                           {'fill_value_base64': base64.b64encode(fillvalue).decode('ascii')})
            fillvalue = 0
        elif raw:
            dtype_name = raw[0]
            if not is_zarr:
                raise ValueError("Raw data types are only supported for zarr")
            # the numeric default 0 means all zero bytes
            if isinstance(fillvalue, numbers.Number) and fillvalue == 0:
                fill_kwargs = {}
            else:
                fill = cls._raw_item(fillvalue, parsed_dtype)
                fill_kwargs = {'fill_value_base64': base64.b64encode(fill).decode('ascii')}
            fillvalue = 0
        else:
            if parsed_dtype.kind in ('U', 'S'):
                raise ValueError("Fixed-length data type {} is not supported; use dtype={} for "
                                 "variable-length {}".format(repr(dtype),
                                                             'str' if parsed_dtype.kind == 'U' else 'bytes',
                                                             'strings' if parsed_dtype.kind == 'U' else 'bytes'))
            if parsed_dtype not in cls._dtype_dict:
                raise ValueError("Invalid data type {} for dataset".format(repr(dtype)))
            dtype_name = cls._dtype_dict[parsed_dtype]
            if parsed_dtype.kind == 'c':
                # complex128 doesn't fit the bits; both parts are exact as doubles
                fill_kwargs = {'fill_value_complex': complex(cls._fill_value_as(fillvalue, parsed_dtype))}
                fillvalue = 0
            else:
                fill_kwargs = {'fill_value_bits': cls._fill_value_bits(fillvalue, parsed_dtype)}

        if shards is not None and not (is_zarr and zarr_format == 3):
            raise ValueError("Sharding is only supported for zarr v3 datasets")
        if shards is not None and len(shape) == 0:
            raise ValueError("Sharding is not supported for zero-dimensional datasets")
        if transpose is not None and not (is_zarr and zarr_format == 3):
            raise ValueError("Transpose is only supported for zarr v3 datasets")
        if crc32c and not (is_zarr and zarr_format == 3):
            raise ValueError("crc32c is only supported for zarr v3 datasets")
        if transpose is not None and sorted(transpose) != list(range(len(shape))):
            raise ValueError("Transpose %s must be a permutation of the %i axes"
                             % (str(tuple(transpose)), len(shape)))

        if dimension_names is not None:
            if not (is_zarr and zarr_format == 3):
                raise ValueError("Dimension names are only supported for zarr v3 datasets")
            if len(dimension_names) != len(shape):
                raise ValueError("Dimension names %s must have same length as shape %s" % (str(dimension_names),
                                                                                           str(shape)))

        if is_zarr and zarr_format == 3:
            # zarr v3
            if compression is None:
                compression = cls.zarr_v3_default_compressor
            if compression not in cls.compressors_zarr_v3:
                raise ValueError("Compression filter \"%s\" is unavailable for zarr v3" % compression)
            copts = json.dumps(cls._to_zarr_v3_compression_options(compression, compression_options))
            # the chunk key separator is determined by the chunk key encoding
            separator = '.' if chunk_key_encoding == 'v2' else '/'
            shards_arg = list(shards) if shards is not None else []
            names_arg = list(dimension_names) if dimension_names is not None else []
            transpose_arg = [int(axis) for axis in transpose] if transpose is not None else []
            impl = _z5py.create_dataset(ghandle, name, dtype_name,
                                        shape, chunks, compression, copts, fillvalue,
                                        separator, 3, shards_arg, chunk_key_encoding,
                                        names_arg, transpose_arg, bool(crc32c), **fill_kwargs)
        else:
            # zarr v2 / n5
            # check compression / get default compression if no compression is given
            if compression is None:
                compression = cls.zarr_default_compressor if is_zarr else cls.n5_default_compressor
            else:
                valid_compression = compression in cls.compressors_zarr if is_zarr else\
                    compression in cls.compressors_n5
                if not valid_compression:
                    raise ValueError("Compression filter \"%s\" is unavailable" % compression)

            # update the compression options
            if is_zarr:
                copts = cls._to_zarr_compression_options(compression, compression_options)
            else:
                copts = cls._to_n5_compression_options(compression, compression_options)

            # convert the copts to json parseable string
            copts = json.dumps(copts)
            impl = _z5py.create_dataset(ghandle, name, dtype_name,
                                        shape, chunks, compression, copts, fillvalue,
                                        dimension_separator, **fill_kwargs)

        handle = ghandle.get_dataset_handle(name)
        ds = cls(impl, handle, group, group._name + '/' + name, n_threads)
        if have_data:
            # ``...`` also selects the single element of a zero-dimensional dataset
            ds[...] = data
        return ds

    @classmethod
    def _open_dataset(cls, group, name):
        ghandle = group._handle
        ds = _z5py.open_dataset(ghandle, name)
        handle = ghandle.get_dataset_handle(name)
        return cls(ds, handle, group, group._name + '/' + name)

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
    def transpose(self):
        """ Axis order of the zarr v3 ``transpose`` codec (``numpy.transpose``
        semantics), or None if chunks are stored untransposed.
        """
        order = tuple(self._impl.transpose)
        return order if order else None

    @property
    def fillvalue(self):
        """ Fill value of this dataset: the value of elements in chunks that
        were never written.

        n5 doesn't store a fill value, so it is 0 for an opened n5 dataset.
        """
        if self._vlen:
            return self._impl.fill_value_vlen
        return np.frombuffer(self._impl.fill_value_bytes, dtype=self.dtype)[0]

    @property
    def crc32c(self):
        """ True if the chunks carry a zarr v3 ``crc32c`` checksum.
        """
        return self._impl.crc32c

    @property
    def shards(self):
        """ Shard shape of this dataset (zarr v3 sharding), or None if not sharded.
        """
        shards = tuple(self._impl.shards)
        return shards if shards else None

    @property
    def dimension_names(self):
        """ Names of the dimensions of this dataset (zarr v3), with ``None`` for an
        unnamed dimension, or None if the dataset has no dimension names.
        """
        names = tuple(self._impl.dimension_names)
        return names if names else None

    @property
    def dtype(self):
        """ Datatype of this dataset: ``STRING_DTYPE`` for variable-length strings,
        object (arrays of bytes) for variable-length bytes.
        """
        if self._vlen:
            return VLEN_KINDS[self._vlen][1]
        if self._raw:
            return np.dtype('V%i' % self._impl.element_size)
        return np.dtype(self._impl.dtype)

    @property
    def _vlen(self):
        """ The variable-length kind of this dataset (see VLEN_KINDS), or None. """
        dtype = self._impl.dtype
        return dtype if dtype in VLEN_KINDS else None

    def _storage(self, arr, writing):
        """ ``arr`` as the type the C++ layer takes for this dataset (see _STORAGE_DTYPE).

        An array to write must have the dataset's dtype; it isn't converted.
        """
        storage = _STORAGE_DTYPE.get(self.dtype)
        if storage is None:
            return arr
        if arr.dtype != self.dtype:
            if writing:
                raise TypeError("Data of dtype %s can't be written to a %s dataset" % (arr.dtype, self.dtype))
            return arr
        return arr.view(storage)

    @property
    def _raw(self):
        """ True for a raw dataset (see RAW_KINDS). """
        return self._impl.dtype in RAW_KINDS

    @staticmethod
    def _raw_item(item, dtype):
        """ One raw element: ``bytes`` (or a ``np.void``) of exactly ``dtype.itemsize`` bytes. """
        if isinstance(item, np.void):
            item = bytes(item)
        if not isinstance(item, bytes) or len(item) != dtype.itemsize:
            raise ValueError("A %s element must be %i bytes, got %r" % (dtype, dtype.itemsize, item))
        return item

    def _read_raw(self, start, shape):
        """ Read the region ``start`` + ``shape`` of a raw dataset. """
        if 0 in shape:
            return np.empty(shape, dtype=self.dtype)
        data = _z5py.read_subarray_raw(self._impl, list(start), list(shape), n_threads=self.n_threads)
        return np.frombuffer(data, dtype=self.dtype).reshape(shape).copy()

    def _raw_bytes(self, item, shape):
        """ The bytes of ``item`` (one element, broadcast, or an array of ``shape``) in C order. """
        if isinstance(item, (bytes, np.void)):
            return self._raw_item(item, self.dtype) * int(np.prod(shape))
        arr = np.asarray(item)
        # no conversion: numpy would pad or cut other data to the element size
        if arr.dtype != self.dtype:
            raise TypeError("A %s dataset takes %s data, got %s" % (self.dtype, self.dtype, arr.dtype))
        return np.ascontiguousarray(rectify_shape(arr, tuple(shape))).tobytes()

    def _write_raw(self, start, shape, item):
        """ Write ``item`` to the region ``start`` + ``shape`` of a raw dataset. """
        _z5py.write_subarray_raw(self._impl, self._raw_bytes(item, shape), list(start), list(shape),
                                 n_threads=self.n_threads)

    def _read_vlen(self, start, shape):
        """ Read the region ``start`` + ``shape`` of a variable-length dataset. """
        dtype = VLEN_KINDS[self._vlen][1]
        if 0 in shape:
            return np.empty(shape, dtype=dtype)
        read = getattr(_z5py, 'read_subarray_' + _VLEN_SUFFIX[self._vlen])
        items = read(self._impl, list(start), list(shape), n_threads=self.n_threads)
        return np.array(items, dtype=dtype).reshape(shape)

    def _vlen_items(self, item, shape):
        """ The items of ``item`` (one item, broadcast, or an array of ``shape``) as a flat list. """
        item_type, _, kinds = VLEN_KINDS[self._vlen]
        if isinstance(item, item_type):
            return [item] * int(np.prod(shape))
        arr = np.asarray(item)
        if arr.dtype.kind not in kinds:
            raise TypeError("%s datasets take %s data, got %s" % (self._vlen, item_type.__name__, arr.dtype))
        return rectify_shape(arr, shape).ravel().tolist()

    def _write_vlen(self, start, shape, item):
        """ Write ``item`` to the region ``start`` + ``shape`` of a variable-length dataset. """
        write = getattr(_z5py, 'write_subarray_' + _VLEN_SUFFIX[self._vlen])
        write(self._impl, self._vlen_items(item, shape), list(start), list(shape), n_threads=self.n_threads)

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
        """ Name of the compression library (codec) used by this dataset.
        """
        return self._impl.compressor

    @property
    def compression_opts(self):
        """ Compression library options of this dataset.
        """
        copts = self._impl.compression_options
        # decode to json
        copts = json.loads(copts)
        # 'useZlib' is an internal option: it is derived from the compression
        # name ('zlib' vs 'gzip'), so it must not show up in the user-facing
        # options (which can be passed back to create_dataset)
        copts.pop('useZlib', None)
        return copts

    @property
    def parent(self):
        """ The :class:`z5py.Group` (or :class:`z5py.File`) that contains this dataset.
        """
        return self._parent

    @property
    def name(self):
        """ The absolute path of this dataset within its container (e.g. ``/group/data``).
        """
        return self._name

    @property
    def file(self):
        """ The root :class:`z5py.File` of the container this dataset belongs to.
        """
        # we find the file by going up the parents till we find
        # the root (which is it's own parent)
        parent = self.parent
        while parent is not parent.parent:
            parent = parent.parent
        return parent

    def __len__(self):
        """ The length of the first (outermost) dimension of the dataset.

        Raises:
            TypeError: for a zero-dimensional dataset, as numpy does.
        """
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
        """ Read a region of the dataset into a numpy array.

        Supports numpy-style indexing with integers, slices and a single
        ellipsis (advanced / boolean indexing is not supported). Steps other
        than 1 are not supported. Out-of-bounds positions raise; regions that
        were never written are filled with the dataset's fill-value.

        Integer indices squeeze out the corresponding dimension, mirroring numpy
        (e.g. ``ds[0]`` drops the leading axis); if every dimension is indexed
        by an integer a scalar is returned.

        Args:
            index (int, slice, ellipsis or tuple): the region to read.

        Returns:
            np.ndarray: the requested data (a scalar if fully integer-indexed,
            or for ``ds[()]`` on a zero-dimensional dataset), with this
            dataset's dtype.
        """
        roi_begin, shape, to_squeeze = self.index_to_roi(index)
        if self._vlen:
            out = self._read_vlen(roi_begin, shape)
        elif self._raw:
            out = self._read_raw(roi_begin, shape)
        else:
            out = np.empty(shape, dtype=self.dtype)
            if 0 not in shape:
                _z5py.read_subarray(self._impl,
                                    self._storage(out, False), roi_begin,
                                    n_threads=self.n_threads)

        # todo: this probably has more copies than necessary
        if not shape:
            # zero-dimensional, as numpy: ds[()] is a scalar, ds[...] a 0-d array
            return out if index is Ellipsis or (isinstance(index, tuple) and Ellipsis in index) else out[()]
        if len(to_squeeze) == len(shape):
            return out.flatten()[0]
        elif to_squeeze:
            return out.squeeze(to_squeeze)
        else:
            return out

    # most checks are done in c++
    def __setitem__(self, index, item):
        """ Write data into a region of the dataset.

        Supports numpy-style indexing with integers, slices and a single
        ellipsis (steps other than 1 are not supported). A scalar ``item`` is
        broadcast to fill the selected region. An array ``item`` must match the
        selected region's shape after rectification; leading and trailing
        singleton dimensions may be added or removed, but other broadcasting is
        not supported. The data is converted to the dataset's dtype (a
        ``TypeError`` / ``OSError`` is raised if there is no conversion path).

        Args:
            index (int, slice, ellipsis or tuple): the region to write to.
            item (scalar or np.ndarray): the value(s) to write.
        """
        roi_begin, shape, _ = self.index_to_roi(index)
        if 0 in shape:
            return
        if self._vlen:
            self._write_vlen(roi_begin, shape, item)
            return
        if self._raw:
            self._write_raw(roi_begin, shape, item)
            return

        # broadcast scalar
        if isinstance(item, (numbers.Number, np.number, np.bool_)):
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
                             self._storage(item_arr, True),
                             roi_begin,
                             n_threads=self.n_threads)

    def read_direct(self, dest, source_sel=None, dest_sel=None):
        """ Read from the dataset into ``dest`` (h5py-compatible wrapper around ``read_subarray``).

        The regions selected by ``source_sel`` and ``dest_sel`` must have the
        same size, but need not have the same offset.

        Args:
            dest (np.ndarray): destination array the read data is written into.
            source_sel (tuple[slice]): selection in the dataset to read from.
                Defaults to the whole dataset (default: None).
            dest_sel (tuple[slice]): selection in ``dest`` to write to.
                Defaults to the whole of ``dest`` (default: None).
        """
        if source_sel is None:
            source_sel = tuple(slice(0, sh) for sh in self.shape)
        if dest_sel is None:
            dest_sel = tuple(slice(0, sh) for sh in dest.shape)
        start = [s.start for s in source_sel]
        stop = [s.stop for s in source_sel]
        dest[dest_sel] = self.read_subarray(start, stop)

    def write_direct(self, source, source_sel=None, dest_sel=None):
        """ Write to the dataset from ``source`` (h5py-compatible wrapper around ``write_subarray``).

        The regions selected by ``source_sel`` and ``dest_sel`` must have the
        same size, but need not have the same offset.

        Args:
            source (np.ndarray): source array the written data is taken from.
            source_sel (tuple[slice]): selection in ``source`` to read from.
                Defaults to the whole of ``source`` (default: None).
            dest_sel (tuple[slice]): selection in the dataset to write to.
                Defaults to the whole dataset (default: None).
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
        if self._vlen:
            data = np.asarray(data)
            self._write_vlen(start, data.shape, data)
            return
        if self._raw:
            data = np.asarray(data)
            self._write_raw(start, data.shape, data)
            return
        _z5py.write_subarray(self._impl,
                             self._storage(np.require(data, requirements='C'), True),
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
        if self._vlen:
            return self._read_vlen(start, shape)
        if self._raw:
            return self._read_raw(start, shape)
        out = np.empty(shape, dtype=self.dtype)
        _z5py.read_subarray(self._impl, self._storage(out, False), start, n_threads=self.n_threads)
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
        if self._vlen:
            if varlen:
                raise ValueError("Variable-length datasets have no varlen (n5) chunks")
            write = getattr(_z5py, 'write_chunk_' + _VLEN_SUFFIX[self._vlen])
            write(self._impl, chunk_indices, np.asarray(data, dtype=object).ravel().tolist())
            return
        if self._raw:
            if varlen:
                raise ValueError("Raw datasets have no varlen (n5) chunks")
            _z5py.write_chunk_raw(self._impl, chunk_indices, self._raw_bytes(data, self.chunks))
            return
        if self.dtype in _STORAGE_DTYPE:
            data = self._storage(np.asarray(data), True)
        _z5py.write_chunk(self._impl, chunk_indices, data, varlen)

    def read_chunk(self, chunk_indices):
        """ Read a single chunk.

        Args:
            chunk_indices (tuple): chunk grid indices of the chunk to read.

        Returns:
            np.ndarray or None: the chunk data, or None if the chunk is empty.
        """
        # the existence check happens in C++ (returning None for a missing chunk);
        # checking here as well would double the existence probe per chunk read
        if self._vlen:
            items = getattr(_z5py, 'read_chunk_' + _VLEN_SUFFIX[self._vlen])(self._impl, chunk_indices)
            return None if items is None else np.array(items, dtype=self.dtype).reshape(self.chunks)
        if self._raw:
            data = _z5py.read_chunk_raw(self._impl, chunk_indices)
            return None if data is None else np.frombuffer(data, dtype=self.dtype).reshape(self.chunks).copy()
        storage = _STORAGE_DTYPE.get(self.dtype, self.dtype)
        chunk = getattr(_z5py, 'read_chunk_%s' % storage.name)(self._impl, chunk_indices)
        return None if chunk is None else chunk.view(self.dtype)

    def get_chunk_shape(self, chunk_indices, from_header=False):
        """ Get the shape of chunk.

        This returns the actual chunk shape, which can be different
        from self.chunks for border chunks.

        Args:
            chunk_indices (tuple): chunk grid indices of the chunk.
            from_header (bool): whether to read the chunk shape from the
                chunk header (only applicable for n5 format). (default: False)

        Returns:
            tuple: shape of the chunk.
        """
        return self._impl.getChunkShape(chunk_indices, from_header)
