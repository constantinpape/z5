import numpy as np
import numbers
from ._z5py import DatasetImpl, open_dataset, create_dataset
from ._z5py import write_subarray, write_scalar, read_subarray
from .attribute_manager import AttributeManager


class Dataset(object):

    dtype_dict = {np.dtype('uint8'): 'uint8',
                  np.dtype('uint16'): 'uint16',
                  np.dtype('uint32'): 'uint32',
                  np.dtype('uint64'): 'uint64',
                  np.dtype('int8'): 'int8',
                  np.dtype('int16'): 'int16',
                  np.dtype('int32'): 'int32',
                  np.dtype('int64'): 'int64',
                  np.dtype('float32'): 'float32',
                  np.dtype('float64'): 'float64'}

    # FIXME for now we hardcode all compressors
    # but we should instead check which ones are present
    # (similar to nifty WITH_CPLEX, etc.)
    compressors_zarr = ['raw', 'blosc', 'zlib', 'bzip2']
    compressors_n5 = ['raw', 'gzip', 'bzip2']
    zarr_default_compressor = 'blosc'
    n5_default_compressor = 'gzip'

    def __init__(self, path, dset_impl):
        assert isinstance(dset_impl, DatasetImpl)
        self._impl = dset_impl
        self._attrs = AttributeManager(path, self._impl.is_zarr)

    @classmethod
    def create_dataset(cls, path, dtype,
                       shape, chunks, is_zarr,
                       fill_value, compressor,
                       codec, level, shuffle):
        if is_zarr and compressor not in cls.compressors_zarr:
            compressor = cls.zarr_default_compressor
        elif not is_zarr and compressor not in cls.compressors_n5:
            compressor = cls.n5_default_compressor

        # support for numpy datatypes
        if not isinstance(dtype, str):
            assert dtype in cls.dtype_dict, "z5py.Dataset: invalid data type"
            dtype_ = cls.dtype_dict[dtype]
        else:
            dtype_ = dtype

        return cls(path, create_dataset(path, dtype_,
                                        shape, chunks,
                                        is_zarr, fill_value,
                                        compressor, codec,
                                        level, shuffle))

    @classmethod
    def open_dataset(cls, path):
        return cls(path, open_dataset(path))

    @property
    def is_zarr(self):
        return self._impl.is_zarr

    @property
    def attrs(self):
        return self._attrs

    @property
    def shape(self):
        return tuple(self._impl.shape)

    @property
    def ndim(self):
        return self._impl.ndim

    @property
    def size(self):
        return self._impl.size

    @property
    def chunks(self):
        return tuple(self._impl.chunks)

    @property
    def dtype(self):
        return np.dtype(self._impl.dtype)

    @property
    def chunks_per_dimension(self):
        return self._impl.chunks_per_dimension

    @property
    def number_of_chunks(self):
        return self._impl.number_of_chunks

    def __len__(self):
        return self._impl.len

    # TODO support ellipsis
    def index_to_roi(self, index):

        # check index types of index and normalize the index
        assert isinstance(index, (slice, tuple)), \
            "z5py.Dataset: index must be slice or tuple of slices"
        index_ = (index,) if isinstance(index, slice) else index

        # check lengths of index
        assert len(index_) <= self.ndim, "z5py.Dataset: index is longer than dimension"

        # check the individual slices
        assert all(isinstance(ii, slice) for ii in index_), \
                "z5py.Dataset: index must be slice or tuple of slices"
        assert all(ii.step is None for ii in index_), \
                "z5py.Dataset: slice with non-trivial step is not supported"
        # get the roi begin and shape from the slicing
        roi_begin = [
            (0 if index_[d].start is None else index_[d].start)
            if d < len(index_) else 0 for d in range(self.ndim)
        ]
        roi_shape = tuple(
            ((self.shape[d] if index_[d].stop is None else index_[d].stop)
             if d < len(index_) else self.shape[d]) - roi_begin[d] for d in range(self.ndim)
        )
        return roi_begin, roi_shape

    # most checks are done in c++
    def __getitem__(self, index):
        roi_begin, shape = self.index_to_roi(index)
        out = np.empty(shape, dtype=self.dtype)
        read_subarray(self._impl, out, roi_begin)
        return out

    # most checks are done in c++
    def __setitem__(self, index, item):
        assert isinstance(item, (numbers.Number, np.ndarray))
        roi_begin, shape = self.index_to_roi(index)

        # n5 input must be transpsed due to different axis convention
        # write the complete array
        if isinstance(item, np.ndarray):
            assert item.ndim == self.ndim, \
                "z5py.Dataset: complicated broadcasting is not supported"
            write_subarray(self._impl, item, roi_begin)

        # broadcast scalar
        else:
            write_scalar(self._impl, roi_begin, list(shape), item)

    def find_minimum_coordinates(self, dim):
        return self._impl.findMinimumCoordinates(dim)

    def find_maximum_coordinates(self, dim):
        return self._impl.findMaximumCoordinates(dim)
