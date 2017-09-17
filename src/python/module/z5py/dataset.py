import numpy as np
from ._z5py import DatasetImpl, open_dataset, create_dataset
from .attribute_manager import AttributeManager


class Dataset(object):

    def __init__(self, path, dset_impl):
        assert isinstance(dset_impl, DatasetImpl)
        self._impl = dset_impl
        self._attrs = AttributeManager(path, self._impl.is_zarr)

    @classmethod
    def create_dataset(
        cls, path, dtype, shape, chunks, is_zarr, fill_value, compressor, codec, level, shuffle
    ):
        return cls.__init(
            path,
            create_dataset(
                path,
                dtype,
                list(shape),
                list(chunks),
                is_zarr,
                fill_value,
                compressor,
                codec,
                level,
                shuffle
            )
        )

    @classmethod
    def open_dataset(cls, path):
        return cls.__init__(
            path,
            open_dataset(path)
        )

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

    def __len__(self):
        return self._impl.len

    # TODO
    @staticmethod
    def index_to_roi(index):
        pass

    def __getitem__(self, index):
        pass

    def __setitem__(self, index, array):
        pass
