import numpy as np
from ._z5py import DatasetImpl
from .attribute_manager import AttributeManager


class Dataset(object):

    def __init__(self):
        pass

    @classmethod
    def create_dataset(cls):
        pass

    @classmethod
    def open_dataset(cls):
        pass

    @property
    def shape(self):
        return _impl.shape

    @property
    def ndim(self):
        return _impl.ndim

    @property
    def size(self):
        return _impl.size

    @property
    def chunks(self):
        return _impl.size

    @property
    def dtype(self):
        return np.dtype(_impl.dtype)

    def __len__(self):
        return _impl.len

    @staticmethod
    def index_to_roi(index):
        pass

    def __getitem__(self, index):
        pass

    def __setitem__(self, index, array):
        pass
