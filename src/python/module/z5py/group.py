import os
from .base import Base
from ._z5py import createGroup
from .dataset import Dataset


class Group(Base):

    def __init__(self, path, is_zarr=True):
        super(Base, self).__init__(path, is_zarr)

    @classmethod
    def make_group(cls, path, is_zarr):
        createGroup(path, is_zarr)
        return cls.__init(path, is_zarr)

    @classmethod
    def open_group(cls, path, is_zarr):
        return cls.__init__(path, is_zarr)

    def create_group(self, key):
        assert key not in self.keys(), "Group is already existing"
        path = os.path.join(self.path, key)
        return Group.make_group(path, self.is_zarr)

    def __getitem__(self, key):
        path = os.path.join(self.path, key)
        assert os.path.exists(path), "Key is not existing"
        if self.is_group(path):
            return Group.open_group(path, self.is_zarr)
        else:
            return Dataset.open_dataset(path)
