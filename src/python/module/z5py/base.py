import os
import errno
import json
from .dataset import Dataset
from .attribute_manager import AttributeManager
from ._z5py import FileMode


class Base(object):
    # the python / h5py file modes and the corresponding internal types
    # TODO as far as I can tell there is no difference between 'w-' and 'x',
    # so for now they get mapped to the same internal type
    file_modes = {'a': FileMode.a, 'r': FileMode.r,
                  'r+': FileMode.r_p, 'w': FileMode.w,
                  'w-': FileMode.w_m, 'x': FileMode.w_m}

    def __init__(self, path, is_zarr=True, mode='a'):
        assert mode in self.file_modes, "Invalid mode %s" % mode
        # x is not a mode in c++, because it has the same properties
        # as w- (as far as I can see)
        self.mode = mode
        self._internal_mode = self.file_modes[mode]
        self._permissions = FileMode(self._internal_mode)
        self.path = path
        self.is_zarr = is_zarr
        self._attrs = AttributeManager(path, is_zarr)

    @property
    def attrs(self):
        return self._attrs

    # FIXME this is not what we wan't, because
    # a) we want to have the proper python key syntax
    # b) this will not list nested paths in file properly,
    # like 'volumes/raw'
    def keys(self):
        return os.listdir(self.path)

    def __contains__(self, key):
        return os.path.exists(os.path.join(self.path, key))

    # TODO open_dataset, open_group and close_group should also be implemented here

    # TODO allow creating with data ?!
    def create_dataset(self, key, dtype, shape, chunks,
                       fill_value=0, compression='raw',
                       **compression_options):
        if not self._permissions.can_write():
            raise OSError(errno.EROFS, os.strerror(errno.EROFS), self.path)
        assert key not in self.keys(), "Dataset is already existing"
        path = os.path.join(self.path, key)
        return Dataset.create_dataset(path, dtype, shape,
                                      chunks, self.is_zarr,
                                      compression, compression_options,
                                      fill_value, self._internal_mode)

    def is_group(self, key):
        path = os.path.join(self.path, key)
        if self.is_zarr:
            return os.path.exists(os.path.join(path, '.zgroup'))
        else:
            meta_path = os.path.join(path, 'attributes.json')
            if not os.path.exists(meta_path):
                return True
            with open(meta_path, 'r') as f:
                # attributes for n5 file can be empty which cannot be parsed by json
                try:
                    attributes = json.load(f)
                except ValueError:
                    attributes = {}
            # The dimensions key is only present in a dataset
            return 'dimensions' not in attributes
