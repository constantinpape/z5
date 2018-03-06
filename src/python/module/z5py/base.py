import os
import json
import numpy as np

from .modes import FileMode
from .dataset import Dataset
from .attribute_manager import AttributeManager


class Base(object):

    def __init__(self, path, is_zarr=True, mode='a'):
        self.path = path
        self.is_zarr = is_zarr
        self._attrs = AttributeManager(path, is_zarr)
        self.mode = FileMode(mode)

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

    def create_dataset(self, key, dtype=None, shape=None, chunks=None,
                       fill_value=0, compression='raw', data=None,
                       **compression_options):
        assert key not in self.keys(), "Dataset is already existing"
        path = os.path.join(self.path, key)
        self.mode.check_write(path)

        if data is None:
            assert shape is not None, "Datasets must be given a shape"
            assert chunks is not None, "Datasets must be given a chunk size"
            dtype = dtype or np.dtype('float64')
        else:
            data = np.asarray(data)
            if dtype is None:
                dtype = data.dtype
            else:
                assert dtype == data.dtype, "Given dtype ({}) conflicts with type of given data ({})".format(
                    dtype, data.dtype
                )  # could coerce instead

            if shape is None:
                shape = dtype.shape
            else:
                assert shape == data.shape, "Given shape ({}) conflicts with shape of given data ({})".format(
                    shape, data.shape
                )

            if chunks is None:
                chunks = shape  # contiguous

        ds = Dataset.create_dataset(path, dtype, shape,
                                      chunks, self.is_zarr,
                                      compression, compression_options,
                                      fill_value, mode=self.mode)

        if data is None:
            return ds

        ds[:, :, :] = data
        return ds

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
