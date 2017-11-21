import os
import json
from .dataset import Dataset
from .attribute_manager import AttributeManager


class Base(object):

    def __init__(self, path, is_zarr=True):
        self.path = path
        self.is_zarr = is_zarr
        self._attrs = AttributeManager(path, is_zarr)

    @property
    def attrs(self):
        return self._attrs

    def keys(self):
        return os.listdir(self.path)

    # TODO allow creating with data ?!
    def create_dataset(
        self,
        key,
        dtype,
        shape,
        chunks,
        fill_value=0,
        compressor='blosc',  # TODO change default value depending on zarr / n5
        codec='lz4',  # TODO change default value depending on zarr / n5
        level=4,
        shuffle=1
    ):
        assert key not in self.keys(), "Dataset is already existing"
        path = os.path.join(self.path, key)
        return Dataset.create_dataset(path, dtype, shape,
                                      chunks, self.is_zarr,
                                      fill_value, compressor,
                                      codec, level, shuffle)

    def is_group(self, path):
        if self.is_zarr:
            return os.path.exists(
                os.path.join(path, '.zgroup')
            )
        else:
            with open(os.path.join(path, 'attributes.json'), 'r') as f:
                # attributes for n5 file can be empty which cannot be parsed by json
                try:
                    attributes = json.load(f)
                except ValueError:
                    attributes = {}
            return 'dimensions' not in attributes
