from .dataset import Dataset


class File(object):

    def __init__(self, path, zarr_format=True):
        self.path = path
        self.zarr_format = zarr_format
        self.attrs = AttributeManager(path, zarr_format)
        self._key_storage = {}

    def keys(self):
        pass

    def create_group(self, key):
        pass

    def create_dataset(self, key):
        pass

    def __getitem__(self, key):
        pass

    # TODO setitem ?
