import json
import os


class AttributeManager(object):

    n5_keys = ('dimensions', 'blockSize', 'dataType', 'compresssionType')

    def __init__(self, path, is_zarr):
        self.path = os.path.join(path, '.zattrs' if is_zarr else 'attributes.json')
        self.is_zarr = is_zarr

    def __getitem__(self, key):
        with open(self.path, 'r') as f:
            attributes = json.load(f)
        assert key in attributes, "Key does not exist"
        return attributes[key]

    def __setitem__(self, key, item):
        if not self.is_zarr:
            assert key not in self.n5_keys, "Not allowed to write N5 dataset metadata"
        with open(self.path, 'r') as f:
            attributes = json.load(f)
        attributes[key] = item
        with open(self.path, 'w') as f:
            json.dump(attributes, f)
