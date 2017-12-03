import json
import os


class AttributeManager(object):

    n5_keys = ('dimensions', 'blockSize', 'dataType', 'compressionType')

    def __init__(self, path, is_zarr):
        self.path = os.path.join(path, '.zattrs' if is_zarr else 'attributes.json')
        self.is_zarr = is_zarr

    def __getitem__(self, key):

        assert os.path.exists(self.path), \
            "z5py.AttributeManager.__getitem__: no attributes present"

        with open(self.path, 'r') as f:
            attributes = json.load(f)

        assert key in attributes, \
            "z5py.AttributeManager.__getitem__: key is not existing"
        return attributes[key]

    def __setitem__(self, key, item):

        if not self.is_zarr:
            assert key not in self.n5_keys, \
                "z5py.AttributeManager.__getitem__: not allowed to write N5 metadata keys"

        if os.path.exists(self.path):
            with open(self.path, 'r') as f:
                # json cannot decode empty files,
                # which may appear for N5 files
                try:
                    attributes = json.load(f)
                except ValueError:
                    attributes = {}
        else:
            attributes = {}

        attributes[key] = item
        with open(self.path, 'w') as f:
            json.dump(attributes, f)

    def _get_attributes(self):
        try:
            with open(self.path, 'r') as f:
                attrs = json.load(f)
        except ValueError:
            attrs = {}
        return attrs

    def _get_n5_attributes(self):
        attrs = self._get_attributes()
        for key in self.n5_keys:
            if key in attrs:
                del attrs[key]
        return attrs

    def __contains__(self, item):
        attrs = self._get_attributes() if self.is_zarr else self._get_n5_attributes()
        return item in attrs

    def items(self):
        attrs = self._get_attributes() if self.is_zarr else self._get_n5_attributes()
        return attrs.items()

    def keys(self):
        attrs = self._get_attributes() if self.is_zarr else self._get_n5_attributes()
        return attrs.keys()

    def values(self):
        attrs = self._get_attributes() if self.is_zarr else self._get_n5_attributes()
        return attrs.values()
