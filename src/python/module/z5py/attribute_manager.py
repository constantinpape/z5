import json
import os
import errno
from functools import wraps
from contextlib import contextmanager

try:
    from collections.abc import MutableMapping
except ImportError:
    from collections import MutableMapping       


__all__ = ['AttributeManager']


def restrict_n5_keys(fn):
    """
    Decorator for AttributeManager methods which checks that, if the manager is for N5, the key argument is not a
    reserved metadata name.
    """
    @wraps(fn)
    def wrapped(obj, key, *args, **kwargs):
        if not obj.is_zarr and key in obj.n5_keys:
            raise RuntimeError("N5 metadata (key {}) cannot be mutated".format(repr(key)))
        return fn(obj, key, *args, **kwargs)
    return wrapped


class AttributeManager(MutableMapping):
    zarr_fname = '.zattributes'
    n5_fname = 'attributes.json'
    n5_keys = frozenset(('dimensions', 'blockSize', 'dataType', 'compressionType', 'compression'))

    def __init__(self, path, is_zarr):
        self.path = os.path.join(path, self.zarr_fname if is_zarr else self.n5_fname)
        self.is_zarr = is_zarr

    def __getitem__(self, key):
        with self._open_attributes() as attributes:
            return attributes[key]

    @restrict_n5_keys
    def __setitem__(self, key, item):
        with self._open_attributes(True) as attributes:
            attributes[key] = item

    @restrict_n5_keys
    def __delitem__(self, key):
        with self._open_attributes(True) as attributes:
            del attributes[key]

    @contextmanager
    def _open_attributes(self, write=False):
        """Context manager for JSON attribute store. Caller needs to distinguish between valid and N5 metadata keys.

        Set ``write`` to True to dump out changes."""
        attributes = self._read_attributes()
        yield attributes
        if write:
            self._write_attributes(attributes)

    def _read_attributes(self):
        """Return dict from JSON attribute store. Caller needs to distinguish between valid and N5 metadata keys."""
        try:
            with open(self.path, 'r') as f:
                attributes = json.load(f)
        except ValueError:
            attributes = {}
        except IOError as e:
            if e.errno == errno.ENOENT:
                attributes = {}
            else:
                raise

        return attributes

    def _write_attributes(self, attributes):
        """Dump ``attributes`` to JSON"""
        with open(self.path, 'w') as f:
            json.dump(attributes, f)

    def __iter__(self):
        with self._open_attributes() as attributes:
            if not self.is_zarr:
                for key in self.n5_keys:
                    attributes.pop(key, None)
            return iter(attributes)

    def __len__(self):
        return len(list(self))

    def __contains__(self, item):
        with self._open_attributes() as attributes:
            return item in attributes and (self.is_zarr or item not in self.n5_keys)
