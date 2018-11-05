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


# json encoders and decoder
# if None, the default encoder / decoders are used
_JSON_ENCODER = None
_JSON_DECODER = None


def set_json_encoder(encoder):
    """ Set the encoder to be used in `json.dump` for attribute serialization.
    """
    global _JSON_ENCODER
    if encoder is None:
        _JSON_ENCODER = None
        return
    if not isinstance(encoder(), json.JSONEncoder):
        raise RuntimeError("Encoder must inherit from JSONEncoder")
    _JSON_ENCODER = encoder


def get_json_encoder():
    return _JSON_ENCODER


def set_json_decoder(decoder):
    """ Set the decoder to be used in `json.load` for attribute de-serialization.
    """
    global _JSON_DECODER
    if decoder is None:
        _JSON_DECODER = None
        return
    if not isinstance(decoder(), json.JSONDecoder):
        raise RuntimeError("Decoder must inherit from JSONDecoder")
    _JSON_DECODER = decoder


def restrict_metadata_keys(fn):
    """ Decorator for AttributeManager methods which checks that,
    if the manager is for N5, the key argument is not a
    reserved metadata name.
    """
    @wraps(fn)
    def wrapped(obj, key, *args, **kwargs):
        if key in obj.reserved_keys:
            raise RuntimeError("Reserved metadata (key {}) cannot be mutated".format(repr(key)))
        return fn(obj, key, *args, **kwargs)
    return wrapped


class AttributeManager(MutableMapping):
    """ Provides access to custom user attributes.

    Attributes will be saved as json in `attributes.json` for n5, `.zattributes` for zarr.
    Supports the default python dict api.
    N5 stores the dataset attributes in the same file; these attributes are
    NOT mutable via the AttributeManager.
    """
    _zarr_fname = '.zattributes'
    _n5_fname = 'attributes.json'
    _n5_keys = frozenset(('dimensions', 'blockSize',
                          'dataType', 'compressionType', 'compression'))

    def __init__(self, path, is_zarr):
        self.path = os.path.join(path, self._zarr_fname if is_zarr else self._n5_fname)
        self.reserved_keys = frozenset() if is_zarr else self._n5_keys

    def __getitem__(self, key):
        with self._open_attributes() as attributes:
            return attributes[key]

    @restrict_metadata_keys
    def __setitem__(self, key, item):
        with self._open_attributes(True) as attributes:
            attributes[key] = item

    @restrict_metadata_keys
    def __delitem__(self, key):
        with self._open_attributes(True) as attributes:
            del attributes[key]

    @contextmanager
    def _open_attributes(self, write=False):
        """Context manager for JSON attribute store.

        Set ``write`` to True to dump out changes."""
        attributes = self._read_attributes()
        hidden_attrs = {key: attributes.pop(key)
                        for key in self.reserved_keys.intersection(attributes)}
        yield attributes
        if write:
            hidden_attrs.update(attributes)
            self._write_attributes(hidden_attrs)

    def _read_attributes(self):
        """Return dict from JSON attribute store.
        Caller needs to distinguish between valid and N5 metadata keys."""
        global _JSON_DECODER
        try:
            with open(self.path, 'r') as f:
                attributes = json.load(f, cls=_JSON_DECODER)
        except ValueError:
            attributes = {}
        except IOError as e:
            if e.errno == errno.ENOENT:
                attributes = {}
            else:
                raise

        return attributes

    def _write_attributes(self, attributes):
        """Dump ``attributes`` to JSON. Potentially dangerous for N5 metadata."""
        global _JSON_ENCODER
        with open(self.path, 'w') as f:
            json.dump(attributes, f, cls=_JSON_ENCODER)

    def __iter__(self):
        with self._open_attributes() as attributes:
            return iter(attributes)

    def __len__(self):
        with self._open_attributes() as attributes:
            return len(attributes)
