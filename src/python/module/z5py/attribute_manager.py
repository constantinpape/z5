"""Access to the custom user attributes (JSON metadata) of groups and datasets.

The :class:`AttributeManager` exposes a container's attributes as a mutable
dict. :func:`set_json_encoder` / :func:`set_json_decoder` let you customize how
attributes are (de-)serialized, e.g. to support non-standard JSON types.
"""
import json
from collections.abc import MutableMapping

from . import _z5py


__all__ = ['AttributeManager', 'set_json_encoder', 'set_json_decoder']


# json encoders and decoder
# if None, the default encoder / decoders are used
_JSON_ENCODER = None
_JSON_DECODER = None


def set_json_encoder(encoder):
    """ Set the encoder used by ``json.dump`` when serializing attributes.

    Args:
        encoder (type): a :class:`json.JSONEncoder` subclass, or None to restore
            the default encoder.

    Raises:
        RuntimeError: if ``encoder`` is not a :class:`json.JSONEncoder` subclass.
    """
    global _JSON_ENCODER
    if encoder is None:
        _JSON_ENCODER = None
        return
    if not isinstance(encoder(), json.JSONEncoder):
        raise RuntimeError("Encoder must inherit from JSONEncoder")
    _JSON_ENCODER = encoder


def get_json_encoder():
    """ Return the currently configured JSON encoder (or None for the default).
    """
    return _JSON_ENCODER


def set_json_decoder(decoder):
    """ Set the decoder used by ``json.load`` when de-serializing attributes.

    Args:
        decoder (type): a :class:`json.JSONDecoder` subclass, or None to restore
            the default decoder.

    Raises:
        RuntimeError: if ``decoder`` is not a :class:`json.JSONDecoder` subclass.
    """
    global _JSON_DECODER
    if decoder is None:
        _JSON_DECODER = None
        return
    if not isinstance(decoder(), json.JSONDecoder):
        raise RuntimeError("Decoder must inherit from JSONDecoder")
    _JSON_DECODER = decoder


class AttributeManager(MutableMapping):
    """ Provides access to custom user attributes.

    Attributes will be saved as json in `attributes.json` for n5, `.zattributes` for zarr.
    Supports the default python dict api.
    N5 stores the dataset attributes in the same file; these attributes are
    NOT mutable via the AttributeManager.
    """

    def __init__(self, handle):
        self._handle = handle

    def __getitem__(self, key):
        attributes = self._read_attributes()
        return attributes[key]

    def __setitem__(self, key, item):
        self._write_attributes({key: item})

    def __delitem__(self, key):
        _z5py.remove_attribute(self._handle, key)

    def update(self, *args, **kwargs):
        """ Update multiple attributes with a single read-modify-write.

        The MutableMapping default calls ``__setitem__`` once per key, which
        costs one IO round trip per key (expensive over S3).
        """
        self._write_attributes(dict(*args, **kwargs))

    def _read_attributes(self):
        """Return dict from JSON attribute store."""
        attributes = _z5py.read_attributes(self._handle)
        attributes = json.loads(attributes, cls=_JSON_DECODER)
        attributes = {} if attributes is None else attributes
        return attributes

    def _write_attributes(self, attributes):
        attributes = json.dumps(attributes, cls=_JSON_ENCODER)
        _z5py.write_attributes(self._handle, attributes)

    def __iter__(self):
        attributes = self._read_attributes()
        return iter(attributes)

    def __len__(self):
        attributes = self._read_attributes()
        return len(attributes)
