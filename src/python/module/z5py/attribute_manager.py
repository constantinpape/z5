import json
from collections.abc import MutableMapping

from . import _z5py


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

    def _read_attributes(self):
        """Return dict from JSON attribute store."""
        global _JSON_DECODER
        attributes = _z5py.read_attributes(self._handle)
        attributes = json.loads(attributes, cls=_JSON_DECODER)
        attributes = {} if attributes is None else attributes
        return attributes

    def _write_attributes(self, attributes):
        global _JSON_ENCODER
        attributes = json.dumps(attributes, cls=_JSON_ENCODER)
        _z5py.write_attributes(self._handle, attributes)

    def __iter__(self):
        attributes = self._read_attributes()
        return iter(attributes)

    def __len__(self):
        attributes = self._read_attributes()
        return len(attributes)
