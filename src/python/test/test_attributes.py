from __future__ import print_function

import unittest
import json
from shutil import rmtree
from six import add_metaclass
from abc import ABCMeta

import numpy as np

import sys
try:
    import z5py
except ImportError:
    sys.path.append('..')
    import z5py


# Just a dummy numpy encoder to test custom encoders
class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NumpyEncoder, self).default(obj)


class DecoratingDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook,
                                  *args, **kwargs)

    def object_hook(self, obj):
        return {k: '_' + v + '_' if isinstance(v, str) else v
                for k, v in obj.items()}


class Dummy(object):
    pass


@add_metaclass(ABCMeta)
class AttributesTestMixin(object):

    def setUp(self):
        self.shape = (100, 100, 100)

        self.root_file = z5py.File('array.%s' % self.data_format)
        self.root_file.create_dataset('ds', dtype='float32',
                                      shape=self.shape, chunks=(10, 10, 10))
        self.root_file.create_group('group')

    def tearDown(self):
        try:
            rmtree('array.%s' % self.data_format)
        except OSError:
            pass

    def check_attrs(self, attrs):
        self.assertFalse('not_an_attr' in attrs)

        attrs["a"] = 1
        attrs["b"] = [1, 2, 3]
        attrs["c"] = "whooosa"
        self.assertEqual(attrs["a"], 1)
        self.assertEqual(attrs["b"], [1, 2, 3])
        self.assertEqual(attrs["c"], "whooosa")

    def check_ds_attrs(self, attrs):
        for key in attrs.reserved_keys:
            with self.assertRaises(RuntimeError):
                attrs[key] = 5

            with self.assertRaises(RuntimeError):
                del attrs[key]

            self.assertIsNone(attrs.get(key))
            self.assertFalse(key in attrs)
            self.assertFalse(key in set(attrs))

    def test_attrs(self):

        # test file attributes
        f_attrs = self.root_file.attrs
        self.check_attrs(f_attrs)

        # test group attributes
        f_group = self.root_file["group"].attrs
        self.check_attrs(f_group)

        # test ds attributes
        f_ds = self.root_file["ds"].attrs
        self.check_attrs(f_ds)
        if not self.root_file.is_zarr:
            self.check_ds_attrs(f_ds)

    def test_custom_encoder(self):
        # check that we can't set arbitrary encoders
        with self.assertRaises(RuntimeError):
            z5py.set_json_encoder(Dummy)

        def _check():
            x = np.arange(10, dtype='int32')
            self.root_file.attrs['x'] = x
            y = self.root_file.attrs['x']
            self.assertTrue(np.allclose(x, y))

        # check that the encoder is used
        z5py.set_json_encoder(NumpyEncoder)
        _check()
        # check that the encoder is properly reset
        z5py.set_json_encoder(None)
        with self.assertRaises(TypeError):
            _check()

    def test_custom_decoder(self):
        # check that we can't set arbitrary decoders
        with self.assertRaises(RuntimeError):
            z5py.set_json_decoder(Dummy)

        def _check():
            self.root_file.attrs['a'] = 'bcde'
            self.assertEqual(self.root_file.attrs['a'], '_bcde_')

        # check that the decoder is used
        z5py.set_json_decoder(DecoratingDecoder)
        _check()
        # check that the decoder is properly reset
        z5py.set_json_decoder(None)
        with self.assertRaises(AssertionError):
            _check()


class TestAttributesZarr(AttributesTestMixin, unittest.TestCase):
    data_format = 'zarr'


class TestAttributesN5(AttributesTestMixin, unittest.TestCase):
    data_format = 'n5'


if __name__ == '__main__':
    unittest.main()
