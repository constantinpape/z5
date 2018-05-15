from __future__ import print_function

import unittest
from shutil import rmtree
from six import add_metaclass
from abc import ABCMeta

import sys
try:
    import z5py
except ImportError:
    sys.path.append('..')
    import z5py


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
        print("File Attribute Test")
        self.check_attrs(f_attrs)

        # test group attributes
        f_group = self.root_file["group"].attrs
        print("Group Attribute Test")
        self.check_attrs(f_group)

        # test ds attributes
        f_ds = self.root_file["ds"].attrs
        print("Dataset Attribute Test")
        self.check_attrs(f_ds)
        if not self.root_file.is_zarr:
            self.check_ds_attrs(f_ds)


class TestAttributesZarr(AttributesTestMixin, unittest.TestCase):
    data_format = 'zarr'


class TestAttributesN5(AttributesTestMixin, unittest.TestCase):
    data_format = 'n5'


if __name__ == '__main__':
    unittest.main()
