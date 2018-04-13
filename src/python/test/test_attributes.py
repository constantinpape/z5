from __future__ import print_function

import unittest
import os
from shutil import rmtree

import sys
try:
    import z5py
except ImportError:
    sys.path.append('..')
    import z5py


class TestAttributes(unittest.TestCase):

    def check_attrs(self, attrs):
        self.assertFalse('not_an_attr' in attrs)

        attrs["a"] = 1
        attrs["b"] = [1, 2, 3]
        attrs["c"] = "whooosa"
        self.assertEqual(attrs["a"], 1)
        self.assertEqual(attrs["b"], [1, 2, 3])
        self.assertEqual(attrs["c"], "whooosa")

    def check_n5_ds_attrs(self, attrs):
        for key in attrs.n5_keys:
            with self.assertRaises(RuntimeError):
                attrs[key] = 5

            with self.assertRaises(RuntimeError):
                del attrs[key]

            self.assertFalse(key in attrs)

    def setUp(self):
        self.shape = (100, 100, 100)

        self.ff_zarr = z5py.File('array.zr', True)
        self.ff_zarr.create_dataset(
            'ds', dtype='float32', shape=self.shape, chunks=(10, 10, 10)
        )
        self.ff_zarr.create_group('group')

        self.ff_n5 = z5py.File('array.n5', False)
        self.ff_n5.create_dataset(
            'ds', dtype='float32', shape=self.shape, chunks=(10, 10, 10)
        )
        self.ff_n5.create_group('group')

    def tearDown(self):
        if(os.path.exists('array.zr')):
            rmtree('array.zr')
        if(os.path.exists('array.n5')):
            rmtree('array.n5')

    def test_attrs_zarr(self):

        # test file attributes
        f_attrs = self.ff_zarr.attrs
        print("Zarr: File Attribute Test")
        self.check_attrs(f_attrs)

        # test group attributes
        f_group = self.ff_zarr["group"].attrs
        print("Zarr: Group Attribute Test")
        self.check_attrs(f_group)

        # test ds attributes
        f_ds = self.ff_zarr["ds"].attrs
        print("Zarr: Dataset Attribute Test")
        self.check_attrs(f_ds)

    def test_attrs_n5(self):

        # test file attributes
        print("N5: File Attribute Test")
        f_attrs = self.ff_n5.attrs
        self.check_attrs(f_attrs)

        # test group attributes
        print("N5: Group Attribute Test")
        f_group = self.ff_n5["group"].attrs
        self.check_attrs(f_group)

        # test ds attributes
        print("N5: Dataset Attribute Test")
        f_ds = self.ff_n5["ds"].attrs
        self.check_attrs(f_ds)
        self.check_n5_ds_attrs(f_ds)


if __name__ == '__main__':
    unittest.main()
