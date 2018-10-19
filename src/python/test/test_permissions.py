import unittest
import os
from shutil import rmtree
import numpy as np

import sys
try:
    import z5py
except ImportError:
    sys.path.append('..')
    import z5py


# We just test for the n5 ending, as this is independent of
# the data format
class TestPermissions(unittest.TestCase):
    def setUp(self):
        self.path1 = 'array1.n5'
        self.path2 = 'array2.n5'
        f = z5py.File(self.path1)
        f.create_dataset('data', dtype='uint8', shape=(10, 10), chunks=(10, 10))

    def tearDown(self):
        try:
            rmtree(self.path1)
        except OSError:
            pass
        try:
            rmtree(self.path2)
        except OSError:
            pass

    # test read permissions ('r')
    def test_read_permission(self):
        self.assertTrue(os.path.exists(self.path1))
        self.assertFalse(os.path.exists(self.path2))

        # test that a new file can't be created
        with self.assertRaises(OSError):
            z5py.File(self.path2, mode='r')

        # open existing file
        f = z5py.File(self.path1, mode='r')
        self.assertFalse(f.is_zarr)

        # TODO would be nice to have consistent exceptions here
        # test that a new group can't be created
        with self.assertRaises(ValueError):
            f.create_group('g')

        # test that a new dataset can't be created
        # with self.assertRaises(RuntimeError):
        with self.assertRaises(ValueError):
            f.create_dataset('ds', dtype='uint8', shape=(10, 10), chunks=(10, 10))

        # test that we cannot write to an existing dataset
        ds = f['data']
        with self.assertRaises(ValueError):
            ds[:] = np.zeros((10, 10), dtype='uint8')

    # generic test for all default operations
    def generic_test(self, f):
        self.assertFalse(f.is_zarr)
        f.create_group('g')
        ds = f.create_dataset('ds', dtype='uint8', shape=(10, 10), chunks=(10, 10))
        ds[:] = np.zeros((10, 10), dtype='uint8')

    # test write permissions ('w')
    def test_write_permission(self):
        self.assertTrue(os.path.exists(self.path1))
        self.assertFalse(os.path.exists(self.path2))

        # we should be able to do all the default operations in write
        f = z5py.File(self.path2, mode='w')
        self.generic_test(f)

        # make sure that append truncates
        f = z5py.File(self.path1, mode='w')
        in_dir = os.listdir(self.path1)
        self.assertEqual(len(in_dir), 0)

    # test append permissions ('a')
    def test_append_permission(self):
        self.assertTrue(os.path.exists(self.path1))
        self.assertFalse(os.path.exists(self.path2))

        # we should be able to do all the default operations in append
        f = z5py.File(self.path2, mode='a')
        self.generic_test(f)

    # test read-plus permissions ('r+')
    def test_readplus_permission(self):
        self.assertTrue(os.path.exists(self.path1))
        self.assertFalse(os.path.exists(self.path2))

        # we should be able to do all the default operations in read-plus (except creation)
        f = z5py.File(self.path1, mode='r+')
        self.generic_test(f)

        # we are not allowed to do creation in readplus
        with self.assertRaises(OSError):
            z5py.File(self.path2, mode='r+')

    # test write-minus permissions ('w-')
    def test_writeminus_permission(self):
        self.assertTrue(os.path.exists(self.path1))
        self.assertFalse(os.path.exists(self.path2))

        # we should be able to do all the default operations in read-minus (except opening file that already exists)
        f = z5py.File(self.path2, mode='w-')
        self.generic_test(f)

        # we are not allowed to do creation in readplus
        with self.assertRaises(OSError):
            z5py.File(self.path1, mode='w-')


if __name__ == '__main__':
    unittest.main()
