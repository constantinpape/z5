import unittest
import numpy as np
import os
from shutil import rmtree

# hacky import
import sys
sys.path.append('..')
import z5py


class TestGroup(unittest.TestCase):

    def setUp(self):
        self.shape = (100, 100, 100)
        self.ff_zarr = z5py.File('array.zr', True)
        g = self.ff_zarr.create_group('test')
        g.create_dataset('test', dtype='float32' self.shape, chunks=(10, 10, 10))
        self.ff_n5 = z5py.File('array.n5', False)

    def tearDown(self):
        if(os.path.exists('array.zr')):
            rmtree('array.zr')
        if(os.path.exists('array.n5')):
            rmtree('array.n5')

    def test_open_empty_group(self):
        g = self.ff_zarr['test']
        ds = g['test']
        out = ds[:]
        self.assertEqual(out.shape, self.shape)
        self.assertTrue((out == 0).all())

    def test_open_empty_dataset(self):
        ds = g['test/test']
        out = ds[:]
        self.assertEqual(out.shape, self.shape)
        self.assertTrue((out == 0).all())

    def test_group_zarr(self):
        g = self.ff_zarr.create_group('group')
        ds = g.create_dataset(
            'data', dtype='float32', shape=self.shape, chunks=(10, 10, 10)
        )
        in_array = 42 * np.ones(self.shape, dtype='float32')
        ds[:] = in_array
        out_array = ds[:]
        self.assertEqual(out_array.shape, in_array.shape)
        self.assertTrue(np.allclose(out_array, in_array))

    def test_group_n5(self):
        g = self.ff_n5.create_group('group')
        ds = g.create_dataset(
            'data', dtype='float32', shape=self.shape, chunks=(10, 10, 10)
        )
        in_array = 42 * np.ones(self.shape, dtype='float32')
        ds[:] = in_array
        out_array = ds[:]
        self.assertEqual(out_array.shape, in_array.shape)
        self.assertTrue(np.allclose(out_array, in_array))


if __name__ == '__main__':
    unittest.main()
