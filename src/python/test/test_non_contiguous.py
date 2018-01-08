import unittest
import sys
import numpy as np
import os
from shutil import rmtree

try:
    import z5py
except ImportError:
    sys.path.append('..')
    import z5py


class TestNonContiguous(unittest.TestCase):
    def setUp(self):
        self.shape = (100, 100, 100)
        self.ff = z5py.File('array.n5', False)
        self.ff.create_dataset('test',
                               dtype='float32',
                               shape=self.shape,
                               chunks=(10, 10, 10))

    def tearDown(self):
        if(os.path.exists('array.n5')):
            rmtree('array.n5')

    def test_readwrite_noncontig(self):
        # make a non-contiguous 3d array of the correct shape (100)^3
        vol = np.arange(200**3).astype('float32').reshape((200, 200, 200))
        data = vol[::2, ::2, ::2]
        ds = self.ff['test']
        ds[:] = data
        data_out = ds[:]
        self.assertEqual(data.shape, data_out.shape)
        self.assertTrue(np.allclose(data, data_out))

    def test_readwrite_contig(self):
        # make a non-contiguous 3d array of the correct shape (100)^3
        vol = np.arange(200**3).astype('float32').reshape((200, 200, 200))
        data = vol[::2, ::2, ::2].copy('C')
        ds = self.ff['test']
        ds[:] = data
        data_out = ds[:]
        self.assertEqual(data.shape, data_out.shape)
        self.assertTrue(np.allclose(data, data_out))


if __name__ == '__main__':
    unittest.main()
