import sys
import unittest
import numpy as np
import os
from shutil import rmtree
import h5py

try:
    import z5py
except ImportError:
    sys.path.append('..')
    import z5py


class TestConverter(unittest.TestCase):
    tmp_dir = './tmp_dir'
    shape = (100, 100, 100)
    chunks = (10, 10, 10)

    def setUp(self):
        if not os.path.exists(self.tmp_dir):
            os.mkdir(self.tmp_dir)

    def tearDown(self):
        if os.path.exists(self.tmp_dir):
            rmtree(self.tmp_dir)

    def test_h5_to_n5(self):
        from z5py.converter import convert_h5_to_n5
        h5_file = os.path.join(self.tmp_dir, 'tmp.h5')
        with h5py.File(h5_file, 'w') as f:
            ds = f.create_dataset('data',
                                  shape=self.shape,
                                  chunks=self.chunks)
            data = np.arange(ds.size).reshape(ds.shape).astype(ds.dtype)
            ds[:] = data

        n5_file = os.path.join(self.tmp_dir, 'tmp.n5')
        convert_h5_to_n5(h5_file, n5_file, 'data', 'data', self.chunks, n_threads=8)

        fn5 = z5py.File(n5_file)
        data_n5 = fn5['data'][:]
        self.assertTrue(np.allclose(data, data_n5))
        print("Test h5 to n5 passed")

    def _test_n5_to_h5(self):
        from z5py.converter import convert_n5_to_h5
        n5_file = os.path.join(self.tmp_dir, 'tmp.n5')
        f = z5py.File(n5_file, use_zarr_format=False)
        ds = f.create_dataset('data',
                              dtype='float32',
                              compressor='raw',
                              shape=self.shape,
                              chunks=self.chunks)
        data = np.arange(ds.size).reshape(ds.shape).astype(ds.dtype)
        ds[:] = data

        h5_file = os.path.join(self.tmp_dir, 'tmp.h5')
        convert_n5_to_h5(n5_file, h5_file, 'data', 'data', self.chunks, n_threads=1)

        with h5py.File(h5_file, 'w') as fh5:
            data_h5 = fh5['data'][:]
        self.assertTrue(np.allclose(data, data_h5))
        print("Test n5 to h5 passed")


if __name__ == '__main__':
    unittest.main()
