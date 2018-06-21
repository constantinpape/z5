import sys
import unittest
import numpy as np
import os
from shutil import rmtree

try:
    from concurrent import futures
except ImportError:
    futures = False

try:
    import h5py
except ImportError:
    h5py = False

try:
    import z5py
except ImportError:
    sys.path.append('..')
    import z5py


class TestConverter(unittest.TestCase):
    tmp_dir = './tmp_dir'
    shape = (100, 100, 100)
    chunks = (10, 10, 10)

    compressor_pairs = [('raw', 'raw'), ('gzip', 'gzip'),
                        ('raw', 'gzip'), ('gzip', 'raw')]

    def setUp(self):
        if not os.path.exists(self.tmp_dir):
            os.mkdir(self.tmp_dir)

    def tearDown(self):
        if os.path.exists(self.tmp_dir):
            rmtree(self.tmp_dir)

    @unittest.skipUnless(h5py, 'Requires h5py')
    @unittest.skipUnless(futures, 'Needs 3rd party concurrent.futures in python 2')
    def test_h5_to_n5(self):
        from z5py.converter import convert_from_h5
        h5_file = os.path.join(self.tmp_dir, 'tmp.h5')
        n5_file = os.path.join(self.tmp_dir, 'tmp.n5')

        # test conversion for 4 different compression pairs
        for pair in self.compressor_pairs:

            key = 'data_%s_%s' % pair
            compression_a, compression_b = pair
            compression_a = None if compression_a == 'raw' else compression_a

            with h5py.File(h5_file, 'w') as f:
                ds = f.create_dataset(key,
                                      shape=self.shape,
                                      chunks=self.chunks,
                                      compression=compression_a)
                data = np.arange(ds.size).reshape(ds.shape).astype(ds.dtype)
                ds[:] = data

            convert_from_h5(h5_file, n5_file,
                            key, key, self.chunks,
                            n_threads=8, compression=compression_b)

            fn5 = z5py.File(n5_file)
            data_n5 = fn5[key][:]
            self.assertTrue(np.allclose(data, data_n5))
        print("Test h5 to n5 passed")

    @unittest.skipUnless(h5py, 'Requires h5py')
    @unittest.skipUnless(futures, 'Needs 3rd party concurrent.futures in python 2')
    def test_n5_to_h5(self):
        from z5py.converter import convert_to_h5
        n5_file = os.path.join(self.tmp_dir, 'tmp.n5')
        h5_file = os.path.join(self.tmp_dir, 'tmp.h5')
        f = z5py.File(n5_file, use_zarr_format=False)

        # test conversion for 4 different compression pairs
        for pair in self.compressor_pairs:

            key = 'data_%s_%s' % pair
            compression_a, compression_b = pair

            ds = f.create_dataset(key,
                                  dtype='float32',
                                  compression=compression_a,
                                  shape=self.shape,
                                  chunks=self.chunks)
            data = np.arange(ds.size).reshape(ds.shape).astype(ds.dtype)
            ds[:] = data

            compression_b = None if compression_b == 'raw' else compression_b
            convert_to_h5(n5_file, h5_file,
                          key, key, self.chunks,
                          n_threads=1, compression=compression_b)

            with h5py.File(h5_file, 'r') as fh5:
                data_h5 = fh5[key][:]
            self.assertTrue(np.allclose(data, data_h5))
        print("Test n5 to h5 passed")


if __name__ == '__main__':
    unittest.main()
