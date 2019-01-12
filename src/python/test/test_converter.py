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
        try:
            rmtree(self.tmp_dir)
        except OSError:
            pass

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
                            key, key, chunks=self.chunks,
                            n_threads=8, compression=compression_b)

            fn5 = z5py.File(n5_file)
            data_n5 = fn5[key][:]
            self.assertTrue(np.allclose(data, data_n5))

    @unittest.skipUnless(h5py, 'Requires h5py')
    @unittest.skipUnless(futures, 'Needs 3rd party concurrent.futures in python 2')
    def test_h5_to_n5_with_roi(self):
        from z5py.converter import convert_from_h5
        h5_file = os.path.join(self.tmp_dir, 'tmp.h5')
        n5_file = os.path.join(self.tmp_dir, 'tmp.n5')

        h5_key = 'data'
        with h5py.File(h5_file, 'w') as f:
            ds = f.create_dataset(h5_key, shape=self.shape, chunks=self.chunks)
            data = np.arange(ds.size).reshape(ds.shape).astype(ds.dtype)
            ds[:] = data

        # define roi
        roi = np.s_[5:45, 9:83, 10:60]
        out_of_roi_mask = np.ones(self.shape, dtype='bool')
        out_of_roi_mask[roi] = False
        roi_shape = tuple(rr.stop - rr.start for rr in roi)

        # convert dataset with roi
        n5_key = 'data_roi'
        convert_from_h5(h5_file, n5_file,
                        h5_key, n5_key, n_threads=8,
                        roi=roi, fit_to_roi=False)

        fn5 = z5py.File(n5_file)
        data_n5 = fn5[n5_key][:]
        self.assertEqual(data.shape, data_n5.shape)
        self.assertTrue(np.allclose(data[roi], data_n5[roi]))
        self.assertTrue(np.allclose(data_n5[out_of_roi_mask], 0))

        # convert dataset with roi and fit_to_roi
        n5_key = 'data_roi_fit'
        convert_from_h5(h5_file, n5_file,
                        h5_key, n5_key, n_threads=8,
                        roi=roi, fit_to_roi=True)

        fn5 = z5py.File(n5_file)
        data_n5 = fn5[n5_key][:]
        self.assertEqual(roi_shape, data_n5.shape)
        self.assertTrue(np.allclose(data[roi], data_n5))

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
                          key, key, chunks=self.chunks,
                          n_threads=1, compression=compression_b)

            with h5py.File(h5_file, 'r') as fh5:
                data_h5 = fh5[key][:]
            self.assertTrue(np.allclose(data, data_h5))

    @unittest.skipUnless(h5py, 'Requires h5py')
    @unittest.skipUnless(futures, 'Needs 3rd party concurrent.futures in python 2')
    def test_n5_to_h5_with_roi(self):
        from z5py.converter import convert_to_h5
        h5_file = os.path.join(self.tmp_dir, 'tmp.h5')
        n5_file = os.path.join(self.tmp_dir, 'tmp.n5')

        n5_key = 'data'
        with z5py.File(n5_file, 'w') as f:
            ds = f.create_dataset(n5_key, shape=self.shape, chunks=self.chunks,
                                  dtype='float64')
            data = np.arange(ds.size).reshape(ds.shape).astype(ds.dtype)
            ds[:] = data

        # define roi
        roi = np.s_[5:45, 9:83, 10:60]
        out_of_roi_mask = np.ones(self.shape, dtype='bool')
        out_of_roi_mask[roi] = False
        roi_shape = tuple(rr.stop - rr.start for rr in roi)

        # convert dataset with roi
        h5_key = 'data_roi'
        convert_to_h5(n5_file, h5_file,
                      n5_key, h5_key, n_threads=8,
                      roi=roi, fit_to_roi=False)

        with h5py.File(h5_file, 'r') as f:
            data_h5 = f[h5_key][:]
        self.assertEqual(data.shape, data_h5.shape)
        self.assertTrue(np.allclose(data[roi], data_h5[roi]))
        self.assertTrue(np.allclose(data_h5[out_of_roi_mask], 0))

        # convert dataset with roi and fit_to_roi
        h5_key = 'data_roi_fit'
        convert_to_h5(n5_file, h5_file,
                      n5_key, h5_key, n_threads=8,
                      roi=roi, fit_to_roi=True)

        with h5py.File(h5_file, 'r') as f:
            data_h5 = f[h5_key][:]
        self.assertEqual(roi_shape, data_h5.shape)
        self.assertTrue(np.allclose(data[roi], data_h5))


if __name__ == '__main__':
    unittest.main()
