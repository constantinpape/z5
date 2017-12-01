import sys
import unittest
import numpy as np
import os
from shutil import rmtree

try:
    import z5py
except ImportError:
    sys.path.append('..')
    import z5py


class TestUtil(unittest.TestCase):
    tmp_dir = './tmp_dir'
    shape = (100, 100, 100)
    chunks = (10, 10, 10)

    def setUp(self):
        if not os.path.exists(self.tmp_dir):
            os.mkdir(self.tmp_dir)

    def tearDown(self):
        if os.path.exists(self.tmp_dir):
            rmtree(self.tmp_dir)

    def test_rechunk(self):
        from z5py.util import rechunk
        in_path = os.path.join(self.tmp_dir, 'in.n5')
        out_path = os.path.join(self.tmp_dir, 'out.n5')

        # create input file
        in_file = z5py.File(in_path, use_zarr_format=False)
        ds_in = in_file.create_dataset('data', dtype='float32',
                                       shape=self.shape, chunks=self.chunks,
                                       compressor='gzip')
        # write test data
        data = np.arange(ds_in.size).reshape(ds_in.shape).astype(ds_in.dtype)
        ds_in[:] = data
        # rechunk
        new_chunks = (20, 20, 20)
        rechunk(in_path, out_path, 'data', 'data', new_chunks,
                n_threads=8,
                compressor='raw')
        # make sure that new data agrees
        out_file = z5py.File(out_path, use_zarr_format=False)
        ds_out = out_file['data']
        data_out = ds_out[:]
        self.assertEqual(data_out.shape, data.shape)
        self.assertEqual(ds_out.chunks, new_chunks)
        self.assertTrue(np.allclose(data, data_out))


if __name__ == '__main__':
    unittest.main()
