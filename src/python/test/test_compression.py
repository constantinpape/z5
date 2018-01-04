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


class TestCompression(unittest.TestCase):

    def tearDown(self):
        if(os.path.exists('array.n5')):
            rmtree('array.n5')

    def test_ds_n5(self):
        dtypes = ('int8', 'int16', 'int32', 'int64',
                  'uint8', 'uint16', 'uint32', 'uint64',
                  'float32', 'float64')

        f = z5py.File('array.n5', use_zarr_format=False)
        # TODO check more compressors
        for compression in ('raw', 'gzip'):
            for dtype in dtypes:
                for dim in (1, 2, 3):  # TODO 4d and more once fixed
                    print("Running N5-Test for compression %s, dtype %s, dimension %i"
                          % (compression, dtype, dim))

                    shape = dim * (100,) if dim > 2 else dim * (1000,)
                    chunks = dim * (10,) if dim > 2 else dim * (100,)
                    # need bigger chunks for gzip in 1D case
                    if dim == 1:
                        chunks = (1000,)
                    ds_name = "ds_%s_%s_%i" % (compression, dtype, dim)
                    ds = f.create_dataset(ds_name,
                                          dtype=dtype,
                                          shape=shape,
                                          chunks=chunks,
                                          compressor=compression)
                    in_array = np.random.random(size=shape).astype(dtype)
                    ds[:] = in_array
                    out_array = ds[:]
                    self.assertEqual(out_array.shape, in_array.shape)
                    self.assertTrue(np.allclose(out_array, in_array))


if __name__ == '__main__':
    unittest.main()
