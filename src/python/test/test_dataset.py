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


class TestDataset(unittest.TestCase):

    def setUp(self):
        self.shape = (100, 100, 100)
        self.ff_zarr = z5py.File('array.zr', True)
        self.ff_zarr.create_dataset(
            'test', dtype='float32', shape=self.shape, chunks=(10, 10, 10)
        )
        self.ff_n5 = z5py.File('array.n5', False)
        self.ff_n5.create_dataset(
            'test', dtype='float32', shape=self.shape, chunks=(10, 10, 10)
        )

        base_dtypes = [
            'int8', 'int16', 'int32', 'int64',
            'uint8', 'uint16', 'uint32', 'uint64',
            'float32', 'float64'
        ]
        self.dtypes = tuple(
            base_dtypes +
            [
                np.dtype(s) for s in base_dtypes
            ] +
            [
                '<i1', '<i2', '<i4', '<i8',
                '<u1', '<u2', '<u4', '<u8',
                '<f4', '<f8'
            ] +
            [
                np.int8, np.int16, np.int32, np.int64,
                np.uint8, np.uint16, np.uint32, np.uint64,
                np.float32, np.float64
            ]
        )

    def tearDown(self):
        if(os.path.exists('array.zr')):
            rmtree('array.zr')
        if(os.path.exists('array.n5')):
            rmtree('array.n5')

    def test_ds_open_empty_zarr(self):
        print("open empty zarr array")
        ds = self.ff_zarr['test']
        out = ds[:]
        self.assertEqual(out.shape, self.shape)
        self.assertTrue((out == 0).all())

    def test_ds_open_empty_n5(self):
        print("open empty n5 array")
        ds = self.ff_n5['test']
        out = ds[:]
        self.assertEqual(out.shape, self.shape)
        self.assertTrue((out == 0).all())

    def test_ds_zarr(self):
        for dtype in self.dtypes:
            print("Running Zarr-Test for %s" % dtype)
            ds = self.ff_zarr.create_dataset(
                'data_%s' % hash(dtype), dtype=dtype, shape=self.shape, chunks=(10, 10, 10)
            )
            in_array = 42 * np.ones(self.shape, dtype=dtype)
            ds[:] = in_array
            out_array = ds[:]
            self.assertEqual(out_array.shape, in_array.shape)
            self.assertTrue(np.allclose(out_array, in_array))

    def test_ds_n5(self):
        for dtype in self.dtypes:
            print("Running N5-Test for %s" % dtype)
            ds = self.ff_n5.create_dataset(
                'data_%s' % hash(dtype), dtype=dtype, shape=self.shape, chunks=(10, 10, 10)
            )
            in_array = 42 * np.ones(self.shape, dtype=dtype)
            ds[:] = in_array
            out_array = ds[:]
            self.assertEqual(out_array.shape, in_array.shape)
            self.assertTrue(np.allclose(out_array, in_array))

    @unittest.skipIf(sys.version_info.major < 3, "This fails in python 2")
    def test_ds_n5_array_to_format(self):
        for dtype in self.dtypes:
            ds = self.ff_n5.create_dataset('data_%s' % hash(dtype),
                                           dtype=dtype,
                                           shape=self.shape,
                                           chunks=(10, 10, 10))
            in_array = 42 * np.ones((10, 10, 10), dtype=dtype)
            ds[:10, :10, :10] = in_array

            path = os.path.join(os.path.dirname(ds.attrs.path), '0', '0', '0')
            with open(path, 'rb') as f:
                read_from_file = np.array([byte for byte in f.read()], dtype='int8')

            converted_data = ds.array_to_format(in_array)

            self.assertEqual(len(read_from_file), len(converted_data))
            self.assertTrue(np.allclose(read_from_file, converted_data))


if __name__ == '__main__':
    unittest.main()
