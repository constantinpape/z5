import unittest
import sys
import numpy as np
import os
from shutil import rmtree
from six import add_metaclass
from abc import ABCMeta

try:
    import z5py
except ImportError:
    sys.path.append('..')
    import z5py


@add_metaclass(ABCMeta)
class DatasetTestMixin(object):
    def setUp(self):
        self.shape = (100, 100, 100)
        self.root_file = z5py.File('array.' + self.data_format, use_zarr_format=self.data_format == 'zarr')

        base_dtypes = [
            'int8', 'int16', 'int32', 'int64',
            'uint8', 'uint16', 'uint32', 'uint64',
            'float32', 'float64'
        ]
        self.dtypes = tuple(
            base_dtypes +
            [np.dtype(s) for s in base_dtypes] +
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
        try:
            rmtree('array.' + self.data_format)
        except OSError:
            pass

    def check_array(self, result, expected, msg=None):
        self.assertEqual(result.shape, expected.shape, msg)
        self.assertTrue(np.allclose(result, expected), msg)

    def test_ds_open_empty(self):
        print("open empty {} array".format(self.data_format))
        self.root_file.create_dataset(
            'test', dtype='float32', shape=self.shape, chunks=(10, 10, 10)
        )
        ds = self.root_file['test']
        out = ds[:]
        self.check_array(out, np.zeros(self.shape))

    def test_ds_dtypes(self):
        for dtype in self.dtypes:
            print("Running %s-Test for %s" % (self.data_format.title(), dtype))
            ds = self.root_file.create_dataset(
                'data_%s' % hash(dtype), dtype=dtype, shape=self.shape, chunks=(10, 10, 10)
            )
            in_array = 42 * np.ones(self.shape, dtype=dtype)
            ds[:] = in_array
            out_array = ds[:]
            self.check_array(out_array, in_array)

    def check_ds_indexing(self, sliced_ones, expected_shape, msg=None):
        self.check_array(sliced_ones, np.ones(expected_shape, dtype=np.uint8), msg)

    def test_ds_indexing(self):
        ds = self.root_file.create_dataset('ones', dtype=np.uint8, shape=self.shape, chunks=(10, 10, 10))
        ds[:] = np.ones(self.shape, np.uint8)

        self.check_ds_indexing(ds[:], self.shape, 'full index failed')

        self.check_ds_indexing(ds[1, ...], (1, 100, 100), 'trailing ellipsis failed')
        self.check_ds_indexing(ds[..., 1], (100, 100, 1), 'leading ellipsis failed')
        self.check_ds_indexing(ds[1], (1, 100, 100), 'implicit ellipsis failed')
        self.check_ds_indexing(ds[:, :, :, ...], self.shape, 'superfluous ellipsis failed')
        self.check_ds_indexing(ds[500:501, :, :], (0, 100, 100), 'out-of-bounds slice failed')
        self.check_ds_indexing(ds[-501:500, :, :], (0, 100, 100), 'negative out-of-bounds slice failed')

        self.check_ds_indexing(ds[1, :, :], (1, 100, 100), 'integer index failed')
        self.check_ds_indexing(ds[-20:, :, :], (20, 100, 100), 'negative slice failed')

        self.assertEqual(ds[1, 1, 1], 1, 'point index failed')

        with self.assertRaises(ValueError):
            ds[500, :, :]
        with self.assertRaises(ValueError):
            ds[-500, :, :]
        with self.assertRaises(ValueError):
            ds[..., :, ...]
        with self.assertRaises(ValueError):
            ds[1, 1, slice(0, 100, 2)]
        with self.assertRaises(TypeError):
            ds[[1, 1, 1]]  # explicitly test behaviour different to h5py

        class NotAnIndex(object):
            pass

        with self.assertRaises(TypeError):
            ds[1, 1, NotAnIndex()]


class TestZarrDataset(DatasetTestMixin, unittest.TestCase):
    data_format = 'zarr'


class TestN5Dataset(DatasetTestMixin, unittest.TestCase):
    data_format = 'n5'

    @unittest.skipIf(sys.version_info.major < 3, "This fails in python 2")
    def test_ds_array_to_format(self):
        for dtype in self.dtypes:
            ds = self.root_file.create_dataset('data_%s' % hash(dtype),
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
