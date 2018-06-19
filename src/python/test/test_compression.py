import unittest
import sys
import numpy as np
from shutil import rmtree
from six import add_metaclass
from abc import ABCMeta

try:
    import z5py
except ImportError:
    sys.path.append('..')
    import z5py


@add_metaclass(ABCMeta)
class CompressionTestMixin(object):
    def setUp(self):
        self.shape = (100, 100, 100)
        self.root_file = z5py.File('array.' + self.data_format, use_zarr_format=self.data_format == 'zarr')

        self.dtypes = [
            'int8', 'int16', 'int32', 'int64',
            'uint8', 'uint16', 'uint32', 'uint64',
            'float32', 'float64'
        ]

    def tearDown(self):
        try:
            rmtree('array.' + self.data_format)
        except OSError:
            pass

    def check_array(self, result, expected, msg=None):
        self.assertEqual(result.shape, expected.shape, msg)
        self.assertTrue(np.allclose(result, expected), msg)

    def test_compression(self):
        from z5py.dataset import Dataset
        f = self.root_file
        compressions = Dataset.compressors_n5 if self.data_format == 'n5' else\
            Dataset.compressors_zarr

        # iterate over the compression libraries
        for compression in compressions:
            for dtype in self.dtypes:

                ds_name = "ds_%s_%s" % (compression, dtype)
                # generate random data scaled to the range of the current datatype
                min_val, max_val = self.dtype_min_max(dtype)
                in_array = np.random.rand(*self.shape).astype(dtype)
                np.multiply(in_array, max_val, casting='unsafe')
                in_array += min_val

                # FIXME not all compression methods in travis yet
                try:
                    ds = f.create_dataset(ds_name,
                                          data=in_array,
                                          chunks=(10, 10, 10),
                                          compression=compression)
                    out_array = ds[:]
                    self.check_array(out_array, in_array,
                                     'failed for compression %s, dtype %s, format %s' % (compression,
                                                                                         dtype,
                                                                                         self.data_format))
                except RuntimeError:
                    print("Compression", compression, "not found!")
                    continue

    def dtype_min_max(self, dt):
        dtype = np.dtype(dt)
        if issubclass(dtype.type, np.integer):
            info = np.iinfo(dtype)
        else:
            info = np.finfo(dtype)
        return info.min + 1, info.max - 1

    def test_large_values_gzip(self):
        f = self.root_file
        compression = 'zlib' if f.is_zarr else 'gzip'
        size = np.product(self.shape)
        for dtype in self.dtypes:
            ds_name = "ds_%s" % dtype
            min_val, max_val = self.dtype_min_max(dtype)
            # FIXME this causes an overflow warning for float64
            in_array = np.linspace(min_val, max_val, size).astype('uint8')
            in_array = in_array.reshape(self.shape)
            np.random.shuffle(in_array)
            ds = f.create_dataset(ds_name,
                                  data=in_array,
                                  chunks=(10, 10, 10),
                                  compression=compression)
            out_array = ds[:]
            self.check_array(out_array, in_array,
                             'gzip large values failed for, dtype %s, format %s' % (dtype,
                                                                                    self.data_format))

    def test_small_chunks_gzip(self):
        f = self.root_file
        # 22 is the chunk-size at which gzip compression fails
        # (interestingly this is NOT the case for zlib encoding)
        shape = (22,)
        chunks = shape
        compression = 'zlib' if f.is_zarr else 'gzip'
        for dtype in self.dtypes:
            ds_name = "ds_%s" % dtype
            ds = f.create_dataset(ds_name, shape=shape, chunks=chunks,
                                  compression=compression, dtype=dtype)
            # generate random data scaled to the range of the current datatype
            min_val, max_val = self.dtype_min_max(dtype)
            in_array = np.random.rand(*shape).astype(dtype)
            np.multiply(in_array, max_val, casting='unsafe')
            in_array += min_val

            ds[:] = in_array
            out = ds[:]
            self.check_array(out, in_array,
                             'gzip small chunks failed for, dtype %s, format %s' % (dtype,
                                                                                    self.data_format))


class TestZarrCompression(CompressionTestMixin, unittest.TestCase):
    data_format = 'zarr'


class TestN5Compression(CompressionTestMixin, unittest.TestCase):
    data_format = 'n5'


if __name__ == '__main__':
    unittest.main()
