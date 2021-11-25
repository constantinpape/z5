import os
import unittest
from abc import ABC
from shutil import rmtree

import numpy as np
import z5py

try:
    import zarr
    import numcodecs
except ImportError:
    zarr = None


class ZarrTestMixin(ABC):
    shape = (100, 100)
    chunks = (20, 20)

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    def test_read_zarr_irregular(self):
        shape = (123, 97)
        chunks = (17, 32)
        data = np.random.rand(*shape)
        fz = zarr.open(self.path)
        fz.create_dataset('test', data=data, chunks=chunks)

        f = z5py.File(self.path)
        out = f['test'][:]

        self.assertEqual(data.shape, out.shape)
        self.assertTrue(np.allclose(data, out))

    def test_write_zarr_irregular(self):
        shape = (123, 97)
        chunks = (17, 32)
        data = np.random.rand(*shape)
        f = z5py.File(self.path)
        f.create_dataset('test', data=data, chunks=chunks)

        fz = z5py.File(self.path)
        out = fz['test'][:]

        self.assertEqual(data.shape, out.shape)
        self.assertTrue(np.allclose(data, out))

    def test_read_zarr(self):
        from z5py.dataset import Dataset
        dtypes = list(Dataset._dtype_dict.keys())
        zarr_compressors = {'blosc': numcodecs.Blosc(),
                            'zlib': numcodecs.Zlib(),
                            'raw': None,
                            'bzip2': numcodecs.BZ2()}
        # TODO lz4 compression is currently not compatible with zarr
        # 'lz4': numcodecs.LZ4()}

        # conda-forge version of numcodecs is not up-to-data
        # for python 3.5 and GZip is missing
        # that's why we need to check explicitly here to not fail the test
        if hasattr(numcodecs, 'GZip'):
            zarr_compressors.update({'gzip': numcodecs.GZip()})

        f_zarr = zarr.open(self.path, mode='a')
        f_z5 = z5py.File(self.path, mode='r')
        for dtype in dtypes:
            for compression in zarr_compressors:
                data = np.random.randint(0, 127, size=self.shape).astype(dtype)
                # write the data with zarr
                key = 'test_%s_%s' % (dtype, compression)
                ar = f_zarr.create_dataset(key,
                                           shape=self.shape,
                                           chunks=self.chunks,
                                           dtype=dtype,
                                           compressor=zarr_compressors[compression])
                ar[:] = data
                # read with z5py
                out = z5py.File(self.path)[key][:]
                self.assertEqual(data.shape, out.shape)
                self.assertTrue(np.allclose(data, out))

    def test_write_zarr(self):
        from z5py.dataset import Dataset
        dtypes = list(Dataset._dtype_dict.keys())
        compressions = Dataset.compressors_zarr if self.path.endswith('.zr') else Dataset.compressors_n5

        f_z5 = z5py.File(self.path, mode='a')
        f_zarr = zarr.open(self.path, mode='r')
        for dtype in dtypes:
            for compression in compressions:

                # lz4 compressions are not compatible
                if compression == 'lz4':
                    continue

                data = np.random.randint(0, 127, size=self.shape).astype(dtype)
                key = 'test_%s_%s' % (dtype, compression)
                # write with z5py
                f_z5.create_dataset(key, data=data, compression=compression,
                                    chunks=self.chunks)
                # read the data with zarr
                out = f_zarr[key][:]
                self.assertEqual(data.shape, out.shape)
                self.assertTrue(np.allclose(data, out))

    def test_attributes(self):
        f = zarr.open(self.path)
        test_attrs = {"a": "b", "1": 2, "x": ["y", "z"]}
        attrs = f.attrs
        for k, v in test_attrs.items():
            attrs[k] = v

        f = z5py.File(self.path)
        attrs = f.attrs
        for k, v in test_attrs.items():
            self.assertTrue(k in attrs)
            self.assertEqual(attrs[k], v)


@unittest.skipUnless(zarr, 'Requires zarr package')
class TestZarrZarr(ZarrTestMixin, unittest.TestCase):
    path = 'f.zr'

    # custom fill-value is only supported in zarr format
    def test_fillvalue(self):
        test_values = [0, 10, 42, 255]
        zarr.open(self.path)
        for val in test_values:
            key = 'test_%i' % val
            zarr.open(os.path.join(self.path, key), shape=self.shape,
                      fill_value=val, dtype='<u1')
            out = z5py.File(self.path)[key][:]
            self.assertEqual(self.shape, out.shape)
            self.assertTrue(np.allclose(val, out))


@unittest.skipUnless(zarr, 'Requires zarr package')
class TestZarrN5(ZarrTestMixin, unittest.TestCase):
    path = 'f.n5'


if __name__ == '__main__':
    unittest.main()
