"""The zarr data types n5 lacks: bool, float16.

Each data type runs the same round trip in zarr v2, v3 and v3 sharded, and
has its own class for its fill value rules and on-disk layout. n5 refuses
these types.
"""
import json
import os
import unittest
from shutil import rmtree

import numpy as np
import z5py


class DtypeMixin:
    """Round trip of one data type. Subclasses set ``dtype``, ``fill`` (a
    non-default fill value) and ``values(n)`` (``n`` sample values)."""
    zarr_format = None
    shards = None
    dtype = None
    fill = None
    shape = (9, 7)
    chunks = (4, 3)

    def setUp(self):
        self.root = z5py.File(self.path, mode='w', use_zarr_format=True,
                              zarr_format=self.zarr_format)

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    def _create(self, key='data', compression='raw', **kwargs):
        if self.shards is not None:
            kwargs.setdefault('shards', self.shards)
        kwargs.setdefault('fillvalue', self.fill)
        return self.root.create_dataset(key, shape=self.shape, chunks=self.chunks, dtype=self.dtype,
                                        compression=compression, **kwargs)

    def _data(self):
        return self.values(int(np.prod(self.shape))).reshape(self.shape)

    def _meta(self, key='data'):
        name = 'zarr.json' if self.zarr_format == 3 else '.zarray'
        with open(os.path.join(self.path, key, name)) as f:
            return json.load(f)

    def assert_same(self, a, b, msg=None):
        self.assertEqual(a.dtype, b.dtype, msg)
        np.testing.assert_array_equal(a, b, msg)

    def test_round_trip(self):
        data = self._data()
        compressions = [c for c in ('raw', 'gzip', 'blosc', 'zstd') if z5py.dataset.AVAILABLE_COMPRESSORS[c]]
        for compression in compressions:
            key = 'data_' + compression
            ds = self._create(key, compression=compression)
            self.assertEqual(ds.dtype, np.dtype(self.dtype), compression)
            # a partial write across chunk borders; the rest stays at the fill value
            ds[1:8, 2:] = data[1:8, 2:]
            expected = np.full(self.shape, self.fill, dtype=self.dtype)
            expected[1:8, 2:] = data[1:8, 2:]
            self.assert_same(ds[:], expected, compression)
            self.assert_same(z5py.File(self.path, mode='r')[key][:], expected, compression)
            self.assert_same(ds.read_subarray((0, 0), self.shape), expected, compression)

    def test_fill_value(self):
        ds = self._create()
        fill = ds.fillvalue
        self.assertEqual(fill.dtype, np.dtype(self.dtype))
        self.assert_same(np.asarray(fill), np.asarray(self.fill, dtype=self.dtype))
        self.assert_same(ds[:], np.full(self.shape, self.fill, dtype=self.dtype))
        self.assertEqual(os.listdir(os.path.join(self.path, 'data')), [self._meta_name()])

    def _meta_name(self):
        return 'zarr.json' if self.zarr_format == 3 else '.zarray'

    def test_scalar_write(self):
        ds = self._create()
        value = self.values(3)[1]
        ds[2:5, 1] = value
        self.assert_same(ds[2:5, 1], np.full(3, value, dtype=self.dtype))

    def test_chunks(self):
        ds = self._create()
        data = self._data()
        ds[:] = data
        chunk = ds.read_chunk((0, 0))
        self.assert_same(chunk, data[:4, :3])
        ds.write_chunk((0, 0), data[4:8, 3:6].copy())
        self.assert_same(ds[:4, :3], data[4:8, 3:6])
        self.assertIsNone(self._create('empty').read_chunk((1, 1)))

    def test_write_subarray_needs_dtype(self):
        # the IO is typed: data of another dtype is refused, not reinterpreted
        ds = self._create()
        with self.assertRaises(TypeError):
            ds.write_subarray((0, 0), np.ones((2, 2), dtype='int64'))

    def test_require_dataset(self):
        self._create()[:] = self._data()
        ds = self.root.require_dataset('data', shape=self.shape, dtype=self.dtype)
        self.assert_same(ds[:], self._data())

    def test_create_with_data(self):
        ds = self.root.create_dataset('data', data=self._data(), chunks=self.chunks, compression='raw')
        self.assertEqual(ds.dtype, np.dtype(self.dtype))
        self.assert_same(ds[:], self._data())


class V3Mixin:
    zarr_format = 3

    def test_transpose_crc32c(self):
        data = self._data()
        ds = self._create(transpose=(1, 0), crc32c=True)
        ds[:] = data
        self.assert_same(z5py.File(self.path, mode='r')['data'][:], data)


def _refused_for_n5(test, dtype):
    path = 'test_dtypes_refused.n5'
    try:
        f = z5py.File(path, mode='w', use_zarr_format=False)
        with test.assertRaises(RuntimeError):
            f.create_dataset('data', shape=(4,), chunks=(2,), dtype=dtype)
    finally:
        rmtree(path, ignore_errors=True)


#
# bool
#

class BoolMixin(DtypeMixin):
    dtype = 'bool'
    fill = True

    @staticmethod
    def values(n):
        return np.random.RandomState(0).rand(n) > .5

    def test_bool_metadata(self):
        self._create(fillvalue=True)
        self._create('false', fillvalue=False)
        meta, false_meta = self._meta(), self._meta('false')
        if self.zarr_format == 3:
            self.assertEqual(meta['data_type'], 'bool')
        else:
            self.assertEqual(meta['dtype'], '|b1')
        self.assertIs(meta['fill_value'], True)
        self.assertIs(false_meta['fill_value'], False)

    def test_bytes_on_disk(self):
        # one byte per element, 0 or 1
        ds = self._create(fillvalue=False)
        ds[:4, :3] = np.array([[True, False, True]] * 4)
        if self.shards is not None:
            return
        name = 'c/0/0' if self.zarr_format == 3 else '0.0'
        with open(os.path.join(self.path, 'data', name), 'rb') as f:
            self.assertEqual(f.read(), b'\x01\x00\x01' * 4)

    def test_scalar_truth_value(self):
        # as numpy, any value is written as its truth value
        ds = self._create(fillvalue=False)
        ds[0, 0] = 2
        ds[0, 1] = np.True_
        ds[0, 2] = 0.5
        ds[0, 3] = True
        ds[0, 3] = np.False_
        self.assertEqual(ds[0, :4].tolist(), [True, True, True, False])

    def test_fill_value_truth_value(self):
        # as numpy and zarr-python, the fill value is cast to bool
        self.assertIs(bool(self._create(fillvalue=2).fillvalue), True)
        self.assertIs(self._meta()['fill_value'], True)


class TestBoolV2(BoolMixin, unittest.TestCase):
    path = 'test_dtypes_bool_v2.zr'
    zarr_format = 2

    def _edit_fill(self, value):
        if 'data' not in self.root:
            self._create()
        path = os.path.join(self.path, 'data', '.zarray')
        meta = self._meta()
        meta['fill_value'] = value
        with open(path, 'w') as f:
            json.dump(meta, f)
        return z5py.File(self.path, mode='r')['data']

    def test_lenient_fill_values(self):
        # zarr v2 is read leniently: null is false, and 0 / 1 are accepted
        for value, expected in [(None, False), (0, False), (1, True), (1.0, True)]:
            self.assertIs(bool(self._edit_fill(value).fillvalue), expected, value)
        for value in (2, -1, 'true'):
            with self.assertRaises(RuntimeError):
                self._edit_fill(value)


class TestBoolV3(V3Mixin, BoolMixin, unittest.TestCase):
    path = 'test_dtypes_bool_v3.zr'

    def test_strict_fill_values(self):
        # zarr v3 takes only true / false
        self._create()
        path = os.path.join(self.path, 'data', 'zarr.json')
        for value in (None, 0, 1, 'true'):
            meta = self._meta()
            meta['fill_value'] = value
            with open(path, 'w') as f:
                json.dump(meta, f)
            with self.assertRaises(RuntimeError):
                z5py.File(self.path, mode='r')['data']


class TestBoolV3Sharded(V3Mixin, BoolMixin, unittest.TestCase):
    path = 'test_dtypes_bool_v3_sharded.zr'
    shards = (8, 6)


class TestBoolN5(unittest.TestCase):

    def test_refused(self):
        _refused_for_n5(self, 'bool')


#
# float16
#

# doubles that test the rounding to half precision: ties (to even), the
# subnormal range, the largest half and the overflow threshold, specials
ROUNDING = [0.1, 1 / 3, 1 + 2**-11, 1 + 3 * 2**-11, 2049., 2051., 65504., 65519.99, 65520., 1e6,
            2**-24, 2**-25, 2**-25 + 2**-40, 3 * 2**-26, 2**-14, 2**-14 - 2**-25, 6e-8, 1e-10,
            -0., -2.5e-5, float('inf'), -float('inf')]


class Float16Mixin(DtypeMixin):
    dtype = 'float16'
    fill = -2.5

    @staticmethod
    def values(n):
        values = np.random.RandomState(0).randn(n).astype('float16')
        specials = [np.inf, -0., 6e-8, 65504.][:n]
        values[:len(specials)] = specials
        return values

    def test_float16_metadata(self):
        self._create()
        meta = self._meta()
        self.assertEqual(meta['data_type'] if self.zarr_format == 3 else meta['dtype'],
                         'float16' if self.zarr_format == 3 else '<f2')
        self.assertEqual(meta['fill_value'], -2.5)

    def test_bytes_on_disk(self):
        # the little-endian half-precision bits
        ds = self._create(fillvalue=0)
        values = np.array([1., -2., 0.5], dtype='float16')
        ds[:4, :3] = np.tile(values, (4, 1))
        if self.shards is not None:
            return
        name = 'c/0/0' if self.zarr_format == 3 else '0.0'
        with open(os.path.join(self.path, 'data', name), 'rb') as f:
            self.assertEqual(f.read(), values.astype('<f2').tobytes() * 4)

    def test_scalar_rounding(self):
        # a scalar is rounded to half precision as numpy rounds it
        ds = self._create('round', fillvalue=0)
        for i, value in enumerate(ROUNDING):
            ds[i // 7, i % 7] = value
        expected = np.zeros(self.shape, dtype='float16').ravel()
        expected[:len(ROUNDING)] = np.array(ROUNDING).astype('float16')
        out = ds[:].ravel()
        self.assertEqual(out.view('uint16').tolist(), expected.view('uint16').tolist())

    def test_fill_value_rounding(self):
        for i, value in enumerate(ROUNDING):
            ds = self._create('fill%i' % i, fillvalue=value)
            expected = np.float16(value)
            self.assertEqual(ds.fillvalue.view('uint16'), expected.view('uint16'), value)
            self.assertEqual(ds[0, 0].view('uint16'), expected.view('uint16'), value)

    def test_nan_fill_value(self):
        ds = self._create(fillvalue=np.nan)
        self.assertTrue(np.isnan(ds.fillvalue))
        self.assertEqual(self._meta()['fill_value'], 'NaN')
        self.assertTrue(np.isnan(ds[:]).all())


class TestFloat16V2(Float16Mixin, unittest.TestCase):
    path = 'test_dtypes_float16_v2.zr'
    zarr_format = 2


class TestFloat16V3(V3Mixin, Float16Mixin, unittest.TestCase):
    path = 'test_dtypes_float16_v3.zr'

    def test_hex_fill_value(self):
        # zarr v3 hex fill values are the half's bits, four digits
        self._create()
        path = os.path.join(self.path, 'data', 'zarr.json')
        for value, expected in [('0x3c00', 1.), ('0xfc00', -np.inf), ('0x0001', 2**-24)]:
            meta = self._meta()
            meta['fill_value'] = value
            with open(path, 'w') as f:
                json.dump(meta, f)
            self.assertEqual(z5py.File(self.path, mode='r')['data'].fillvalue, expected, value)
        for value in ('0x3c', '0x00003c00'):
            meta['fill_value'] = value
            with open(path, 'w') as f:
                json.dump(meta, f)
            with self.assertRaises(RuntimeError):
                z5py.File(self.path, mode='r')['data']

    def test_big_endian(self):
        data = self._data()
        self._create()
        path = os.path.join(self.path, 'data', 'zarr.json')
        meta = self._meta()
        meta['codecs'][0]['configuration']['endian'] = 'big'
        with open(path, 'w') as f:
            json.dump(meta, f)
        ds = z5py.File(self.path, mode='a')['data']
        ds[:] = data
        self.assert_same(ds[:], data)
        with open(os.path.join(self.path, 'data', 'c', '0', '0'), 'rb') as f:
            self.assertEqual(f.read(), data[:4, :3].astype('>f2').tobytes())


class TestFloat16V3Sharded(V3Mixin, Float16Mixin, unittest.TestCase):
    path = 'test_dtypes_float16_v3_sharded.zr'
    shards = (8, 6)


class TestFloat16N5(unittest.TestCase):

    def test_refused(self):
        _refused_for_n5(self, 'float16')


if __name__ == '__main__':
    unittest.main()
