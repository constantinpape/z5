"""Fill values that a double can't hold, and fill_value parsing.

z5 used to keep fill values as a double, which rounds int64 / uint64 values
beyond 2^53. These tests write with z5, read the
metadata as JSON, and edit it by hand where needed.
"""
import json
import os
import unittest
from shutil import rmtree

import numpy as np
import z5py


def _bits(value, dtype):
    """The value's bytes in ``dtype``, read as a little-endian unsigned integer."""
    value = np.asarray(value, dtype=dtype)
    return int(value.view('<u%i' % value.itemsize))


# exact integers: the extremes of the 64-bit types and values just past 2^53
EXACT_INTEGERS = [('uint64', 2**64 - 1), ('uint64', 2**53 + 1), ('int64', -2**63),
                  ('int64', 2**63 - 1), ('int64', 2**53 + 1), ('int64', -2**53 - 1),
                  ('int8', -128), ('uint32', 2**32 - 1)]

# NaNs other than the default: payloads, negative, signaling
NAN_PAYLOADS = [('float32', 0x7fc00001), ('float32', 0xffc00000), ('float32', 0x7f800001),
                ('float64', 0x7ff8000000000001), ('float64', 0xfff8000000000000)]


class FillValueMixin:
    shape = (16, 16)
    chunks = (8, 8)
    zarr_format = None
    shards = None

    def setUp(self):
        self.root = z5py.File(self.path, mode='w', use_zarr_format=True,
                              zarr_format=self.zarr_format)

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    def _meta_path(self, key):
        name = 'zarr.json' if self.zarr_format == 3 else '.zarray'
        return os.path.join(self.path, key, name)

    def _read_meta(self, key):
        with open(self._meta_path(key)) as f:
            return json.load(f)

    def _set_fill(self, key, value):
        meta = self._read_meta(key)
        meta['fill_value'] = value
        with open(self._meta_path(key), 'w') as f:
            json.dump(meta, f)

    def _create(self, key, dtype, fillvalue=0):
        kwargs = {}
        if self.shards is not None:
            kwargs['shards'] = self.shards
        return self.root.create_dataset(key, shape=self.shape, chunks=self.chunks, dtype=dtype,
                                        compression='raw', fillvalue=fillvalue, **kwargs)

    def _open(self, key):
        return z5py.File(self.path, mode='a')[key]

    def _assert_fill_bits(self, ds, dtype, bits, msg):
        """The fill value and every element of unwritten chunks have exactly ``bits``."""
        self.assertEqual(ds.fillvalue.dtype, np.dtype(dtype), msg)
        self.assertEqual(_bits(ds.fillvalue, dtype), bits, msg)
        out = ds[:]
        self.assertTrue((out.view('<u%i' % out.itemsize) == bits).all(), msg)

    def test_exact_integers(self):
        for i, (dtype, fv) in enumerate(EXACT_INTEGERS):
            key = 'int%i' % i
            msg = '%s %i' % (dtype, fv)
            ds = self._create(key, dtype, fv)
            self.assertEqual(self._read_meta(key)['fill_value'], fv, msg)
            self.assertEqual(int(ds.fillvalue), fv, msg)

            ds = self._open(key)
            self.assertEqual(int(ds.fillvalue), fv, msg)
            self.assertTrue((ds[:] == fv).all(), msg)

            # a partly written chunk keeps the fill value elsewhere
            ds[:3, :3] = np.zeros((3, 3), dtype=dtype)
            out = self._open(key)[:]
            self.assertTrue((out[:3, :3] == 0).all(), msg)
            out[:3, :3] = fv
            self.assertTrue((out == fv).all(), msg)

    def test_fill_chunks_not_stored(self):
        # chunks that only hold the fill value are not written; this needs the
        # exact value to compare against
        ds = self._create('data', 'uint64', 2**64 - 1)
        ds[:] = np.full(self.shape, 2**64 - 1, dtype='uint64')
        stored = [name for name in os.listdir(os.path.join(self.path, 'data'))
                  if not name.startswith('.') and name != 'zarr.json']
        self.assertEqual(stored, [])

    def test_nan_payloads_dropped(self):
        # every NaN becomes the default NaN, written as "NaN" (as zarr-python does)
        for i, (dtype, bits) in enumerate(NAN_PAYLOADS):
            key = 'nan%i' % i
            msg = '%s %x' % (dtype, bits)
            fv = np.array(bits, dtype='<u%i' % np.dtype(dtype).itemsize).view(dtype)[()]
            ds = self._create(key, dtype, fv)
            default = _bits(np.nan, dtype)
            self._assert_fill_bits(ds, dtype, default, msg)
            self.assertEqual(self._read_meta(key)['fill_value'], 'NaN', msg)
            self._assert_fill_bits(self._open(key), dtype, default, msg)

    def test_plain_floats(self):
        # the default NaN and ordinary values keep their usual notation
        for i, (dtype, fv, expected) in enumerate([('float32', np.nan, 'NaN'),
                                                   ('float64', np.inf, 'Infinity'),
                                                   ('float32', -np.inf, '-Infinity'),
                                                   ('float64', 0.1, 0.1),
                                                   ('float32', 0.5, 0.5),
                                                   ('float64', -0.0, -0.0)]):
            key = 'f%i' % i
            ds = self._create(key, dtype, fv)
            self.assertEqual(self._read_meta(key)['fill_value'], expected, key)
            self._assert_fill_bits(self._open(key), dtype, _bits(fv, dtype), key)
            self.assertEqual(ds.fillvalue.dtype, np.dtype(dtype))

    def test_integer_fill_in_metadata(self):
        # integer fill values are written as JSON integers
        self._create('data', 'int16', -7)
        fill = self._read_meta('data')['fill_value']
        self.assertEqual(fill, -7)
        self.assertIsInstance(fill, int)

    def test_creation_refused(self):
        for dtype, fv in [('int32', np.nan), ('int64', np.inf), ('int8', 300),
                          ('uint8', -1), ('uint64', 2**64), ('int16', 'a')]:
            with self.assertRaises(ValueError, msg='%s %r' % (dtype, fv)):
                self._create('data', dtype, fv)

    def test_metadata_refused(self):
        # out of range for the type, or not a fill value at all
        for dtype, fv in [('int8', 128), ('int8', -129), ('uint8', 256), ('uint8', -1),
                          ('int64', 2**63), ('uint64', 2**64), ('int32', 2.0**31),
                          ('float32', 'NaNs'), ('float32', [1, 2])]:
            self._create('data', dtype)
            self._set_fill('data', fv)
            with self.assertRaises(RuntimeError, msg='%s %r' % (dtype, fv)):
                self._open('data')
            rmtree(os.path.join(self.path, 'data'))


class TestFillValueV2(FillValueMixin, unittest.TestCase):
    path = 'test_fill_value_v2.zr'
    zarr_format = 2

    def test_lenient_reading(self):
        # zarr v2 metadata is read as before, so existing arrays still open:
        # null is NaN for floats and 0 for integers, non-integral values are
        # truncated, a string for an integer type is 0
        for i, (dtype, fv, expected) in enumerate([('float32', None, np.nan),
                                                   ('int32', None, 0),
                                                   ('uint64', None, 0),
                                                   ('int16', 2.7, 2),
                                                   ('int16', -2.7, -2),
                                                   ('int32', 'NaN', 0)]):
            key = 'l%i' % i
            self._create(key, dtype)
            self._set_fill(key, fv)
            ds = self._open(key)
            msg = '%s %r' % (dtype, fv)
            if np.isnan(expected):
                self.assertTrue(np.isnan(ds.fillvalue), msg)
                self.assertTrue(np.isnan(ds[:]).all(), msg)
            else:
                self.assertEqual(ds.fillvalue, expected, msg)
                self.assertTrue((ds[:] == expected).all(), msg)

    def test_hex_refused(self):
        # hex floats are zarr v3 notation
        self._create('data', 'float32')
        self._set_fill('data', '0x3f800000')
        with self.assertRaises(RuntimeError):
            self._open('data')


class TestFillValueV3(FillValueMixin, unittest.TestCase):
    path = 'test_fill_value_v3.zr'
    zarr_format = 3

    def test_hex_floats(self):
        # NaN payloads are dropped: every NaN reads as the default NaN
        for i, (dtype, hexval, bits) in enumerate([('float32', '0x3f800000', 0x3f800000),
                                                   ('float32', '0x3F800000', 0x3f800000),
                                                   ('float32', '0x80000000', 0x80000000),
                                                   ('float32', '0x7fc00000', 0x7fc00000),
                                                   ('float32', '0x7fc00001', 0x7fc00000),
                                                   ('float32', '0xffc00000', 0x7fc00000),
                                                   ('float32', '0x7f800001', 0x7fc00000),
                                                   ('float64', '0x7ff0000000000000',
                                                    0x7ff0000000000000),
                                                   ('float64', '0x7ff8000000000001',
                                                    0x7ff8000000000000),
                                                   ('float64', '0xbff0000000000000',
                                                    0xbff0000000000000)]):
            key = 'h%i' % i
            self._create(key, dtype)
            self._set_fill(key, hexval)
            self._assert_fill_bits(self._open(key), dtype, bits, '%s %s' % (dtype, hexval))

    def test_hex_refused(self):
        # hex needs two digits per byte, and is for floats only
        for dtype, fv in [('float32', '0x3f80'), ('float32', '0x3f8000000'), ('float32', '0x'),
                          ('float64', '0x3f800000'), ('float32', '0x7fc0000g'),
                          ('float32', '0X3F800000'), ('int32', '0x00000001')]:
            self._create('data', dtype)
            self._set_fill('data', fv)
            with self.assertRaises(RuntimeError, msg='%s %r' % (dtype, fv)):
                self._open('data')
            rmtree(os.path.join(self.path, 'data'))

    def test_v3_refused(self):
        # null (the spec requires a value), non-integral numbers and strings
        # for integer types, as zarr-python refuses them
        for dtype, fv in [('float32', None), ('int32', None), ('int32', 1.5), ('int32', 'NaN'),
                          ('uint8', 'Infinity')]:
            self._create('data', dtype)
            self._set_fill('data', fv)
            with self.assertRaises(RuntimeError, msg='%s %r' % (dtype, fv)):
                self._open('data')
            rmtree(os.path.join(self.path, 'data'))

    def test_integral_float_accepted(self):
        self._create('data', 'int32')
        self._set_fill('data', 2.0)
        ds = self._open('data')
        self.assertEqual(ds.fillvalue, 2)
        self.assertTrue((ds[:] == 2).all())


class TestFillValueV3Sharded(TestFillValueV3):
    path = 'test_fill_value_v3_sharded.zr'
    shards = (16, 16)

    def test_fill_chunks_not_stored(self):
        # a shard whose inner chunks all hold the fill value is not written
        ds = self._create('data', 'uint64', 2**64 - 1)
        ds[:] = np.full(self.shape, 2**64 - 1, dtype='uint64')
        self.assertFalse(os.path.exists(os.path.join(self.path, 'data', 'c')))


if __name__ == '__main__':
    unittest.main()
