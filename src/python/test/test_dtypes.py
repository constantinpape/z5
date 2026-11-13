"""The zarr data types n5 lacks: bool, float16, complex64, complex128 and raw bytes.

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
        # (TypeError from z5py for bool / float16, RuntimeError from C++ else)
        ds = self._create()
        with self.assertRaises((TypeError, RuntimeError)):
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
        # RuntimeError from C++, ValueError from z5py (raw types)
        with test.assertRaises((RuntimeError, ValueError)):
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


#
# complex64, complex128
#

class ComplexMixin(DtypeMixin):
    fill = complex(1.5, -np.inf)

    def values(self, n):
        rng = np.random.RandomState(0)
        return (rng.randn(n) + 1j * rng.randn(n)).astype(self.dtype)

    def _edit_fill(self, value):
        if 'data' not in self.root:
            self._create()
        name = 'zarr.json' if self.zarr_format == 3 else '.zarray'
        meta = self._meta()
        meta['fill_value'] = value
        with open(os.path.join(self.path, 'data', name), 'w') as f:
            json.dump(meta, f)
        return z5py.File(self.path, mode='r')['data']

    def test_complex_metadata(self):
        self._create()
        meta = self._meta()
        name = self.dtype if self.zarr_format == 3 else {'complex64': '<c8', 'complex128': '<c16'}[self.dtype]
        self.assertEqual(meta['data_type'] if self.zarr_format == 3 else meta['dtype'], name)
        # [real, imag], each part as a float fill value
        self.assertEqual(meta['fill_value'], [1.5, '-Infinity'])

    def test_bytes_on_disk(self):
        # little-endian real part, then imaginary part
        ds = self._create(fillvalue=0)
        values = np.array([1 + 2j, -3j, 0.5], dtype=self.dtype)
        ds[:4, :3] = np.tile(values, (4, 1))
        if self.shards is not None:
            return
        name = 'c/0/0' if self.zarr_format == 3 else '0.0'
        with open(os.path.join(self.path, 'data', name), 'rb') as f:
            self.assertEqual(f.read(), values.tobytes() * 4)

    def test_nan_fill_value(self):
        ds = self._create(fillvalue=complex(np.nan, 2))
        self.assertTrue(np.isnan(ds.fillvalue.real))
        self.assertEqual(ds.fillvalue.imag, 2)
        self.assertEqual(self._meta()['fill_value'], ['NaN', 2.0])

    def test_real_fill_value(self):
        ds = self._create(fillvalue=-4)
        self.assertEqual(ds.fillvalue, -4 + 0j)
        self.assertEqual(self._meta()['fill_value'], [-4.0, 0.0])

    def test_fill_value_parts(self):
        for value, expected in [([1, 2], 1 + 2j), (['Infinity', -0.5], complex(np.inf, -.5))]:
            self.assertEqual(self._edit_fill(value).fillvalue, expected, value)
        for value in ([1], [1, 2, 3], ['x', 1], [[1], 2]):
            with self.assertRaises(RuntimeError):
                self._edit_fill(value)


class ComplexV2Mixin(ComplexMixin):
    zarr_format = 2

    def test_lenient_fill_values(self):
        # zarr v2 also takes a number (the real part), and null
        self.assertEqual(self._edit_fill(2.5).fillvalue, 2.5 + 0j)
        self.assertTrue(np.isnan(self._edit_fill(None).fillvalue.real))


class ComplexV3Mixin(V3Mixin, ComplexMixin):

    def test_strict_fill_values(self):
        for value in (2.5, None, 'NaN'):
            with self.assertRaises(RuntimeError):
                self._edit_fill(value)

    def test_hex_fill_value(self):
        # each part has the bits of the float type of half the size
        part = 8 if self.dtype == 'complex64' else 16
        one = '0x3f800000' if part == 8 else '0x3ff0000000000000'
        self.assertEqual(self._edit_fill([one, '0x' + '0' * part]).fillvalue, 1 + 0j)
        with self.assertRaises(RuntimeError):
            self._edit_fill(['0x3c00', 0])

    def test_big_endian(self):
        # a big-endian bytes codec swaps each part on its own
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
        if self.shards is None:
            with open(os.path.join(self.path, 'data', 'c', '0', '0'), 'rb') as f:
                self.assertEqual(f.read(), data[:4, :3].astype(np.dtype(self.dtype).newbyteorder('>')).tobytes())


class TestComplex64V2(ComplexV2Mixin, unittest.TestCase):
    path = 'test_dtypes_complex64_v2.zr'
    dtype = 'complex64'


class TestComplex128V2(ComplexV2Mixin, unittest.TestCase):
    path = 'test_dtypes_complex128_v2.zr'
    dtype = 'complex128'


class TestComplex64V3(ComplexV3Mixin, unittest.TestCase):
    path = 'test_dtypes_complex64_v3.zr'
    dtype = 'complex64'


class TestComplex128V3(ComplexV3Mixin, unittest.TestCase):
    path = 'test_dtypes_complex128_v3.zr'
    dtype = 'complex128'


class TestComplex64V3Sharded(ComplexV3Mixin, unittest.TestCase):
    path = 'test_dtypes_complex64_v3_sharded.zr'
    dtype = 'complex64'
    shards = (8, 6)


class TestComplex128V3Sharded(ComplexV3Mixin, unittest.TestCase):
    path = 'test_dtypes_complex128_v3_sharded.zr'
    dtype = 'complex128'
    shards = (8, 6)


class TestComplexN5(unittest.TestCase):

    def test_refused(self):
        _refused_for_n5(self, 'complex64')
        _refused_for_n5(self, 'complex128')


#
# raw bytes: zarr-python's raw_bytes ("V<n>"), zarr v3 raw bits ("r<bits>")
#

class RawMixin(DtypeMixin):
    dtype = 'V3'
    fill = b'\x01\x02\x03'

    @staticmethod
    def values(n):
        return np.frombuffer(np.random.RandomState(0).bytes(3 * n), dtype='V3').copy()

    def _create(self, key='data', compression='raw', **kwargs):
        # dtype_arg: the name asked for at creation ('V3' or 'r24'); reads as V3
        kwargs.setdefault('fillvalue', self.fill)
        if self.shards is not None:
            kwargs.setdefault('shards', self.shards)
        return self.root.create_dataset(key, shape=self.shape, chunks=self.chunks, dtype=self.dtype_arg,
                                        compression=compression, **kwargs)

    def _edit(self, field, value):
        if 'data' not in self.root:
            self._create()
        name = 'zarr.json' if self.zarr_format == 3 else '.zarray'
        meta = self._meta()
        meta[field] = value
        with open(os.path.join(self.path, 'data', name), 'w') as f:
            json.dump(meta, f)
        return z5py.File(self.path, mode='r')['data']

    def test_raw_metadata(self):
        self._create()
        meta = self._meta()
        self.assertEqual(meta['data_type'] if self.zarr_format == 3 else meta['dtype'], self.json_dtype)
        self.assertEqual(meta['fill_value'], self.json_fill)

    def test_bytes_on_disk(self):
        # the elements' bytes, back to back
        ds = self._create(fillvalue=0)
        values = self.values(3)
        ds[:4, :3] = np.tile(values, (4, 1))
        if self.shards is not None:
            return
        name = 'c/0/0' if self.zarr_format == 3 else '0.0'
        with open(os.path.join(self.path, 'data', name), 'rb') as f:
            self.assertEqual(f.read(), values.tobytes() * 4)

    def test_zero_fill_value(self):
        ds = self._create(fillvalue=0)
        self.assertEqual(bytes(ds.fillvalue), b'\0\0\0')

    def test_element_size_checked(self):
        # numpy would pad or cut bytes of another size; z5py refuses them
        with self.assertRaises(ValueError):
            self._create('short_fill', fillvalue=b'\x01')
        ds = self._create()
        for item in (b'ab', b'abcd'):
            with self.assertRaises(ValueError):
                ds[0, 0] = item
        with self.assertRaises(TypeError):
            ds[0, :2] = np.array([b'ab', b'cd'], dtype='V2')

    def test_bytes_item(self):
        ds = self._create()
        ds[1, 2:4] = b'xyz'
        self.assertEqual([bytes(v) for v in ds[1, 1:5]], [self.fill, b'xyz', b'xyz', self.fill])


class RawV3Mixin(V3Mixin, RawMixin):

    def test_endian_ignored(self):
        # raw elements have no byte order: an endian setting changes nothing
        data = self._data()
        self._create()[:] = data
        meta = self._meta()
        codecs = meta['codecs'][0]['configuration']['codecs'] if self.shards else meta['codecs']
        codecs[0]['configuration'] = {'endian': 'big'}
        self.assert_same(self._edit('codecs', meta['codecs'])[:], data)

    def test_unknown_data_type(self):
        # object data types other than raw_bytes are refused with a clear error
        for value in ({'name': 'fixed_length_utf32', 'configuration': {'length_bytes': 4}},
                      {'name': 'raw_bytes', 'configuration': {'length_bytes': 0}},
                      {'name': 'raw_bytes', 'configuration': {}}, 'r12', 'r0', 'V3', 'r'):
            with self.assertRaises(RuntimeError) as cm:
                self._edit('data_type', value)
            self.assertIn('data_type', str(cm.exception), value)


class TestRawBytesV2(RawMixin, unittest.TestCase):
    path = 'test_dtypes_raw_v2.zr'
    zarr_format = 2
    dtype_arg = 'V3'
    json_dtype = '|V3'
    json_fill = 'AQID'

    def test_raw_bits_in_v2(self):
        # zarr v2 has only |V<n>
        ds = self.root.create_dataset('r', shape=(4,), chunks=(2,), dtype='r24', compression='raw')
        self.assertEqual(ds.dtype, np.dtype('V3'))
        self.assertEqual(self._meta('r')['dtype'], '|V3')

    def test_fill_values(self):
        self.assertEqual(bytes(self._edit('fill_value', None).fillvalue), b'\0\0\0')
        for value in ('AQI=', 'AQIDBA==', '!!'):
            with self.assertRaises(RuntimeError):
                self._edit('fill_value', value)


class TestRawBytesV3(RawV3Mixin, unittest.TestCase):
    path = 'test_dtypes_raw_v3.zr'
    dtype_arg = 'V3'
    json_dtype = {'name': 'raw_bytes', 'configuration': {'length_bytes': 3}}
    json_fill = 'AQID'


class TestRawBytesV3Sharded(RawV3Mixin, unittest.TestCase):
    path = 'test_dtypes_raw_v3_sharded.zr'
    dtype_arg = 'V3'
    json_dtype = {'name': 'raw_bytes', 'configuration': {'length_bytes': 3}}
    json_fill = 'AQID'
    shards = (8, 6)


class TestRawBitsV3(RawV3Mixin, unittest.TestCase):
    path = 'test_dtypes_raw_bits_v3.zr'
    dtype_arg = 'r24'
    json_dtype = 'r24'
    json_fill = [1, 2, 3]

    def test_fill_values(self):
        # a list of the element's byte values
        self.assertEqual(bytes(self._edit('fill_value', [255, 0, 7]).fillvalue), b'\xff\x00\x07')
        for value in ([1, 2], [1, 2, 3, 4], [1, 2, 256], [1, 2, -1], [1, 2, 1.5], 'AQID', None):
            with self.assertRaises(RuntimeError):
                self._edit('fill_value', value)

    def test_names(self):
        for name in ('r0', 'r12'):
            with self.assertRaises(ValueError):
                self.root.create_dataset(name, shape=(4,), dtype=name)
        ds = self.root.create_dataset('r8', shape=(4,), dtype='r8', compression='raw')
        self.assertEqual(ds.dtype, np.dtype('V1'))
        self.assertEqual(self._meta('r8')['data_type'], 'r8')
        ds = self.root.require_dataset('r8', shape=(4,), dtype='r8')
        self.assertEqual(ds.dtype, np.dtype('V1'))


class TestRawBitsV3Sharded(RawV3Mixin, unittest.TestCase):
    path = 'test_dtypes_raw_bits_v3_sharded.zr'
    dtype_arg = 'r24'
    json_dtype = 'r24'
    json_fill = [1, 2, 3]
    shards = (8, 6)


class TestRawN5(unittest.TestCase):

    def test_refused(self):
        _refused_for_n5(self, 'V3')
        _refused_for_n5(self, 'r24')


if __name__ == '__main__':
    unittest.main()
