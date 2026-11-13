"""Variable-length byte string arrays (zarr-python's ``VariableLengthBytes``).

zarr v3 stores them with data type "variable_length_bytes" and the ``vlen-bytes``
codec in place of ``bytes``; zarr v2 as dtype ``|O`` with a ``vlen-bytes``
filter. The chunk framing is the one of ``vlen-utf8`` (see test_vlen_string.py,
which covers the shared IO paths); the items are raw bytes, and a fill value is
stored base64-encoded. These tests need no zarr-python.
"""
import base64
import gzip
import json
import os
import struct
import unittest
from shutil import rmtree

import numpy as np
import z5py

from test_zarr_v3 import _crc32c


def _vlen(items):
    out = struct.pack('<I', len(items))
    for item in items:
        out += struct.pack('<I', len(item)) + item
    return out


def _unvlen(data):
    count, = struct.unpack_from('<I', data)
    pos, items = 4, []
    for _ in range(count):
        n, = struct.unpack_from('<I', data, pos)
        items.append(data[pos + 4:pos + 4 + n])
        pos += 4 + n
    assert pos == len(data)
    return items


def _object_array(items, shape):
    out = np.empty(len(items), dtype=object)
    out[:] = items
    return out.reshape(shape)


# 3 x 4 in 2 x 2 chunks: NULs (also trailing ones, which numpy's S dtype would
# drop), bytes that aren't UTF-8, an empty item
DATA = _object_array([b'a', b'', b'\x00\xff\x00', b'x' * 300, b'nul\x00', b'\xe9\xfe', b'q', b'r',
                      b's', b't', b'u', b'\x00'], (3, 4))
CHUNKS = (2, 2)


class BytesMixin:
    zarr_format = None

    def setUp(self):
        self.root = z5py.File(self.path, mode='w', use_zarr_format=True, zarr_format=self.zarr_format)

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    def _meta_path(self, key):
        return os.path.join(self.path, key, 'zarr.json' if self.zarr_format == 3 else '.zarray')

    def _chunk_path(self, key, chunk_id):
        if self.zarr_format == 3:
            return os.path.join(self.path, key, 'c', *[str(c) for c in chunk_id])
        return os.path.join(self.path, key, '.'.join(str(c) for c in chunk_id))

    def _read_meta(self, key='data'):
        with open(self._meta_path(key)) as f:
            return json.load(f)

    def _edit_meta(self, edit, key='data'):
        meta = self._read_meta(key)
        edit(meta)
        with open(self._meta_path(key), 'w') as f:
            json.dump(meta, f)

    def _create(self, key='data', shape=DATA.shape, **kwargs):
        kwargs.setdefault('compression', 'raw')
        return self.root.create_dataset(key, shape=shape, chunks=CHUNKS[:len(shape)], dtype=bytes, **kwargs)

    def _open(self, key='data'):
        return z5py.File(self.path, mode='a')[key]

    def _assert_equal(self, out, expected, msg=None):
        self.assertEqual(out.dtype, np.dtype(object), msg)
        self.assertEqual(out.shape, expected.shape, msg)
        self.assertEqual(out.tolist(), expected.tolist(), msg)

    def test_roundtrip(self):
        ds = self._create(fillvalue=b'f\x00')
        self.assertEqual(ds.dtype, np.dtype(object))
        ds[:] = DATA
        reopened = self._open()
        self._assert_equal(reopened[:], DATA)
        self.assertIsInstance(reopened[1, 1], bytes)
        self.assertEqual(reopened[1, 1], b'\xe9\xfe')
        self._assert_equal(reopened[1:, 2:], DATA[1:, 2:])
        # the edge chunk holds the full chunk shape, padded with the fill value
        with open(self._chunk_path('data', (1, 1)), 'rb') as f:
            self.assertEqual(_unvlen(f.read()), [b'u', b'\x00', b'f\x00', b'f\x00'])

    def test_metadata(self):
        self._create(fillvalue=b'\x01\xff')
        meta = self._read_meta()
        self.assertEqual(meta['fill_value'], base64.b64encode(b'\x01\xff').decode())
        if self.zarr_format == 3:
            self.assertEqual(meta['data_type'], 'variable_length_bytes')
            self.assertEqual(meta['codecs'], [{'name': 'vlen-bytes', 'configuration': {}}])
        else:
            self.assertEqual(meta['dtype'], '|O')
            self.assertEqual(meta['filters'], [{'id': 'vlen-bytes'}])
        self.assertEqual(self._open().fillvalue, b'\x01\xff')

    def test_fill_value(self):
        ds = self._create()
        self.assertEqual(ds.fillvalue, b'')
        self.assertEqual(self._read_meta()['fill_value'], '')
        ds = self._create('other', fillvalue=b'--')
        ds[0, 0] = b'x'
        self.assertEqual(self._open('other')[0:2, 0:2].tolist(), [[b'x', b'--'], [b'--', b'--']])
        # a fill value that is not valid base64 is refused
        for bad in ['not base64!', 'YWI', 5]:
            self._edit_meta(lambda meta: meta.update(fill_value=bad), key='other')
            with self.assertRaises(RuntimeError, msg=repr(bad)):
                self._open('other')

    def test_compressed(self):
        ds = self._create(compression='gzip')
        ds[:] = DATA
        self._assert_equal(self._open()[:], DATA)

    def test_input_types(self):
        ds = self._create()
        ds[0, 0] = b'one'
        ds[0, 1] = np.bytes_(b'two')
        ds[1, :] = np.array([b'a', b'bb', b'c', b'd'])  # an S array (drops trailing NULs)
        self.assertEqual(ds[0, :2].tolist(), [b'one', b'two'])
        self.assertEqual(ds[1].tolist(), [b'a', b'bb', b'c', b'd'])
        # str is not bytes
        for bad in ['text', np.array(['a', 'b', 'c', 'd']), np.zeros(4)]:
            with self.assertRaises(TypeError, msg=repr(bad)):
                ds[2, :] = bad
        mixed = DATA.copy()
        mixed[0, 0] = 'text'
        with self.assertRaises(TypeError):
            ds[:] = mixed

    def test_creation(self):
        for i, dtype in enumerate([bytes, 'bytes', np.dtype('S'), 'S']):
            ds = self.root.create_dataset('d%i' % i, shape=(3,), chunks=(2,), dtype=dtype, compression='raw')
            self.assertEqual(ds.dtype, np.dtype(object))
            self.assertEqual(ds.fillvalue, b'')
        ds = self.root.create_dataset('from_data', data=DATA, dtype=bytes, chunks=CHUNKS, compression='raw')
        self._assert_equal(ds[:], DATA)
        # fixed-length S<n> (zarr-python's null_terminated_bytes) isn't supported
        with self.assertRaises(ValueError):
            self.root.create_dataset('fixed', shape=(3,), dtype='S5')
        with self.assertRaises(ValueError):
            self.root.create_dataset('fixed', data=np.array([b'a', b'bc']))
        with self.assertRaises(ValueError):
            self.root.create_dataset('bad_fill', shape=(3,), dtype=bytes, fillvalue='text')
        self.assertEqual(self.root.require_dataset('d0', shape=(3,), dtype=bytes).dtype, np.dtype(object))
        with self.assertRaises(TypeError):
            self.root.require_dataset('d0', shape=(3,), dtype=str)

    def test_chunk_api(self):
        ds = self._create()
        chunk = _object_array([b'a', b'\x00', b'', b'\xff'], (2, 2))
        ds.write_chunk((0, 1), chunk)
        self._assert_equal(ds.read_chunk((0, 1)), chunk)
        self.assertIsNone(ds.read_chunk((1, 1)))

    def test_zero_dim(self):
        ds = self._create(shape=())
        ds[()] = b'\x00only'
        self.assertEqual(self._open()[()], b'\x00only')

    def test_read_hand_written(self):
        # chunks written without z5: gzip-compressed vlen framing
        ds = self._create(compression='gzip')
        ds[:] = DATA
        block = _object_array([b'\xaa', b'\xbb\x00', b'', b'c'], (2, 2))
        with open(self._chunk_path('data', (0, 0)), 'wb') as f:
            f.write(gzip.compress(_vlen(list(block.ravel()))))
        expected = DATA.copy()
        expected[:2, :2] = block
        self._assert_equal(self._open()[:], expected)


class TestBytesV2(BytesMixin, unittest.TestCase):
    path = 'test_vlen_bytes_v2.zr'
    zarr_format = 2

    def test_null_fill_value(self):
        self._create()
        self._edit_meta(lambda meta: meta.update(fill_value=None))
        self.assertEqual(self._open().fillvalue, b'')

    def test_filter_selects_type(self):
        # the same |O array reads as str with vlen-utf8 and as bytes with vlen-bytes
        ds = self._create()
        ds[0, 0] = b'abc'
        self._edit_meta(lambda meta: meta.update(filters=[{'id': 'vlen-utf8'}]))
        self.assertEqual(self._open()[0, 0], 'abc')


class TestBytesV3(BytesMixin, unittest.TestCase):
    path = 'test_vlen_bytes_v3.zr'
    zarr_format = 3

    def test_sharded_crc32c(self):
        ds = self._create(shards=(2, 4), crc32c=True)
        ds[:] = DATA
        self._assert_equal(self._open()[:], DATA)

    def test_codec_must_match(self):
        # vlen-utf8 on a bytes array (and vlen-bytes on a string array) is refused
        self._create()
        self._edit_meta(lambda meta: meta.update(codecs=[{'name': 'vlen-utf8'}]))
        with self.assertRaises(RuntimeError):
            self._open()
        self._edit_meta(lambda meta: meta.update(codecs=[{'name': 'vlen-bytes'}], data_type='string'))
        with self.assertRaises(RuntimeError):
            self._open()

    def test_crc32c_by_hand(self):
        ds = self._create()
        ds[:] = DATA
        self._edit_meta(lambda meta: meta['codecs'].append({'name': 'crc32c'}))
        for chunk_id in np.ndindex(2, 2):
            path = self._chunk_path('data', chunk_id)
            with open(path, 'rb') as f:
                data = f.read()
            with open(path, 'wb') as f:
                f.write(data + struct.pack('<I', _crc32c(data)))
        self._assert_equal(self._open()[:], DATA)


if __name__ == '__main__':
    unittest.main()
