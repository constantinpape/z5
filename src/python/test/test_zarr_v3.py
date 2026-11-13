"""zarr v3 specific tests (metadata layout, codecs, sharding, chunk-key encoding).

These complement the format-parametrized mixin tests (test_dataset.py etc.) with
checks that only make sense for zarr v3. The whole module is skip-gated on z5
gaining v3 support; the sharding class is additionally gated on sharding support
(which implies crc32c, the default shard-index codec).

This file is the executable specification for the upcoming v3 implementation:
it targets the proposed z5py API (``zarr_format=3`` on ``File``/``ZarrFile`` and
``shards=`` / ``chunk_key_encoding=`` on ``create_dataset``).
"""
import json
import os
import unittest
from shutil import rmtree

import numpy as np
import z5py

from _v3_capability import (available_v3_compressors, requires_z5_v3,
                            requires_z5_v3_sharding)


DTYPES = ['int8', 'int16', 'int32', 'int64',
          'uint8', 'uint16', 'uint32', 'uint64',
          'float32', 'float64']


def _random(shape, dtype):
    if np.issubdtype(np.dtype(dtype), np.floating):
        return np.random.rand(*shape).astype(dtype)
    return np.random.randint(0, 100, size=shape).astype(dtype)


@requires_z5_v3
class TestZarrV3(unittest.TestCase):
    path = 'test_v3.zr'

    def setUp(self):
        self.root = z5py.File(self.path, mode='w', use_zarr_format=True,
                              zarr_format=3, dimension_separator='/')

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    # ------------------------------------------------------------------ utils
    def _read_meta(self, key):
        with open(os.path.join(self.path, key, 'zarr.json')) as f:
            return json.load(f)

    def _roundtrip(self, name, shape, chunks, dtype,
                   compression='raw', **kwargs):
        ds = self.root.create_dataset(name, shape=shape, chunks=chunks,
                                      dtype=dtype, compression=compression,
                                      **kwargs)
        data = _random(shape, dtype)
        ds[:] = data
        out = ds[:]
        self.assertEqual(out.shape, data.shape)
        self.assertTrue(np.allclose(out, data),
                        'roundtrip failed for %s (%s, %s)' % (name, dtype, compression))
        return ds, data

    # --------------------------------------------------------------- metadata
    def test_v3_array_metadata(self):
        self.root.create_dataset('a', shape=(100, 100), chunks=(10, 10),
                                 dtype='float32', compression='gzip')
        meta = self._read_meta('a')
        self.assertEqual(meta['zarr_format'], 3)
        self.assertEqual(meta['node_type'], 'array')
        self.assertEqual(meta['data_type'], 'float32')
        self.assertEqual(meta['shape'], [100, 100])
        self.assertEqual(
            meta['chunk_grid']['configuration']['chunk_shape'], [10, 10])
        codec_names = [c['name'] for c in meta['codecs']]
        self.assertIn('bytes', codec_names)
        self.assertIn('gzip', codec_names)

    def test_v3_array_metadata_raw(self):
        # 'raw' must produce a bytes-only codec pipeline (no compressor)
        self.root.create_dataset('a', shape=(20, 20), chunks=(10, 10),
                                 dtype='uint8', compression='raw')
        codec_names = [c['name'] for c in self._read_meta('a')['codecs']]
        self.assertEqual(codec_names, ['bytes'])

    def test_v3_group_metadata(self):
        g = self.root.create_group('g')
        g.create_group('h')
        for rel in ('', 'g', 'g/h'):
            meta = self._read_meta(rel)
            self.assertEqual(meta['zarr_format'], 3)
            self.assertEqual(meta['node_type'], 'group')

    # ----------------------------------------------------- chunk key encoding
    def test_v3_chunk_key_encoding_default(self):
        ds = self.root.create_dataset('a', shape=(20, 20), chunks=(10, 10),
                                      dtype='uint8', compression='raw')
        ds[:] = 1
        # default v3 encoding: nested under 'c/' with '/' separator
        self.assertTrue(
            os.path.exists(os.path.join(self.path, 'a', 'c', '0', '0')))
        enc = self._read_meta('a')['chunk_key_encoding']
        self.assertEqual(enc['name'], 'default')

    def test_v3_chunk_key_encoding_v2(self):
        ds = self.root.create_dataset('a', shape=(20, 20), chunks=(10, 10),
                                      dtype='uint8', compression='raw',
                                      chunk_key_encoding='v2')
        ds[:] = 1
        # v2 encoding: flat keys joined by '.'
        self.assertTrue(os.path.exists(os.path.join(self.path, 'a', '0.0')))
        self.assertEqual(self._read_meta('a')['chunk_key_encoding']['name'], 'v2')

    # ------------------------------------------------------------------ codecs
    def test_v3_codecs(self):
        for comp in available_v3_compressors():
            for dtype in ('uint8', 'int32', 'float32', 'float64'):
                self._roundtrip('ds_%s_%s' % (comp, dtype), (64, 64), (16, 16),
                                dtype, compression=comp)

    def test_v3_all_dtypes(self):
        for dtype in DTYPES:
            self._roundtrip('ds_%s' % dtype, (50, 50), (10, 10), dtype)

    def test_v3_multidim(self):
        for ndim in range(1, 6):
            size = 40 if ndim < 4 else 12
            shape = (size,) * ndim
            chunks = (10,) * ndim if ndim < 4 else (4,) * ndim
            self._roundtrip('nd_%i' % ndim, shape, chunks, 'float64')

    def test_v3_irregular(self):
        self._roundtrip('irr', (123, 54, 211), (13, 33, 22), 'float64')

    # -------------------------------------------------------------- fill value
    def test_v3_fill_values(self):
        shape, chunks = (40, 40), (10, 10)
        for i, fillval in enumerate([0, 42, np.nan, np.inf, -np.inf]):
            name = 'fv_%i' % i
            ds = self.root.create_dataset(name, shape=shape, chunks=chunks,
                                          dtype='float32', fillvalue=fillval)
            out = ds[:]
            exp = np.full(shape, fillval, dtype='float32')
            self.assertTrue(np.allclose(out, exp, equal_nan=np.isnan(fillval)))
            # v3 encodes special floats as JSON strings
            meta = self._read_meta(name)
            if np.isnan(fillval):
                self.assertEqual(meta['fill_value'], 'NaN')
            elif fillval == np.inf:
                self.assertEqual(meta['fill_value'], 'Infinity')
            elif fillval == -np.inf:
                self.assertEqual(meta['fill_value'], '-Infinity')
            else:
                self.assertEqual(meta['fill_value'], fillval)

    # ------------------------------------------------------------ nested groups
    def test_v3_nested_groups(self):
        data = np.random.rand(20, 20)
        ds = self.root.create_dataset('a/b/c/data', data=data, chunks=(10, 10))
        self.assertTrue(np.allclose(ds[:], data))
        for rel in ('a', 'a/b', 'a/b/c'):
            self.assertEqual(self._read_meta(rel)['node_type'], 'group')

    # --------------------------------------------------------- dimension names
    def test_v3_dimension_names(self):
        ds = self.root.create_dataset('a', shape=(4, 8), chunks=(2, 4),
                                      dtype='float32',
                                      dimension_names=('y', 'x'))
        self.assertEqual(ds.dimension_names, ('y', 'x'))
        self.assertEqual(self._read_meta('a')['dimension_names'], ['y', 'x'])
        # the names survive reopening the dataset
        f = z5py.File(self.path, mode='r')
        self.assertEqual(f['a'].dimension_names, ('y', 'x'))

    def test_v3_dimension_names_unnamed(self):
        ds = self.root.create_dataset('a', shape=(4, 8), chunks=(2, 4),
                                      dtype='float32',
                                      dimension_names=(None, 'x'))
        self.assertEqual(ds.dimension_names, (None, 'x'))
        self.assertEqual(self._read_meta('a')['dimension_names'], [None, 'x'])

    def test_v3_dimension_names_empty_is_not_unnamed(self):
        # "" is a name; only None (null) is an unnamed dimension
        ds = self.root.create_dataset('a', shape=(4, 8), chunks=(2, 4),
                                      dtype='float32',
                                      dimension_names=('', None))
        self.assertEqual(ds.dimension_names, ('', None))
        self.assertEqual(self._read_meta('a')['dimension_names'], ['', None])
        f = z5py.File(self.path, mode='r')
        self.assertEqual(f['a'].dimension_names, ('', None))

    def test_v3_dimension_names_all_unnamed(self):
        # a list of nulls is kept: it differs from no dimension_names at all
        ds = self.root.create_dataset('a', shape=(4, 8), chunks=(2, 4),
                                      dtype='float32',
                                      dimension_names=(None, None))
        self.assertEqual(ds.dimension_names, (None, None))
        self.assertEqual(self._read_meta('a')['dimension_names'], [None, None])

    def test_v3_dimension_names_invalid_metadata(self):
        self.root.create_dataset('a', shape=(4, 8), chunks=(2, 4), dtype='float32')
        meta_path = os.path.join(self.path, 'a', 'zarr.json')
        for bad in ([1, 'x'], 'yx', ['x']):
            meta = self._read_meta('a')
            meta['dimension_names'] = bad
            with open(meta_path, 'w') as f:
                json.dump(meta, f)
            with self.assertRaises(RuntimeError, msg=str(bad)):
                z5py.File(self.path, mode='r')['a']

    def test_v3_dimension_names_default(self):
        ds = self.root.create_dataset('a', shape=(4, 8), chunks=(2, 4),
                                      dtype='float32')
        self.assertIsNone(ds.dimension_names)
        self.assertNotIn('dimension_names', self._read_meta('a'))

    def test_v3_dimension_names_require_dataset(self):
        ds = self.root.require_dataset('a', shape=(4, 8), chunks=(2, 4),
                                       dtype='float32',
                                       dimension_names=('y', 'x'))
        self.assertEqual(ds.dimension_names, ('y', 'x'))

    # --------------------------------------------------------------- rejections
    def test_v3_reject_invalid_shard(self):
        # shard shape must be a multiple of the chunk shape
        with self.assertRaises((ValueError, RuntimeError)):
            self.root.create_dataset('bad_shard', shape=(40, 40),
                                     chunks=(10, 10), shards=(15, 15),
                                     dtype='uint8')

    def test_v3_reject_invalid_dimension_names(self):
        # one name per dimension
        with self.assertRaises(ValueError):
            self.root.create_dataset('bad_names', shape=(40, 40),
                                     chunks=(10, 10), dtype='uint8',
                                     dimension_names=('x',))

    def test_reject_dimension_names_v2(self):
        path = 'test_v2_names.zr'
        try:
            f = z5py.File(path, mode='w', use_zarr_format=True)
            with self.assertRaises(ValueError):
                f.create_dataset('a', shape=(40, 40), chunks=(10, 10),
                                 dtype='uint8', dimension_names=('y', 'x'))
        finally:
            rmtree(path, ignore_errors=True)


@requires_z5_v3
class TestZarrV3Endian(unittest.TestCase):
    """Byte order requested by the v3 ``bytes`` codec (``endian``).

    Datasets are written by z5 (little-endian), then the ``endian`` field in
    zarr.json is flipped without touching the chunk bytes: a correct reader must
    then see every multi-byte element byte-swapped.
    """
    path = 'test_v3_endian.zr'

    def setUp(self):
        self.root = z5py.File(self.path, mode='w', use_zarr_format=True,
                              zarr_format=3, dimension_separator='/')

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    def _meta_path(self, key):
        return os.path.join(self.path, key, 'zarr.json')

    def _bytes_codecs(self, meta):
        codecs = meta['codecs']
        if codecs[0]['name'] == 'sharding_indexed':
            codecs = codecs[0]['configuration']['codecs']
        return [c for c in codecs if c['name'] == 'bytes']

    def _set_endian(self, key, endian):
        with open(self._meta_path(key)) as f:
            meta = json.load(f)
        for codec in self._bytes_codecs(meta):
            if endian is None:
                codec.pop('configuration', None)
            else:
                codec.setdefault('configuration', {})['endian'] = endian
        with open(self._meta_path(key), 'w') as f:
            json.dump(meta, f)

    def _open(self, key, mode='r'):
        return z5py.File(self.path, mode=mode)[key]

    def _check_read_and_write(self, name, dtype, compression='raw', **kwargs):
        shape, chunks = (37, 45), (10, 16)
        data = _random(shape, dtype)
        self.root.create_dataset(name, data=data, chunks=chunks,
                                 compression=compression, **kwargs)

        # read: the little-endian chunk bytes reinterpreted as big-endian
        self._set_endian(name, 'big')
        swapped = data.byteswap()
        out = self._open(name)[:]
        self.assertEqual(out.dtype, np.dtype(dtype))
        np.testing.assert_array_equal(out, swapped)

        # write: z5 must store big-endian bytes, i.e. flipping the metadata back
        # to little-endian exposes the byte-swapped values
        new = _random(shape, dtype)
        ds = self._open(name, mode='a')
        ds[:] = new
        # partial write exercises the read-modify-write of existing chunks
        ds[3:14, 5:30] = data[3:14, 5:30]
        expected = new.copy()
        expected[3:14, 5:30] = data[3:14, 5:30]
        np.testing.assert_array_equal(self._open(name)[:], expected)

        self._set_endian(name, 'little')
        np.testing.assert_array_equal(self._open(name)[:], expected.byteswap())

    def test_v3_big_endian_dtypes(self):
        for dtype in DTYPES:
            self._check_read_and_write('ds_%s' % dtype, dtype)

    def test_v3_big_endian_codecs(self):
        for comp in available_v3_compressors():
            for dtype in ('int16', 'float32', 'float64'):
                self._check_read_and_write('ds_%s_%s' % (comp, dtype), dtype,
                                           compression=comp)

    @requires_z5_v3_sharding
    def test_v3_big_endian_sharded(self):
        for dtype in ('uint16', 'int32', 'float64'):
            self._check_read_and_write('ds_%s' % dtype, dtype, shards=(20, 32))

    def test_v3_big_endian_read_chunk(self):
        data = _random((20, 20), 'uint32')
        self.root.create_dataset('a', data=data, chunks=(10, 10),
                                 compression='raw')
        self._set_endian('a', 'big')
        out = self._open('a').read_chunk((1, 0))
        np.testing.assert_array_equal(out, data[10:20, 0:10].byteswap())

    def test_v3_writes_little_endian(self):
        self.root.create_dataset('a', shape=(10,), chunks=(5,), dtype='int32')
        with open(self._meta_path('a')) as f:
            meta = json.load(f)
        self.assertEqual(self._bytes_codecs(meta)[0]['configuration']['endian'],
                         'little')

    def test_v3_endian_missing(self):
        # 'endian' may be omitted for single-byte types only
        data = _random((10, 10), 'uint8')
        self.root.create_dataset('u8', data=data, chunks=(5, 5))
        self._set_endian('u8', None)
        np.testing.assert_array_equal(self._open('u8')[:], data)

        self.root.create_dataset('i32', shape=(10, 10), chunks=(5, 5),
                                 dtype='int32')
        self._set_endian('i32', None)
        with self.assertRaises(RuntimeError):
            self._open('i32')

    def test_v3_endian_invalid(self):
        self.root.create_dataset('a', shape=(10, 10), chunks=(5, 5),
                                 dtype='int32')
        self._set_endian('a', 'middle')
        with self.assertRaises(RuntimeError):
            self._open('a')


@requires_z5_v3
class TestZarrV3Transpose(unittest.TestCase):
    """The v3 ``transpose`` array-to-array codec.

    A single-chunk dataset is written by z5 (no transpose), then a transpose codec
    is added to zarr.json without touching the chunk bytes: the stored C-order
    buffer must now be read as ``chunk.transpose(order)``, i.e. the logical chunk
    is ``stored.transpose(argsort(order))``.
    """
    path = 'test_v3_transpose.zr'
    shape = (6, 7, 8)

    def setUp(self):
        self.root = z5py.File(self.path, mode='w', use_zarr_format=True,
                              zarr_format=3, dimension_separator='/')

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    def _meta_path(self, key):
        return os.path.join(self.path, key, 'zarr.json')

    def _edit_codecs(self, key, edit):
        """Apply ``edit`` to the (inner, if sharded) codec list of ``key``."""
        with open(self._meta_path(key)) as f:
            meta = json.load(f)
        codecs = meta['codecs']
        if codecs[0]['name'] == 'sharding_indexed':
            codecs = codecs[0]['configuration']['codecs']
        edit(codecs)
        with open(self._meta_path(key), 'w') as f:
            json.dump(meta, f)

    def _add_transpose(self, key, *orders):
        def edit(codecs):
            for order in reversed(orders):
                codecs.insert(0, {'name': 'transpose',
                                  'configuration': {'order': order}})
        self._edit_codecs(key, edit)

    def _drop_transpose(self, key):
        def edit(codecs):
            codecs[:] = [c for c in codecs if c['name'] != 'transpose']
        self._edit_codecs(key, edit)

    def _open(self, key, mode='r'):
        return z5py.File(self.path, mode=mode)[key]

    @staticmethod
    def _as_logical(stored_flat, shape, order):
        stored_shape = tuple(shape[o] for o in order)
        return stored_flat.reshape(stored_shape).transpose(np.argsort(order))

    def _check_read_and_write(self, name, dtype, order, compression='raw',
                              **kwargs):
        data = _random(self.shape, dtype)
        self.root.create_dataset(name, data=data, chunks=self.shape,
                                 compression=compression, **kwargs)

        # read: the stored buffer is interpreted as the transposed chunk
        self._add_transpose(name, list(order))
        np.testing.assert_array_equal(
            self._open(name)[:], self._as_logical(data.ravel(), self.shape, order))

        # write: full and partial writes are stored transposed
        new = _random(self.shape, dtype)
        ds = self._open(name, mode='a')
        ds[:] = new
        ds[1:4, 2:6, 3:7] = data[1:4, 2:6, 3:7]
        expected = new.copy()
        expected[1:4, 2:6, 3:7] = data[1:4, 2:6, 3:7]
        np.testing.assert_array_equal(self._open(name)[:], expected)

        self._drop_transpose(name)
        np.testing.assert_array_equal(
            self._open(name)[:],
            expected.transpose(order).ravel().reshape(self.shape))

    def test_v3_transpose_orders(self):
        for i, order in enumerate([(0, 1, 2), (2, 1, 0), (2, 0, 1), (1, 2, 0)]):
            self._check_read_and_write('ds_%i' % i, 'float32', order)

    def test_v3_transpose_dtypes(self):
        for dtype in DTYPES:
            self._check_read_and_write('ds_%s' % dtype, dtype, (1, 2, 0))

    def test_v3_transpose_codecs(self):
        for comp in available_v3_compressors():
            self._check_read_and_write('ds_%s' % comp, 'int32', (2, 0, 1),
                                       compression=comp)

    @requires_z5_v3_sharding
    def test_v3_transpose_sharded(self):
        self._check_read_and_write('ds', 'uint16', (2, 0, 1),
                                   shards=self.shape)

    def test_v3_transpose_big_endian(self):
        data = _random(self.shape, 'int32')
        self.root.create_dataset('a', data=data, chunks=self.shape)
        self._add_transpose('a', [1, 2, 0])

        def make_big(codecs):
            for codec in codecs:
                if codec['name'] == 'bytes':
                    codec['configuration']['endian'] = 'big'
        self._edit_codecs('a', make_big)
        expected = self._as_logical(data.byteswap().ravel(), self.shape, (1, 2, 0))
        np.testing.assert_array_equal(self._open('a')[:], expected)

    def test_v3_transpose_composed(self):
        # transpose(p1) then transpose(p2) == transpose(p1[p2])
        data = _random(self.shape, 'uint8')
        self.root.create_dataset('a', data=data, chunks=self.shape)
        p1, p2 = (1, 2, 0), (2, 0, 1)
        self._add_transpose('a', list(p1), list(p2))
        composed = tuple(p1[i] for i in p2)
        np.testing.assert_array_equal(
            self._open('a')[:], self._as_logical(data.ravel(), self.shape, composed))

    def test_v3_transpose_legacy_strings(self):
        data = _random(self.shape, 'uint8')
        self.root.create_dataset('c', data=data, chunks=self.shape)
        self._add_transpose('c', 'C')
        np.testing.assert_array_equal(self._open('c')[:], data)

        self.root.create_dataset('f', data=data, chunks=self.shape)
        self._add_transpose('f', 'F')
        np.testing.assert_array_equal(
            self._open('f')[:], self._as_logical(data.ravel(), self.shape, (2, 1, 0)))

    def test_v3_transpose_invalid(self):
        bad_orders = [[0, 1], [0, 1, 1], [0, 1, 3], [-1, 0, 1], 'X']
        for i, order in enumerate(bad_orders):
            name = 'bad_%i' % i
            self.root.create_dataset(name, shape=self.shape, chunks=self.shape,
                                     dtype='uint8')
            self._add_transpose(name, order)
            with self.assertRaises(RuntimeError, msg=str(order)):
                self._open(name)

    def test_v3_transpose_after_bytes(self):
        self.root.create_dataset('a', shape=self.shape, chunks=self.shape,
                                 dtype='uint8')
        self._edit_codecs('a', lambda codecs: codecs.append(
            {'name': 'transpose', 'configuration': {'order': [2, 1, 0]}}))
        with self.assertRaises(RuntimeError):
            self._open('a')

    # ------------------------------------------------- creation via transpose=
    def _codec_names(self, key):
        with open(self._meta_path(key)) as f:
            codecs = json.load(f)['codecs']
        if codecs[0]['name'] == 'sharding_indexed':
            codecs = codecs[0]['configuration']['codecs']
        return codecs

    def test_v3_create_transpose(self):
        order = (2, 0, 1)
        data = _random(self.shape, 'int32')
        ds = self.root.create_dataset('a', data=data, chunks=self.shape,
                                      compression='raw', transpose=order)
        np.testing.assert_array_equal(ds[:], data)
        codecs = self._codec_names('a')
        self.assertEqual(codecs[0], {'name': 'transpose',
                                     'configuration': {'order': list(order)}})
        self.assertEqual(codecs[1]['name'], 'bytes')
        # the single chunk is stored as data.transpose(order) in C order
        with open(os.path.join(self.path, 'a', 'c', '0', '0', '0'), 'rb') as f:
            stored = np.frombuffer(f.read(), dtype='int32')
        np.testing.assert_array_equal(stored, data.transpose(order).ravel())

    def test_v3_create_transpose_multichunk(self):
        for comp in available_v3_compressors():
            data = _random((23, 37, 18), 'float64')
            ds = self.root.create_dataset('a_%s' % comp, data=data,
                                          chunks=(10, 16, 8), compression=comp,
                                          transpose=[1, 2, 0])
            ds[5:19, 3:30, 2:17] = 0
            data[5:19, 3:30, 2:17] = 0
            np.testing.assert_array_equal(
                z5py.File(self.path, 'r')['a_%s' % comp][:], data)

    @requires_z5_v3_sharding
    def test_v3_create_transpose_sharded(self):
        data = _random((40, 40), 'uint16')
        ds = self.root.create_dataset('a', data=data, chunks=(10, 10),
                                      shards=(20, 20), transpose=(1, 0))
        np.testing.assert_array_equal(ds[:], data)
        self.assertEqual(self._codec_names('a')[0]['name'], 'transpose')

    def test_v3_transpose_property(self):
        ds = self.root.create_dataset('t', shape=self.shape, chunks=self.shape,
                                      dtype='uint8', transpose=[2, 0, 1])
        self.assertEqual(ds.transpose, (2, 0, 1))
        self.assertEqual(z5py.File(self.path, 'r')['t'].transpose, (2, 0, 1))

        # identity / no transpose codec
        for name, kwargs in (('i', {'transpose': (0, 1, 2)}), ('n', {})):
            ds = self.root.create_dataset(name, shape=self.shape, chunks=self.shape,
                                          dtype='uint8', **kwargs)
            self.assertIsNone(ds.transpose)

        # read from metadata: legacy "F" and composed codecs
        self.root.create_dataset('f', shape=self.shape, chunks=self.shape, dtype='uint8')
        self._add_transpose('f', 'F')
        self.assertEqual(self._open('f').transpose, (2, 1, 0))
        self.root.create_dataset('c', shape=self.shape, chunks=self.shape, dtype='uint8')
        self._add_transpose('c', [1, 0, 2], [0, 2, 1])
        # composed order: p1[p2[i]] -> (1, 2, 0)
        self.assertEqual(self._open('c').transpose, (1, 2, 0))
        # (1, 2, 0) followed by (2, 0, 1) cancels out -> no transpose
        self.root.create_dataset('x', shape=self.shape, chunks=self.shape, dtype='uint8')
        self._add_transpose('x', [1, 2, 0], [2, 0, 1])
        self.assertIsNone(self._open('x').transpose)

    @requires_z5_v3_sharding
    def test_v3_transpose_property_sharded(self):
        ds = self.root.create_dataset('a', shape=(40, 40), chunks=(10, 10),
                                      shards=(20, 20), dtype='uint8', transpose=(1, 0))
        self.assertEqual(ds.transpose, (1, 0))
        self.assertEqual(z5py.File(self.path, 'r')['a'].transpose, (1, 0))

    def test_v3_create_transpose_identity(self):
        self.root.create_dataset('a', shape=self.shape, chunks=self.shape,
                                 dtype='uint8', transpose=(0, 1, 2))
        self.assertNotIn('transpose', [c['name'] for c in self._codec_names('a')])

    def test_v3_create_transpose_require(self):
        data = _random(self.shape, 'uint8')
        ds = self.root.require_dataset('a', shape=self.shape, dtype='uint8',
                                       chunks=self.shape, transpose=(2, 1, 0))
        ds[:] = data
        self.assertEqual(self._codec_names('a')[0]['name'], 'transpose')
        np.testing.assert_array_equal(self.root['a'][:], data)

    def test_v3_create_transpose_invalid(self):
        for i, order in enumerate([(0, 1), (0, 1, 1), (0, 1, 3)]):
            with self.assertRaises(ValueError, msg=str(order)):
                self.root.create_dataset('bad_%i' % i, shape=self.shape,
                                         dtype='uint8', transpose=order)

    def test_v3_create_transpose_not_v3(self):
        path = 'test_v2_transpose.zr'
        try:
            f = z5py.File(path, mode='w', use_zarr_format=True)
            with self.assertRaises(ValueError):
                f.create_dataset('a', shape=(10, 10), dtype='uint8',
                                 transpose=(1, 0))
            ds = f.create_dataset('b', shape=(10, 10), dtype='uint8')
            self.assertIsNone(ds.transpose)
        finally:
            rmtree(path, ignore_errors=True)

    @requires_z5_v3_sharding
    def test_v3_transpose_outside_sharding(self):
        # a transpose of the whole shard is not supported and must not be ignored
        self.root.create_dataset('a', shape=self.shape, chunks=(3, 7, 8),
                                 shards=self.shape, dtype='uint8')
        with open(self._meta_path('a')) as f:
            meta = json.load(f)
        meta['codecs'].insert(0, {'name': 'transpose',
                                  'configuration': {'order': [2, 1, 0]}})
        with open(self._meta_path('a'), 'w') as f:
            json.dump(meta, f)
        with self.assertRaises(RuntimeError):
            self._open('a')


@requires_z5_v3
class TestZarrV3Validation(unittest.TestCase):
    """Arrays whose metadata z5 can't honour must fail to open, not be misread.

    Each test writes a valid dataset with z5, edits its zarr.json by hand and
    checks that opening it is refused (or still works, for allowed edits).
    """
    path = 'test_v3_validation.zr'
    shape = (16, 16)
    chunks = (8, 8)

    def setUp(self):
        self.root = z5py.File(self.path, mode='w', use_zarr_format=True,
                              zarr_format=3, dimension_separator='/')

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    def _create(self, key='data', **kwargs):
        kwargs.setdefault('compression', 'gzip')
        ds = self.root.create_dataset(key, shape=self.shape, chunks=self.chunks,
                                      dtype='int32', **kwargs)
        data = np.arange(np.prod(self.shape), dtype='int32').reshape(self.shape)
        ds[:] = data
        return data

    def _meta_path(self, key):
        return os.path.join(self.path, key, 'zarr.json')

    def _edit(self, edit, key='data'):
        with open(self._meta_path(key)) as f:
            meta = json.load(f)
        edit(meta)
        with open(self._meta_path(key), 'w') as f:
            json.dump(meta, f)

    def _open(self, key='data', mode='r'):
        return z5py.File(self.path, mode=mode)[key]

    def _assert_refused(self, edit, **kwargs):
        self._create(**kwargs)
        self._edit(edit)
        with self.assertRaises(RuntimeError):
            self._open()

    def _assert_accepted(self, edit, **kwargs):
        data = self._create(**kwargs)
        self._edit(edit)
        np.testing.assert_array_equal(self._open()[:], data)

    def _set_codecs(self, *names):
        """Replace the (unsharded) codec list by ``names`` with default configs."""
        configs = {'bytes': {'endian': 'little'}, 'gzip': {'level': 5},
                   'transpose': {'order': [1, 0]}}

        def edit(meta):
            meta['codecs'] = [{'name': n, 'configuration': configs[n]} if n in configs
                              else {'name': n} for n in names]
        return edit

    def test_baseline_opens(self):
        self._assert_accepted(lambda meta: None)

    def test_chunk_grid_name(self):
        def edit(meta):
            meta['chunk_grid']['name'] = 'rectilinear'
        self._assert_refused(edit)

    def test_chunk_key_encoding_name(self):
        def edit(meta):
            meta['chunk_key_encoding']['name'] = 'custom'
        self._assert_refused(edit)

    def test_chunk_key_separator(self):
        def edit(meta):
            meta['chunk_key_encoding']['configuration']['separator'] = '-'
        self._assert_refused(edit)

    def test_unknown_field(self):
        def edit(meta):
            meta['custom_extension'] = {'name': 'x'}
        self._assert_refused(edit)

    def test_unknown_field_not_an_object(self):
        def edit(meta):
            meta['custom_extension'] = 'x'
        self._assert_refused(edit)

    def test_unknown_field_must_understand(self):
        def edit(meta):
            meta['custom_extension'] = {'name': 'x', 'must_understand': True}
        self._assert_refused(edit)

    def test_unknown_field_may_be_ignored(self):
        def edit(meta):
            meta['custom_extension'] = {'name': 'x', 'must_understand': False}
        self._assert_accepted(edit)

    def test_dimension_names_accepted(self):
        def edit(meta):
            meta['dimension_names'] = ['y', None]
        self._assert_accepted(edit)

    def test_storage_transformers(self):
        def edit(meta):
            meta['storage_transformers'] = [{'name': 'chunk-manifest-json'}]
        self._assert_refused(edit)

    def test_empty_storage_transformers(self):
        def edit(meta):
            meta['storage_transformers'] = []
        self._assert_accepted(edit)

    def test_codecs_compressor_before_bytes(self):
        self._assert_refused(self._set_codecs('gzip', 'bytes'))

    def test_codecs_without_bytes(self):
        self._assert_refused(self._set_codecs('gzip'))

    def test_codecs_empty(self):
        self._assert_refused(self._set_codecs())

    def test_codecs_two_bytes(self):
        self._assert_refused(self._set_codecs('bytes', 'bytes', 'gzip'))

    def test_codecs_two_compressors(self):
        # z5 applies a single compressor: the second one must not be dropped
        self._assert_refused(self._set_codecs('bytes', 'gzip', 'gzip'))

    def test_codecs_transpose_after_compressor(self):
        self._assert_refused(self._set_codecs('bytes', 'gzip', 'transpose'))

    def test_codecs_unknown(self):
        self._assert_refused(self._set_codecs('bytes', 'gzip', 'crc32c'))

    def test_attribute_write_keeps_unknown_fields(self):
        self._create()
        extension = {'name': 'x', 'must_understand': False, 'configuration': {'a': 1}}

        def edit(meta):
            meta['custom_extension'] = extension
            meta['dimension_names'] = ['y', 'x']
        self._edit(edit)

        ds = self._open(mode='a')
        ds.attrs['answer'] = 42
        with open(self._meta_path('data')) as f:
            meta = json.load(f)
        self.assertEqual(meta['custom_extension'], extension)
        self.assertEqual(meta['dimension_names'], ['y', 'x'])
        self.assertEqual(meta['attributes'], {'answer': 42})


@requires_z5_v3_sharding
class TestZarrV3ShardValidation(TestZarrV3Validation):
    """The same checks for the shard index and the codecs inside a shard."""
    path = 'test_v3_shard_validation.zr'

    def _create(self, key='data', **kwargs):
        kwargs.setdefault('shards', (16, 16))
        return super()._create(key, **kwargs)

    @staticmethod
    def _sharding(meta):
        return meta['codecs'][0]['configuration']

    def _set_codecs(self, *names):
        outer = super()._set_codecs(*names)

        def edit(meta):
            inner = {}
            outer(inner)
            self._sharding(meta)['codecs'] = inner['codecs']
        return edit

    def test_index_location_end(self):
        def edit(meta):
            self._sharding(meta)['index_location'] = 'end'
        self._assert_accepted(edit)

    def test_index_location_start(self):
        def edit(meta):
            self._sharding(meta)['index_location'] = 'start'
        self._assert_refused(edit)

    def test_index_without_crc32c(self):
        def edit(meta):
            self._sharding(meta)['index_codecs'] = [
                {'name': 'bytes', 'configuration': {'endian': 'little'}}]
        self._assert_refused(edit)

    def test_index_big_endian(self):
        def edit(meta):
            self._sharding(meta)['index_codecs'][0]['configuration']['endian'] = 'big'
        self._assert_refused(edit)


@requires_z5_v3_sharding
class TestZarrV3Sharding(unittest.TestCase):
    path = 'test_v3_shard.zr'

    def setUp(self):
        self.root = z5py.File(self.path, mode='w', use_zarr_format=True,
                              zarr_format=3, dimension_separator='/')

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    def _read_meta(self, key):
        with open(os.path.join(self.path, key, 'zarr.json')) as f:
            return json.load(f)

    def _roundtrip(self, name, shape, chunks, shards, dtype, compression='raw'):
        ds = self.root.create_dataset(name, shape=shape, chunks=chunks,
                                      shards=shards, dtype=dtype,
                                      compression=compression)
        data = _random(shape, dtype)
        ds[:] = data
        self.assertTrue(np.allclose(ds[:], data),
                        'sharded roundtrip failed for %s' % name)
        return ds, data

    def test_sharding_metadata(self):
        self.root.create_dataset('a', shape=(64, 64), chunks=(16, 16),
                                 shards=(32, 32), dtype='float32',
                                 compression='gzip')
        meta = self._read_meta('a')
        # the outer chunk grid is the shard shape; the inner chunk shape lives
        # in the sharding codec configuration
        self.assertEqual(
            meta['chunk_grid']['configuration']['chunk_shape'], [32, 32])
        codec_names = [c['name'] for c in meta['codecs']]
        self.assertIn('sharding_indexed', codec_names)
        sharding = next(c for c in meta['codecs']
                        if c['name'] == 'sharding_indexed')
        self.assertEqual(sharding['configuration']['chunk_shape'], [16, 16])
        index_codecs = [c['name']
                        for c in sharding['configuration']['index_codecs']]
        self.assertIn('crc32c', index_codecs)

    def test_sharding_single_file_per_shard(self):
        ds = self.root.create_dataset('a', shape=(64, 64), chunks=(16, 16),
                                      shards=(32, 32), dtype='uint8',
                                      compression='raw')
        ds[:] = 1
        # one file per shard (2x2 shards), nested under 'c/'
        self.assertTrue(
            os.path.exists(os.path.join(self.path, 'a', 'c', '0', '0')))

    def test_sharding_codecs(self):
        for comp in available_v3_compressors():
            self._roundtrip('s_%s' % comp, (64, 64), (16, 16), (32, 32),
                            'float32', compression=comp)

    def test_sharding_partial(self):
        # shape not a multiple of the shard shape -> partial outer shards
        self._roundtrip('partial', (100, 100), (10, 10), (40, 40), 'float64')

    def test_sharding_3d(self):
        self._roundtrip('d3', (40, 40, 40), (10, 10, 10), (20, 20, 20),
                        'float32')

    def test_sharding_all_dtypes(self):
        for dtype in DTYPES:
            self._roundtrip('sd_%s' % dtype, (32, 32), (8, 8), (16, 16), dtype)

    def test_sharding_scalar_write(self):
        # regression: writeScalar was not shard-aware and sized its buffer by
        # the clipped chunk shape (heap overflow on zarr edge chunks)
        ds = self.root.create_dataset('scalar', shape=(50, 50), chunks=(8, 8),
                                      shards=(16, 16), dtype='int32')
        ds[:] = 3
        self.assertTrue(np.allclose(ds[:], 3))
        # partial scalar write into existing shards/edge chunks
        ds[10:30, 10:30] = 5
        expected = np.full((50, 50), 3, dtype='int32')
        expected[10:30, 10:30] = 5
        self.assertTrue(np.allclose(ds[:], expected))

    def test_sharding_read_only_write_raises(self):
        # regression: the sharded write path bypassed writeChunk and with it the
        # file-mode check, so writes on a read-only dataset silently succeeded
        ds, data = self._roundtrip('ro', (32, 32), (8, 8), (16, 16), 'uint8')
        f = z5py.File(self.path, mode='r')
        ds_ro = f['ro']
        with self.assertRaises((RuntimeError, ValueError)):
            ds_ro[:16, :16] = np.ones((16, 16), dtype='uint8')
        with self.assertRaises((RuntimeError, ValueError)):
            ds_ro._impl.remove_chunk((0, 0))
        # the data must be unchanged
        self.assertTrue(np.allclose(ds_ro[:], data))

    def test_sharding_corrupt_shard_raises(self):
        # regression: a corrupt shard index was treated as an empty shard by the
        # write read-modify-write path, silently discarding all other inner chunks
        ds, _ = self._roundtrip('corrupt', (32, 32), (8, 8), (16, 16), 'uint8')
        shard_file = os.path.join(self.path, 'corrupt', 'c', '0', '0')
        self.assertTrue(os.path.exists(shard_file))
        # truncate the shard so index + checksum are damaged
        with open(shard_file, 'r+b') as fh:
            fh.truncate(max(os.path.getsize(shard_file) - 5, 1))
        with self.assertRaises(RuntimeError):
            ds[:8, :8]
        with self.assertRaises(RuntimeError):
            ds[:8, :8] = np.ones((8, 8), dtype='uint8')


if __name__ == '__main__':
    unittest.main()
