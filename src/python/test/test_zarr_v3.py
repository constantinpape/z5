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

    # --------------------------------------------------------------- rejections
    def test_v3_reject_invalid_shard(self):
        # shard shape must be a multiple of the chunk shape
        with self.assertRaises((ValueError, RuntimeError)):
            self.root.create_dataset('bad_shard', shape=(40, 40),
                                     chunks=(10, 10), shards=(15, 15),
                                     dtype='uint8')


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
