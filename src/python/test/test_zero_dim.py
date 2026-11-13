"""Zero-dimensional datasets (``shape=()``), for zarr v2 and v3.

A 0-d dataset holds one element in one chunk. The chunk key is "0" for
zarr v2 and the v3 ``v2`` chunk key encoding, and "c" for the v3 ``default``
encoding. n5 has no place for such a chunk, so z5 refuses 0-d n5 datasets.
"""
import json
import os
import unittest
from shutil import rmtree

import numpy as np
import z5py


class ZeroDimMixin:
    zarr_format = None
    chunk_key_encoding = 'default'
    chunk_key = None

    def setUp(self):
        self.root = z5py.File(self.path, mode='w', use_zarr_format=True,
                              zarr_format=self.zarr_format)

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    def _kwargs(self):
        if self.zarr_format == 3:
            return {'chunk_key_encoding': self.chunk_key_encoding}
        return {}

    def _create(self, key='data', dtype='int32', **kwargs):
        kwargs.setdefault('compression', 'raw')
        kwargs.update(self._kwargs())
        return self.root.create_dataset(key, shape=(), dtype=dtype, **kwargs)

    def _stored(self, key='data'):
        return sorted(name for name in os.listdir(os.path.join(self.path, key))
                      if name not in ('.zarray', '.zattrs', 'zarr.json'))

    def test_properties(self):
        ds = self._create()
        self.assertEqual(ds.shape, ())
        self.assertEqual(ds.chunks, ())
        self.assertEqual(ds.ndim, 0)
        self.assertEqual(ds.size, 1)
        self.assertEqual(ds.number_of_chunks, 1)
        with self.assertRaises(TypeError):
            len(ds)

    def test_fill_value(self):
        ds = self._create(fillvalue=7)
        self.assertEqual(ds[()], 7)
        self.assertEqual(self._stored(), [])

    def test_write_read(self):
        ds = self._create(fillvalue=7)
        ds[()] = 42
        self.assertEqual(self._stored(), [self.chunk_key])
        self.assertEqual(z5py.File(self.path, mode='r')['data'][()], 42)
        # the chunk holds the one element
        with open(os.path.join(self.path, 'data', self.chunk_key), 'rb') as f:
            self.assertEqual(np.frombuffer(f.read(), dtype='<i4').tolist(), [42])

    def test_indexing(self):
        # as numpy: ds[()] is a scalar, ds[...] a 0-d array
        ds = self._create(dtype='float64')
        ds[...] = np.array(1.5)
        out = ds[()]
        self.assertIsInstance(out, np.float64)
        self.assertEqual(out, 1.5)
        out = ds[...]
        self.assertIsInstance(out, np.ndarray)
        self.assertEqual(out.shape, ())
        self.assertEqual(out[()], 1.5)
        np.testing.assert_array_equal(np.asarray(ds), np.array(1.5))
        # a scalar is broadcast to the single element
        ds[()] = 2
        self.assertEqual(ds[()], 2.)
        with self.assertRaises(TypeError):
            ds[0]
        with self.assertRaises(ValueError):
            ds[()] = np.zeros(2)

    def test_create_with_data(self):
        ds = self.root.create_dataset('data', data=np.array(3, dtype='int16'), compression='raw',
                                      **self._kwargs())
        self.assertEqual(ds.shape, ())
        self.assertEqual(ds.dtype, np.dtype('int16'))
        self.assertEqual(ds[()], 3)

    def test_require_dataset(self):
        self._create()[()] = 5
        ds = self.root.require_dataset('data', shape=(), dtype='int32')
        self.assertEqual(ds[()], 5)

    def test_chunk_api(self):
        ds = self._create()
        self.assertFalse(ds.chunk_exists(()))
        ds[()] = 9
        self.assertTrue(ds.chunk_exists(()))
        self.assertEqual(ds.read_chunk(()), 9)

    def test_compressors(self):
        compressors = ['gzip']
        if self.zarr_format == 3:
            compressors = [c for c in z5py.dataset.Dataset.compressors_zarr_v3 if c != 'raw']
        for compression in compressors:
            key = 'c_' + compression
            ds = self._create(key, dtype='float32', compression=compression)
            ds[()] = 0.25
            self.assertEqual(z5py.File(self.path, mode='r')[key][()], 0.25, compression)

    def test_metadata(self):
        self._create()
        name = 'zarr.json' if self.zarr_format == 3 else '.zarray'
        with open(os.path.join(self.path, 'data', name)) as f:
            meta = json.load(f)
        self.assertEqual(meta['shape'], [])
        if self.zarr_format == 3:
            self.assertEqual(meta['chunk_grid']['configuration']['chunk_shape'], [])
        else:
            self.assertEqual(meta['chunks'], [])


class TestZeroDimV2(ZeroDimMixin, unittest.TestCase):
    path = 'test_zero_dim_v2.zr'
    zarr_format = 2
    chunk_key = '0'


class TestZeroDimV3(ZeroDimMixin, unittest.TestCase):
    path = 'test_zero_dim_v3.zr'
    zarr_format = 3
    chunk_key = 'c'

    def test_crc32c_transpose(self):
        ds = self._create(crc32c=True, transpose=())
        ds[()] = 11
        self.assertTrue(ds.crc32c)
        self.assertIsNone(ds.transpose)
        self.assertEqual(z5py.File(self.path, mode='r')['data'][()], 11)

    def test_sharding_refused(self):
        # z5 marks unsharded datasets by an empty shard shape, so it can't
        # represent a 0-d shard: creating one is refused, and so is opening
        # one, which would otherwise read the shard as a plain chunk
        with self.assertRaises(ValueError):
            self._create(shards=())
        self._create()
        meta_path = os.path.join(self.path, 'data', 'zarr.json')
        with open(meta_path) as f:
            meta = json.load(f)
        meta['codecs'] = [{'name': 'sharding_indexed',
                           'configuration': {'chunk_shape': [], 'codecs': meta['codecs'],
                                             'index_codecs': [{'name': 'bytes',
                                                               'configuration': {'endian': 'little'}},
                                                              {'name': 'crc32c'}],
                                             'index_location': 'end'}}]
        with open(meta_path, 'w') as f:
            json.dump(meta, f)
        with self.assertRaises(RuntimeError):
            z5py.File(self.path, mode='r')['data']


class TestZeroDimV3FlatKey(ZeroDimMixin, unittest.TestCase):
    path = 'test_zero_dim_v3_flat.zr'
    zarr_format = 3
    chunk_key_encoding = 'v2'
    chunk_key = '0'


class TestZeroDimN5(unittest.TestCase):
    path = 'test_zero_dim.n5'

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    def test_refused(self):
        f = z5py.File(self.path, mode='w', use_zarr_format=False)
        with self.assertRaises(RuntimeError):
            f.create_dataset('data', shape=(), dtype='int32')


if __name__ == '__main__':
    unittest.main()
