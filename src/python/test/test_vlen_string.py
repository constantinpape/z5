"""Variable-length UTF-8 string arrays (zarr-python's ``str`` arrays).

zarr v3 stores them with data type "string" and the ``vlen-utf8`` codec in place
of ``bytes``; zarr v2 as dtype ``|O`` with a ``vlen-utf8`` filter. A chunk is a
uint32 LE item count, then for each item a uint32 LE byte length and its UTF-8
bytes, compressed as usual. These tests build such arrays by hand (metadata and
chunk files, compressed with the standard library), and decode the chunks z5
writes the same way, so they need no zarr-python.
"""
import bz2
import gzip
import json
import lzma
import os
import struct
import unittest
import zlib
from shutil import rmtree

import numpy as np
import z5py
from z5py.dataset import AVAILABLE_COMPRESSORS, STRING_DTYPE

from test_zarr_v3 import _crc32c


def _vlen(items):
    """The vlen-utf8 framing of ``items``."""
    out = struct.pack('<I', len(items))
    for item in items:
        data = item.encode('utf-8')
        out += struct.pack('<I', len(data)) + data
    return out


# compressors as (v2 compressor, v3 codec, compress function, z5 codec needed)
COMPRESSORS = {
    'raw': (None, None, lambda b: b, None),
    'gzip': ({'id': 'gzip', 'level': 1}, {'name': 'gzip', 'configuration': {'level': 1}},
             gzip.compress, 'zlib'),
    'zlib': ({'id': 'zlib', 'level': 1}, {'name': 'numcodecs.zlib', 'configuration': {'level': 1}},
             zlib.compress, 'zlib'),
    'bz2': ({'id': 'bz2', 'level': 1}, {'name': 'numcodecs.bz2', 'configuration': {'level': 1}},
            bz2.compress, 'bzip2'),
    'lzma': (None, {'name': 'numcodecs.lzma', 'configuration': {}}, lzma.compress, 'xz'),
}


def _unvlen(data):
    """Split a vlen-utf8 chunk into its items."""
    count, = struct.unpack_from('<I', data)
    pos, items = 4, []
    for _ in range(count):
        n, = struct.unpack_from('<I', data, pos)
        items.append(data[pos + 4:pos + 4 + n].decode('utf-8'))
        pos += 4 + n
    assert pos == len(data)
    return items


# z5 compressor name -> stdlib decompress function, for checking written chunks
DECOMPRESS = {'raw': lambda b: b, 'gzip': gzip.decompress, 'zlib': zlib.decompress,
              'bzip2': bz2.decompress, 'xz': lzma.decompress}


def _available(name):
    needed = COMPRESSORS[name][3]
    return needed is None or AVAILABLE_COMPRESSORS.get(needed, False)


# a 3 x 5 array in 2 x 2 chunks: edge chunks, empty strings, non-ASCII text
DATA = np.array([['a', 'héllo', '', 'xyz€', 'q'],
                 ['long ' * 50, 'b', 'c', '日本語', 'z'],
                 ['x', '', 'y', 'tab\there', 'end']], dtype=object)
CHUNKS = (2, 2)


class StringArrayMixin:
    """Write string arrays by hand and read them with z5."""
    zarr_format = None

    def setUp(self):
        os.makedirs(self.path, exist_ok=True)
        if self.zarr_format == 3:
            self._write_json(os.path.join(self.path, 'zarr.json'),
                             {'zarr_format': 3, 'node_type': 'group', 'attributes': {}})
        else:
            self._write_json(os.path.join(self.path, '.zgroup'), {'zarr_format': 2})

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    @staticmethod
    def _write_json(path, obj):
        with open(path, 'w') as f:
            json.dump(obj, f)

    def _metadata(self, shape, chunks, compressor, fill_value):
        if self.zarr_format == 3:
            codecs = [{'name': 'vlen-utf8', 'configuration': {}}]
            if COMPRESSORS[compressor][1] is not None:
                codecs.append(COMPRESSORS[compressor][1])
            return {'zarr_format': 3, 'node_type': 'array', 'shape': list(shape),
                    'data_type': 'string',
                    'chunk_grid': {'name': 'regular', 'configuration': {'chunk_shape': list(chunks)}},
                    'chunk_key_encoding': {'name': 'default', 'configuration': {'separator': '/'}},
                    'fill_value': fill_value, 'codecs': codecs, 'attributes': {}}
        return {'zarr_format': 2, 'shape': list(shape), 'chunks': list(chunks), 'dtype': '|O',
                'fill_value': fill_value, 'order': 'C', 'filters': [{'id': 'vlen-utf8'}],
                'compressor': COMPRESSORS[compressor][0], 'dimension_separator': '.'}

    def _meta_path(self, key):
        return os.path.join(self.path, key, 'zarr.json' if self.zarr_format == 3 else '.zarray')

    def _chunk_path(self, key, chunk_id):
        if self.zarr_format == 3:
            return os.path.join(self.path, key, 'c', *[str(c) for c in chunk_id])
        return os.path.join(self.path, key, '.'.join(str(c) for c in chunk_id) or '0')

    def _write_array(self, key, data, chunks=CHUNKS, compressor='raw', fill_value='', skip=(),
                     encode=None):
        """Write ``data`` chunk by chunk (edge chunks padded with the fill value);
        chunks in ``skip`` are left out."""
        os.makedirs(os.path.join(self.path, key), exist_ok=True)
        self._write_json(self._meta_path(key), self._metadata(data.shape, chunks, compressor, fill_value))
        compress = encode or COMPRESSORS[compressor][2]
        grid = [-(-s // c) for s, c in zip(data.shape, chunks)]
        for chunk_id in np.ndindex(*grid):
            if chunk_id in skip:
                continue
            block = np.full(chunks, fill_value, dtype=object)
            region = tuple(slice(i * c, min((i + 1) * c, s)) for i, c, s in zip(chunk_id, chunks, data.shape))
            block[tuple(slice(0, r.stop - r.start) for r in region)] = data[region]
            path = self._chunk_path(key, chunk_id)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'wb') as f:
                f.write(compress(_vlen(list(block.ravel()))))

    def _open(self, key='data'):
        return z5py.File(self.path, mode='r')[key]

    def _assert_equal(self, out, expected, msg=None):
        self.assertEqual(out.dtype, STRING_DTYPE, msg)
        self.assertEqual(out.shape, expected.shape, msg)
        self.assertEqual(out.tolist(), expected.tolist(), msg)

    def test_compressors(self):
        for compressor in COMPRESSORS:
            if self.zarr_format == 2 and COMPRESSORS[compressor][0] is None and compressor != 'raw':
                continue
            if not _available(compressor):
                continue
            key = 'c_' + compressor
            self._write_array(key, DATA, compressor=compressor)
            ds = self._open(key)
            self.assertEqual(ds.dtype, STRING_DTYPE)
            self._assert_equal(ds[:], DATA, compressor)

    def test_indexing(self):
        self._write_array('data', DATA)
        ds = self._open()
        self.assertEqual(ds.shape, DATA.shape)
        self.assertEqual(ds.chunks, CHUNKS)
        self._assert_equal(ds[1:3, 1:4], DATA[1:3, 1:4])
        self._assert_equal(ds[2], DATA[2])
        self.assertEqual(ds[1, 3], '日本語')
        self.assertIsInstance(ds[1, 3], str)
        self._assert_equal(ds.read_subarray((0, 3), (3, 5)), DATA[:, 3:])
        self._assert_equal(np.asarray(ds), DATA)

    def test_fill_value(self):
        # missing chunks read as the fill value
        self._write_array('data', DATA, fill_value='--', skip={(0, 1), (1, 2)})
        ds = self._open()
        self.assertEqual(ds.fillvalue, '--')
        expected = DATA.copy()
        expected[0:2, 2:4] = '--'
        expected[2:, 4:] = '--'
        self._assert_equal(ds[:], expected)

    def test_read_chunk(self):
        self._write_array('data', DATA, fill_value='f', skip={(0, 0)})
        ds = self._open()
        self.assertIsNone(ds.read_chunk((0, 0)))
        # the edge chunk holds the full chunk shape, padded with the fill value
        self._assert_equal(ds.read_chunk((1, 2)), np.array([['end', 'f'], ['f', 'f']], dtype=object))

    def test_zero_dim(self):
        self._write_array('data', np.array('scalar ünïcode', dtype=object), chunks=())
        ds = self._open()
        self.assertEqual(ds.shape, ())
        self.assertEqual(ds[()], 'scalar ünïcode')

    def test_corrupt_chunks(self):
        # wrong item count, truncated item, bytes after the last item
        for i, encode in enumerate([lambda b: struct.pack('<I', 3) + b[4:],
                                    lambda b: b[:-1],
                                    lambda b: b + b'x']):
            key = 'bad%i' % i
            self._write_array(key, DATA, encode=encode)
            with self.assertRaises(RuntimeError, msg=key):
                self._open(key)[:]

    #
    # writing
    #

    def _create(self, key='data', shape=DATA.shape, **kwargs):
        kwargs.setdefault('compression', 'raw')
        f = z5py.File(self.path, mode='a')
        return f.create_dataset(key, shape=shape, chunks=CHUNKS[:len(shape)], dtype=str, **kwargs)

    def _stored_chunks(self, key):
        found = {}
        for root, _, files in os.walk(os.path.join(self.path, key)):
            for name in files:
                if name not in ('.zarray', '.zattrs', 'zarr.json'):
                    found[os.path.relpath(os.path.join(root, name), os.path.join(self.path, key))] = \
                        os.path.join(root, name)
        return found

    def test_write_chunk_bytes(self):
        # the stored chunks decode with the standard library to the vlen framing
        # of the full (fill-padded) chunk
        for compression, decompress in DECOMPRESS.items():
            if not AVAILABLE_COMPRESSORS.get(compression, compression == 'raw'):
                continue
            if compression == 'xz' and self.zarr_format == 2:
                continue
            key = 'w_' + compression
            ds = self._create(key, compression=compression, fillvalue='f')
            ds[:] = DATA
            # z5py's zarr v2 "gzip" writes the zlib codec (and says so in .zarray)
            if self.zarr_format == 2 and compression == 'gzip':
                decompress = zlib.decompress
            with open(self._chunk_path(key, (1, 2)), 'rb') as fh:
                self.assertEqual(_unvlen(decompress(fh.read())), ['end', 'f', 'f', 'f'], compression)
            with open(self._chunk_path(key, (0, 1)), 'rb') as fh:
                self.assertEqual(_unvlen(decompress(fh.read())), list(DATA[:2, 2:4].ravel()), compression)
            self._assert_equal(self._open(key)[:], DATA, compression)

    def test_metadata_written(self):
        self._create(fillvalue='x')
        with open(self._meta_path('data')) as f:
            meta = json.load(f)
        self.assertEqual(meta['fill_value'], 'x')
        if self.zarr_format == 3:
            self.assertEqual(meta['data_type'], 'string')
            self.assertEqual(meta['codecs'], [{'name': 'vlen-utf8', 'configuration': {}}])
        else:
            self.assertEqual(meta['dtype'], '|O')
            self.assertEqual(meta['filters'], [{'id': 'vlen-utf8'}])

    def test_partial_writes(self):
        # writes across chunk boundaries keep the rest of each chunk
        ds = self._create(fillvalue='-')
        expected = np.full(DATA.shape, '-', dtype=object)
        for region, value in [((slice(0, 3), slice(1, 4)), DATA[:, 1:4]),
                              ((slice(1, 2), slice(0, 5)), np.array(['r%i' % i for i in range(5)], dtype=object)),
                              ((2, 4), 'corner'),
                              ((slice(0, 2), slice(3, 5)), 'bcast')]:
            ds[region] = value
            expected[region] = value
            self._assert_equal(self._open()[:], expected)

    def test_fill_chunks_not_stored(self):
        ds = self._create(fillvalue='-')
        ds[:] = np.full(DATA.shape, '-', dtype=object)
        self.assertEqual(self._stored_chunks('data'), {})
        ds[0, 0] = 'x'
        self.assertEqual(len(self._stored_chunks('data')), 1)
        # writing the fill value back removes the chunk
        ds[0, 0] = '-'
        self.assertEqual(self._stored_chunks('data'), {})

    def test_input_types(self):
        ds = self._create()
        for data in [DATA.astype(str), DATA.astype(STRING_DTYPE), DATA]:
            ds[:] = data
            self._assert_equal(ds[:], DATA)
        ds[0, 0] = np.str_('numpy str')
        self.assertEqual(ds[0, 0], 'numpy str')
        # not text
        for bad in [np.zeros(DATA.shape), np.full(DATA.shape, b'x'), 5]:
            with self.assertRaises(TypeError):
                ds[:] = bad
        bad = DATA.copy()
        bad[1, 1] = 3
        with self.assertRaises(TypeError):
            ds[:] = bad

    def test_creation(self):
        f = z5py.File(self.path, mode='a')
        for i, dtype in enumerate([str, 'str', STRING_DTYPE, object, np.dtype('U')]):
            ds = f.create_dataset('d%i' % i, shape=(3,), chunks=(2,), dtype=dtype, compression='raw')
            self.assertEqual(ds.dtype, STRING_DTYPE)
            self.assertEqual(ds.fillvalue, '')
        # from data; a fixed-length U<n> array needs dtype=str (zarr-python stores
        # U<n> as fixed-length strings, which z5 doesn't support yet)
        ds = f.create_dataset('data', data=DATA.astype(str), dtype=str, chunks=CHUNKS, compression='raw')
        self._assert_equal(ds[:], DATA)
        ds = f.create_dataset('data_t', data=DATA.astype(STRING_DTYPE), chunks=CHUNKS, compression='raw')
        self._assert_equal(ds[:], DATA)
        with self.assertRaises(ValueError):
            f.create_dataset('fixed', data=DATA.astype(str), chunks=CHUNKS)
        with self.assertRaises(ValueError):
            f.create_dataset('fixed', shape=(3,), dtype='U5')
        with self.assertRaises(ValueError):
            f.create_dataset('bad_fill', shape=(3,), dtype=str, fillvalue=1)
        with self.assertRaises(ValueError):
            f.create_dataset('bad_data', data=np.zeros(3), dtype=str)
        # require_dataset matches string data types
        self.assertEqual(f.require_dataset('d0', shape=(3,), dtype=str).dtype, STRING_DTYPE)
        with self.assertRaises(TypeError):
            f.require_dataset('d0', shape=(3,), dtype='int32')

    def test_chunk_api(self):
        ds = self._create(fillvalue='-')
        chunk = np.array([['a', 'b'], ['c', 'é']], dtype=object)
        ds.write_chunk((1, 1), chunk)
        self._assert_equal(ds.read_chunk((1, 1)), chunk)
        self.assertEqual(ds[2, 2:4].tolist(), ['a', 'b'])
        with self.assertRaises(RuntimeError):
            ds.write_chunk((0, 0), np.array(['a', 'b', 'c'], dtype=object))

    def test_multithreaded(self):
        data = np.array(['item %i %s' % (i, 'ü' * (i % 7)) for i in range(40 * 30)], dtype=object)
        data = data.reshape(40, 30)
        f = z5py.File(self.path, mode='a')
        ds = f.create_dataset('data', shape=data.shape, chunks=(7, 4), dtype=str, compression='gzip',
                              n_threads=4)
        ds[:] = data
        ds[5:33, 3:21] = data[:28, :18]
        data[5:33, 3:21] = data[:28, :18].copy()
        self._assert_equal(ds[:], data)

    def test_zero_dim_write(self):
        ds = self._create(shape=())
        ds[()] = 'only'
        self.assertEqual(self._open()[()], 'only')

    def test_n5_and_transpose_refused(self):
        with self.assertRaises(ValueError):
            z5py.File(self.path + '.n5', mode='w', use_zarr_format=False).create_dataset(
                'data', shape=(3,), dtype=str)
        rmtree(self.path + '.n5')
        if self.zarr_format == 3:
            with self.assertRaises(ValueError):
                self._create(transpose=(1, 0))

    def _assert_refused(self, edit, key='data'):
        self._write_array(key, DATA)
        with open(self._meta_path(key)) as f:
            meta = json.load(f)
        edit(meta)
        self._write_json(self._meta_path(key), meta)
        with self.assertRaises(RuntimeError):
            self._open(key)
        rmtree(os.path.join(self.path, key))


class TestStringV2(StringArrayMixin, unittest.TestCase):
    path = 'test_vlen_string_v2.zr'
    zarr_format = 2

    def test_null_fill_value(self):
        # zarr v2 object arrays may have no fill value: read as ""
        self._write_array('data', DATA, skip={(0, 0)})
        with open(self._meta_path('data')) as f:
            meta = json.load(f)
        meta['fill_value'] = None
        self._write_json(self._meta_path('data'), meta)
        ds = self._open()
        self.assertEqual(ds.fillvalue, '')
        self.assertEqual(ds[0, 0], '')

    def test_refused(self):
        def set_filters(filters):
            return lambda meta: meta.update(filters=filters)
        # no filter, another filter, extra settings, a numeric fill value
        for edit in [set_filters(None), set_filters([]), set_filters([{'id': 'pickle'}]),
                     set_filters([{'id': 'vlen-utf8'}, {'id': 'vlen-utf8'}]),
                     set_filters([{'id': 'vlen-utf8', 'x': 1}]),
                     lambda meta: meta.update(fill_value=0)]:
            self._assert_refused(edit)


class TestStringV3(StringArrayMixin, unittest.TestCase):
    path = 'test_vlen_string_v3.zr'
    zarr_format = 3

    def test_crc32c(self):
        def with_checksum(data):
            return data + struct.pack('<I', _crc32c(data))
        self._write_array('data', DATA, encode=with_checksum)
        with open(self._meta_path('data')) as f:
            meta = json.load(f)
        meta['codecs'].append({'name': 'crc32c'})
        self._write_json(self._meta_path('data'), meta)
        self._assert_equal(self._open()[:], DATA)

    def test_sharded(self):
        # one 3 x 5 shard of 2 x 2 inner chunks (2 x 3 slots), index at the end
        # with its crc32c; slot (1, 2) is empty
        key = 'sharded'
        os.makedirs(os.path.join(self.path, key))
        meta = self._metadata((3, 5), (4, 6), 'raw', '')
        meta['codecs'] = [{'name': 'sharding_indexed', 'configuration': {
            'chunk_shape': [2, 2], 'codecs': [{'name': 'vlen-utf8'}],
            'index_codecs': [{'name': 'bytes', 'configuration': {'endian': 'little'}}, {'name': 'crc32c'}],
            'index_location': 'end'}}]
        self._write_json(self._meta_path(key), meta)
        blobs, index = b'', b''
        for chunk_id in np.ndindex(2, 3):
            if chunk_id == (1, 2):
                index += struct.pack('<QQ', 2**64 - 1, 2**64 - 1)
                continue
            block = np.full((2, 2), '', dtype=object)
            region = tuple(slice(i * 2, min((i + 1) * 2, s)) for i, s in zip(chunk_id, DATA.shape))
            block[tuple(slice(0, r.stop - r.start) for r in region)] = DATA[region]
            blob = _vlen(list(block.ravel()))
            index += struct.pack('<QQ', len(blobs), len(blob))
            blobs += blob
        os.makedirs(os.path.join(self.path, key, 'c', '0'))
        with open(os.path.join(self.path, key, 'c', '0', '0'), 'wb') as f:
            f.write(blobs + index + struct.pack('<I', _crc32c(index)))
        expected = DATA.copy()
        expected[2:, 4:] = ''
        self._assert_equal(self._open(key)[:], expected)

    def test_refused(self):
        def set_codecs(codecs):
            return lambda meta: meta.update(codecs=codecs)
        vlen = {'name': 'vlen-utf8'}
        # bytes instead of vlen-utf8, both, none, a configuration, a transpose,
        # null or numeric fill values
        for edit in [set_codecs([{'name': 'bytes', 'configuration': {'endian': 'little'}}]),
                     set_codecs([vlen, {'name': 'bytes'}]),
                     set_codecs([]),
                     set_codecs([{'name': 'vlen-utf8', 'configuration': {'x': 1}}]),
                     set_codecs([{'name': 'transpose', 'configuration': {'order': [1, 0]}}, vlen]),
                     set_codecs([vlen, vlen]),
                     lambda meta: meta.update(fill_value=None),
                     lambda meta: meta.update(fill_value=0)]:
            self._assert_refused(edit)

    def test_write_sharded_crc32c(self):
        for key, kwargs in [('sharded', {'shards': (2, 4)}), ('crc32c', {'crc32c': True}),
                            ('both', {'shards': (4, 4), 'crc32c': True, 'compression': 'gzip'})]:
            ds = self._create(key, fillvalue='-', **kwargs)
            ds[:, 1:] = DATA[:, 1:]
            ds[1, 0] = 'x'
            expected = DATA.copy()
            expected[:, 0] = '-'
            expected[1, 0] = 'x'
            self._assert_equal(self._open(key)[:], expected, key)

    def test_vlen_codec_on_numbers_refused(self):
        self._write_array('data', DATA)
        with open(self._meta_path('data')) as f:
            meta = json.load(f)
        meta['data_type'] = 'int32'
        meta['fill_value'] = 0
        self._write_json(self._meta_path('data'), meta)
        with self.assertRaises(RuntimeError):
            self._open()


class TestStringN5(unittest.TestCase):
    path = 'test_vlen_string.n5'

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    def test_refused(self):
        # n5's own "string" type is encoded differently from zarr's vlen-utf8
        f = z5py.File(self.path, mode='w', use_zarr_format=False)
        f.create_dataset('data', shape=(4,), chunks=(2,), dtype='int8')
        attrs = os.path.join(self.path, 'data', 'attributes.json')
        with open(attrs) as fh:
            meta = json.load(fh)
        meta['dataType'] = 'string'
        with open(attrs, 'w') as fh:
            json.dump(meta, fh)
        with self.assertRaises(RuntimeError):
            z5py.File(self.path, mode='r')['data']


if __name__ == '__main__':
    unittest.main()
