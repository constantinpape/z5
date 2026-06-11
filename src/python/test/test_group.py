import unittest
import os
import sys
from shutil import rmtree
from abc import ABC

import numpy as np
import z5py

from _v3_capability import format_ext, open_root_file, requires_z5_v3


class GroupTestMixin(ABC):

    def setUp(self):
        self.shape = (100, 100, 100)

        self.path = 'array.' + format_ext(self.data_format)
        self.root_file = open_root_file(self.path, self.data_format)
        g = self.root_file.create_group('test')
        g.create_dataset('test',
                         dtype='float32',
                         shape=self.shape,
                         chunks=(10, 10, 10))

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    def test_contains(self):
        self.assertTrue('test' in self.root_file)
        self.assertTrue('/test' in self.root_file)
        self.assertTrue('/test/test' in self.root_file)
        self.assertFalse('fake' in self.root_file)

    def test_open_group(self):
        g = self.root_file['test']
        ds = g['test']
        out = ds[:]
        self.assertEqual(out.shape, self.shape)
        self.assertTrue((out == 0).all())

    def test_open_dataset(self):
        ds = self.root_file['test/test']
        out = ds[:]
        self.assertEqual(out.shape, self.shape)
        self.assertTrue((out == 0).all())

    def test_group(self):
        g = self.root_file.create_group('group')
        ds = g.create_dataset('data',
                              dtype='float32',
                              shape=self.shape,
                              chunks=(10, 10, 10))
        in_array = np.random.rand(*self.shape).astype('float32')
        ds[:] = in_array
        out_array = ds[:]
        self.assertEqual(out_array.shape, in_array.shape)
        self.assertTrue(np.allclose(out_array, in_array))

    def test_create_nested_group(self):
        self.root_file.create_group('foo/bar/baz')
        g = self.root_file['foo/bar/baz']
        self.assertTrue(os.path.exists(os.path.join(self.path, 'foo', 'bar', 'baz')))

        g.create_group('blub/blob')
        g['blub/blob']
        self.assertTrue(os.path.exists(os.path.join(self.path, 'foo', 'bar', 'baz', 'blub', 'blob')))

    def test_require_group(self):
        self.root_file.require_group('group')
        self.root_file.require_group('group')
        self.assertTrue(os.path.exists(os.path.join(self.path, 'group')))

    def test_delete(self):
        self.assertTrue('test' in self.root_file)
        self.assertTrue('test/test' in self.root_file)
        del self.root_file['test']
        self.assertFalse('test' in self.root_file)
        self.assertFalse('test/test' in self.root_file)

    def test_leading_slash(self):
        # make sure that we can read slashes
        g = self.root_file['/test']
        ds1 = g['/test']
        ds2 = self.root_file['/test/test']
        self.assertTrue(np.allclose(ds1[:], ds2[:]))

    def test_require_dataset_with_compression_options(self):
        """Issue #98

        https://github.com/constantinpape/z5/issues/98
        """
        g = self.root_file['/test']
        ds = g.require_dataset('new', shape=(100, 100), chunks=(10, 10),
                               dtype='uint8', compression='gzip',
                               level=4)
        self.assertTrue(os.path.exists(os.path.join(self.path, 'test')))
        compression_opts = ds.compression_opts
        self.assertEqual(compression_opts['level'], 4)

    # cf. https://github.com/constantinpape/z5/issues/100
    @unittest.skipIf(sys.platform.startswith('win'), 'path encoding not compatible with windows')
    def test_visititems(self):
        """ Issue #121

        https://github.com/constantinpape/z5/issues/121
        """
        f = self.root_file
        f.create_group('g1')
        f.create_dataset('g1/d1', shape=(10, 10), dtype='uint8')
        f.create_dataset('g1/d2', shape=(12, 12), dtype='uint8')

        names = []

        def visitor(name, obj):
            names.append(name)

        f.visititems(visitor)
        names = set(names)
        expected_names = {'g1', 'g1/d1', 'g1/d2', 'test', 'test/test'}
        self.assertEqual(names, expected_names)

        names = []
        g = f['g1']
        g.visititems(visitor)
        names = set(names)
        expected_names = {'d1', 'd2'}
        self.assertEqual(names, expected_names)

    def test_visititems_early_return(self):
        # regression: a non-None return from inside a nested group neither
        # stopped the iteration nor was propagated (h5py semantics)
        f = self.root_file
        f.create_group('g1')
        f.create_dataset('g1/d1', shape=(10, 10), dtype='uint8')

        def find_d1(name, obj):
            if name.endswith('d1'):
                return name

        self.assertEqual(f.visititems(find_d1), 'g1/d1')

    def test_require_dataset_read_only(self):
        # regression: require_dataset refused to return an EXISTING, consistent
        # dataset on a read-only file; write permissions are only needed when
        # the dataset has to be created
        f = z5py.File(self.path, mode='r')
        ds = f.require_dataset('test/test', shape=self.shape, dtype='float32',
                               chunks=(10, 10, 10))
        self.assertEqual(ds.shape, self.shape)
        with self.assertRaises(ValueError):
            f.require_dataset('new_ds', shape=self.shape, dtype='float32',
                              chunks=(10, 10, 10))

    def test_parent(self):
        f = self.root_file
        g = f.create_group('g')
        self.assertIs(f, g.parent)
        gg = g.create_group('g')
        self.assertIs(g, gg.parent)
        self.assertIs(f, gg.parent.parent)

    def test_name(self):
        f = self.root_file
        g = f.create_group('g')
        self.assertEqual(g.name, '/g')
        gg = f.create_group('g/g')
        self.assertEqual(gg.name, '/g/g')
        g2 = g['g']
        self.assertEqual(g2.name, '/g/g')

    def test_file(self):
        f = self.root_file
        g = f.create_group('g')
        self.assertIs(g.file, f)
        g = g.create_group('g')
        self.assertIs(g.file, f)


class TestGroupZarr(GroupTestMixin, unittest.TestCase):
    data_format = 'zr'

    def test_metadata_creation(self):
        """Issue #231

        Creating a dataset in an implicitly created subgroup must produce valid
        group metadata, so that the hierarchy can be visited afterwards.
        """
        f = self.root_file
        data = (np.random.rand(1, 10, 20, 1, 1) * 255).astype(np.uint8)

        def collect(visited):
            def visitor(name, obj):
                visited.append(name)
            return visitor

        f.create_dataset("data", data=data)
        visited = []
        f.visititems(collect(visited))
        self.assertEqual(set(visited), {'data', 'test', 'test/test'})

        f.create_dataset("volume/data", data=data)
        visited = []
        f.visititems(collect(visited))
        self.assertEqual(set(visited),
                         {'data', 'test', 'test/test', 'volume', 'volume/data'})


@requires_z5_v3
class TestGroupZarrV3(GroupTestMixin, unittest.TestCase):
    data_format = 'zarr_v3'

    def test_group_metadata_v3(self):
        import json
        self.root_file.create_group('sub')
        # root, the group created in setUp, and a freshly created group all
        # carry a v3 group ``zarr.json``
        for rel in ('', 'test', 'sub'):
            meta_path = os.path.join(self.path, rel, 'zarr.json')
            self.assertTrue(os.path.exists(meta_path))
            with open(meta_path) as fobj:
                meta = json.load(fobj)
            self.assertEqual(meta['zarr_format'], 3)
            self.assertEqual(meta['node_type'], 'group')


class TestGroupN5(GroupTestMixin, unittest.TestCase):
    data_format = 'n5'


if __name__ == '__main__':
    unittest.main()
