import unittest
import os
import numpy as np
from shutil import rmtree
from six import add_metaclass
from abc import ABCMeta

import sys
try:
    import z5py
except ImportError:
    sys.path.append('..')
    import z5py


@add_metaclass(ABCMeta)
class GroupTestMixin(object):

    def setUp(self):
        self.shape = (100, 100, 100)

        self.root_file = z5py.File('array.%s' % self.data_format)
        g = self.root_file.create_group('test')
        g.create_dataset('test',
                         dtype='float32',
                         shape=self.shape,
                         chunks=(10, 10, 10))

    def tearDown(self):
        try:
            rmtree('array.%s' % self.data_format)
        except OSError:
            pass

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
        self.assertEqual(g.path, os.path.join(self.root_file.path, 'foo/bar/baz'))

    def test_require_group(self):
        self.root_file.require_group('group')
        g = self.root_file.require_group('group')
        self.assertEqual(g.path, os.path.join(self.root_file.path, 'group'))

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


class TestGroupZarr(GroupTestMixin, unittest.TestCase):
    data_format = 'zr'


class TestGroupN5(GroupTestMixin, unittest.TestCase):
    data_format = 'n5'


if __name__ == '__main__':
    unittest.main()
