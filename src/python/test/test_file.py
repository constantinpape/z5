import unittest
import os
import json
from shutil import rmtree
from abc import ABC
import pathlib

import z5py

from _v3_capability import requires_z5_v3


class FileTestMixin(ABC):
    def setUp(self):
        self.path = 'file.%s' % self.data_format

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    def test_context_manager(self):
        self.assertFalse(os.path.exists(self.path))

        with z5py.File(self.path) as f:
            self.assertIsInstance(f, z5py.File)
            if self.data_format == 'n5':
                self.assertFalse(f.is_zarr)
            else:
                self.assertTrue(f.is_zarr)
        self.assertTrue(os.path.exists(self.path))

    def test_extension_detect(self):
        self.assertFalse(os.path.exists(self.path))

        f = z5py.File(self.path, use_zarr_format=None)
        if self.data_format == 'n5':
            self.assertFalse(f.is_zarr)
        else:
            self.assertTrue(f.is_zarr)

    def test_wrong_ext_fails(self):
        is_n5 = self.data_format == 'n5'
        with self.assertRaises(RuntimeError):
            z5py.File(self.path, use_zarr_format=is_n5)

    def test_filename(self):
        f = z5py.File(self.path)
        self.assertEqual(f.filename, self.path)

    def test_parent(self):
        f = z5py.File(self.path)
        self.assertIs(f, f.parent)

    def test_name(self):
        f = z5py.File(self.path)
        self.assertEqual(f.name, '/')

    def test_file(self):
        f = z5py.File(self.path)
        self.assertIs(f.file, f)


class TestZarrFile(FileTestMixin, unittest.TestCase):
    data_format = 'zarr'

    def test_direct_constructor(self):
        self.assertFalse(os.path.exists(self.path))
        f = z5py.ZarrFile(self.path)
        self.assertTrue(f.is_zarr)


class TestZrFile(FileTestMixin, unittest.TestCase):
    data_format = 'zr'

    def test_direct_constructor(self):
        self.assertFalse(os.path.exists(self.path))
        f = z5py.ZarrFile(self.path)
        self.assertTrue(f.is_zarr)


class TestN5File(FileTestMixin, unittest.TestCase):
    data_format = 'n5'

    def test_direct_constructor(self):
        self.assertFalse(os.path.exists(self.path))
        f = z5py.N5File(self.path)
        self.assertFalse(f.is_zarr)


class TestPathlibPath(FileTestMixin, unittest.TestCase):
    data_format = 'n5'

    def test_direct_constructor(self):
        self.assertFalse(os.path.exists(self.path))
        path = pathlib.Path(self.path)
        f = z5py.N5File(path)
        self.assertFalse(f.is_zarr)


@requires_z5_v3
class TestZarrV3File(unittest.TestCase):
    path = 'file_v3.zr'

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    def _read_root_meta(self):
        with open(os.path.join(self.path, 'zarr.json')) as f:
            return json.load(f)

    def test_create_v3(self):
        self.assertFalse(os.path.exists(self.path))
        f = z5py.File(self.path, use_zarr_format=True, zarr_format=3)
        self.assertTrue(f.is_zarr)
        meta = self._read_root_meta()
        self.assertEqual(meta['zarr_format'], 3)
        self.assertEqual(meta['node_type'], 'group')

    def test_zarrfile_v3(self):
        f = z5py.ZarrFile(self.path, zarr_format=3)
        self.assertTrue(f.is_zarr)
        self.assertEqual(self._read_root_meta()['zarr_format'], 3)

    def test_reopen_v3(self):
        z5py.File(self.path, use_zarr_format=True, zarr_format=3)
        # re-opening must accept the v3 metadata (version check)
        f = z5py.File(self.path)
        self.assertTrue(f.is_zarr)


if __name__ == '__main__':
    unittest.main()
