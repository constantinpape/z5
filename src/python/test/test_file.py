import unittest
import os
from shutil import rmtree

import sys
try:
    import z5py
except ImportError:
    sys.path.append('..')
    import z5py


class TestFile(unittest.TestCase):
    def setUp(self):
        self.n5_path = 'array.n5'
        self.zarr_path = 'array.zr'

    def tearDown(self):
        if(os.path.exists(self.n5_path)):
            rmtree(self.n5_path)
        if(os.path.exists(self.zarr_path)):
            rmtree(self.zarr_path)

    def test_context_manager_n5(self):
        self.assertFalse(os.path.exists(self.n5_path))

        with z5py.File(self.n5_path, False) as f:
            self.assertIsInstance(f, z5py.File)
            self.assertFalse(f.is_zarr)

        self.assertTrue(os.path.exists(self.n5_path))

    def test_context_manager_zarr(self):
        self.assertFalse(os.path.exists(self.zarr_path))

        with z5py.File(self.zarr_path, True) as f:
            self.assertIsInstance(f, z5py.File)
            self.assertTrue(f.is_zarr)

        self.assertTrue(os.path.exists(self.zarr_path))
