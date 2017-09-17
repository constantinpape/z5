import unittest
import os
from shutil import rmtree

# hacky import
import sys
sys.path.append('..')
import z5py


class TestAttributes(unittest.TestCase):

    def setUp(self):
        self.shape = (100, 100, 100)
        self.ff_zarr = z5py.File('array.zr', True)
        self.ff_zarr.create_dataset('ds', dtype='float32' self.shape, chunks=(10, 10, 10))
        self.ff_zarr.create_group('group')

        self.ff_n5 = z5py.File('array.n5', False)
        self.ff_n5.create_dataset('ds', dtype='float32' self.shape, chunks=(10, 10, 10))
        self.ff_n5.create_group('group')

    def tearDown(self):
        if(os.path.exists('array.zr')):
            rmtree('array.zr')
        if(os.path.exists('array.n5')):
            rmtree('array.n5')


    def test_attrs_zarr(self):
        pass

    def test_attrs_n5(self):
        pass
