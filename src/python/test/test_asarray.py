import unittest
import pickle
import sys
from shutil import rmtree

import numpy as np

try:
    import z5py
except ImportError:
    sys.path.append('..')
    import z5py


class TestAsarray(unittest.TestCase):

    def setUp(self):
        sample_data = np.array([ [3, 4], [7, 8]])
        self.test_fn = 'test.n5'
        self.test_ds = 'test'
        with z5py.File(self.test_fn, 'w') as zfh:
            zfh.create_dataset(self.test_ds, data=sample_data)
        self.ff = z5py.File(self.test_fn, 'r')
        

    def tearDown(self):
        try:
            rmtree(self.test_fn)
        except OSError:
            pass

    def test_asarray(self):
        uniques = np.unique(self.ff[self.test_ds])
        self.assertEqual(len(uniques), 4)

if __name__ == '__main__':
    unittest.main()
