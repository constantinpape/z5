import unittest
import sys
import numpy as np
import os
from shutil import rmtree

try:
    import cPickle as pickle
except ImportError:
    import pickle

try:
    import z5py
except ImportError:
    sys.path.append('..')
    import z5py


class TestPickle(unittest.TestCase):

    def setUp(self):
        self.shape = (100, 100, 100)
        self.ff = z5py.File('array.n5', False)

    def tearDown(self):
        try:
            rmtree('array.n5')
        except OSError:
            pass

    @unittest.skipIf(sys.version_info.major < 3, "Pickle test segfaults in python 2")
    def test_pickle(self):
        dtypes = ('int8', 'int16', 'int32', 'int64',
                  'uint8', 'uint16', 'uint32', 'uint64',
                  'float32', 'float64')

        for dtype in dtypes:
            print("Running Pickle-Test for %s" % dtype)
            ds = self.ff.create_dataset('data_%s' % dtype,
                                        dtype=dtype,
                                        shape=self.shape,
                                        chunks=(10, 10, 10))
            in_array = 42 * np.ones(self.shape, dtype=dtype)
            ds[:] = in_array

            # test the original dataset
            out_array = ds[:]
            self.assertEqual(out_array.shape, in_array.shape)
            self.assertTrue(np.allclose(out_array, in_array))

            # pickle, unpickle and then test the datasets
            pickled = pickle.dumps(ds)
            ds_pickle = pickle.loads(pickled)
            out_pickle = ds_pickle[:]
            self.assertEqual(out_pickle.shape, in_array.shape)
            self.assertTrue(np.allclose(out_pickle, in_array))


if __name__ == '__main__':
    unittest.main()
