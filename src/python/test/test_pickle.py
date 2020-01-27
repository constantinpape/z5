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


class TestPickle(unittest.TestCase):

    def setUp(self):
        self.shape = (100, 100, 100)
        self.ff = z5py.File('array.n5')

    def tearDown(self):
        try:
            rmtree('array.n5')
        except OSError:
            pass

    def test_pickle_dataset(self):
        ds = self.ff.create_dataset('data', dtype='float64',
                                    shape=self.shape,
                                    chunks=(10, 10, 10))
        in_array = np.random.rand(*self.shape)
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

    def test_pickle_file(self):
        f = self.ff
        pickled = pickle.dumps(f)
        f_pickle = pickle.loads(pickled)
        self.assertEqual(f.filename, f_pickle.filename)

    def test_pickle_group(self):
        f = self.ff
        g = f.create_group('g')
        pickled = pickle.dumps(g)
        g_pickle = pickle.loads(pickled)
        self.assertEqual(g.name, g_pickle.name)
        self.assertEqual(g.file.filename, g_pickle.file.filename)


if __name__ == '__main__':
    unittest.main()
