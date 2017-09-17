import unittest
import numpy as np

# hacky import
#import sys
#sys.path.append('..')
import z5py


class TestDataset(unittest.TestCase):

    def test_ds(self):
        ff = z5py.File('array.zr', True)
        ds = ff.create_dataset(
            'data', dtype='float32', shape=[100, 100, 100], chunks=[10, 10, 10]
        )
        in_array = 42 * np.ones(shape, dtype='float32')
        ds[:] = in_array
        out_array = np.zeros(shape, dtype='float32')
        out_array = ds[:]
        self.assertEqual(out_array.shape, in_array.shape)
        self.assertTrue(np.allclose(out_array, in_array))


if __name__ == '__main__':
    unittest.main()
