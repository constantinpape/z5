import sys
import unittest

from shutil import rmtree
from six import add_metaclass
from abc import ABCMeta

import numpy as np

try:
    import z5py
except ImportError:
    sys.path.append('..')
    import z5py


@add_metaclass(ABCMeta)
class RegressionTestMixin(object):
    @classmethod
    def setUpClass(cls):
        cls.data = z5py.util.fetch_test_data()

    def setUp(self):
        self.root_file = z5py.File('array.' + self.data_format)
        self.chunks = (64,) * 3
        self.n_reps = 10

    def tearDown(self):
        try:
            rmtree('array.' + self.data_format)
        except OSError:
            pass

    def test_regression_read(self):
        for compression, expected in self.compressions_read.items():
            key = 'ds_%s' % compression
            ds = self.root_file.create_dataset(key, data=self.data, chunks=self.chunks,
                                               compression=compression)
            times = []
            for _ in range(self.n_reps):
                with z5py.util.Timer() as t:
                    ds[:]
                times.append(t.elapsed)
            mint = np.min(times)
            self.assertLess(mint, expected)

    def test_regression_write(self):
        for compression, expected in self.compressions_write.items():
            key = 'ds_%s' % compression
            times = []
            for _ in range(self.n_reps):
                with z5py.util.Timer() as t:
                    self.root_file.create_dataset(
                        key, data=self.data, chunks=self.chunks, compression=compression
                    )
                times.append(t.elapsed)
                del self.root_file[key]
            mint = np.min(times)
            self.assertLess(mint, expected)


# TODO might need to adjust values for travis
class TestN5Regression(RegressionTestMixin, unittest.TestCase):
    data_format = 'n5'
    # initial notebook values
    # compressions_read = {'raw': 0.0055, 'gzip': 0.085}
    # compressions_write = {'raw': 0.0085, 'gzip': 0.55}
    # more lenient values for travis
    compressions_read = {'raw': 0.015, 'gzip': 0.15}
    compressions_write = {'raw': 0.015, 'gzip': 0.75}


class TestZarrRegression(RegressionTestMixin, unittest.TestCase):
    data_format = 'zarr'
    compressions_read = {'raw': 0.015, 'zlib': 0.15}
    compressions_write = {'raw': 0.02, 'zlib': 0.75}


if __name__ == '__main__':
    unittest.main()
