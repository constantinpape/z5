import sys
import unittest

from shutil import rmtree
from abc import ABC

import numpy as np

try:
    import z5py
except ImportError:
    sys.path.append('..')
    import z5py
import z5py.util


class RegressionTestMixin(ABC):
    @classmethod
    def setUpClass(cls):
        cls.data = z5py.util.fetch_test_data_stent()

    def setUp(self):
        self.root_file = z5py.File('array.' + self.data_format)
        self.chunks = (64,) * 3
        self.n_reps = 10

    def tearDown(self):
        try:
            rmtree('array.' + self.data_format)
        except OSError:
            pass

    def _test_regression_read(self):
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
            # maxt = np.max(times)
            # print(self.data_format, compression, maxt)

    def _test_regression_write(self):
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
            # maxt = np.max(times)
            # print(self.data_format, compression, maxt)


class TestN5Regression(RegressionTestMixin, unittest.TestCase):
    data_format = 'n5'
    compressions_read = {'raw': 0.005, 'gzip': 0.04}
    compressions_write = {'raw': 0.006, 'gzip': 0.4}


class TestZarrRegression(RegressionTestMixin, unittest.TestCase):
    data_format = 'zarr'
    compressions_read = {'raw': 0.004, 'zlib': 0.05}
    compressions_write = {'raw': 0.005, 'zlib': 0.4}


if __name__ == '__main__':
    unittest.main()
