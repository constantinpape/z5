import unittest


class TestS3(unittest.TestCase):
    # this is just a dummy test that checks the
    # handle imports and constructors
    def test_dummy(self):
        from z5py import _z5py
        m = _z5py.FileMode(_z5py.FileMode.a)
        fhandle = _z5py.S3File(m)
        ghandle = _z5py.S3Group(fhandle, "test")
        _z5py.S3DatasetHandle(ghandle, "test")


if __name__ == '__main__':
    unittest.main()
