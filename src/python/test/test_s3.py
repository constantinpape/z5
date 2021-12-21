import os
import unittest
import numpy as np

# by default, we don't run any actual s3 tests,
# because this will not work in CI due to missing credentials.
# set the environment variable "Z5PY_TEST_S3" to 1 in order to run tests
TEST_S3 = bool(os.environ.get("Z5PY_S3_RUN_TEST", False))
BUCKET_NAME = os.environ.get("Z5PY_S3_BUCKET_NAME", 'z5-test-data')


def get_test_data():
    from skimage.data import astronaut
    return astronaut()


@unittest.skipUnless(TEST_S3, "Disabled by default")
class TestS3(unittest.TestCase):
    bucket_name = BUCKET_NAME

    def setUp(self):
        self.data = get_test_data()

    @staticmethod
    def make_test_data(bucket_name=None):
        """ Make zarr test data in an s3 bucket.

        The bucket `bucket_name` must already exist and
        access credentials must be stored in a way that can be accessed by
        s3fs.
        """
        import s3fs
        import zarr
        import numcodecs

        if bucket_name is None:
            bucket_name = TestS3.bucket_name

        # access the s3 filesystem
        fs = s3fs.S3FileSystem(anon=False)
        store = s3fs.S3Map(root=bucket_name, s3=fs)

        # test data image
        data = TestS3.data

        # write remote zarr data
        f = zarr.group(store)
        f.attrs['Q'] = 42

        # create dataset
        ds = f.create_dataset('data', shape=data.shape, chunks=(256, 256, 3), dtype=data.dtype,
                              compressor=numcodecs.Zlib())
        ds[:] = data
        ds.attrs['x'] = 'y'

        # create a and dataset group
        g = f.create_group('group')
        ds = g.create_dataset('data', shape=data.shape, chunks=(256, 256, 3), dtype=data.dtype,
                              compressor=numcodecs.Zlib())
        ds[:] = data
        ds.attrs['x'] = 'y'

    # this is just a dummy test that checks the
    # handle imports and constructors
    def test_dummy(self):
        from z5py import _z5py
        m = _z5py.FileMode(_z5py.FileMode.a)
        fhandle = _z5py.S3File(self.bucket_name, m)
        ghandle = _z5py.S3Group(fhandle, "test")
        _z5py.S3DatasetHandle(ghandle, "test")

    def test_s3_file(self):
        from z5py import S3File
        f = S3File(self.bucket_name)

        self.assertTrue('data' in f)
        self.assertTrue('group' in f)
        self.assertTrue('group/data' in f)

        keys = f.keys()
        expected = {'data'}
        expected = {'data', 'group'}
        self.assertEqual(set(keys), expected)

        attrs = f.attrs
        self.assertEqual(attrs['Q'], 42)

    def test_s3_group(self):
        from z5py import S3File
        f = S3File(self.bucket_name)
        g = f['group']

        self.assertTrue('data' in g)
        keys = g.keys()
        expected = {'data'}
        self.assertEqual(set(keys), expected)

    # currently fails with:
    # RuntimeError: Exception during zlib decompression: (-3)
    def test_s3_dataset(self):
        from z5py import S3File

        def check_ds(ds):
            # check the data
            data = ds[:]
            self.assertEqual(data.shape, self.data.shape)
            self.assertTrue(np.allclose(data, self.data))

            # check the attributes
            attrs = ds.attrs
            self.assertEqual(attrs['x'], 'y')

        f = S3File(self.bucket_name)
        ds = f['data']
        check_ds(ds)

        # g = f['group']
        # ds = g['data']
        # check_ds(ds)


if __name__ == '__main__':
    # TestS3.make_test_data()
    unittest.main()
