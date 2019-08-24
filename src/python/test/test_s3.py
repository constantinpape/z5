import os
import unittest

# by default, we don't run any actual s3 tests,
# because this will not work in CI due to missing credentials.
# set the environment variable "Z5PY_TEST_S3" to 1 in order to run tests
TEST_S3 = bool(os.environ.get("Z5PY_TEST_S3", False))
BUCKET_NAME = os.environ.get("Z5PY_S3_BUCKET_NAME", 'z5-zarr-test-data')


class TestS3(unittest.TestCase):
    bucket_name = BUCKET_NAME

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
        from skimage.data import astronaut

        if bucket_name is None:
            bucket_name = TestS3.bucket_name

        # access the s3 filesysyem
        fs = s3fs.S3FileSystem(anon=False)
        store = s3fs.S3Map(root=bucket_name, s3=fs)

        # test data image
        data = astronaut()

        # write remote zarr data
        f = zarr.group(store)
        ds = f.create_dataset('data', shape=data.shape, chunks=(256, 256, 3), dtype=data.dtype,
                              compressor=numcodecs.Zlib())
        ds[:] = data

    # this is just a dummy test that checks the
    # handle imports and constructors
    def test_dummy(self):
        from z5py import _z5py
        m = _z5py.FileMode(_z5py.FileMode.a)
        fhandle = _z5py.S3File(self.bucket_name, m)
        ghandle = _z5py.S3Group(fhandle, "test")
        _z5py.S3DatasetHandle(ghandle, "test")

    # FIXME the test does not terminate.
    # the problem might be the api shutdown call in the destructor
    @unittest.skipUnless(TEST_S3, "Disabled by default")
    def test_s3_file(self):
        from z5py import S3File
        f = S3File(self.bucket_name)
        keys = f.keys()
        print(list(keys))
        expected = {'data'}
        self.assertEqual(set(keys), expected, use_zarr_format=True)


if __name__ == '__main__':
    unittest.main()
