import os
import numpy as np
import z5py
from shutil import rmtree
from tqdm import trange


# this is to test read/write of some actual and large enough data.
# not supposed to be run as a unit test
def stress_test(input_path, input_key, compression, n_threads, format_='n5'):
    assert format_ in ('n5', 'zr', 'zarr')
    N = 5
    f = z5py.File(input_path)
    ds = f[input_key]
    chunks = ds.chunks
    shape = ds.shape
    dtype = ds.dtype
    data = ds[:]

    out_path = 'tmp.%s' % format_

    for _ in trange(N):
        f_out = z5py.File(out_path)
        ds_out = f_out.create_dataset('test', shape=shape, chunks=chunks, dtype=dtype,
                                      compression=compression)
        ds_out.n_threads = n_threads

        ds_out[:] = data
        out = ds_out[:]
        assert np.allclose(out, data)
        rmtree(out_path)
    print("All passed")


if __name__ == '__main__':
    path = '/home/pape/Work/data/cremi/example/sampleA.n5'
    key = 'volumes/raw/s0'

    stress_test(path, key, 'gzip', 4, 'n5')
    stress_test(path, key, 'gzip', 4, 'zarr')
