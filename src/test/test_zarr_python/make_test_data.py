import numpy as np
import zarr


def make_testdata():
    xx = np.random.random([100, 100, 100]).astype('float32')
    zz = zarr.open_array(
        "array.zr", mode='w', shape=xx.shape, chunks=[10, 10, 10], dtype='f8'
    )
    zz[:] = xx


if __name__ == '__main__':
    make_testdata()
