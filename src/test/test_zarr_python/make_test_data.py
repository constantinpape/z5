import numpy as np
import zarr


def make_testdata():
    zz = zarr.open_array(
        "array.zr", mode='w', shape=xx.shape, chunks=[10, 10, 10], dtype='f8'
    )
    zz[:] = 42


if __name__ == '__main__':
    make_testdata()
