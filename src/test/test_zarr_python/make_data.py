# import numpy as np
import zarr


def make_testdata():
    zz = zarr.open_array(
        "array.zr", mode='w', shape=[100, 100, 100], chunks=[10, 10, 10], dtype='f8'
    )
    zz[:] = 42


def make_testdata_fillvalue():
    zarr.open_array(
        "array_fv.zr", mode='w', shape=[100, 100, 100], chunks=[10, 10, 10], dtype='f8', fill_value=42
    )


if __name__ == '__main__':
    make_testdata()
    make_testdata_fillvalue()
