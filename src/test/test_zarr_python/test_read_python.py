import zarr


def check_zarray():
    zz = zarr.open_array(
        "array1.zr", mode='r'
    )
    assert (zz[:] == 42).all()
    print("Python read test passed!")


if __name__ == '__main__':
    check_zarray()
