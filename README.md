# z5

Lightweight C++ and python wrapper for reading and writing zarr 
(https://github.com/alimanfoo/zarr) and N5 (https://github.com/saalfeldlab/n5) arrays to / from disc.

Read and write compressed chunked arrays on filesystem.
Offers support for the following compression codecs:
- Blosc (https://github.com/Blosc/c-blosc)
- Zlib / Gzip (https://zlib.net/)
- Bzip2 (http://www.bzip.org/)
- TODO: XY, LZ4, LZMA

## When to use this library

This library offers access to zarr arrays from C++ and access to
N5 arrays from C++ and python.
If you only need to access zarr / N5 arrays from python / Java,
I recommend to use the native implementations, which are more thoroughly tested.

## Installation

### Python

You can install the package via conda:

```
conda install -c cpape z5py
```

### C++

The library itself is header-only, however you need to link against the relevant compression codecs.
TODO CMake build.

An interface to the marray (https://github.com/bjoern-andres/marray) multiarray is implemented in 
https://github.com/constantinpape/zarr_pp/blob/master/include/zarr%2B%2B/multiarray/marray_access.hxx.
To interface with other multiarray implementation, reimplement `readSubarray` and `writeSubarray`.
Pull requests for additional multiarray support are welcome.

## Examples / Usage

The python API is very similar to h5py.
The `File` class-constructor takes the boolean argument `use_zarr_format`, which determines whether
the zarr or N5 format is used (if set to `None`, an attempt is made to automatically infer the format).

```
import z5py
import numpy as np

# create a file and a dataset
f = z5py.File('array.zr', use_zarr_format=True)
ds = f.create_dataset('data', shape=(1000, 1000), chunks=(100, 100), dtype='float32')

# write array to a roi
x = np.random.random_sample(size=(50, 50)).astype('float32')
ds[:50, :50] = x

# broadcast a scalar to a roi
ds[50:, 50:] = 42.

# read array from a roi
y = ds[25:75, 25:75]

# create a group and create a dataset in the group
g = f.create_group('local_group')
g.create_dataset('local_data', shape=(100, 100), chunks=(10, 10), dtype='uint32')

# open dataset from group or file
ds_local1 = f['local_group/local_data']
ds_local2 = g['local_data']

# read and write attributes
attributes = ds.attrs
attributes['foo'] = 'bar'
baz = attributes['foo']
```

## Limitations

- No thread / process synchonization -> writing (reading?) to the same chunk will lead to undefined behavior.
- The N5 varlength array is not supported yet.
