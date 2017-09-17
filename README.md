# z5

Lightweight C++ and python wrapper for reading and writing zarr 
(https://github.com/alimanfoo/zarr) and N5 (https://github.com/saalfeldlab/n5) arrays to / from disc.

Read and write compressed chunked arrays on filesystem.
Offers support for the following compression codecs:
- Blosc (https://github.com/Blosc/c-blosc)
- Zlib / Gzip (https://zlib.net/)
- Bzip2 (http://www.bzip.org/)
- TODO: XY, LZ4, LZMA

## Installation

### Python

You can install the package via conda:

```
conda install -c cpape z5py
```

### C++

The library itself is header-only, however you need to link against the relevant compression codecs.
TODO CMake build.


## Examples / Usage

### Python

The python API is very similar to h5py.
Some differences are: 
- The constructor of `File` takes the boolean argument `use_zarr_format`, which determines whether
the zarr or N5 format is used (if set to `None`, an attempt is made to automatically infer the format).
- `File` does not support different read/write modes.
- There is no need to close `File`, hence the `with` block isn't necessary.

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

### C++

The library is intended to be used with a multiarray, that holds data in memory.
An interface to `marray` (https://github.com/bjoern-andres/marray) is implemented in 
https://github.com/constantinpape/zarr_pp/blob/master/include/zarr%2B%2B/multiarray/marray_access.hxx.
To interface with other multiarray implementation, reimplement `readSubarray` and `writeSubarray`.
Pull requests for additional multiarray support are welcome.

## When to use this library?

This library implements the zarr and N5 data specification in C++ and Python.
Use it, if you need access to these formats from these languages.
Zarr / N5 have native implementations in Python / Java.
If you only need access in the respective native language,
I recommend to use these implementations, which are more thoroughly tested.


## Current Limitations / TODOs

- No thread / process synchonization -> writing (reading?) to the same chunk will lead to undefined behavior.
- The N5 varlength array is not supported yet.
- Supports only little endianness for the zarr format.
