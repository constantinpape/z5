# z5

Lightweight C++ and Python wrapper for reading and writing zarr 
(https://github.com/alimanfoo/zarr) and N5 (https://github.com/saalfeldlab/n5) arrays to / from disc.

Read and write compressed chunked arrays on filesystem.
Offers support for the following compression codecs:
- Blosc (https://github.com/Blosc/c-blosc)
- Zlib / Gzip (https://zlib.net/)
- Bzip2 (http://www.bzip.org/)
- TODO: XY, LZ4, LZMA

## Installation

### Python

You can install the package via conda (only Linux for now):

```
conda install -c cpape z5py
```

### C++

The library itself is header-only, however you need to link against the relevant compression codecs.
TODO describe CMake options.


## Examples / Usage

### Python

The Python API is very similar to h5py.
Some differences are: 
- The constructor of `File` takes the boolean argument `use_zarr_format`, which determines whether
the zarr or N5 format is used (if set to `None`, an attempt is made to automatically infer the format).
- `File` does not support different read/write modes.
- There is no need to close `File`, hence the `with` block isn't necessary.

Some examples:

```python
import z5py
import numpy as np

# create a file and a dataset
f = z5py.File('array.zr', use_zarr_format=True)
ds = f.create_dataset('data', shape=(1000, 1000), chunks=(100, 100), dtype='float32')

# write array to a roi
x = np.random.random_sample(size=(500, 500)).astype('float32')
ds[:500, :500] = x

# broadcast a scalar to a roi
ds[500:, 500:] = 42.

# read array from a roi
y = ds[250:750, 250:750]

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

There are convenience functions to convert n5 files to popular data formats.
(TODO also zarr format, convert to from png, jpeg, tiff)

So far, only h5 is supported.

```python
# convert existing h5 file to n5
# this only works if h5py is available
from z5py.converter import convert_n5_to_h5

h5_file = '/path/to/h5'
n5_file = '/path/to/n5'
h5_key = n5_key = 'data'
target_chunks = (64, 64, 64)
n_threads = 8

convert_h5_to_n5(h5_file, n5_file,
                 in_path_in_file=h5_key,
                 out_path_in_file=n5_key,
                 out_chunks=target_chunks,
                 n_threads=n_threads,
                 compressor='gzip')
```

### C++

The library is intended to be used with a multiarray, that holds data in memory.
By default, `xtensor` (https://github.com/QuantStack/xtensor) is used. 
See https://github.com/constantinpape/z5/blob/master/include/z5/multiarray/xtensor_access.hxx
There also exists an interface for `marray` (https://github.com/bjoern-andres/marray).
See https://github.com/constantinpape/z5/blob/master/include/z5/multiarray/marray_access.hxx.
To interface with other multiarray implementation, reimplement `readSubarray` and `writeSubarray`.
Pull requests for additional multiarray support are welcome.

Some examples:

```c++
#include "xtensor/xarray.hxx"
#include "z5/dataset_factory.hxx"
#include "z5/multiarray/xtensor_access.hxx"
#include "json.hpp"

int main() {
  // create a new zarr dataset
  std::vector<size_t> shape = {1000, 1000, 1000};
  std::vector<size_t> chunks = {100, 100, 100};
  bool asZarr = true;
  auto ds = z5::createDataset("ds.zr", "float32", shape, chunks, asZarr);
  
  // write marray to roi
  std::vector<size_t> offset1 = {50, 100, 150};
  std::vector<size_t> shape1 = {150, 200, 100};
  xt::xarray<float> array1(shape1, 42.);
  z5::multiarray::writeSubarray(ds, array1, offset1.begin());

  // read marray from roi (values that were not written before are filled with a fill-value)
  std::vector<size_t> offset2 = {100, 100, 100};
  std::vector<size_t> shape2 = {300, 200, 75};
  xt::xarray<float> array2(shape2);
  z5::multiarray::readSubarray(ds, array2, offset2.begin());

  // read and write json attributes
  nlohmann::json attributesIn;
  attributesIn["bar"] = "foo";
  attributesIn["pi"] = 3.141593
  z5::writeAttributes(ds->handle(), attributesIn);
  
  nlohmann::json attributesOut;
  z5::readAttributes(ds->handle(), attributesOut);
  
  return 0;
}
```

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


## A note on axis ordering

Internally, n5 uses column-major (i.e. x, y, z) axis ordering, while z5 does row-major (i.e. z, y,x) axis ordering.
While this is mostly handled internally, it means that the metadata does not transfer
1 to 1, but needs to be reversed for most shapes. Concretely:

|           |n5                      |z5              |
|----------:|-----------------------:|---------------:|  
|Shape      | s_x, s_y, s_z          |s_z, s_y, s_x   |
|Chunk-Shape| c_x, c_y, c_z          |c_z, c_y, c_x   | 
|Chunk-Ids  | i_x, i_y, i_z          |i_z, i_y, i_x   |


