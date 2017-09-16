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
conda install -c cpape zarr_pp
```

### C++

The library itself is header-only, however you need to link against the relevant compression codecs.
TODO CMake build.

An interface to the marray (https://github.com/bjoern-andres/marray) multiarray is implemented in 
https://github.com/constantinpape/zarr_pp/blob/master/include/zarr%2B%2B/multiarray/marray_access.hxx.
To interface with other multiarray implementation, reimplement `readSubarray` and `writeSubarray`.
Pull requests for additional multiarray support are welcome.

## Examples / Usage

## Limitations

- No thread / process synchonization -> writing (reading?) to the same chunk wiill lead to undefined behavior.
- The N5 varlength array is not supported yet.
