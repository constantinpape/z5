# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.
z5 is a library for reading and writing zarr and n5 files. It is written in C++ and uses the xtensor library to represent multi-dimensional arrays. It provides python bindngs via pybind11.
The python library is called z5py. The library uses CMake as build system.

## Common Commands

### Compilation
```bash
# Create an empty build folder, configure cmake, and build it.
rm -rf bld
mkdir bld
cd bld
cmake ..
make -j 4
```

### Tests
```bash
# To run all python tests, first compile the package as described above.
# Then change to the directory 'python' within the 'bld' folder.
cd python
# And run all tests.
python -m unittest discover -s test -v
```

## Code Architecture

### Overall Organization

The C++ headers are located in `include/z5`, the code for compiling the python bindings is in `src/python/lib`, additional python code of the library in `src/python/module`, and the tests for the python library in `src/python/test`.

### Key Design Patterns

The headers in `include/z5` implement the abstract base classes for file handles, groups, and datasets, which represent the structure of the zarr or n5 containers.
The actual implementations of these classes can be found in `include/z5/filesystem` for the file syste, `include/z5/gcs` for google cloud storage and `include/z5/s3` for s3 storage. Note that GCS and S3 storage are not fully implemented.

In addition, `include/z5/compression` implements the different compression libraries that are supported for compressing chunks. `include/z5/multiarray` implements IO to and from memory. Other files implement helper functionality.
