#!/bin/bash
set -e

# build and run the C++ multiarray tests (covers the strided ArrayView IO and
# the copyView / fillView / subview logic) out-of-source against the bundled
# googletest submodule.
mkdir -p bld_cpp
cd bld_cpp

cmake .. \
    -DWITH_BLOSC=ON \
    -DWITH_ZLIB=ON \
    -DWITH_BZIP2=ON \
    -DWITH_XZ=ON \
    -DWITH_LZ4=ON \
    -DWITH_ZSTD=ON \
    -DCMAKE_PREFIX_PATH="$CONDA_PREFIX" \
    -DCMAKE_CXX_FLAGS="-std=c++20" \
    -DBUILD_Z5PY=OFF \
    -DBUILD_TESTS=ON

make -j 4 test_array_util test_broadcast test_array test_array_nd

cd src/test/multiarray
./test_array_util
./test_broadcast
./test_array
./test_array_nd
