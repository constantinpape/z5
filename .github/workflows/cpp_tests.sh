#!/bin/bash
set -e

# Build and run the C++ multiarray tests (covers the strided ArrayView IO and
# the copyView / fillView / subview logic) out-of-source against the bundled
# googletest submodule. Explicit -S/-B keeps this independent of any build tree
# the preceding conda build may have created.
cmake -S . -B bld_cpp \
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

cmake --build bld_cpp -j 4 --target test_array_util test_broadcast test_array test_array_nd

cd bld_cpp/src/test/multiarray
./test_array_util
./test_broadcast
./test_array
./test_array_nd
