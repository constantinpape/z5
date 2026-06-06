#!/bin/bash
set -e

# Build out-of-source (into bld/) so the repo root stays clean. An in-source
# `cmake .` would leave a CMakeCache.txt in the root, which makes the later
# out-of-source C++ test build (cpp_tests.sh) misresolve its build directory.
export PY_BIN="$CONDA_PREFIX/bin/python"
cmake -S . -B bld \
    -DWITHIN_TRAVIS=ON \
    -DWITH_BLOSC=ON \
    -DWITH_ZLIB=ON \
    -DWITH_BZIP2=ON \
    -DWITH_XZ=ON \
    -DWITH_LZ4=ON \
    -DWITH_ZSTD=ON \
    -DWITH_S3=OFF \
    -DCMAKE_PREFIX_PATH="$CONDA_PREFIX" \
    -DPYTHON_EXECUTABLE="$PY_BIN" \
    -DCMAKE_CXX_FLAGS="-std=c++20" \
    -DBUILD_TESTS=OFF \
    -DCMAKE_INSTALL_PREFIX="$CONDA_PREFIX" \
    -DBUILD_Z5PY=ON
cmake --build bld -j 4
cmake --install bld
