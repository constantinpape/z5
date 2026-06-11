#!/bin/bash
set -e

# Build and run ALL C++ tests out-of-source against the bundled googletest
# submodule. Explicit -S/-B keeps this independent of any build tree the
# preceding conda build may have created. (This used to build only the four
# multiarray binaries, which let the other test sources rot unnoticed.)
cmake -S . -B bld_cpp \
    -DWITH_BLOSC=ON \
    -DWITH_ZLIB=ON \
    -DWITH_BZIP2=ON \
    -DWITH_XZ=ON \
    -DWITH_LZ4=ON \
    -DWITH_ZSTD=ON \
    -DCMAKE_PREFIX_PATH="$CONDA_PREFIX" \
    -DBUILD_Z5PY=OFF \
    -DBUILD_TESTS=ON

cmake --build bld_cpp -j 4

cd bld_cpp/src/test
bash "$GITHUB_WORKSPACE/src/test/run_all_tests.sh"
