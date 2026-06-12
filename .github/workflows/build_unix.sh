#!/bin/bash
set -e

# Build out-of-source (into bld/) so the repo root stays clean. An in-source
# `cmake .` would leave a CMakeCache.txt in the root, which makes the later
# out-of-source C++ test build (cpp_tests.sh) misresolve its build directory.
export PY_BIN="$CONDA_PREFIX/bin/python"
# WITH_S3 is OFF by default; the dedicated s3 CI job sets Z5_WITH_S3=ON
# (the env then also needs aws-sdk-cpp, which is in .github/environment.yaml).
cmake -S . -B bld \
    -DWITHIN_TRAVIS=ON \
    -DWITH_BLOSC=ON \
    -DWITH_ZLIB=ON \
    -DWITH_BZIP2=ON \
    -DWITH_XZ=ON \
    -DWITH_LZ4=ON \
    -DWITH_ZSTD=ON \
    -DWITH_S3="${Z5_WITH_S3:-OFF}" \
    -DCMAKE_PREFIX_PATH="$CONDA_PREFIX" \
    -DPython_EXECUTABLE="$PY_BIN" \
    -DBUILD_TESTS=OFF \
    -DCMAKE_INSTALL_PREFIX="$CONDA_PREFIX" \
    -DBUILD_Z5PY=ON
cmake --build bld -j 4
cmake --install bld
