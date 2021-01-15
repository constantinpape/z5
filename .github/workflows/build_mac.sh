#!/bin/bash

export PY_BIN="$CONDA_PREFIX/bin/python"
cmake . \
    -DWITHIN_TRAVIS=ON \
    -DWITH_BLOSC=ON \
    -DWITH_ZLIB=ON \
    -DWITH_BZIP2=ON \
    -DWITH_XZ=ON \
    -DWITH_LZ4=ON \
    -DWITH_S3=OFF \
    -DCMAKE_PREFIX_PATH="$CONDA_PREFIX" \
    -DPYTHON_EXECUTABLE="$PY_BIN" \
    -DCMAKE_CXX_FLAGS="-std=c++17" \
    -DWITH_BOOST_FS=ON \
    -DBUILD_TESTS=ON \
    -DCMAKE_INSTALL_PREFIX="$CONDA_PREFIX" \
    -DBUILD_Z5PY=ON
make -j 4
make install
