#!/bin/bash
  
# install conda
if [ $TRAVIS_OS_NAME = 'osx' ]; then
    wget https://repo.continuum.io/miniconda/Miniconda3-latest-MacOSX-x86_64.sh -O miniconda.sh;
else
    wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh;
fi
bash miniconda.sh -b -p $HOME/miniconda
export PATH="$HOME/miniconda/bin:$PATH"
# update conda
conda config --set always_yes yes --set changeps1 no
conda update -q conda
conda info -a
# create the test environment
conda env create -f ./environments/unix/$CONDA_PYTHON_VERSION.yml -n build-env
source activate build-env
# install additional dependencies for tests
conda install -c conda-forge zarr h5py  # aws-sdk-cpp

###############################################
# configure cmake
###############################################
source ./environments/unix/env.sh
if [ $TRAVIS_OS_NAME = 'osx' ]; then
    cmake . \
        -DWITHIN_TRAVIS=ON \
        -DWITH_BLOSC=ON \
        -DWITH_ZLIB=ON \
        -DWITH_BZIP2=ON \
        -DWITH_XZ=ON \
        -DWITH_LZ4=ON \
        -DWITH_S3=OFF \
        -DCMAKE_PREFIX_PATH="$ENV_ROOT" \
        -DPYTHON_EXECUTABLE="$PY_BIN" \
        -DCMAKE_CXX_FLAGS="-std=c++17" \
        -DWITH_BOOST_FS=OFF \
        -DBUILD_TESTS=ON \
        -DBUILD_Z5PY=ON
else
    cmake . \
        -DWITHIN_TRAVIS=ON \
        -DWITH_BLOSC=ON \
        -DWITH_ZLIB=ON \
        -DWITH_BZIP2=ON \
        -DWITH_XZ=ON \
        -DWITH_LZ4=ON \
        -DWITH_S3=OFF \
        -DCMAKE_PREFIX_PATH="$ENV_ROOT" \
        -DPYTHON_EXECUTABLE="$PY_BIN" \
        -DCMAKE_CXX_FLAGS="-std=c++17" \
        -DBUILD_TESTS=ON \
        -DBUILD_Z5PY=ON
fi

###############################################
# the actual build
###############################################
make -j 4
