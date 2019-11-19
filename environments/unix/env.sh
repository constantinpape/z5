#!/bin/sh

# Assumes that the conda environment is activated.
# Tests will only work after build.

# for cmake
export ENV_BIN=$CONDA_PREFIX/bin
export PY_BIN=$ENV_BIN/python
# export CC="$ENV_BIN/cc"
# export CXX="$ENV_BIN/c++"

export BOOST_INCLUDEDIR=$CONDA_PREFIX/include
