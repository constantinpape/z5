## Installation

z5py is distributed for Linux, macOS and Windows on conda-forge and PyPI.

### Conda

Conda packages (built with all compression codecs and the S3 backend) are hosted
on conda-forge:

```bash
conda install -c conda-forge z5py
```

### Pip

Wheels are published on PyPI:

```bash
pip install z5py
```

The PyPI wheels are built with all compression codecs but **without the S3
backend** — constructing a `z5py.S3File` then raises
*"z5 was not compiled with s3 support"*. For S3 support, install via conda or
build from source with `-DWITH_S3=ON`.

### From source

The easiest way to build from source is a conda environment with all
dependencies. Environment files live in `environments/unix`:

```bash
conda env create -f environments/unix/z5-dev.yaml
conda activate z5-dev
mkdir bld && cd bld
cmake .. \
    -DWITH_BLOSC=ON -DWITH_ZLIB=ON -DWITH_BZIP2=ON \
    -DWITH_XZ=ON -DWITH_LZ4=ON -DWITH_ZSTD=ON \
    -DCMAKE_INSTALL_PREFIX=/path/to/install
make -j4 install
```

A codec must be compiled in to read or write chunks that use it; the
`WITH_<CODEC>` options other than `WITH_ZLIB` are **off by default**. CMake tries
to auto-detect the active conda environment as `CMAKE_PREFIX_PATH`; if that
fails, pass `-DCMAKE_PREFIX_PATH=/path/to/conda-env` explicitly. To install the
Python package somewhere other than the active environment's `site-packages`,
set `PYTHON_MODULE_INSTALL_DIR`.

If you do not want to use conda, the build needs
[nlohmann_json](https://github.com/nlohmann/json), plus
[nanobind](https://github.com/wjakob/nanobind) and
[numpy](https://numpy.org/) for the Python bindings, and the development
libraries of whichever compression codecs you enable.
