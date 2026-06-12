"""z5py: Pythonic reading and writing of zarr and n5 containers.

z5py is the Python interface to **z5**, a C++ library for the
[zarr](https://zarr.dev/) and [n5](https://github.com/saalfeldlab/n5) chunked
array formats. Numpy arrays are passed in and out directly, and the API closely
mirrors [h5py](https://www.h5py.org/): a :class:`File` is a container on disc (or
in an S3 bucket, see :class:`S3File`), :class:`Group` nodes form a hierarchy, and
:class:`Dataset` holds a chunked, optionally compressed nd array that is read and
written with numpy-style indexing.

zarr v2, zarr v3 (including sharding) and n5 are supported, along with the blosc,
zlib/gzip, bzip2, xz, lz4 and zstd compression codecs (whichever were compiled
in).

Quick start:
    >>> import z5py
    >>> import numpy as np
    >>> with z5py.File("data.zarr") as f:                      # create / open a container
    ...     ds = f.create_dataset("raw", shape=(100, 100),
    ...                           chunks=(32, 32), dtype="uint8")
    ...     ds[:50, :50] = np.ones((50, 50), dtype="uint8")    # write a region
    ...     ds[50:, 50:] = 42                                  # broadcast a scalar
    ...     block = ds[:10, :10]                               # read a region
    ...     ds.attrs["description"] = "example data"           # custom attributes

Beyond the core API there are a few helper modules:

- `z5py.util` — block-wise iteration, parallel dataset / group copying, chunk
  maintenance and unique-value computation.
- `z5py.converter` — convert datasets to and from HDF5 and TIFF.
- `z5py.attribute_manager` — the attribute (``.attrs``) interface and hooks to
  customize JSON (de)serialization.

See the pages below for installation, using z5 as a C++ library, and a
performance comparison against zarr-python and tensorstore.

.. include:: docs/installation.md
.. include:: docs/cpp.md
.. include:: docs/performance.md
"""
__docformat__ = "google"

from .file import File, N5File, ZarrFile, S3File
from .dataset import Dataset
from .group import Group
from .attribute_manager import set_json_encoder, set_json_decoder

__all__ = ['File', 'N5File', 'ZarrFile', 'S3File', 'Dataset', 'Group',
           'set_json_encoder', 'set_json_decoder']

# Version is single-sourced from include/z5/z5.hxx. CMake generates _version.py
# from those macros at build time (see src/python/_version.py.in), covering the
# make-install and in-place build-dir layouts. For wheel and editable installs
# the generated file isn't on the import path, so fall back to the installed
# package metadata, which scikit-build-core fills from the same z5.hxx macros.
try:
    from ._version import __version__, __version_info__
except ImportError:
    from importlib.metadata import PackageNotFoundError, version as _pkg_version
    try:
        __version__ = _pkg_version("z5py")
    except PackageNotFoundError:  # unbuilt source checkout
        __version__ = "0.0.0+unknown"
    __version_info__ = tuple(
        int(p) for p in __version__.split("+")[0].split(".") if p.isdigit()
    )
