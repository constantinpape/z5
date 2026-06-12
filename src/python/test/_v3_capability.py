"""Shared helpers for the format-parametrized test suite.

This module provides:

* capability probes (``z5_supports_v3`` / ``z5_supports_v3_sharding``) so the
  zarr v3 tests skip cleanly until z5 implements v3 support, instead of failing,
* external-library availability flags (zarr, numcodecs, tensorstore),
* the corresponding ``unittest`` skip decorators,
* small helpers to pick the on-disk extension and open a root file for a given
  test ``data_format`` (``'zarr'`` / ``'zr'`` -> zarr v2, ``'n5'`` -> n5,
  ``'zarr_v3'`` -> zarr v3).

It must never raise at import time -- every probe and optional import is guarded.
"""
import functools
import os
import shutil
import tempfile
import unittest

import numpy as np


# ---------------------------------------------------------------------------
# external library availability (gate interop tests on importability only)
# ---------------------------------------------------------------------------
try:
    import zarr
    HAVE_ZARR = True
    try:
        ZARR_MAJOR = int(zarr.__version__.split(".")[0])
    except Exception:
        ZARR_MAJOR = 0
except Exception:
    zarr = None
    HAVE_ZARR = False
    ZARR_MAJOR = 0

try:
    import numcodecs
    HAVE_NUMCODECS = True
except Exception:
    numcodecs = None
    HAVE_NUMCODECS = False

try:
    # NOTE: tensorstore exposes no ``__version__`` -- never gate on a version
    # string, only on importability.
    import tensorstore
    HAVE_TS = True
except Exception:
    tensorstore = None
    HAVE_TS = False


# ---------------------------------------------------------------------------
# format helpers
# ---------------------------------------------------------------------------
#: core compression codecs exercised for zarr v3. The v3 ``bytes`` codec (no
#: compression) corresponds to z5's ``'raw'``.
V3_CORE_COMPRESSORS = ("raw", "gzip", "blosc", "zstd")


def format_ext(data_format):
    """On-disk extension for a given test ``data_format``.

    zarr v3 still uses a zarr extension (``.zr``); only ``zarr_format`` differs.
    """
    return "zr" if data_format == "zarr_v3" else data_format


def open_root_file(path, data_format, mode="a"):
    """Open a z5py root file for the given ``data_format``.

    ``'zarr_v3'`` requests zarr format 3 (nested ``c/`` chunk keys);
    ``'zarr'`` / ``'zr'`` request zarr format 2, ``'n5'`` requests n5.
    """
    import z5py
    if data_format == "zarr_v3":
        return z5py.File(path, mode=mode, use_zarr_format=True,
                         zarr_format=3, dimension_separator="/")
    return z5py.File(path, mode=mode,
                     use_zarr_format=data_format in ("zarr", "zr"))


def available_v3_compressors():
    """Core v3 compressors that are actually compiled into this z5 build."""
    from z5py.dataset import Dataset
    return tuple(c for c in V3_CORE_COMPRESSORS if c in Dataset.compressors_zarr)


# ---------------------------------------------------------------------------
# z5 capability probes (used to skip v3 tests until v3 is implemented)
# ---------------------------------------------------------------------------
# Any of these means "the capability is not (yet) present"; a missing kwarg
# raises TypeError, an unimplemented path raises RuntimeError, etc.
_PROBE_ERRORS = (TypeError, RuntimeError, AttributeError, ValueError,
                 OSError, KeyError)


@functools.lru_cache(maxsize=1)
def z5_supports_v3():
    """True iff z5py can create and round-trip a tiny zarr v3 dataset."""
    import z5py
    tmp = tempfile.mkdtemp()
    path = os.path.join(tmp, "probe.zr")
    try:
        f = z5py.File(path, mode="w", use_zarr_format=True, zarr_format=3)
        ds = f.create_dataset("p", shape=(4, 4), chunks=(2, 2),
                              dtype="int32", compression="raw")
        expected = np.arange(16, dtype="int32").reshape(4, 4)
        ds[:] = expected
        if not np.array_equal(ds[:], expected):
            return False
        return os.path.exists(os.path.join(path, "zarr.json"))
    except _PROBE_ERRORS:
        return False
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


@functools.lru_cache(maxsize=1)
def z5_supports_v3_sharding():
    """True iff z5py can round-trip a sharded zarr v3 dataset.

    The probe uses the library-default crc32c shard index, so this implies
    crc32c support as well.
    """
    if not z5_supports_v3():
        return False
    import z5py
    tmp = tempfile.mkdtemp()
    path = os.path.join(tmp, "probe_shard.zr")
    try:
        f = z5py.File(path, mode="w", use_zarr_format=True, zarr_format=3)
        ds = f.create_dataset("p", shape=(8, 8), chunks=(2, 2), shards=(4, 4),
                              dtype="int32", compression="raw")
        expected = np.arange(64, dtype="int32").reshape(8, 8)
        ds[:] = expected
        return np.array_equal(ds[:], expected)
    except _PROBE_ERRORS:
        return False
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# ---------------------------------------------------------------------------
# skip decorators
# ---------------------------------------------------------------------------
requires_z5_v3 = unittest.skipUnless(
    z5_supports_v3(), "z5 has no zarr v3 support yet")
requires_z5_v3_sharding = unittest.skipUnless(
    z5_supports_v3_sharding(), "z5 has no zarr v3 sharding support yet")
requires_zarr = unittest.skipUnless(HAVE_ZARR, "requires the zarr package")
requires_zarr_v3 = unittest.skipUnless(
    HAVE_ZARR and ZARR_MAJOR >= 3, "requires zarr-python >= 3")
requires_numcodecs = unittest.skipUnless(HAVE_NUMCODECS, "requires numcodecs")
requires_tensorstore = unittest.skipUnless(HAVE_TS, "requires tensorstore")
