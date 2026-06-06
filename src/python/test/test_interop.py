"""Bidirectional interop tests between z5 and external libraries.

For every (library, format) combination we round-trip data in *both* directions
(z5 writes -> external reads, and external writes -> z5 reads) across dtypes,
compression codecs, irregular (non-chunk-aligned) shapes, custom fill values and
dimension separators.

Libraries:
* zarr-python (>= 3): zarr v2 (``zarr_format=2``) and zarr v3.
* tensorstore: zarr v2 (``zarr``), zarr v3 (``zarr3``) and n5 (``n5``).

zarr-python v3 dropped N5 support, so n5 <-> external interop is covered by
tensorstore. n5 stores axes in the opposite (column-major) order to z5/zarr, so
the tensorstore n5 helpers transpose and reverse shape/chunks; every comparison
is therefore against the z5-logical array.

The zarr v3 classes are skip-gated until z5 implements v3; the sharding test is
additionally gated on v3 sharding (which implies the crc32c shard index).
"""
import os
import unittest
from abc import ABC
from shutil import rmtree

import numpy as np
import z5py

from _v3_capability import (HAVE_TS, HAVE_ZARR, requires_tensorstore,
                            requires_z5_v3, requires_z5_v3_sharding,
                            requires_zarr_v3)

if HAVE_ZARR:
    import zarr
    import numcodecs
if HAVE_TS:
    import tensorstore as ts


# ---------------------------------------------------------------------------
# codec specs per library / format
# ---------------------------------------------------------------------------
def _zpy_v2_compressor(name):
    return {"raw": None,
            "zlib": numcodecs.Zlib(),
            "gzip": numcodecs.GZip(),
            "blosc": numcodecs.Blosc(),
            "zstd": numcodecs.Zstd(),
            "bzip2": numcodecs.BZ2()}[name]


def _zpy_v3_compressors(name):
    from zarr.codecs import GzipCodec, ZstdCodec, BloscCodec
    # ``None`` -> bytes-only pipeline (no compressor)
    return {"raw": None,
            "gzip": GzipCodec(),
            "zstd": ZstdCodec(),
            "blosc": BloscCodec()}[name]


def _ts_v2_compressor(name):
    return {"raw": None,
            "zlib": {"id": "zlib", "level": 5},
            "blosc": {"id": "blosc", "cname": "lz4", "clevel": 5, "shuffle": 1},
            "zstd": {"id": "zstd", "level": 3},
            "bzip2": {"id": "bz2", "level": 5}}[name]


def _ts_n5_compression(name):
    return {"raw": {"type": "raw"},
            "gzip": {"type": "gzip"},
            "zstd": {"type": "zstd"},
            "bzip2": {"type": "bzip2"},
            "xz": {"type": "xz"}}[name]


def _ts_v3_codecs(name):
    bytes_c = {"name": "bytes", "configuration": {"endian": "little"}}
    if name == "raw":
        return [bytes_c]
    comp = {"gzip": {"name": "gzip", "configuration": {"level": 5}},
            "zstd": {"name": "zstd", "configuration": {"level": 3}},
            "blosc": {"name": "blosc",
                      "configuration": {"cname": "lz4", "clevel": 5,
                                        "shuffle": "shuffle"}}}[name]
    return [bytes_c, comp]


def _kv(path):
    return {"driver": "file", "path": path}


def _json_fill(value):
    """JSON-encode a fill value, mapping special floats to their string form."""
    if isinstance(value, float):
        if np.isnan(value):
            return "NaN"
        if value == np.inf:
            return "Infinity"
        if value == -np.inf:
            return "-Infinity"
    return value


# ---------------------------------------------------------------------------
# z5 helpers
# ---------------------------------------------------------------------------
def open_z5(path, fmt, mode="a", dimension_separator=None):
    if fmt == "zarr_v3":
        return z5py.File(path, mode=mode, use_zarr_format=True, zarr_format=3,
                         dimension_separator=dimension_separator or "/")
    if fmt == "n5":
        return z5py.File(path, mode=mode, use_zarr_format=False)
    return z5py.File(path, mode=mode, use_zarr_format=True,
                     dimension_separator=dimension_separator or ".")


# ---------------------------------------------------------------------------
# external write / read (always operate in z5-logical axis order)
# ---------------------------------------------------------------------------
def external_write(library, fmt, path, key, data, chunks, compression="raw",
                   shards=None, fill_value=0, dimension_separator=None):
    """Write ``data`` (z5-logical) with the external library.

    After this call, ``z5py`` opening ``path``/``key`` must read back ``data``.
    """
    kpath = os.path.join(path, key)
    shape = data.shape
    if library == "zarr":
        zf = 3 if fmt == "zarr_v3" else 2
        g = zarr.open_group(store=path, mode="a", zarr_format=zf)
        kwargs = dict(name=key, shape=shape, chunks=chunks, dtype=data.dtype,
                      fill_value=fill_value)
        if fmt == "zarr_v3":
            kwargs["compressors"] = _zpy_v3_compressors(compression)
            if shards is not None:
                kwargs["shards"] = shards
            if dimension_separator is not None:
                kwargs["chunk_key_encoding"] = {
                    "name": "v2",
                    "configuration": {"separator": dimension_separator}}
        else:
            kwargs["compressors"] = _zpy_v2_compressor(compression)
            if dimension_separator is not None:
                kwargs["chunk_key_encoding"] = {
                    "name": "v2",
                    "configuration": {"separator": dimension_separator}}
        arr = g.create_array(**kwargs)
        arr[:] = data
        return

    if library == "tensorstore":
        if fmt == "zarr_v2":
            meta = {"shape": list(shape), "chunks": list(chunks),
                    "dtype": data.dtype.str,
                    "compressor": _ts_v2_compressor(compression),
                    "fill_value": _json_fill(float(fill_value)
                                             if np.issubdtype(data.dtype, np.floating)
                                             else fill_value)}
            if dimension_separator is not None:
                meta["dimension_separator"] = dimension_separator
            ts.open({"driver": "zarr", "kvstore": _kv(kpath), "metadata": meta,
                     "create": True, "delete_existing": True}).result()[...] = data
        elif fmt == "zarr_v3":
            if shards is None:
                codecs = _ts_v3_codecs(compression)
                grid = chunks
            else:
                codecs = [{"name": "sharding_indexed",
                           "configuration": {
                               "chunk_shape": list(chunks),
                               "codecs": _ts_v3_codecs(compression),
                               "index_codecs": [
                                   {"name": "bytes",
                                    "configuration": {"endian": "little"}},
                                   {"name": "crc32c"}]}}]
                grid = shards
            meta = {"shape": list(shape),
                    "chunk_grid": {"name": "regular",
                                   "configuration": {"chunk_shape": list(grid)}},
                    "data_type": np.dtype(data.dtype).name, "codecs": codecs}
            ts.open({"driver": "zarr3", "kvstore": _kv(kpath), "metadata": meta,
                     "create": True, "delete_existing": True}).result()[...] = data
        elif fmt == "n5":
            # n5 stores axes reversed: write the transpose with reversed meta so
            # that z5py reads back the z5-logical ``data``.
            meta = {"dimensions": list(shape[::-1]),
                    "blockSize": list(chunks[::-1]),
                    "dataType": np.dtype(data.dtype).name,
                    "compression": _ts_n5_compression(compression)}
            ts.open({"driver": "n5", "kvstore": _kv(kpath), "metadata": meta,
                     "create": True, "delete_existing": True}).result()[...] = \
                np.ascontiguousarray(np.transpose(data))
        else:
            raise ValueError(fmt)
        return

    raise ValueError(library)


def external_read(library, fmt, path, key):
    """Read an array written by z5py, returning it in z5-logical axis order."""
    kpath = os.path.join(path, key)
    if library == "zarr":
        zf = 3 if fmt == "zarr_v3" else 2
        g = zarr.open_group(store=path, mode="r", zarr_format=zf)
        return g[key][:]
    if library == "tensorstore":
        driver = {"zarr_v2": "zarr", "zarr_v3": "zarr3", "n5": "n5"}[fmt]
        store = ts.open({"driver": driver, "kvstore": _kv(kpath),
                         "open": True}).result()
        out = store.read().result()
        if fmt == "n5":
            out = np.transpose(out)
        return out
    raise ValueError(library)


# ---------------------------------------------------------------------------
# shared test logic
# ---------------------------------------------------------------------------
class InteropMixin(ABC):
    #: subclasses set: library, fmt, ext, codecs
    dtypes = ["int8", "int16", "int32", "int64",
              "uint8", "uint16", "uint32", "uint64",
              "float32", "float64"]
    shape = (60, 40)
    chunks = (20, 20)
    supports_fillvalue = True
    supports_dimsep = True

    def setUp(self):
        self.path = "interop." + self.ext

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    def _data(self, dtype, shape=None):
        shape = self.shape if shape is None else shape
        if np.issubdtype(np.dtype(dtype), np.floating):
            return (np.random.rand(*shape) * 100).astype(dtype)
        return np.random.randint(0, 100, size=shape).astype(dtype)

    def _msg(self, what, extra):
        return "%s failed for %s/%s %s" % (what, self.library, self.fmt, extra)

    # ----------------------------------------------------------------- dtypes
    def test_z5_to_external_dtypes(self):
        f = open_z5(self.path, self.fmt)
        for dtype in self.dtypes:
            data = self._data(dtype)
            key = "d_%s" % dtype
            f.create_dataset(key, data=data, chunks=self.chunks, compression="raw")
            out = external_read(self.library, self.fmt, self.path, key)
            self.assertEqual(out.shape, data.shape, self._msg("dtype", dtype))
            self.assertTrue(np.array_equal(out, data), self._msg("dtype", dtype))

    def test_external_to_z5_dtypes(self):
        open_z5(self.path, self.fmt)
        for dtype in self.dtypes:
            data = self._data(dtype)
            key = "d_%s" % dtype
            external_write(self.library, self.fmt, self.path, key, data,
                           self.chunks, compression="raw")
            out = open_z5(self.path, self.fmt, mode="r")[key][:]
            self.assertEqual(out.shape, data.shape, self._msg("dtype", dtype))
            self.assertTrue(np.array_equal(out, data), self._msg("dtype", dtype))

    # ----------------------------------------------------------------- codecs
    def test_z5_to_external_codecs(self):
        f = open_z5(self.path, self.fmt)
        for comp in self.codecs:
            data = self._data("int32")
            key = "c_%s" % comp
            f.create_dataset(key, data=data, chunks=self.chunks, compression=comp)
            out = external_read(self.library, self.fmt, self.path, key)
            self.assertTrue(np.array_equal(out, data), self._msg("codec", comp))

    def test_external_to_z5_codecs(self):
        open_z5(self.path, self.fmt)
        for comp in self.codecs:
            data = self._data("int32")
            key = "c_%s" % comp
            external_write(self.library, self.fmt, self.path, key, data,
                           self.chunks, compression=comp)
            out = open_z5(self.path, self.fmt, mode="r")[key][:]
            self.assertTrue(np.array_equal(out, data), self._msg("codec", comp))

    # -------------------------------------------------------------- irregular
    def test_irregular_roundtrip(self):
        shape, chunks = (123, 97), (17, 32)
        data = self._data("float64", shape)
        f = open_z5(self.path, self.fmt)
        f.create_dataset("irr", data=data, chunks=chunks, compression="raw")
        out = external_read(self.library, self.fmt, self.path, "irr")
        self.assertTrue(np.array_equal(out, data), self._msg("irregular", shape))

        data2 = self._data("float64", shape)
        external_write(self.library, self.fmt, self.path, "irr2", data2, chunks)
        out2 = open_z5(self.path, self.fmt, mode="r")["irr2"][:]
        self.assertTrue(np.array_equal(out2, data2), self._msg("irregular", shape))

    # ------------------------------------------------------------- fill value
    def test_fillvalue_roundtrip(self):
        if not self.supports_fillvalue:
            self.skipTest("%s has no custom fill value" % self.fmt)
        f = open_z5(self.path, self.fmt)
        shape, chunks = (40, 40), (10, 10)
        for i, fv in enumerate([0, 42, np.nan, np.inf, -np.inf]):
            key = "fv_%i" % i
            f.create_dataset(key, shape=shape, chunks=chunks, dtype="float32",
                             fillvalue=fv)
            out = external_read(self.library, self.fmt, self.path, key)
            exp = np.full(shape, fv, dtype="float32")
            self.assertTrue(
                np.allclose(out, exp, equal_nan=np.isnan(fv) if isinstance(fv, float) else False),
                self._msg("fillvalue", fv))

    # ------------------------------------------------------ dimension separator
    def test_dimsep_roundtrip(self):
        if not self.supports_dimsep:
            self.skipTest("dimension separator not applicable for %s" % self.fmt)
        sep = "/"
        f = open_z5(self.path, self.fmt, dimension_separator=sep)
        d1 = self._data("int32")
        f.create_dataset("d1", data=d1, chunks=self.chunks, compression="raw")
        out1 = external_read(self.library, self.fmt, self.path, "d1")
        self.assertTrue(np.array_equal(out1, d1), self._msg("dimsep z5->ext", sep))

        d2 = self._data("int32")
        external_write(self.library, self.fmt, self.path, "d2", d2, self.chunks,
                       dimension_separator=sep)
        out2 = open_z5(self.path, self.fmt, mode="r")["d2"][:]
        self.assertTrue(np.array_equal(out2, d2), self._msg("dimsep ext->z5", sep))

    # ---------------------------------------------------------------- sharding
    @requires_z5_v3_sharding
    def test_sharding_roundtrip(self):
        if self.fmt != "zarr_v3":
            self.skipTest("sharding is zarr v3 only")
        shape, chunks, shards = (64, 64), (16, 16), (32, 32)
        f = open_z5(self.path, self.fmt)
        for comp in self.codecs:
            data = self._data("float32", shape)
            key = "s_%s" % comp
            f.create_dataset(key, data=data, chunks=chunks, shards=shards,
                             compression=comp)
            out = external_read(self.library, self.fmt, self.path, key)
            self.assertTrue(np.array_equal(out, data),
                            self._msg("sharding z5->ext", comp))

            data2 = self._data("float32", shape)
            key2 = "se_%s" % comp
            external_write(self.library, self.fmt, self.path, key2, data2,
                           chunks, compression=comp, shards=shards)
            out2 = open_z5(self.path, self.fmt, mode="r")[key2][:]
            self.assertTrue(np.array_equal(out2, data2),
                            self._msg("sharding ext->z5", comp))


# ---------------------------------------------------------------------------
# concrete combinations
# ---------------------------------------------------------------------------
@requires_zarr_v3
class TestInteropZarrPyV2(InteropMixin, unittest.TestCase):
    library = "zarr"
    fmt = "zarr_v2"
    ext = "zr"
    codecs = ["raw", "zlib", "gzip", "blosc", "zstd", "bzip2"]


@requires_tensorstore
class TestInteropTsV2(InteropMixin, unittest.TestCase):
    library = "tensorstore"
    fmt = "zarr_v2"
    ext = "zr"
    codecs = ["raw", "zlib", "blosc", "zstd", "bzip2"]


@requires_tensorstore
class TestInteropTsN5(InteropMixin, unittest.TestCase):
    library = "tensorstore"
    fmt = "n5"
    ext = "n5"
    # tensorstore's n5 blosc is not byte-compatible with z5's n5 blosc
    codecs = ["raw", "gzip", "zstd", "bzip2", "xz"]
    supports_fillvalue = False
    supports_dimsep = False


@requires_z5_v3
@requires_zarr_v3
class TestInteropZarrPyV3(InteropMixin, unittest.TestCase):
    library = "zarr"
    fmt = "zarr_v3"
    ext = "zr"
    codecs = ["raw", "gzip", "zstd", "blosc"]
    # v3 chunk-key encoding is covered in test_zarr_v3.py
    supports_dimsep = False


@requires_z5_v3
@requires_tensorstore
class TestInteropTsV3(InteropMixin, unittest.TestCase):
    library = "tensorstore"
    fmt = "zarr_v3"
    ext = "zr"
    codecs = ["raw", "gzip", "zstd", "blosc"]
    supports_dimsep = False


if __name__ == '__main__':
    unittest.main()
