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
import json
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

    @property
    def _available_codecs(self):
        """self.codecs restricted to codecs actually compiled into this z5 build.

        Not every codec is compiled on every platform (e.g. xz is not enabled in
        the Windows build), so skip codecs that this z5 build cannot handle.
        """
        from z5py.dataset import Dataset
        if self.fmt == "n5":
            avail = set(Dataset.compressors_n5)
        elif self.fmt == "zarr_v3":
            avail = set(Dataset.compressors_zarr_v3)
        else:  # zarr_v2
            avail = set(Dataset.compressors_zarr)
        return [c for c in self.codecs if c in avail]

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
        for comp in self._available_codecs:
            data = self._data("int32")
            key = "c_%s" % comp
            f.create_dataset(key, data=data, chunks=self.chunks, compression=comp)
            out = external_read(self.library, self.fmt, self.path, key)
            self.assertTrue(np.array_equal(out, data), self._msg("codec", comp))

    def test_external_to_z5_codecs(self):
        open_z5(self.path, self.fmt)
        for comp in self._available_codecs:
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
        for comp in self._available_codecs:
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
@requires_zarr_v3
class TestInteropZarrPyV3BigEndian(unittest.TestCase):
    """zarr-python arrays whose ``bytes`` codec stores elements big-endian."""
    path = "interop_big_endian.zr"
    shape = (45, 37)
    chunks = (20, 16)

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    def _data(self, dtype):
        if np.issubdtype(np.dtype(dtype), np.floating):
            return (np.random.rand(*self.shape) * 100).astype(dtype)
        return np.random.randint(0, 100, size=self.shape).astype(dtype)

    def _create_zarr(self, name, dtype, comp, sharded=False):
        from zarr.codecs import BytesCodec
        compressor = _zpy_v3_compressors(comp)
        kwargs = {}
        if sharded:
            kwargs["shards"] = (40, 32)
        return zarr.create_array(
            store=self.path, name=name, shape=self.shape, chunks=self.chunks,
            dtype=dtype, serializer=BytesCodec(endian="big"),
            compressors=None if compressor is None else [compressor],
            zarr_format=3, **kwargs)

    def _roundtrip(self, sharded):
        for comp in ("raw", "gzip"):
            for dtype in ("int16", "uint32", "float32", "float64"):
                name = "%s_%s" % (comp, dtype)
                data = self._data(dtype)
                self._create_zarr(name, dtype, comp, sharded=sharded)[:] = data

                # zarr (big-endian) -> z5
                ds = z5py.File(self.path, mode="a")[name]
                np.testing.assert_array_equal(ds[:], data, err_msg=name)

                # z5 -> zarr: partial write into the existing big-endian chunks
                new = self._data(dtype)[5:30, 3:20]
                ds[5:30, 3:20] = new
                data[5:30, 3:20] = new
                out = zarr.open_array(store=self.path, path=name, mode="r")[:]
                np.testing.assert_array_equal(out, data, err_msg=name)

    def test_big_endian_roundtrip(self):
        self._roundtrip(sharded=False)

    @requires_z5_v3_sharding
    def test_big_endian_sharded_roundtrip(self):
        self._roundtrip(sharded=True)


@requires_z5_v3
@requires_zarr_v3
class TestInteropZarrPyV3DimensionNames(unittest.TestCase):
    """``dimension_names`` written by one library and read by the other. An
    unnamed dimension (None) and an empty name ("") are different values."""
    path = "interop_dimension_names.zr"
    shape = (4, 6, 8)
    chunks = (2, 3, 4)
    names = {"named": ("z", "y", "x"),
             "unnamed": (None, "y", None),
             "all_unnamed": (None, None, None),
             "empty": ("", None, "x")}

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    def test_z5_to_zarr(self):
        f = z5py.File(self.path, mode="w", use_zarr_format=True, zarr_format=3)
        for key, names in self.names.items():
            f.create_dataset(key, shape=self.shape, chunks=self.chunks, dtype="float32",
                             dimension_names=names)
            za = zarr.open_array(store=self.path, path=key, mode="r")
            self.assertEqual(za.metadata.dimension_names, names, key)
        f.create_dataset("no_names", shape=self.shape, chunks=self.chunks, dtype="float32")
        za = zarr.open_array(store=self.path, path="no_names", mode="r")
        self.assertIsNone(za.metadata.dimension_names)

    def test_zarr_to_z5(self):
        # uncompressed, so a build without zarr-python's default codec reads them
        for key, names in self.names.items():
            zarr.create_array(store=self.path, name=key, shape=self.shape, chunks=self.chunks,
                              dtype="float32", dimension_names=names, compressors=None, zarr_format=3)
        zarr.create_array(store=self.path, name="no_names", shape=self.shape, chunks=self.chunks,
                          dtype="float32", compressors=None, zarr_format=3)
        f = z5py.File(self.path, mode="r")
        for key, names in self.names.items():
            self.assertEqual(f[key].dimension_names, names, key)
        self.assertIsNone(f["no_names"].dimension_names)


@requires_z5_v3
@requires_zarr_v3
class TestInteropZarrPyV3Transpose(unittest.TestCase):
    """zarr-python arrays with a ``transpose`` filter (array-to-array codec)."""
    path = "interop_transpose.zr"
    # irregular shape -> padded edge chunks
    shape = (23, 37, 18)
    chunks = (10, 16, 8)
    shards = (20, 32, 16)

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    def _data(self, dtype):
        if np.issubdtype(np.dtype(dtype), np.floating):
            return (np.random.rand(*self.shape) * 100).astype(dtype)
        return np.random.randint(0, 100, size=self.shape).astype(dtype)

    def _roundtrip(self, sharded, endian="little"):
        from zarr.codecs import BytesCodec, TransposeCodec
        for order in ((2, 0, 1), (1, 2, 0), (2, 1, 0)):
            for comp in ("raw", "gzip"):
                for dtype in ("uint8", "int16", "float32", "float64"):
                    name = "%s_%s_%s" % ("".join(map(str, order)), comp, dtype)
                    compressor = _zpy_v3_compressors(comp)
                    kwargs = {"shards": self.shards} if sharded else {}
                    za = zarr.create_array(
                        store=self.path, name=name, shape=self.shape,
                        chunks=self.chunks, dtype=dtype,
                        filters=[TransposeCodec(order=order)],
                        serializer=BytesCodec(endian=endian),
                        compressors=None if compressor is None else [compressor],
                        zarr_format=3, **kwargs)
                    data = self._data(dtype)
                    za[:] = data

                    # zarr (transposed) -> z5
                    ds = z5py.File(self.path, mode="a")[name]
                    self.assertEqual(ds.transpose, order)
                    np.testing.assert_array_equal(ds[:], data, err_msg=name)

                    # z5 -> zarr: partial write across chunk boundaries
                    new = self._data(dtype)[5:19, 3:30, 2:17]
                    ds[5:19, 3:30, 2:17] = new
                    data[5:19, 3:30, 2:17] = new
                    out = zarr.open_array(store=self.path, path=name, mode="r")[:]
                    np.testing.assert_array_equal(out, data, err_msg=name)

    def test_transpose_roundtrip(self):
        self._roundtrip(sharded=False)

    def test_transpose_big_endian_roundtrip(self):
        self._roundtrip(sharded=False, endian="big")

    @requires_z5_v3_sharding
    def test_transpose_sharded_roundtrip(self):
        self._roundtrip(sharded=True)

    def _z5_create(self, sharded):
        f = z5py.File(self.path, mode="w", use_zarr_format=True, zarr_format=3)
        for order in ((2, 0, 1), (1, 2, 0), (2, 1, 0)):
            for comp in ("raw", "gzip"):
                for dtype in ("uint8", "int16", "float32", "float64"):
                    name = "%s_%s_%s" % ("".join(map(str, order)), comp, dtype)
                    kwargs = {"shards": self.shards} if sharded else {}
                    data = self._data(dtype)
                    f.create_dataset(name, data=data, chunks=self.chunks,
                                     compression=comp, transpose=order, **kwargs)
                    za = zarr.open_array(store=self.path, path=name, mode="r")
                    np.testing.assert_array_equal(za[:], data, err_msg=name)

    def test_z5_created_transpose(self):
        self._z5_create(sharded=False)

    @requires_z5_v3_sharding
    def test_z5_created_transpose_sharded(self):
        self._z5_create(sharded=True)


@requires_z5_v3_sharding
@requires_zarr_v3
class TestInteropZarrPyV3ShardIndex(unittest.TestCase):
    """zarr-python shards with a non-default index layout."""
    path = "interop_shard_index.zr"
    shape = (37, 29)
    chunks = (8, 8)
    shards = (16, 16)

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    def test_index_layouts(self):
        from zarr.codecs import BytesCodec, Crc32cCodec, GzipCodec, ShardingCodec
        for location in ("start", "end"):
            for endian in ("little", "big"):
                for checksum in (True, False):
                    name = "%s_%s_%s" % (location, endian, "crc" if checksum else "nocrc")
                    index_codecs = [BytesCodec(endian=endian)]
                    if checksum:
                        index_codecs.append(Crc32cCodec())
                    sharding = ShardingCodec(chunk_shape=self.chunks,
                                             codecs=[BytesCodec(), GzipCodec()],
                                             index_codecs=index_codecs,
                                             index_location=location)
                    za = zarr.create_array(store=self.path, name=name, shape=self.shape,
                                           chunks=self.shards, dtype="int32",
                                           serializer=sharding, compressors=None,
                                           zarr_format=3)
                    data = np.random.randint(0, 1000, size=self.shape).astype("int32")
                    # leave the last inner chunks unwritten -> empty slots
                    data[32:, 24:] = 0
                    za[:] = data

                    # zarr -> z5
                    ds = z5py.File(self.path, mode="a")[name]
                    self.assertEqual(ds.shards, self.shards)
                    np.testing.assert_array_equal(ds[:], data, err_msg=name)

                    # z5 -> zarr: partial write, z5 rebuilds the shards in their layout
                    new = np.random.randint(0, 1000, size=(20, 11)).astype("int32")
                    ds[3:23, 10:21] = new
                    data[3:23, 10:21] = new
                    out = zarr.open_array(store=self.path, path=name, mode="r")[:]
                    np.testing.assert_array_equal(out, data, err_msg=name)


@requires_z5_v3
@requires_zarr_v3
class TestInteropZarrPyV3Crc32c(unittest.TestCase):
    """zarr-python arrays whose bytes-to-bytes codecs include crc32c."""
    path = "interop_crc32c.zr"
    shape = (37, 29)
    chunks = (8, 8)
    shards = (16, 16)

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    @staticmethod
    def _chains():
        from zarr.codecs import BloscCodec, Crc32cCodec, GzipCodec, ZstdCodec
        from _v3_capability import available_v3_compressors
        available = available_v3_compressors()
        chains = {"crc": [Crc32cCodec()],
                  "gzip_crc": [GzipCodec(), Crc32cCodec()],
                  "crc_gzip": [Crc32cCodec(), GzipCodec()],
                  "crc_gzip_crc": [Crc32cCodec(), GzipCodec(), Crc32cCodec()]}
        if "zstd" in available:
            chains["zstdsum"] = [ZstdCodec(checksum=True)]
            chains["zstdsum_crc"] = [ZstdCodec(checksum=True), Crc32cCodec()]
            chains["crc_zstd"] = [Crc32cCodec(), ZstdCodec()]
        if "blosc" in available:
            chains["blosc_crc"] = [BloscCodec(), Crc32cCodec()]
            chains["crc_blosc"] = [Crc32cCodec(), BloscCodec()]
        return chains

    def _check(self, sharded):
        for name, compressors in self._chains().items():
            kwargs = {"chunks": self.chunks}
            if sharded:
                kwargs["shards"] = self.shards
            za = zarr.create_array(store=self.path, name=name, shape=self.shape, dtype="int32",
                                   compressors=compressors, zarr_format=3, **kwargs)
            data = np.random.randint(0, 1000, size=self.shape).astype("int32")
            za[:] = data

            # zarr -> z5
            ds = z5py.File(self.path, mode="a")[name]
            np.testing.assert_array_equal(ds[:], data, err_msg=name)

            # z5 -> zarr: partial write, z5 re-encodes the chunks with the same codecs
            new = np.random.randint(0, 1000, size=(20, 11)).astype("int32")
            ds[3:23, 10:21] = new
            data[3:23, 10:21] = new
            out = zarr.open_array(store=self.path, path=name, mode="r")[:]
            np.testing.assert_array_equal(out, data, err_msg=name)

    def test_chains(self):
        self._check(sharded=False)

    @requires_z5_v3_sharding
    def test_chains_sharded(self):
        self._check(sharded=True)

    def _z5_create_crc32c(self, sharded):
        f = z5py.File(self.path, mode="w", use_zarr_format=True, zarr_format=3)
        ds = f.create_dataset("data", shape=self.shape, chunks=self.chunks, dtype="int32",
                              compression="gzip", crc32c=True,
                              shards=self.shards if sharded else None)
        data = np.random.randint(0, 1000, size=self.shape).astype("int32")
        ds[:] = data
        za = zarr.open_array(store=self.path, path="data", mode="a")
        self.assertIn("crc32c", [type(c).__name__.lower().replace("codec", "") for c in za.compressors])
        np.testing.assert_array_equal(za[:], data)

        # zarr -> z5: zarr rewrites part of the array with the same codecs
        new = np.random.randint(0, 1000, size=(20, 11)).astype("int32")
        za[3:23, 10:21] = new
        data[3:23, 10:21] = new
        np.testing.assert_array_equal(z5py.File(self.path, mode="r")["data"][:], data)

    def test_z5_crc32c(self):
        self._z5_create_crc32c(sharded=False)

    @requires_z5_v3_sharding
    def test_z5_crc32c_sharded(self):
        self._z5_create_crc32c(sharded=True)

    def test_z5_zstd_checksum(self):
        from _v3_capability import available_v3_compressors
        if "zstd" not in available_v3_compressors():
            self.skipTest("zstd is not compiled in")
        f = z5py.File(self.path, mode="w", use_zarr_format=True, zarr_format=3)
        ds = f.create_dataset("data", shape=self.shape, chunks=self.chunks, dtype="int32",
                              compression="zstd", checksum=True)
        data = np.random.randint(0, 1000, size=self.shape).astype("int32")
        ds[:] = data
        za = zarr.open_array(store=self.path, path="data", mode="r")
        self.assertTrue(za.metadata.codecs[1].checksum)
        np.testing.assert_array_equal(za[:], data)


@requires_z5_v3
@requires_zarr_v3
class TestInteropZarrPyV3Numcodecs(unittest.TestCase):
    """zarr-python arrays with numcodecs compressors (``numcodecs.*`` codec names)."""
    path = "interop_numcodecs.zr"
    shape = (37, 29)
    chunks = (8, 8)
    shards = (16, 16)
    # numcodecs name -> z5 compression name
    names = {"zlib": "zlib", "gzip": "gzip", "bz2": "bzip2", "lzma": "xz",
             "lz4": "lz4", "zstd": "zstd", "blosc": "blosc"}

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    def _codecs(self):
        """(label, codec) pairs: each numcodecs compressor with default and other settings."""
        import warnings
        from z5py.dataset import Dataset
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            from zarr.codecs import numcodecs as zn
        variants = {"zlib": [zn.Zlib(), zn.Zlib(level=6)],
                    "gzip": [zn.GZip(), zn.GZip(level=6)],
                    "bz2": [zn.BZ2(), zn.BZ2(level=7)],
                    "lzma": [zn.LZMA(), zn.LZMA(preset=4, check=10)],
                    "lz4": [zn.LZ4(), zn.LZ4(acceleration=3)],
                    "zstd": [zn.Zstd(), zn.Zstd(level=5, checksum=True)],
                    "blosc": [zn.Blosc(), zn.Blosc(cname="zstd", clevel=4, shuffle=2)]}
        return [("%s_%i" % (name, i), codec)
                for name, codecs in variants.items() if self.names[name] in Dataset.compressors_zarr_v3
                for i, codec in enumerate(codecs)]

    def _check(self, sharded):
        for label, codec in self._codecs():
            kwargs = {"chunks": self.chunks}
            if sharded:
                kwargs["shards"] = self.shards
            za = zarr.create_array(store=self.path, name=label, shape=self.shape, dtype="int32",
                                   compressors=[codec], zarr_format=3, **kwargs)
            data = np.random.randint(0, 1000, size=self.shape).astype("int32")
            za[:] = data

            # zarr -> z5
            ds = z5py.File(self.path, mode="a")[label]
            np.testing.assert_array_equal(ds[:], data, err_msg=label)

            # z5 -> zarr: partial write, z5 re-encodes with the same codec
            new = np.random.randint(0, 1000, size=(20, 11)).astype("int32")
            ds[3:23, 10:21] = new
            data[3:23, 10:21] = new
            out = zarr.open_array(store=self.path, path=label, mode="r")[:]
            np.testing.assert_array_equal(out, data, err_msg=label)

    def test_zarr_created(self):
        self._check(sharded=False)

    @requires_z5_v3_sharding
    def test_zarr_created_sharded(self):
        self._check(sharded=True)

    def test_z5_created(self):
        from z5py.dataset import Dataset
        f = z5py.File(self.path, mode="w", use_zarr_format=True, zarr_format=3)
        for compression in ("zlib", "bzip2", "xz", "lz4"):
            if compression not in Dataset.compressors_zarr_v3:
                continue
            ds = f.create_dataset(compression, shape=self.shape, chunks=self.chunks,
                                  dtype="int32", compression=compression)
            data = np.random.randint(0, 1000, size=self.shape).astype("int32")
            ds[:] = data
            out = zarr.open_array(store=self.path, path=compression, mode="r")[:]
            np.testing.assert_array_equal(out, data, err_msg=compression)


@requires_z5_v3
@requires_tensorstore
class TestInteropTsV3(InteropMixin, unittest.TestCase):
    library = "tensorstore"
    fmt = "zarr_v3"
    ext = "zr"
    codecs = ["raw", "gzip", "zstd", "blosc"]
    supports_dimsep = False


@requires_zarr_v3
class TestInteropZarrPyFillValue(unittest.TestCase):
    """Fill values a double can't hold, between zarr-python and z5 (v2 and v3)."""
    path = "interop_fill_value.zr"
    shape = (10, 10)
    chunks = (5, 5)
    values = [("uint64", 2**64 - 1), ("uint64", 2**53 + 1), ("int64", -2**63),
              ("int64", 2**63 - 1), ("int64", 2**53 + 1)]

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    def _check_integers(self, zarr_format):
        for i, (dtype, fv) in enumerate(self.values):
            msg = "v%i %s %i" % (zarr_format, dtype, fv)
            # zarr -> z5
            key = "zarr_%i" % i
            za = zarr.create_array(store=self.path, name=key, shape=self.shape, chunks=self.chunks,
                                   dtype=dtype, fill_value=fv, zarr_format=zarr_format,
                                   compressors=None)
            za[:2, :2] = np.zeros((2, 2), dtype=dtype)
            ds = z5py.File(self.path, mode="r")[key]
            self.assertEqual(int(ds.fillvalue), fv, msg)
            out = ds[:]
            self.assertTrue((out[:2, :2] == 0).all(), msg)
            out[:2, :2] = fv
            self.assertTrue((out == fv).all(), msg)

            # z5 -> zarr
            key = "z5_%i" % i
            ds = z5py.File(self.path, mode="a").create_dataset(
                key, shape=self.shape, chunks=self.chunks, dtype=dtype, fillvalue=fv,
                compression="raw")
            ds[:2, :2] = np.zeros((2, 2), dtype=dtype)
            za = zarr.open_array(store=self.path, path=key, mode="r")
            self.assertEqual(int(za.fill_value), fv, msg)
            out = za[:]
            out[:2, :2] = fv
            self.assertTrue((out == fv).all(), msg)

    def test_integers_v2(self):
        z5py.File(self.path, mode="w", use_zarr_format=True)
        self._check_integers(2)

    @requires_z5_v3
    def test_integers_v3(self):
        z5py.File(self.path, mode="w", use_zarr_format=True, zarr_format=3)
        self._check_integers(3)

    @requires_z5_v3
    def test_hex_floats_v3(self):
        # z5 drops NaN payloads and writes "NaN", which zarr-python reads. A hex
        # fill value written by hand reads as the same value in both.
        f = z5py.File(self.path, mode="w", use_zarr_format=True, zarr_format=3)
        for i, (dtype, bits) in enumerate([("float32", 0x7fc00001), ("float64", 0x7ff8000000000001)]):
            key = "nan_%i" % i
            fv = np.array(bits, dtype="<u%i" % np.dtype(dtype).itemsize).view(dtype)[()]
            f.create_dataset(key, shape=self.shape, chunks=self.chunks, dtype=dtype, fillvalue=fv,
                             compression="raw")
            out = zarr.open_array(store=self.path, path=key, mode="r")[:]
            self.assertTrue(np.isnan(out).all(), key)

        za = zarr.create_array(store=self.path, name="one", shape=self.shape, chunks=self.chunks,
                               dtype="float32", fill_value=0, zarr_format=3, compressors=None)
        meta_path = os.path.join(self.path, "one", "zarr.json")
        with open(meta_path) as fh:
            meta = json.load(fh)
        meta["fill_value"] = "0x3fc00000"
        with open(meta_path, "w") as fh:
            json.dump(meta, fh)
        self.assertEqual(zarr.open_array(store=self.path, path="one", mode="r").fill_value, 1.5)
        ds = z5py.File(self.path, mode="r")["one"]
        self.assertEqual(ds.fillvalue, 1.5)
        self.assertTrue((ds[:] == 1.5).all())
        del za


@requires_zarr_v3
class TestInteropZarrPyZeroDim(unittest.TestCase):
    """Zero-dimensional arrays between zarr-python and z5: zarr v2, and v3 with
    the default and the v2 chunk key encodings."""
    path = "interop_zero_dim.zr"
    layouts = {"v2": ({"zarr_format": 2}, {}, "0"),
               "v3": ({"zarr_format": 3}, {}, "c"),
               "v3_flat": ({"zarr_format": 3, "chunk_key_encoding": {"name": "v2"}},
                           {"chunk_key_encoding": "v2"}, "0")}

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    def _check(self, name):
        zarr_kwargs, z5_kwargs, chunk_key = self.layouts[name]
        zarr_format = zarr_kwargs["zarr_format"]
        f = z5py.File(self.path, mode="w", use_zarr_format=True, zarr_format=zarr_format)

        # zarr -> z5, then z5 rewrites the element and zarr reads it back
        za = zarr.create_array(store=self.path, name="zarr", shape=(), dtype="float64",
                               fill_value=-1.0, compressors=None, **zarr_kwargs)
        ds = f["zarr"]
        self.assertEqual(ds.shape, (), name)
        self.assertEqual(ds[()], -1.0, name)
        za[()] = 2.5
        self.assertEqual(ds[()], 2.5, name)
        ds[()] = 4.5
        self.assertEqual(zarr.open_array(store=self.path, path="zarr", mode="r")[()], 4.5, name)
        self.assertIn(chunk_key, os.listdir(os.path.join(self.path, "zarr")), name)

        # z5 -> zarr
        ds = f.create_dataset("z5", shape=(), dtype="int16", fillvalue=3, compression="gzip",
                              **z5_kwargs)
        za = zarr.open_array(store=self.path, path="z5", mode="r")
        self.assertEqual(za.shape, (), name)
        self.assertEqual(za[()], 3, name)
        ds[()] = -8
        self.assertEqual(za[()], -8, name)
        self.assertIn(chunk_key, os.listdir(os.path.join(self.path, "z5")), name)

    def test_v2(self):
        self._check("v2")

    @requires_z5_v3
    def test_v3(self):
        self._check("v3")

    @requires_z5_v3
    def test_v3_flat_key(self):
        self._check("v3_flat")

    @requires_z5_v3
    def test_v3_sharded_refused(self):
        # z5 can't represent a 0-d shard; opening must fail rather than read
        # the shard as a plain chunk. The refusal comes from the metadata, so
        # no data is written (zarr-python < 3.2 fails to write 0-d shards).
        z5py.File(self.path, mode="w", use_zarr_format=True, zarr_format=3)
        zarr.create_array(store=self.path, name="sharded", shape=(), dtype="int32",
                          shards=(), zarr_format=3)
        with self.assertRaises(RuntimeError):
            z5py.File(self.path, mode="r")["sharded"]


@requires_zarr_v3
class TestInteropZarrPyStrings(unittest.TestCase):
    """Variable-length string arrays written by zarr-python (``dtype=str``), read by z5."""
    path = "interop_strings.zr"
    data = np.array([["a", "h\u00e9llo", "", "xyz\u20ac", "q"],
                     ["long " * 50, "b", "c", "\u65e5\u672c", "z"],
                     ["x", "", "y", "tab\there", "end"]], dtype=object)

    def tearDown(self):
        try:
            rmtree(self.path)
        except OSError:
            pass

    def _layouts(self, zarr_format):
        from z5py.dataset import AVAILABLE_COMPRESSORS
        layouts = {"default": {}, "raw": {"compressors": None}, "fill": {"fill_value": "zz"}}
        if zarr_format == 2:
            layouts["zlib"] = {"compressors": numcodecs.Zlib()}
            layouts["gzip"] = {"compressors": numcodecs.GZip()}
            if AVAILABLE_COMPRESSORS["blosc"]:
                layouts["blosc"] = {"compressors": numcodecs.Blosc()}
            if not AVAILABLE_COMPRESSORS["zstd"]:
                del layouts["default"], layouts["fill"]
        else:
            from zarr.codecs import BloscCodec, Crc32cCodec, GzipCodec
            layouts["gzip"] = {"compressors": GzipCodec()}
            layouts["crc32c"] = {"compressors": [GzipCodec(), Crc32cCodec()]}
            layouts["sharded"] = {"compressors": GzipCodec(), "shards": (2, 4)}
            layouts["v2_keys"] = {"compressors": None, "chunk_key_encoding": {"name": "v2"}}
            if AVAILABLE_COMPRESSORS["blosc"]:
                layouts["blosc"] = {"compressors": BloscCodec()}
            if not AVAILABLE_COMPRESSORS["zstd"]:
                del layouts["default"], layouts["fill"]
        return layouts

    def _check(self, zarr_format):
        zarr.open_group(self.path, mode="w", zarr_format=zarr_format)
        for name, kwargs in self._layouts(zarr_format).items():
            za = zarr.create_array(store=self.path, name=name, shape=self.data.shape, chunks=(2, 2),
                                   dtype=str, zarr_format=zarr_format, **kwargs)
            # leave the last column unwritten: it reads as the fill value
            za[:, :4] = self.data[:, :4]
            expected = za[:]
            ds = z5py.File(self.path, mode="r")[name]
            out = ds[:]
            self.assertEqual(out.tolist(), expected.tolist(), name)
            self.assertEqual(ds.fillvalue, za.fill_value, name)
            self.assertEqual(ds[1, 3], expected[1, 3], name)
            self.assertEqual(ds[1:, 2:].tolist(), expected[1:, 2:].tolist(), name)

    def test_v2(self):
        self._check(2)

    @requires_z5_v3
    def test_v3(self):
        self._check(3)

    @requires_z5_v3
    def test_zero_dim(self):
        zarr.open_group(self.path, mode="w", zarr_format=3)
        za = zarr.create_array(store=self.path, name="s", shape=(), dtype=str, zarr_format=3,
                               compressors=None)
        za[()] = "only \u00fcne"
        self.assertEqual(z5py.File(self.path, mode="r")["s"][()], "only \u00fcne")


if __name__ == '__main__':
    unittest.main()
