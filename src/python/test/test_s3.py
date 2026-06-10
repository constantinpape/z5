"""Comprehensive, skip-gated TDD suite for z5's S3 backend.

Everything runs against a local ``moto`` S3 server (see ``_s3_capability``) so
no AWS credentials are needed. The suite is written against the *target*
``S3File`` API (endpoint / region / credentials kwargs) and the full set of
backend operations -- most of which are not implemented yet. Until each piece
lands, the corresponding tests skip cleanly via the capability probes; they
turn green as the implementation is built (the v3 spec was developed the same
way).

Gating layers:

* class level ``@requires_s3`` + ``@requires_moto`` -- need a WITH_S3 build and
  a reachable S3 endpoint;
* per-method ``self._require_read()`` / ``self._require_write()`` -- need the
  z5 S3 read / write path to work *for this format* (v2 vs v3 probe);
* ``self._require_interop()`` -- need ``s3fs`` + ``zarr-python >= 3``.
"""
import unittest
from abc import ABC
from uuid import uuid4

import numpy as np

from _s3_capability import (
    BUCKET, HAVE_ZARR, HAVE_S3FS, ZARR_MAJOR,
    has_s3_support, open_s3_file, seed_array, seed_group_hierarchy,
    read_array_external, read_attrs_external,
    cleanup_prefix, object_exists, list_keys, start_moto, stop_moto,
    z5_s3_can_read, z5_s3_can_write, z5_s3_supports_v3, z5_s3_can_read_v3,
    requires_s3, requires_moto,
)
from _v3_capability import requires_z5_v3, requires_z5_v3_sharding


def setUpModule():
    # only boot the test endpoint when z5 actually has an S3 backend to test
    if has_s3_support():
        start_moto()


def tearDownModule():
    stop_moto()


# ---------------------------------------------------------------------------
# A. handle / constructor smoke tests
# ---------------------------------------------------------------------------
@requires_s3
@requires_moto
class TestS3Handles(unittest.TestCase):
    def setUp(self):
        self.prefix = "h-%s" % uuid4().hex

    def tearDown(self):
        cleanup_prefix(self.prefix)

    def test_low_level_handle_constructors(self):
        # the bare nanobind handles construct without any network access
        from z5py import _z5py
        mode = _z5py.FileMode(_z5py.FileMode.a)
        fhandle = _z5py.S3File(BUCKET, self.prefix, mode)
        ghandle = _z5py.S3Group(fhandle, "g")
        _z5py.S3DatasetHandle(ghandle, "d")

    def test_open_s3_file_accepts_endpoint_kwargs(self):
        # TDD: open_s3_file passes the (target) endpoint/credentials kwargs;
        # until S3File grows them the call raises TypeError -> skip.
        try:
            f = open_s3_file(self.prefix, mode="a", zarr_format=2)
        except TypeError as err:
            self.skipTest("S3File endpoint/credentials API not implemented: %s" % err)
        from z5py import S3File
        self.assertIsInstance(f, S3File)


# ---------------------------------------------------------------------------
# shared per-format test logic
# ---------------------------------------------------------------------------
class S3TestMixin(ABC):
    #: subclasses set these
    data_format = "zarr"
    zarr_format = 2
    dimension_separator = None
    codecs = ("raw", "zlib", "gzip", "blosc", "zstd", "bzip2")

    dtypes = ("int8", "int16", "int32", "int64",
              "uint8", "uint16", "uint32", "uint64",
              "float32", "float64")
    shape = (32, 24)
    chunks = (16, 16)

    def setUp(self):
        self.prefix = "t-%s" % uuid4().hex

    def tearDown(self):
        cleanup_prefix(self.prefix)

    # -- gating helpers (per-format, runtime) -------------------------------
    def _require_read(self):
        ok = z5_s3_can_read_v3() if self.zarr_format == 3 else z5_s3_can_read()
        if not ok:
            self.skipTest("z5 S3 read path not available for %s" % self.data_format)

    def _require_write(self):
        ok = z5_s3_supports_v3() if self.zarr_format == 3 else z5_s3_can_write()
        if not ok:
            self.skipTest("z5 S3 write path not available for %s" % self.data_format)

    def _require_interop(self):
        if not (HAVE_S3FS and HAVE_ZARR and ZARR_MAJOR >= 3):
            self.skipTest("requires s3fs and zarr >= 3")

    # -- small utilities ----------------------------------------------------
    def open(self, mode="a"):
        return open_s3_file(self.prefix, mode=mode, use_zarr_format=True,
                            zarr_format=self.zarr_format,
                            dimension_separator=self.dimension_separator)

    def _data(self, dtype, shape=None):
        shape = self.shape if shape is None else shape
        if np.issubdtype(np.dtype(dtype), np.floating):
            return (np.random.rand(*shape) * 100).astype(dtype)
        return np.random.randint(0, 100, size=shape).astype(dtype)

    def check_array(self, result, expected, msg=None):
        self.assertEqual(result.shape, expected.shape, msg)
        self.assertTrue(np.allclose(result, expected), msg)

    @property
    def _available_codecs(self):
        from z5py.dataset import Dataset
        avail = set(Dataset.compressors_zarr_v3 if self.zarr_format == 3
                    else Dataset.compressors_zarr)
        return [c for c in self.codecs if c in avail]

    # ------------------------------------------------------- B. hierarchy ---
    def test_exists_and_keys(self):
        self._require_read()
        data = self._data("uint8")
        seed_group_hierarchy(self.prefix, data, self.chunks,
                             zarr_format=self.zarr_format)
        f = self.open(mode="r")
        self.assertIn("data", f)
        self.assertIn("group", f)
        self.assertEqual(set(f.keys()), {"data", "group"})

    def test_nested_contains(self):
        self._require_read()
        data = self._data("uint8")
        seed_group_hierarchy(self.prefix, data, self.chunks,
                             zarr_format=self.zarr_format)
        f = self.open(mode="r")
        self.assertIn("group/data", f)
        self.assertEqual(set(f["group"].keys()), {"data"})

    def test_open_missing_raises(self):
        self._require_read()
        data = self._data("uint8")
        seed_group_hierarchy(self.prefix, data, self.chunks,
                             zarr_format=self.zarr_format)
        f = self.open(mode="r")
        with self.assertRaises(KeyError):
            f["does-not-exist"]

    def test_create_group(self):
        self._require_write()
        f = self.open(mode="w")
        g = f.create_group("grp")
        self.assertIn("grp", f)
        self.assertTrue(g.is_zarr or self.zarr_format == 3)
        meta = "zarr.json" if self.zarr_format == 3 else ".zgroup"
        self.assertTrue(object_exists("%s/grp/%s" % (self.prefix, meta)))

    def test_remove_group(self):
        self._require_write()
        f = self.open(mode="w")
        f.create_group("grp")
        self.assertIn("grp", f)
        del f["grp"]
        self.assertNotIn("grp", f)

    # ------------------------------------------------------ C. attributes ---
    def test_read_file_attrs(self):
        self._require_read()
        data = self._data("uint8")
        seed_group_hierarchy(self.prefix, data, self.chunks,
                             zarr_format=self.zarr_format,
                             root_attrs={"Q": 42})
        f = self.open(mode="r")
        self.assertEqual(f.attrs["Q"], 42)

    def test_read_dataset_attrs(self):
        self._require_read()
        data = self._data("uint8")
        seed_array(self.prefix, "data", data, self.chunks,
                   zarr_format=self.zarr_format, attrs={"x": "y"})
        f = self.open(mode="r")
        self.assertEqual(f["data"].attrs["x"], "y")

    def test_write_read_file_attrs(self):
        self._require_write()
        f = self.open(mode="w")
        f.attrs["a"] = [1, 2, 3]
        reopened = self.open(mode="r")
        self.assertEqual(list(reopened.attrs["a"]), [1, 2, 3])

    def test_remove_attr(self):
        self._require_write()
        f = self.open(mode="w")
        f.attrs["a"] = 1
        self.assertEqual(f.attrs["a"], 1)
        del f.attrs["a"]
        with self.assertRaises(KeyError):
            f.attrs["a"]

    def test_dataset_attrs_roundtrip(self):
        self._require_write()
        f = self.open(mode="w")
        ds = f.create_dataset("d", shape=self.shape, chunks=self.chunks,
                              dtype="int32", compression="raw")
        ds.attrs["k"] = "v"
        reopened = self.open(mode="r")
        self.assertEqual(reopened["d"].attrs["k"], "v")

    # --------------------------------------------------- D. dataset IO ------
    def test_roundtrip_dtypes(self):
        self._require_write()
        f = self.open(mode="w")
        for dtype in self.dtypes:
            data = self._data(dtype)
            ds = f.create_dataset("d_%s" % dtype, shape=self.shape,
                                  chunks=self.chunks, dtype=dtype,
                                  compression="raw")
            ds[:] = data
            self.check_array(ds[:], data, "dtype %s" % dtype)

    def test_roundtrip_compressors(self):
        self._require_write()
        f = self.open(mode="w")
        for comp in self._available_codecs:
            data = self._data("int32")
            ds = f.create_dataset("c_%s" % comp, shape=self.shape,
                                  chunks=self.chunks, dtype="int32",
                                  compression=comp)
            ds[:] = data
            self.check_array(ds[:], data, "codec %s" % comp)

    def test_fill_value(self):
        self._require_write()
        f = self.open(mode="w")
        ds = f.create_dataset("fv", shape=self.shape, chunks=self.chunks,
                              dtype="float32", fillvalue=42.0,
                              compression="raw")
        # nothing written -> every chunk reads back as the fill value
        self.check_array(ds[:], np.full(self.shape, 42.0, dtype="float32"))

    def test_empty_chunks(self):
        self._require_write()
        f = self.open(mode="w")
        ds = f.create_dataset("ec", shape=self.shape, chunks=self.chunks,
                              dtype="int32", compression="raw")
        block = np.ones(self.chunks, dtype="int32")
        ds[:self.chunks[0], :self.chunks[1]] = block
        out = ds[:]
        self.check_array(out[:self.chunks[0], :self.chunks[1]], block)
        # the untouched remainder stays at the (zero) fill value
        self.assertTrue(np.all(out[self.chunks[0]:, :] == 0))
        self.assertTrue(np.all(out[:, self.chunks[1]:] == 0))

    def test_non_aligned_roi(self):
        self._require_write()
        f = self.open(mode="w")
        ds = f.create_dataset("roi", shape=self.shape, chunks=self.chunks,
                              dtype="float64", compression="raw")
        roi = np.s_[3:20, 5:18]
        block = np.random.rand(17, 13)
        ds[roi] = block
        self.check_array(ds[roi], block)

    def test_multithreaded(self):
        self._require_write()
        f = self.open(mode="w")
        data = self._data("float64")
        ds = f.create_dataset("mt", shape=self.shape, chunks=(8, 8),
                              dtype="float64", compression="raw", n_threads=4)
        ds[:] = data
        self.check_array(ds[:], data)

    # --------------------------------------------------------- F. interop ---
    def test_external_write_z5_read(self):
        self._require_interop()
        self._require_read()
        data = self._data("int32")
        seed_array(self.prefix, "ext", data, self.chunks,
                   zarr_format=self.zarr_format, compression="raw")
        f = self.open(mode="r")
        self.check_array(f["ext"][:], data)

    def test_z5_write_external_read(self):
        self._require_interop()
        self._require_write()
        data = self._data("int32")
        f = self.open(mode="w")
        ds = f.create_dataset("z5d", shape=self.shape, chunks=self.chunks,
                              dtype="int32", compression="raw")
        ds[:] = data
        out = read_array_external(self.prefix, "z5d", zarr_format=self.zarr_format)
        self.check_array(out, data)

    def test_attrs_interop_external_to_z5(self):
        self._require_interop()
        self._require_read()
        data = self._data("uint8")
        seed_array(self.prefix, "ext", data, self.chunks,
                   zarr_format=self.zarr_format, attrs={"hello": "world"})
        f = self.open(mode="r")
        self.assertEqual(f["ext"].attrs["hello"], "world")

    def test_attrs_interop_z5_to_external(self):
        self._require_interop()
        self._require_write()
        f = self.open(mode="w")
        ds = f.create_dataset("z5d", shape=self.shape, chunks=self.chunks,
                              dtype="int32", compression="raw")
        ds.attrs["hello"] = "world"
        attrs = read_attrs_external(self.prefix, "z5d", zarr_format=self.zarr_format)
        self.assertEqual(attrs.get("hello"), "world")


# ---------------------------------------------------------------------------
# concrete format combinations
# ---------------------------------------------------------------------------
@requires_s3
@requires_moto
class TestS3Zarr(S3TestMixin, unittest.TestCase):
    data_format = "zarr"
    zarr_format = 2
    dimension_separator = "."
    codecs = ("raw", "zlib", "gzip", "blosc", "zstd", "bzip2")


@requires_s3
@requires_moto
@requires_z5_v3
class TestS3ZarrV3(S3TestMixin, unittest.TestCase):
    data_format = "zarr_v3"
    zarr_format = 3
    dimension_separator = "/"
    codecs = ("raw", "gzip", "blosc", "zstd")

    # ------------------------------------------------------ E. v3-specific --
    def test_v3_zarr_json_layout(self):
        self._require_write()
        f = self.open(mode="w")
        f.create_dataset("d", shape=self.shape, chunks=self.chunks,
                         dtype="int32", compression="raw")
        self.assertTrue(object_exists("%s/d/zarr.json" % self.prefix))
        self.assertFalse(object_exists("%s/d/.zarray" % self.prefix))

    def test_v3_nested_chunk_keys(self):
        self._require_write()
        f = self.open(mode="w")
        data = self._data("int32")
        ds = f.create_dataset("d", shape=self.shape, chunks=self.chunks,
                              dtype="int32", compression="raw")
        ds[:] = data
        # v3 default chunk-key encoding stores chunks under ``<ds>/c/...``
        keys = list_keys("%s/d/c" % self.prefix)
        self.assertTrue(keys, "no nested c/ chunk objects were written")

    @requires_z5_v3_sharding
    def test_v3_sharding_roundtrip(self):
        self._require_write()
        if not z5_s3_supports_v3():
            self.skipTest("z5 S3 zarr v3 not available")
        shape, chunks, shards = (32, 32), (8, 8), (16, 16)
        f = self.open(mode="w")
        data = self._data("float32", shape)
        ds = f.create_dataset("sd", shape=shape, chunks=chunks, shards=shards,
                              dtype="float32", compression="raw")
        ds[:] = data
        self.check_array(ds[:], data)
        # sharded v3 writes one object per (non-empty) shard under c/
        self.assertTrue(list_keys("%s/sd/c" % self.prefix))


if __name__ == "__main__":
    unittest.main()
