"""Shared harness + capability probes for the S3 backend test suite.

This module provides everything ``test_s3.py`` needs to run a comprehensive,
skip-gated TDD suite against z5's S3 backend, without any real AWS credentials:

* a module-scope ``moto`` S3 server (``moto.server.ThreadedMotoServer``) that a
  real HTTP client -- including the AWS C++ SDK compiled into ``_z5py`` -- can
  reach (the in-process ``@mock_aws`` decorator cannot intercept the C++ SDK),
* the single ``open_s3_file`` factory through which every test opens an
  ``S3File`` (so the target endpoint/credentials API lives in exactly one
  place),
* capability probes (``has_s3_support`` / ``z5_s3_can_read`` /
  ``z5_s3_can_write`` and v3 variants) plus the matching ``requires_*`` skip
  decorators, so tests skip cleanly until each piece of the (currently
  incomplete) implementation lands -- the classic TDD red -> green path,
* ``zarr-python`` + ``s3fs`` seeding/reading helpers used both to bootstrap the
  z5 *read* path (seed with an external writer, read with z5) and for the
  external <-> z5 interop tests.

It must never raise at import time -- every optional import, the server start
and every probe is guarded.

The endpoint can be pointed at an external S3-compatible service (MinIO, a
standalone moto, ...) instead of the embedded server by setting the
``Z5PY_S3_ENDPOINT`` environment variable; the bucket name can be overridden
with ``Z5PY_S3_BUCKET_NAME``.
"""
import atexit
import functools
import importlib.util
import os
import socket
import subprocess
import sys
import time
import uuid

import unittest

import numpy as np

# Reuse the external-library flags and the v3 capability probe so there is a
# single source of truth (zarr availability, v3 support, ...).
from _v3_capability import HAVE_ZARR, ZARR_MAJOR, z5_supports_v3


# ---------------------------------------------------------------------------
# optional imports (gate the harness on importability only)
# ---------------------------------------------------------------------------
try:
    import boto3
    HAVE_BOTO3 = True
except Exception:
    boto3 = None
    HAVE_BOTO3 = False

try:
    import s3fs  # noqa: F401  (only needed so fsspec can resolve s3:// URLs)
    HAVE_S3FS = True
except Exception:
    s3fs = None
    HAVE_S3FS = False

# moto must run OUT OF PROCESS: the C++ AWS SDK holds the Python GIL while it
# blocks on a request, which would starve an in-process (threaded) moto server
# of the GIL and deadlock. We therefore launch ``python -m moto.server`` as a
# subprocess.
try:
    HAVE_MOTO_SERVER = importlib.util.find_spec("moto") is not None
except Exception:
    HAVE_MOTO_SERVER = False


# ---------------------------------------------------------------------------
# configuration
# ---------------------------------------------------------------------------
REGION = "us-east-1"
BUCKET = os.environ.get("Z5PY_S3_BUCKET_NAME", "z5-test-bucket")
#: optional external S3 endpoint (MinIO / standalone moto / ...) to use instead
#: of the embedded moto server.
_EXTERNAL_ENDPOINT = os.environ.get("Z5PY_S3_ENDPOINT")
_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "testing")
_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "testing")


# ---------------------------------------------------------------------------
# z5 compile-time capability
# ---------------------------------------------------------------------------
@functools.lru_cache(maxsize=1)
def has_s3_support():
    """True iff ``_z5py`` was compiled with S3 support (``WITH_S3``)."""
    try:
        from z5py import _z5py
        return hasattr(_z5py, "S3File")
    except Exception:
        return False


# ---------------------------------------------------------------------------
# moto server lifecycle
# ---------------------------------------------------------------------------
_server_proc = None
MOTO_ENDPOINT = None
HAVE_MOTO = False


def _free_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
    finally:
        sock.close()


def _set_dummy_credentials():
    """Make sure the AWS SDKs (Python and C++) find *some* credentials and do
    not stall on the EC2 instance-metadata endpoint."""
    os.environ.setdefault("AWS_ACCESS_KEY_ID", _ACCESS_KEY)
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", _SECRET_KEY)
    os.environ.setdefault("AWS_SESSION_TOKEN", "testing")
    os.environ.setdefault("AWS_SECURITY_TOKEN", "testing")
    os.environ.setdefault("AWS_DEFAULT_REGION", REGION)
    os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")


def _make_boto3_client():
    # path-style addressing: virtual-host style (``bucket.127.0.0.1``) does not
    # resolve against a local server.
    import botocore.config
    cfg = botocore.config.Config(s3={"addressing_style": "path"},
                                 signature_version="s3v4")
    return boto3.client("s3", endpoint_url=MOTO_ENDPOINT, region_name=REGION,
                        aws_access_key_id=_ACCESS_KEY,
                        aws_secret_access_key=_SECRET_KEY, config=cfg)


def ensure_bucket(name=None):
    """Create the test bucket if it does not exist (idempotent, best-effort)."""
    name = name or BUCKET
    if MOTO_ENDPOINT is None or not HAVE_BOTO3:
        return
    try:
        client = _make_boto3_client()
        # us-east-1 must NOT send a LocationConstraint.
        client.create_bucket(Bucket=name)
    except Exception:
        # already exists / racing another worker -> fine
        pass


def start_moto():
    """Start the module-scope S3 test endpoint; return its URL or ``None``.

    Idempotent and never raises: on any failure it returns ``None`` and leaves
    ``HAVE_MOTO`` False, so the whole suite skips cleanly instead of erroring.
    """
    global _server_proc, MOTO_ENDPOINT, HAVE_MOTO
    if MOTO_ENDPOINT is not None:
        return MOTO_ENDPOINT

    # An explicitly configured external endpoint takes precedence over moto.
    if _EXTERNAL_ENDPOINT:
        _set_dummy_credentials()
        MOTO_ENDPOINT = _EXTERNAL_ENDPOINT
        HAVE_MOTO = True
        ensure_bucket()
        return MOTO_ENDPOINT

    if not (HAVE_MOTO_SERVER and HAVE_BOTO3):
        return None

    try:
        _set_dummy_credentials()
        port = _free_port()
        # out-of-process moto (see the note where HAVE_MOTO_SERVER is set)
        proc = subprocess.Popen(
            [sys.executable, "-m", "moto.server", "-p", str(port), "-H", "127.0.0.1"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        endpoint = "http://127.0.0.1:%d" % port

        # readiness poll: make sure the server actually answers before use
        client = boto3.client("s3", endpoint_url=endpoint, region_name=REGION,
                              aws_access_key_id=_ACCESS_KEY,
                              aws_secret_access_key=_SECRET_KEY)
        ready = False
        for _ in range(100):
            if proc.poll() is not None:  # subprocess died (e.g. moto[server] missing)
                break
            try:
                client.list_buckets()
                ready = True
                break
            except Exception:
                time.sleep(0.1)
        if not ready:
            try:
                proc.terminate()
            except Exception:
                pass
            return None

        _server_proc = proc
        MOTO_ENDPOINT = endpoint
        HAVE_MOTO = True
        atexit.register(stop_moto)
        ensure_bucket()
        return MOTO_ENDPOINT
    except Exception:
        return None


def stop_moto():
    """Stop the moto server subprocess (no-op for an external endpoint)."""
    global _server_proc
    if _server_proc is not None:
        try:
            _server_proc.terminate()
            _server_proc.wait(timeout=10)
        except Exception:
            pass
        _server_proc = None


# ---------------------------------------------------------------------------
# bucket inspection helpers (used by tests to assert on-store layout)
# ---------------------------------------------------------------------------
def object_exists(key):
    """True iff ``BUCKET/key`` exists in the test endpoint."""
    try:
        _make_boto3_client().head_object(Bucket=BUCKET, Key=key)
        return True
    except Exception:
        return False


def list_keys(prefix):
    """All object keys under ``BUCKET/prefix``."""
    out = []
    try:
        client = _make_boto3_client()
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                out.append(obj["Key"])
    except Exception:
        pass
    return out


def cleanup_prefix(prefix):
    """Delete every object under ``BUCKET/prefix`` (best-effort test cleanup)."""
    if MOTO_ENDPOINT is None or not HAVE_BOTO3:
        return
    try:
        client = _make_boto3_client()
        paginator = client.get_paginator("list_objects_v2")
        batch = []
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                batch.append({"Key": obj["Key"]})
                if len(batch) == 1000:
                    client.delete_objects(Bucket=BUCKET, Delete={"Objects": batch})
                    batch = []
        if batch:
            client.delete_objects(Bucket=BUCKET, Delete={"Objects": batch})
    except Exception:
        pass


# ---------------------------------------------------------------------------
# external writer (zarr-python + s3fs over fsspec) -- seeding / interop
# ---------------------------------------------------------------------------
def _v2_compressor(name):
    import numcodecs
    return {"raw": None,
            "zlib": numcodecs.Zlib(),
            "gzip": numcodecs.GZip(),
            "blosc": numcodecs.Blosc(),
            "zstd": numcodecs.Zstd(),
            "bzip2": numcodecs.BZ2()}[name]


def _v3_compressors(name):
    from zarr.codecs import GzipCodec, ZstdCodec, BloscCodec
    # ``None`` -> bytes-only pipeline (no compressor) == z5 'raw'
    return {"raw": None,
            "gzip": GzipCodec(),
            "zstd": ZstdCodec(),
            "blosc": BloscCodec()}[name]


def _compressor(name, zarr_format):
    return _v3_compressors(name) if zarr_format == 3 else _v2_compressor(name)


def _storage_options():
    return {"key": _ACCESS_KEY, "secret": _SECRET_KEY, "anon": False,
            "client_kwargs": {"endpoint_url": MOTO_ENDPOINT,
                              "region_name": REGION},
            "config_kwargs": {"s3": {"addressing_style": "path"}}}


def _zarr_group(prefix, zarr_format, mode="a"):
    """Open a zarr-python group at ``s3://BUCKET/prefix`` on the test endpoint.

    Uses fsspec/s3fs via ``storage_options`` (no manual store construction, so
    it is robust to zarr-python store-API drift across 3.x minors).
    """
    import zarr
    return zarr.open_group(store="s3://%s/%s" % (BUCKET, prefix), mode=mode,
                           zarr_format=zarr_format,
                           storage_options=_storage_options())


def seed_array(prefix, key, data, chunks, zarr_format=2, compression="raw",
               attrs=None, shards=None, fill_value=0):
    """Write a known array (z5-logical order) with zarr-python.

    Used to bootstrap z5 *read* tests (and as the external half of interop):
    after this call z5py opening ``BUCKET/prefix`` must read back ``data``.
    """
    grp = _zarr_group(prefix, zarr_format, mode="a")
    kwargs = dict(name=key, shape=data.shape, chunks=chunks, dtype=data.dtype,
                  fill_value=fill_value,
                  compressors=_compressor(compression, zarr_format))
    if zarr_format == 3 and shards is not None:
        kwargs["shards"] = shards
    arr = grp.create_array(**kwargs)
    arr[:] = data
    if attrs:
        arr.attrs.update(attrs)
    return arr


def seed_group_hierarchy(prefix, data, chunks, zarr_format=2,
                         root_attrs=None, ds_attrs=None):
    """Seed a root group holding ``data`` and a nested ``group/data`` dataset.

    Mirrors the layout the legacy ``test_s3`` expected, so the hierarchy tests
    (keys / contains / nested access) have something to read.
    """
    grp = _zarr_group(prefix, zarr_format, mode="a")
    if root_attrs:
        grp.attrs.update(root_attrs)
    comp = _compressor("raw", zarr_format)

    top = grp.create_array(name="data", shape=data.shape, chunks=chunks,
                           dtype=data.dtype, compressors=comp)
    top[:] = data
    if ds_attrs:
        top.attrs.update(ds_attrs)

    sub = grp.create_group("group")
    nested = sub.create_array(name="data", shape=data.shape, chunks=chunks,
                              dtype=data.dtype, compressors=comp)
    nested[:] = data
    if ds_attrs:
        nested.attrs.update(ds_attrs)


def read_array_external(prefix, key, zarr_format=2):
    """Read an array (z5-logical order) written under ``BUCKET/prefix`` with
    zarr-python -- the external half of the z5 -> external interop tests."""
    grp = _zarr_group(prefix, zarr_format, mode="r")
    return grp[key][:]


def read_attrs_external(prefix, key=None, zarr_format=2):
    """Read attributes of the root group (``key=None``) or of ``key``."""
    grp = _zarr_group(prefix, zarr_format, mode="r")
    node = grp if key is None else grp[key]
    return dict(node.attrs)


# ---------------------------------------------------------------------------
# the single S3File factory (the target endpoint/credentials API)
# ---------------------------------------------------------------------------
def open_s3_file(name_in_bucket="", mode="a", use_zarr_format=True,
                 zarr_format=2, dimension_separator=None):
    """Open a ``z5py.S3File`` pointed at the test endpoint.

    This is the *only* place the (target) endpoint/region/credentials kwargs are
    passed, so the API surface the tests assume lives in one spot. Until
    ``S3File`` grows these kwargs the call raises ``TypeError``, which the
    capability probes treat as "unsupported" and turn into a clean skip.
    """
    import z5py
    return z5py.S3File(BUCKET, name_in_bucket=name_in_bucket, mode=mode,
                       use_zarr_format=use_zarr_format,
                       zarr_format=zarr_format,
                       dimension_separator=dimension_separator,
                       endpoint_url=MOTO_ENDPOINT, region=REGION,
                       anon=False, access_key=_ACCESS_KEY,
                       secret_key=_SECRET_KEY)


# ---------------------------------------------------------------------------
# z5 S3 capability probes
# ---------------------------------------------------------------------------
# These touch the network / external libraries, so unlike the v3 probes they
# catch a broad ``Exception`` -- a probe must never raise (it gates a skip).
def _endpoint_ready():
    return has_s3_support() and start_moto() is not None


@functools.lru_cache(maxsize=1)
def z5_s3_can_read():
    """True iff z5 can READ a zarr-python-seeded v2 store over S3."""
    if not _endpoint_ready() or not (HAVE_ZARR and HAVE_S3FS):
        return False
    prefix = "probe-read-%s" % uuid.uuid4().hex
    try:
        expected = np.arange(16, dtype="int32").reshape(4, 4)
        seed_array(prefix, "p", expected, chunks=(2, 2), zarr_format=2)
        out = open_s3_file(prefix, mode="r", zarr_format=2)["p"][:]
        return bool(np.array_equal(out, expected))
    except Exception:
        return False
    finally:
        cleanup_prefix(prefix)


@functools.lru_cache(maxsize=1)
def z5_s3_can_write():
    """True iff z5 can round-trip (write + read back) a v2 store over S3."""
    if not _endpoint_ready():
        return False
    prefix = "probe-write-%s" % uuid.uuid4().hex
    try:
        f = open_s3_file(prefix, mode="w", zarr_format=2)
        ds = f.create_dataset("p", shape=(4, 4), chunks=(2, 2),
                              dtype="int32", compression="raw")
        expected = np.arange(16, dtype="int32").reshape(4, 4)
        ds[:] = expected
        return bool(np.array_equal(ds[:], expected))
    except Exception:
        return False
    finally:
        cleanup_prefix(prefix)


@functools.lru_cache(maxsize=1)
def z5_s3_supports_v3():
    """True iff z5 can round-trip a zarr v3 store over S3."""
    if not z5_supports_v3() or not _endpoint_ready():
        return False
    prefix = "probe-v3w-%s" % uuid.uuid4().hex
    try:
        f = open_s3_file(prefix, mode="w", zarr_format=3,
                         dimension_separator="/")
        ds = f.create_dataset("p", shape=(4, 4), chunks=(2, 2),
                              dtype="int32", compression="raw")
        expected = np.arange(16, dtype="int32").reshape(4, 4)
        ds[:] = expected
        return bool(np.array_equal(ds[:], expected))
    except Exception:
        return False
    finally:
        cleanup_prefix(prefix)


@functools.lru_cache(maxsize=1)
def z5_s3_can_read_v3():
    """True iff z5 can READ a zarr-python-seeded v3 store over S3."""
    if not z5_supports_v3() or not _endpoint_ready():
        return False
    if not (HAVE_ZARR and ZARR_MAJOR >= 3 and HAVE_S3FS):
        return False
    prefix = "probe-v3r-%s" % uuid.uuid4().hex
    try:
        expected = np.arange(16, dtype="int32").reshape(4, 4)
        seed_array(prefix, "p", expected, chunks=(2, 2), zarr_format=3)
        out = open_s3_file(prefix, mode="r", zarr_format=3,
                           dimension_separator="/")["p"][:]
        return bool(np.array_equal(out, expected))
    except Exception:
        return False
    finally:
        cleanup_prefix(prefix)


# ---------------------------------------------------------------------------
# start the endpoint up front so the gates below see a live server, then build
# the skip decorators (each captures its boolean at import, like _v3_capability)
# ---------------------------------------------------------------------------
if has_s3_support() or _EXTERNAL_ENDPOINT:
    start_moto()


requires_s3 = unittest.skipUnless(has_s3_support(), "z5 not compiled WITH_S3")
requires_moto = unittest.skipUnless(
    bool(MOTO_ENDPOINT), "no S3 test endpoint (moto[server] / boto3 missing?)")
requires_boto3 = unittest.skipUnless(HAVE_BOTO3, "requires boto3")
requires_s3fs = unittest.skipUnless(HAVE_S3FS, "requires s3fs")
requires_zarr_v3 = unittest.skipUnless(
    HAVE_ZARR and ZARR_MAJOR >= 3, "requires zarr >= 3")
requires_s3_read = unittest.skipUnless(
    z5_s3_can_read(), "z5 S3 read path not working")
requires_s3_write = unittest.skipUnless(
    z5_s3_can_write(), "z5 S3 write path not implemented")
requires_s3_v3 = unittest.skipUnless(
    z5_s3_supports_v3(), "z5 S3 zarr v3 write not implemented")
requires_s3_read_v3 = unittest.skipUnless(
    z5_s3_can_read_v3(), "z5 S3 zarr v3 read not working")
