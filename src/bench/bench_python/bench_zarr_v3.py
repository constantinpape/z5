"""Cross-library zarr v3 read/write benchmark.

Benchmarks z5's zarr v3 implementation (with and without sharding) against the two
reference Python zarr libraries, ``zarr-python`` (>= 3) and ``tensorstore``, for 2D and
3D data across the codecs common to all three (raw / gzip / blosc / zstd).

Two threading scenarios are run for every configuration:

* **single-threaded** -- every library pinned to one worker. This is the *fair*
  comparison: pure codec + IO speed with no parallelism.
* **multi-threaded** -- every library given ``N`` workers (``N`` defaults to the CPU
  count). z5's C++ chunk-level threadpool tends to win here; all three libraries are
  still given ``N`` workers, so the comparison is honest -- just favourable to z5.

Codec levels are pinned identically across libraries (gzip level 5, zstd level 3, blosc
lz4 clevel 5 with byte shuffle) so the comparison is apples-to-apples. The codec call
patterns mirror the proven interop helpers in ``src/python/test/test_interop.py``.

Reads are *warm-cache* (decode/copy bound) -- the store is written once, then read back
several times. This is identical for all libraries; a cold-cache read benchmark would
need privileges to drop the page cache and is out of scope.

Example::

    # full default run -> bench_v3_results.json
    python bench_zarr_v3.py

    # quick smoke run
    python bench_zarr_v3.py --dims 2d --codecs raw gzip --iterations 1 --warmup 0 \\
        --threads 2 --save-folder /tmp/bz

Plot the results with ``plot_results.py``.
"""
import argparse
import json
import os
import platform
import sys
import time
from shutil import rmtree
from statistics import median

import numpy as np

import z5py
from z5py.dataset import Dataset


# ---------------------------------------------------------------------------
# optional external libraries (gate on importability, like _v3_capability.py)
# ---------------------------------------------------------------------------
try:
    import zarr
    HAVE_ZARR = int(zarr.__version__.split(".")[0]) >= 3
    ZARR_VERSION = zarr.__version__
except Exception:
    zarr = None
    HAVE_ZARR = False
    ZARR_VERSION = None

try:
    import tensorstore as ts
    HAVE_TS = True
    # tensorstore exposes no reliable __version__
    TS_VERSION = getattr(ts, "__version__", "unknown")
except Exception:
    ts = None
    HAVE_TS = False
    TS_VERSION = None


ALL_LIBRARIES = ("z5py", "zarr", "tensorstore")
ALL_CODECS = ("raw", "gzip", "blosc", "zstd")

# data configs: no partial chunks/shards, ~64 MB at uint8, so the comparison is clean.
CONFIGS = {
    "2d": dict(shape=(8192, 8192), chunks=(256, 256), shards=(1024, 1024)),
    "3d": dict(shape=(256, 512, 512), chunks=(64, 64, 64), shards=(128, 256, 256)),
}


# ---------------------------------------------------------------------------
# codec specs -- levels pinned identically across libraries
# ---------------------------------------------------------------------------
def _zarr_compressor(name):
    """zarr-python v3 codec object for ``name`` (None == bytes-only / no compressor)."""
    from zarr.codecs import GzipCodec, ZstdCodec, BloscCodec
    return {
        "raw": None,
        "gzip": GzipCodec(level=5),
        "zstd": ZstdCodec(level=3),
        "blosc": BloscCodec(cname="lz4", clevel=5, shuffle="shuffle"),
    }[name]


def _ts_codecs(name):
    """tensorstore zarr3 codec list for ``name`` (bytes [+ compressor])."""
    bytes_c = {"name": "bytes", "configuration": {"endian": "little"}}
    if name == "raw":
        return [bytes_c]
    comp = {
        "gzip": {"name": "gzip", "configuration": {"level": 5}},
        "zstd": {"name": "zstd", "configuration": {"level": 3}},
        "blosc": {"name": "blosc",
                  "configuration": {"cname": "lz4", "clevel": 5, "shuffle": "shuffle"}},
    }[name]
    return [bytes_c, comp]


# z5py uses the compression name directly; its v3 defaults already are gzip level 5,
# zstd level 3 and blosc lz4 clevel 5 (see z5py/dataset.py _to_zarr_v3_compression_options).


# ---------------------------------------------------------------------------
# per-library write / read adapters (all in z5-logical / row-major axis order)
# ---------------------------------------------------------------------------
def z5_write(path, data, chunks, shards, codec, n_threads):
    f = z5py.File(path, mode="w", use_zarr_format=True, zarr_format=3,
                  dimension_separator="/")
    ds = f.create_dataset("data", shape=data.shape, dtype=data.dtype, chunks=chunks,
                          shards=shards, compression=codec, n_threads=n_threads)
    ds[:] = data


def z5_read(path, n_threads):
    f = z5py.File(path, mode="r", use_zarr_format=True, zarr_format=3,
                  dimension_separator="/")
    ds = f["data"]
    ds.n_threads = n_threads
    return ds[:]


def zarr_write(path, data, chunks, shards, codec, n_threads):
    with zarr.config.set({"threading.max_workers": n_threads,
                          "async.concurrency": n_threads}):
        g = zarr.open_group(store=path, mode="w", zarr_format=3)
        kwargs = dict(name="data", shape=data.shape, chunks=chunks, dtype=data.dtype,
                      compressors=_zarr_compressor(codec))
        if shards is not None:
            kwargs["shards"] = shards
        arr = g.create_array(**kwargs)
        arr[:] = data


def zarr_read(path, n_threads):
    with zarr.config.set({"threading.max_workers": n_threads,
                          "async.concurrency": n_threads}):
        g = zarr.open_group(store=path, mode="r", zarr_format=3)
        return g["data"][:]


def _ts_context(n_threads):
    return {"data_copy_concurrency": {"limit": n_threads},
            "file_io_concurrency": {"limit": n_threads}}


def ts_write(path, data, chunks, shards, codec, n_threads):
    if shards is None:
        codecs = _ts_codecs(codec)
        grid = chunks
    else:
        codecs = [{"name": "sharding_indexed",
                   "configuration": {
                       "chunk_shape": list(chunks),
                       "codecs": _ts_codecs(codec),
                       "index_codecs": [
                           {"name": "bytes", "configuration": {"endian": "little"}},
                           {"name": "crc32c"}]}}]
        grid = shards
    meta = {"shape": list(data.shape),
            "chunk_grid": {"name": "regular",
                           "configuration": {"chunk_shape": list(grid)}},
            "data_type": np.dtype(data.dtype).name,
            "codecs": codecs}
    spec = {"driver": "zarr3", "kvstore": {"driver": "file", "path": path},
            "metadata": meta, "create": True, "delete_existing": True,
            "context": _ts_context(n_threads)}
    ts.open(spec).result()[...] = data


def ts_read(path, n_threads):
    spec = {"driver": "zarr3", "kvstore": {"driver": "file", "path": path},
            "open": True, "context": _ts_context(n_threads)}
    return ts.open(spec).result().read().result()


WRITERS = {"z5py": z5_write, "zarr": zarr_write, "tensorstore": ts_write}
READERS = {"z5py": z5_read, "zarr": zarr_read, "tensorstore": ts_read}


# ---------------------------------------------------------------------------
# data generation
# ---------------------------------------------------------------------------
def make_data(shape, dtype, incompressible=False, seed=0):
    """Deterministic benchmark array.

    By default a smooth low-frequency gradient plus mild noise -- semi-compressible, so
    every codec does real work and compression ratios are meaningful. ``incompressible``
    produces uniform random data (the worst case for compression). Values stay within the
    dtype's representable range to avoid overflow for narrow integer types.
    """
    rng = np.random.default_rng(seed)
    dt = np.dtype(dtype)
    is_float = np.issubdtype(dt, np.floating)

    if incompressible:
        if is_float:
            return rng.standard_normal(shape).astype(dt)
        info = np.iinfo(dt)
        lo = 0 if info.min == 0 else -(min(int(info.max), 1 << 30))
        hi = min(int(info.max), 1 << 30)
        return rng.integers(lo, hi, size=shape, endpoint=True).astype(dt)

    # smooth separable gradient in [0, 1]
    ndim = len(shape)
    field = np.zeros(shape, dtype=np.float32)
    for ax, n in enumerate(shape):
        ramp = np.linspace(0.0, 1.0, n, dtype=np.float32)
        view = ramp.reshape([n if a == ax else 1 for a in range(ndim)])
        field = field + view
    field /= ndim

    if is_float:
        amp, noise_hi = 100.0, 4.0
    else:
        info = np.iinfo(dt)
        amp = min(float(info.max), 200.0) * 0.9
        noise_hi = max(1.0, amp * 0.05)
    out = field * amp + rng.uniform(0.0, noise_hi, size=shape).astype(np.float32)
    return out.astype(dt)


# ---------------------------------------------------------------------------
# timing helpers
# ---------------------------------------------------------------------------
def _time(fn, iterations, warmup):
    for _ in range(warmup):
        fn()
    times = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return times


def _dir_size(path):
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for fn in filenames:
            total += os.path.getsize(os.path.join(dirpath, fn))
    return total


def _record(library, dim, sharded, codec, threads, op, times, nbytes, stored=None):
    best = min(times)
    rec = {"library": library, "dim": dim, "sharded": sharded, "codec": codec,
           "threads": threads, "op": op, "times": times,
           "min": best, "median": median(times),
           "throughput_mbps": (nbytes / 1e6) / best if best > 0 else float("nan")}
    if stored is not None:
        rec["raw_nbytes"] = int(nbytes)
        rec["stored_bytes"] = int(stored)
        rec["ratio"] = nbytes / stored if stored > 0 else float("nan")
    return rec


# ---------------------------------------------------------------------------
# main benchmark loop
# ---------------------------------------------------------------------------
def run(libraries, dims, codecs, thread_modes, iterations, warmup, dtype,
        incompressible, save_folder, keep):
    if os.path.exists(save_folder):
        rmtree(save_folder)
    os.makedirs(save_folder)

    z5_sharding_ok = _z5_supports_sharding()
    if not z5_sharding_ok and "z5py" in libraries:
        print("WARNING: this z5 build has no zarr v3 sharding support; "
              "skipping sharded z5py records.")

    records = []
    for dim in dims:
        cfg = CONFIGS[dim]
        shape, chunks, shards_full = cfg["shape"], cfg["chunks"], cfg["shards"]
        data = make_data(shape, dtype, incompressible=incompressible)
        nbytes = data.nbytes
        print("\n=== %s  shape=%s chunks=%s shards=%s dtype=%s (%.1f MB) ==="
              % (dim, shape, chunks, shards_full, dtype, nbytes / 1e6))

        for sharded in (False, True):
            shards = shards_full if sharded else None
            for n_threads in thread_modes:
                for codec in codecs:
                    for lib in libraries:
                        if lib == "z5py" and sharded and not z5_sharding_ok:
                            continue
                        # the ``.zr`` extension lets z5py infer the zarr format for a
                        # not-yet-existing path (it otherwise guesses n5); zarr-python
                        # and tensorstore ignore the extension.
                        path = os.path.join(
                            save_folder,
                            "%s_%s_%s_%s_t%d.zr" % (lib, dim,
                                                    "shard" if sharded else "flat",
                                                    codec, n_threads))
                        try:
                            rec_w, rec_r = _bench_one(
                                lib, path, data, chunks, shards, codec, n_threads,
                                dim, sharded, iterations, warmup, nbytes)
                        except Exception as exc:  # keep going on a single failure
                            print("  FAILED %-11s %-7s shard=%-5s t=%d: %s"
                                  % (lib, codec, sharded, n_threads, exc))
                            if not keep and os.path.exists(path):
                                rmtree(path, ignore_errors=True)
                            continue
                        records.append(rec_w)
                        records.append(rec_r)
                        print("  %-11s %-7s shard=%-5s t=%d  "
                              "write=%8.1f MB/s  read=%8.1f MB/s  ratio=%5.2f"
                              % (lib, codec, sharded, n_threads,
                                 rec_w["throughput_mbps"], rec_r["throughput_mbps"],
                                 rec_w["ratio"]))
                        if not keep:
                            rmtree(path, ignore_errors=True)
    return records


def _bench_one(lib, path, data, chunks, shards, codec, n_threads, dim, sharded,
               iterations, warmup, nbytes):
    write = WRITERS[lib]
    read = READERS[lib]

    def write_fn():
        if os.path.exists(path):
            rmtree(path)
        write(path, data, chunks, shards, codec, n_threads)

    w_times = _time(write_fn, iterations, warmup)
    # the store now exists from the last write iteration; verify correctness once
    readback = read(path, n_threads)
    if not np.array_equal(readback, data):
        raise AssertionError("round-trip mismatch")
    stored = _dir_size(path)

    r_times = _time(lambda: read(path, n_threads), iterations, warmup)

    rec_w = _record(lib, dim, sharded, codec, n_threads, "write", w_times, nbytes, stored)
    rec_r = _record(lib, dim, sharded, codec, n_threads, "read", r_times, nbytes, stored)
    return rec_w, rec_r


def _z5_supports_sharding():
    import tempfile
    tmp = tempfile.mkdtemp()
    p = os.path.join(tmp, "probe.zr")
    try:
        f = z5py.File(p, mode="w", use_zarr_format=True, zarr_format=3)
        ds = f.create_dataset("p", shape=(8, 8), chunks=(2, 2), shards=(4, 4),
                              dtype="uint8", compression="raw")
        ds[:] = np.arange(64, dtype="uint8").reshape(8, 8)
        return bool(np.array_equal(ds[:], np.arange(64, dtype="uint8").reshape(8, 8)))
    except Exception:
        return False
    finally:
        rmtree(tmp, ignore_errors=True)


# ---------------------------------------------------------------------------
# console summary
# ---------------------------------------------------------------------------
def print_summary(records, libraries):
    by = {}
    for r in records:
        by.setdefault((r["dim"], r["sharded"], r["threads"], r["op"], r["codec"],
                       r["library"]), r)
    dims = sorted({r["dim"] for r in records})
    threads = sorted({r["threads"] for r in records})
    codecs = [c for c in ALL_CODECS if any(r["codec"] == c for r in records)]
    libs = [l for l in ALL_LIBRARIES if l in libraries]

    print("\n" + "=" * 78)
    print("SUMMARY -- throughput in MB/s (from best of all iterations)")
    print("=" * 78)
    for dim in dims:
        for sharded in (False, True):
            for nt in threads:
                block = [r for r in records
                         if r["dim"] == dim and r["sharded"] == sharded
                         and r["threads"] == nt]
                if not block:
                    continue
                tag = "sharded" if sharded else "unsharded"
                print("\n--- %s | %s | threads=%d ---" % (dim, tag, nt))
                header = "%-6s %-7s " % ("op", "codec") + \
                    "".join("%13s" % l for l in libs) + "   ratio"
                print(header)
                for op in ("write", "read"):
                    for codec in codecs:
                        cells = []
                        ratio = None
                        for lib in libs:
                            r = by.get((dim, sharded, nt, op, codec, lib))
                            cells.append("%13.1f" % r["throughput_mbps"] if r else
                                         "%13s" % "-")
                            if r and ratio is None:
                                ratio = r.get("ratio")
                        rtxt = "%7.2f" % ratio if ratio is not None else "%7s" % "-"
                        print("%-6s %-7s " % (op, codec) + "".join(cells) + " " + rtxt)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--libraries", nargs="+", default=list(ALL_LIBRARIES),
                   choices=ALL_LIBRARIES, help="libraries to benchmark")
    p.add_argument("--dims", nargs="+", default=list(CONFIGS), choices=list(CONFIGS),
                   help="data dimensionalities to benchmark")
    p.add_argument("--codecs", nargs="+", default=list(ALL_CODECS), choices=ALL_CODECS,
                   help="codecs to benchmark")
    p.add_argument("--threads", type=int, default=os.cpu_count(),
                   help="worker count for the multi-threaded scenario (default: cpu count)")
    p.add_argument("--iterations", "-i", type=int, default=3,
                   help="timed iterations per measurement")
    p.add_argument("--warmup", "-w", type=int, default=1,
                   help="discarded warmup iterations per measurement")
    p.add_argument("--dtype", default="uint8", help="numpy dtype of the benchmark data")
    p.add_argument("--random", action="store_true",
                   help="use incompressible random data (compression worst case)")
    p.add_argument("--save-folder", "-s", default="./bench_tmp",
                   help="scratch directory for benchmark stores")
    p.add_argument("--keep", action="store_true",
                   help="keep the written stores instead of deleting them")
    p.add_argument("--out", "-o", default="bench_v3_results.json",
                   help="output JSON results file")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    libraries = list(args.libraries)
    if "zarr" in libraries and not HAVE_ZARR:
        print("WARNING: zarr-python >= 3 not available; skipping it.")
        libraries.remove("zarr")
    if "tensorstore" in libraries and not HAVE_TS:
        print("WARNING: tensorstore not available; skipping it.")
        libraries.remove("tensorstore")
    if not libraries:
        sys.exit("No benchmarkable libraries available.")

    thread_modes = sorted({1, args.threads})

    records = run(libraries=libraries, dims=args.dims, codecs=args.codecs,
                  thread_modes=thread_modes, iterations=args.iterations,
                  warmup=args.warmup, dtype=args.dtype, incompressible=args.random,
                  save_folder=args.save_folder, keep=args.keep)

    meta = {
        "versions": {"z5py": z5py.__version__, "zarr": ZARR_VERSION,
                     "tensorstore": TS_VERSION, "numpy": np.__version__,
                     "python": platform.python_version()},
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "libraries": libraries,
        "dims": args.dims,
        "codecs": args.codecs,
        "dtype": args.dtype,
        "incompressible": args.random,
        "thread_modes": thread_modes,
        "iterations": args.iterations,
        "warmup": args.warmup,
        "configs": {d: CONFIGS[d] for d in args.dims},
    }
    with open(args.out, "w") as f:
        json.dump({"meta": meta, "records": records}, f, indent=2)
    print("\nWrote %d records to %s" % (len(records), args.out))

    print_summary(records, libraries)


if __name__ == "__main__":
    main()
