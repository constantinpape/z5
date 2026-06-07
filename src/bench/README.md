# Benchmarks

- **bench-python**: cross-library zarr **v3** read/write benchmark — z5py vs
  [zarr-python](https://github.com/zarr-developers/zarr-python) (>= 3) vs
  [tensorstore](https://github.com/google/tensorstore), with and without sharding.
- **bench-java**: re-implementation of the
  [n5-java benchmarks](https://github.com/saalfeldlab/n5/blob/master/src/test/java/org/janelia/saalfeldlab/n5/N5Benchmark.java).

There is also a [repository with an asv benchmark suite](https://github.com/constantinpape/z5py-benchmarks)
to track z5 performance over time; results are hosted
[here](https://constantinpape.github.io/z5py-benchmarks/).

## bench-python

`bench_python/bench_zarr_v3.py` benchmarks z5's zarr v3 implementation against
zarr-python and tensorstore for 2D and 3D data, across the codecs common to all three
(`raw` / `gzip` / `blosc` / `zstd`), reading and writing.

Two threading scenarios are measured for every configuration:

- **single-threaded** — every library pinned to one worker. The *fair* comparison: pure
  codec + IO speed, no parallelism.
- **multi-threaded** — every library given `N` workers (`N` defaults to the CPU count).
  z5's C++ chunk-level threadpool tends to win here; all three libraries still get `N`
  workers, so the comparison is honest, just favourable to z5.

The threading knobs used: z5py `n_threads`, zarr-python
`zarr.config(threading.max_workers, async.concurrency)`, tensorstore
`data_copy_concurrency` / `file_io_concurrency`.

Codec levels are pinned identically across libraries (gzip level 5, zstd level 3, blosc
lz4 clevel 5 with byte shuffle), so the comparison is apples-to-apples. Reads are
warm-cache (decode/copy bound): each store is written once, then read back several times.
Each measurement round-trips and asserts equality, so a broken codec can't post a fast
(but wrong) number.

Data is semi-compressible synthetic data (smooth gradient + mild noise) by default so the
codecs do real work; pass `--random` for the incompressible worst case.

### Running

Requires the conda dev env (`environments/unix/z5-dev.yaml`), which provides `zarr >= 3`,
`tensorstore`, `matplotlib`, `pandas` and (optionally) `seaborn`. Run from `bld/python`
(where `make` copies `z5py`) or after `make install`.

```bash
# full default run (2D + 3D, all codecs, single- and multi-threaded); reproduces the
# checked-in reference artifacts under results/
python bench_zarr_v3.py --out results/bench_v3_results.json
python plot_results.py --results results/bench_v3_results.json --out-dir results

# quick smoke run
python bench_zarr_v3.py --dims 2d --codecs raw gzip --iterations 1 --warmup 0 --threads 2
```

Useful flags: `--libraries`, `--dims {2d,3d}`, `--codecs`, `--threads N`,
`--iterations`, `--warmup`, `--dtype`, `--random`, `--save-folder`, `--keep`, `--out`.
Libraries that aren't importable (and sharding on a z5 build without it) are skipped with
a warning, so the benchmark still runs with a subset installed.

### Output

`bench_zarr_v3.py` writes a JSON file (`{"meta": ..., "records": [...]}`) — one record per
`(library, dim, sharded, codec, threads, op)` with all per-iteration times, `min`/`median`,
throughput in MB/s, on-disk size and compression ratio — and prints a console summary
table. `plot_results.py` turns that JSON into throughput bar charts (one per
operation/thread-mode, faceted by dim × sharding) and a compression-ratio chart.

### Results

A full reference run is checked in under [`bench_python/results/`](bench_python/results)
(`bench_v3_results.json` plus the generated plots; the pre-optimization run is kept as
`bench_v3_baseline.json` for the sharding before/after below). z5's gzip/zlib codec is built
on [libdeflate](https://github.com/ebiggers/libdeflate) (the `USE_LIBDEFLATE` CMake default);
the gzip before/after below contrasts it against a stock-zlib build. Measured on an 8-core
Linux box with z5py 2.1.2, zarr-python 3.1.3, tensorstore, numpy 1.26.4; `uint8`
semi-compressible data (2D = 8192², 3D = 256×512×512, ≈64 MB each); best-of-3 iterations.
Absolute throughput on the memory-bound paths (raw, blosc) carries run-to-run variance; the
gzip and sharding deltas below are well outside that noise.

Throughput in **MB/s**, higher is better.

**Headline**

- **Unsharded zarr v3 — z5 wins, and the multi-threaded lead is large.** z5's C++
  chunk-level threadpool scales nearly linearly with thread count; zarr-python and
  tensorstore writes barely scale.
- **Sharded zarr v3 — now strong too.** A shard-aware read/write path (one
  read-modify-write per shard, parallel across shards) replaced the old per-inner-chunk,
  globally-locked path. Sharded throughput rose by up to ~100× and once again scales with
  thread count.
- **gzip is no longer z5's soft spot.** Moving the gzip/zlib backend from stock zlib to
  libdeflate made gzip writes ~2.3–3.0× and gzip reads ~1.6–1.9× faster, with no on-disk
  format change. z5's gzip *read* now exceeds tensorstore's (e.g. 3D, 8 threads: 1515 vs
  1189 MB/s), where stock zlib trailed it.
- **Compression ratios match across libraries** for raw (1.00), zstd (~2.12) and blosc
  (1.14) — the `compression_ratio.png` sanity check that the codec settings are
  apples-to-apples. The one exception is gzip: libdeflate's level-5 encoder compresses a
  little harder than zlib's, so z5's 2D gzip ratio is ~2.23 vs ~1.98 for zarr/tensorstore
  (3D is ~unchanged). The streams stay standard gzip and fully interoperable — only the
  encoder differs.

**Unsharded (z5's strong suit), 8 threads**

| measurement | z5py | zarr | tensorstore |
|---|---:|---:|---:|
| 2D raw write | **1154** | 142 | 226 |
| 3D raw write | **1910** | 228 | 131 |
| 3D blosc read | **3944** | 589 | 3330 |
| 3D gzip write | **259** | 18 | 133 |
| 3D gzip read | **1515** | 132 | 1189 |

z5 leads every unsharded write and most reads. The raw/blosc/zstd paths are unchanged code
(differences vs baseline are run-to-run noise); gzip improved via the libdeflate backend and
now leads on both write and read.

![write throughput, 8 threads](bench_python/results/throughput_write_t8.png)

**Sharded — before → after the shard-aware rewrite (z5py only)**

| measurement | baseline | optimized | speedup |
|---|---:|---:|---:|
| 3D raw write, 1 thread  | 12.7 | 282 | 22× |
| 3D raw write, 8 threads | 12.8 | 1287 | ~100× |
| 3D blosc write, 8 threads | 20.0 | 1277 | 64× |
| 2D raw write, 8 threads | 25.3 | 1384 | 55× |
| 3D raw read, 8 threads  | 227 | 3637 | 16× |
| 3D blosc read, 8 threads | 263 | 3990 | 15× |

The old path read-modify-wrote the whole shard once *per inner chunk* under a single
dataset-wide mutex, so sharded writes were slow and got *slower* with more threads. The new
path groups a request's inner chunks by shard, touches each shard file once, and
parallelizes across shards — restoring thread scaling (3D raw write 282 → 1287 from 1 → 8
threads). Sharded z5 is now competitive with or ahead of the other libraries on writes and
much closer on reads. (This is the #247 shard-aware optimization, unchanged here; the
absolute "optimized" numbers are a fresh measurement and carry run-to-run variance.)

![read throughput, 8 threads](bench_python/results/throughput_read_t8.png)

**Codec notes**

- `gzip`: the backend moved from stock zlib to **libdeflate** (`USE_LIBDEFLATE`, on by
  default; build `-DUSE_LIBDEFLATE=OFF` to fall back to zlib). On-disk streams are unchanged
  and stay interoperable with zarr-python/numcodecs and tensorstore (verified by the interop
  tests). The win is large and reproducible:

  | measurement (unsharded) | stock zlib | libdeflate | speedup |
  |---|---:|---:|---:|
  | 3D gzip write, 1 thread  |  17.5 |  53.0 | 3.0× |
  | 3D gzip write, 8 threads |  98.4 | 259.4 | 2.6× |
  | 2D gzip write, 8 threads |  88.8 | 199.3 | 2.2× |
  | 3D gzip read, 1 thread   | 165.5 | 278.9 | 1.7× |
  | 3D gzip read, 8 threads  | 824.3 | 1514.8 | 1.8× |
  | 2D gzip read, 8 threads  | 843.0 | 1548.4 | 1.8× |

  z5's gzip read used to trail tensorstore (the gap was the linked zlib, not z5's code); with
  libdeflate it now leads (3D 8-thread read 1515 vs 1189 MB/s). libdeflate's level-5 encoder
  also compresses slightly harder than zlib's (see the ratio note above).
- `blosc`/`zstd`: z5 is excellent unsharded; on this `uint8` gradient `blosc` (lz4) barely
  compresses (ratio ≈ 1.14), so it is mostly a speed contest, which z5 wins unsharded.

**Takeaway:** z5 leads unsharded zarr v3 by a wide margin and, after the shard-aware
read/write rewrite, is strong on sharded data too (thread scaling restored). The old gzip
*read* gap is now closed: moving the gzip/zlib codec to libdeflate made gzip ~2–3× faster
and put z5's gzip read ahead of tensorstore, with the on-disk format unchanged.
