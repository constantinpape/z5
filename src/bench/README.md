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
`bench_v3_baseline.json` for the before/after below). Measured on an 8-core Linux box with
z5py 2.1.2, zarr-python 3.1.3, tensorstore, numpy 1.26.4; `uint8` semi-compressible data
(2D = 8192², 3D = 256×512×512, ≈64 MB each); best-of-3 iterations.

Throughput in **MB/s**, higher is better.

**Headline**

- **Unsharded zarr v3 — z5 wins, and the multi-threaded lead is large.** z5's C++
  chunk-level threadpool scales nearly linearly with thread count; zarr-python and
  tensorstore writes barely scale.
- **Sharded zarr v3 — now strong too.** A shard-aware read/write path (one
  read-modify-write per shard, parallel across shards) replaced the old per-inner-chunk,
  globally-locked path. Sharded throughput rose by up to ~70× and once again scales with
  thread count.
- **Compression ratios match across all three libraries** (raw 1.00, gzip ~1.98,
  zstd ~2.12, blosc 1.14) — confirming the codec settings are genuinely apples-to-apples
  (the `compression_ratio.png` sanity plot); they are also byte-identical before/after the
  optimization, so the on-disk format is unchanged.

**Unsharded (z5's strong suit), 8 threads**

| measurement | z5py | zarr | tensorstore |
|---|---:|---:|---:|
| 2D raw write | **1642** | 135 | 38 |
| 3D raw write | **1809** | 210 | 110 |
| 3D blosc read | 3263 | 564 | **3116** |

z5 leads every unsharded write and most reads; the unsharded paths were not modified, so
these are unchanged from baseline (run-to-run noise aside).

![write throughput, 8 threads](bench_python/results/throughput_write_t8.png)

**Sharded — before → after the optimization (z5py only)**

| measurement | baseline | optimized | speedup |
|---|---:|---:|---:|
| 3D raw write, 1 thread  | 12.7 | 278 | 21.9× |
| 3D raw write, 8 threads | 12.8 | 890 | 69.7× |
| 3D blosc write, 8 threads | 20.0 | 1192 | 59.7× |
| 2D raw write, 8 threads | 25.3 | 1651 | 65.2× |
| 3D raw read, 8 threads  | 227 | 2530 | 11.1× |
| 3D blosc read, 8 threads | 263 | 3124 | 11.9× |

The old path read-modify-wrote the whole shard once *per inner chunk* under a single
dataset-wide mutex, so sharded writes were slow and got *slower* with more threads. The new
path groups a request's inner chunks by shard, touches each shard file once, and
parallelizes across shards — restoring thread scaling (3D raw write 278 → 890 from 1 → 8
threads). Sharded z5 is now competitive with or ahead of the other libraries on writes and
much closer on reads.

![read throughput, 8 threads](bench_python/results/throughput_read_t8.png)

**Codec notes**

- `gzip` is still z5's softest codec. The compress path was rewritten to a single
  `deflateBound`-sized pass (no scratch buffer / per-block copies), a small write win
  (e.g. 3D gzip write 8 threads 93 → 98); read speed is bounded by the linked zlib itself.
- `blosc`/`zstd`: z5 is excellent unsharded; on this `uint8` gradient `blosc` (lz4) barely
  compresses (ratio ≈ 1.14), so it is mostly a speed contest, which z5 wins unsharded.

**Takeaway:** z5 leads unsharded zarr v3 by a wide margin and, after the shard-aware
read/write rewrite, is now strong on sharded data too (up to ~70× faster than before, with
thread scaling restored). The remaining gap is gzip *read*, which is limited by the zlib
build rather than z5's code.
