## Performance

z5py is fast: because the chunk codec and IO loop run in C++ and release the GIL,
it typically outperforms zarr-python by a wide margin and is competitive with —
often faster than — tensorstore, especially on writes.

The numbers below come from
[`src/bench/bench_python/bench_zarr_v3.py`](https://github.com/constantinpape/z5/blob/main/src/bench/bench_python/bench_zarr_v3.py).
They are hardware-dependent; reproduce them on your own machine before drawing
conclusions.

### Setup

- **Libraries:** z5py 3.0.0, zarr-python 3.2.1, tensorstore — all writing the
  same zarr v3 layout, numpy 2.4.6, Python 3.13.
- **Machine:** 8-core Linux box, warm page cache.
- **Data:** `uint8`, ~67 MB per array; 2D = `(8192, 8192)` with `(256, 256)`
  chunks, 3D = `(256, 512, 512)` with `(64, 64, 64)` chunks. The data is mildly
  compressible (smooth gradient + noise).
- **Codecs:** `raw` (no compression), `blosc` (lz4, clevel 5, byte shuffle),
  `zstd` (level 3), `gzip` (level 5).
- **Metric:** throughput in `MB/s` (higher is better), from the median of 3
  timed iterations after a warmup.

### Multi-threaded throughput (8 threads, unsharded)

This is the common case for bulk IO. **Write** (MB/s):

| codec | z5py | zarr-python | tensorstore |
|-------|-----:|------------:|------------:|
| raw (2D)   | **1637** | 147 | 225 |
| blosc (2D) | **955**  | 102 | 38  |
| zstd (2D)  | **407**  | 61  | 38  |
| gzip (2D)  | **210**  | 14  | 39  |
| raw (3D)   | **1927** | 253 | 135 |
| blosc (3D) | **1538** | 217 | 131 |
| zstd (3D)  | **387**  | 78  | 116 |
| gzip (3D)  | **243**  | 17  | 92  |

**Read** (MB/s):

| codec | z5py | zarr-python | tensorstore |
|-------|-----:|------------:|------------:|
| raw (2D)   | **4482** | 351 | 2464 |
| blosc (2D) | **3331** | 231 | 2709 |
| zstd (2D)  | **2033** | 217 | 1949 |
| gzip (2D)  | **1578** | 105 | 872  |
| raw (3D)   | **3861** | 843 | 2654 |
| blosc (3D) | **3812** | 570 | 2927 |
| zstd (3D)  | 1355     | 354 | **1444** |
| gzip (3D)  | **1469** | 138 | 817  |

z5py leads every write configuration (often 5–13× faster than zarr-python) and
most reads; on reads it trades the lead with tensorstore depending on codec and
dimensionality.

### Single-threaded throughput (fair codec comparison)

With one worker thread, throughput reflects raw codec + IO speed. **Write**
(MB/s):

| codec | z5py | zarr-python | tensorstore |
|-------|-----:|------------:|------------:|
| raw (2D)   | **800** | 78 | 54 |
| blosc (2D) | **244** | 34 | 54 |
| gzip (2D)  | **45**  | 11 | 16 |
| raw (3D)   | **762** | 136 | 33 |
| blosc (3D) | **530** | 86  | 33 |
| gzip (3D)  | **52**  | 15  | 18 |

### Sharding

After the shard-aware write path was optimized, harded zarr v3 performance
is on par with unsharded and scales with threads. For example, 8-thread sharded
writes reach ~1970 MB/s (2D raw) and ~1180 MB/s (3D blosc) for z5py, versus
~170 / ~205 MB/s for zarr-python. On sharded reads z5py and tensorstore are
close, each leading in some configurations.

### Compression ratios

All three libraries produce essentially identical compression ratios for the
same codec (raw ≈ 1.0, blosc-lz4 ≈ 1.0–1.14 on this data, zstd ≈ 1.8–2.1) — the
speed differences are not bought with worse compression. z5py's `gzip` ratio is
slightly better (e.g. 2.23 vs 1.98 in 2D) because it uses the libdeflate
backend, which compresses a little harder at the same level while also being faster.

### Reproducing

```bash
cd src/bench/bench_python
python bench_zarr_v3.py --out results/bench_v3_results.json
python plot_results.py --results results/bench_v3_results.json --out-dir results
```
