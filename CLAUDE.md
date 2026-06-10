# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

z5 is a library for reading and writing zarr and n5 files. It is written in C++ (header-only, requires **C++20**) and represents multi-dimensional arrays with a lightweight, non-owning strided view (`ArrayView`/`ConstArrayView`, numpy-compatible). It provides Python bindings via nanobind that pass numpy arrays directly. The Python library is called z5py. The build system is CMake.

## Common Commands

### Build (Python bindings — the usual case)
Dependencies come from a conda env (`.github/environment.yaml`, or `environments/unix/z5-dev.yaml`). CMake auto-detects the active conda env as `CMAKE_PREFIX_PATH`; if that fails, pass `-DCMAKE_PREFIX_PATH=/path/to/conda-env`.

```bash
rm -rf bld && mkdir bld && cd bld
cmake .. -DWITH_BLOSC=ON -DWITH_ZLIB=ON -DWITH_BZIP2=ON -DWITH_XZ=ON -DWITH_LZ4=ON -DWITH_ZSTD=ON
make -j 4
```

`make` compiles the `_z5py` nanobind extension and copies the pure-Python `z5py` package + tests into `bld/python/`. `make install` additionally installs headers and the Python package into the conda env. nanobind is located via `python -m nanobind --cmake_dir` (or `CMAKE_PREFIX_PATH`); the active conda env must provide `nanobind` and `numpy`.

Key CMake options (see `CMakeLists.txt`): compression codecs `WITH_BLOSC` / `WITH_BZIP2` / `WITH_XZ` / `WITH_LZ4` / `WITH_ZSTD` are **OFF by default** (`WITH_ZLIB` is ON); a codec must be compiled in to read/write chunks using it. Backends: `WITH_S3`, `WITH_GCS` (both incomplete). Also `BUILD_TESTS` (C++ tests, OFF), `BUILD_Z5PY` (ON).

### Python tests
```bash
cd bld/python
python -m unittest discover -s test -v          # all tests
python -m unittest test.test_dataset -v          # one module
python -m unittest test.test_dataset.TestDataset.test_ds_open -v   # one test
```
CI instead runs `make install` then `python -m unittest discover -s src/python/test -v` against the installed package.

### Python coding standards

Python code should be written according to PEP8. 

The following linter commands should pass for each committed python file:
```bashbash
pyflakes <path/to/file.py>
flake8 <path/to/file.py> --max-line-length=120
```
Doc-strings should be written following the google convention.


### C++ tests
Configure with `-DBUILD_TESTS=ON` (uses the bundled googletest submodule — clone with `--recursive` or run `git submodule update --init`), build, then run the gtest binaries under `bld/src/test/`.

There is no configured linter/formatter in this repo.

## Code Architecture

### Layers

- **Abstract base classes** in `include/z5/`: `Dataset` (`dataset.hxx`), and the handle/metadata/attribute interfaces (`handle.hxx`, `metadata.hxx`, `attributes.hxx`). `Dataset` declares pure-virtual chunk-level IO (`writeChunk`/`readChunk`/`readRawChunk`), compression, and path operations.
- **Backend implementations** of those bases live in `include/z5/filesystem/` (complete), `include/z5/s3/`, and `include/z5/gcs/` (both incomplete). Each provides its own `handle`, `dataset`, `metadata`, `attributes`, and `factory`.
- **`factory.hxx`** is the entry point (`createFile`, `createDataset`, `openDataset`, etc.). It dispatches to a backend **at runtime** by inspecting the passed handle (`root.isS3()` / `root.isGcs()` / `root.isZarr()`), guarded by `WITH_S3` / `WITH_GCS` compile flags. To use a backend, call the factory with that backend's handle type (e.g. `z5::filesystem::handle::File`).
- **`include/z5/compression/`**: one compressor per codec, all deriving from `CompressorBase` (`compress`/`decompress`/`type`). Each is gated behind its `WITH_*` define.
- **`include/z5/multiarray/`**: in-memory IO. `array_view.hxx` defines the non-owning strided `ArrayView`/`ConstArrayView` (element strides) plus `cOrderStrides`/`subview`/`makeView`; `array_util.hxx` provides the generic strided `copyView`/`fillView`; `array_access.hxx` implements `readSubarray`/`writeSubarray` (the main user-facing IO functions) on those views; `broadcast.hxx` implements `writeScalar`. To support another multiarray type, wrap its buffer in an `ArrayView` (data pointer + shape + element strides).
- **`include/z5/util/`**: threadpool, chunk blocking/iteration, file modes.

### Reusable building blocks (prefer these over re-implementing)

When adding or changing IO paths, reuse the existing helpers — most "new" chunk/codec/shard
logic is already factored out:

- **Sub-array IO** (`multiarray/array_access.hxx`): `readSubarray`/`writeSubarray` dispatch on
  `ds.isSharded()` to `*Plain` vs `*Sharded` variants. The shared primitives are:
  - `forEachBuffered(nThreads, nUnits, bufSize, init, body)` — the single-/multi-threaded loop
    with one reusable buffer per thread and the `n_threads<=0` guard. Don't hand-roll a
    `ThreadPool` + per-thread buffer vector; `body` is a template param (zero overhead).
  - `prepareChunkWriteBuffer(...)` — complete-overlap / zarr edge-padding / partial-overlap
    write-buffer prep; the "read existing chunk" source is a callable you supply (chunk file
    for plain, in-memory shard blob for sharded).
  - `groupChunksByShard(...)` — bucket inner chunks by shard so each shard is touched once.
- **Codec layer** (`compression/`, `util/format_data.hxx`): go through `util::data_to_buffer`
  (compress a chunk to its on-disk blob, incl. raw + all-fill→empty handling) and
  `util::decompress` rather than calling compressors directly. `CompressorBase::decompress`
  has a primary `(const char*, size_t, T*, size_t)` overload plus a `std::vector<char>`
  forwarder; `Dataset::decompress` mirrors both, so you can decode straight from a larger
  buffer (e.g. a shard) with no intermediate copy.
- **Sharding** (`util/sharding.hxx` + `Dataset` batch API): `util/sharding.hxx` owns the shard
  layout (`chunksPerShard`, `numShardSlots`, `shardId`/`shardSlot`, `parseShardIndex` /
  `buildShard` / `extractShardBlobs`, `allSlotsEmpty`, `crc32c`). `Dataset` exposes the batch
  ops the shard-aware paths use: `isSharded()`/`shardShape()`, `readShardBlobs` /
  `readShardRaw` (read a shard once), `writeShardBlobs` (rebuild/remove), `makeChunkBlob`
  (compress one inner chunk). Always group by shard and do **one** read-modify-write per shard
  (parallel across shards), never a full-shard RMW per inner chunk.

### Python binding layout

- `src/python/lib/*.cxx` — nanobind sources compiled into the `_z5py` extension module (`z5py.cxx` registers submodules: attributes, dataset, factory, handles, util). IO bindings receive/return numpy arrays via `nb::ndarray<nb::numpy, T, nb::c_contig>` and wrap them in `ArrayView`. Pickling of `File`/`Group`/`Dataset` lives in the pure-Python layer (re-opens by path+mode), not in C++.
- `src/python/module/z5py/*.py` — pure-Python package wrapping `_z5py`; the user-facing `h5py`-like API (`File`, `Group`, `Dataset`, `attribute_manager`, `converter`).
- `src/python/test/*.py` — Python tests. `src/test/` holds C++ gtest sources.

### Gotchas

- **Axis ordering**: n5 is column-major (x, y, z), z5 is row-major (z, y, x). Handled internally, but n5 metadata (shape, chunk shape, chunk ids) is stored reversed relative to z5's in-memory order. Both zarr v2 and v3 are exercised in the tests.
- **No thread/process synchronization**: concurrent writes to the same chunk are undefined behavior.
- The zarr format here supports only little-endian and C-order.
- The version is the single source of truth in `include/z5/z5.hxx` (`Z5_VERSION_*`); CMake parses it from there, and it is mirrored in `src/python/module/z5py/__init__.py`.
