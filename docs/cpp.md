## Using z5 as a C++ library

z5py is a thin binding around **z5**, a header-only C++20 library. If you work in
C++ you can use z5 directly; this page is a high-level summary — see the headers
under [`include/z5/`](https://github.com/constantinpape/z5/tree/main/include/z5)
for the full API.

### Integrating z5

z5 is **header-only**: add `include/` to your include path and `#include` the
headers you need. You only have to *link* against the compression codecs you
actually use (blosc, zlib, bzip2, xz, lz4, zstd) and against
[nlohmann_json](https://github.com/nlohmann/json) for metadata handling. A C++20
compiler is required.

### Backends and the factory

The entry points are the factory functions in
[`z5/factory.hxx`](https://github.com/constantinpape/z5/blob/main/include/z5/factory.hxx)
— `createFile`, `createDataset`, `openDataset`, … . The backend (filesystem, S3,
…) is selected by the *handle type* you pass: `z5::filesystem::handle::File` for
local storage, `z5::s3::handle::File` for AWS S3.

### Reading and writing data

z5 is meant to be used with an in-memory multiarray that owns the data. Data is
passed in and out through a lightweight, non-owning strided view —
`z5::multiarray::ArrayView` / `ConstArrayView` (a data pointer plus shape and
element strides, compatible with numpy arrays) — using `readSubarray` /
`writeSubarray` from
[`z5/multiarray/array_access.hxx`](https://github.com/constantinpape/z5/blob/main/include/z5/multiarray/array_access.hxx).
To interface with another array type, wrap its buffer in an `ArrayView`.

### Example

```cpp
#include "z5/factory.hxx"
#include "z5/filesystem/handle.hxx"
#include "z5/multiarray/array_access.hxx"
#include "z5/attributes.hxx"

int main() {
    // handle to a File on the filesystem (use z5::s3::handle::File for S3)
    z5::filesystem::handle::File f("data.zr");

    // create the container in zarr format
    z5::createFile(f, true);

    // create a dataset
    std::vector<size_t> shape = {1000, 1000, 1000};
    std::vector<size_t> chunks = {100, 100, 100};
    auto ds = z5::createDataset(f, "data", "float32", shape, chunks);

    // write a region: wrap a C-contiguous buffer in a (non-owning) strided view
    z5::types::ShapeType offset = {50, 100, 150};
    z5::types::ShapeType roiShape = {150, 200, 100};
    std::vector<float> buffer(150 * 200 * 100, 42.0f);
    z5::multiarray::ConstArrayView<float> in(
        buffer.data(), roiShape, z5::multiarray::cOrderStrides(roiShape));
    z5::multiarray::writeSubarray<float>(ds, in, offset.begin());

    // read a region (never-written values come back as the fill-value)
    std::vector<float> out(150 * 200 * 100);
    z5::multiarray::ArrayView<float> view(
        out.data(), roiShape, z5::multiarray::cOrderStrides(roiShape));
    z5::multiarray::readSubarray<float>(ds, view, offset.begin());

    // read / write json attributes
    const auto dsHandle = z5::filesystem::handle::Dataset(f, "data");
    nlohmann::json attrs;
    attrs["pi"] = 3.141593;
    z5::writeAttributes(dsHandle, attrs);

    return 0;
}
```

### Notes

- Axis ordering differs between formats: n5 is column-major (x, y, z), z5 is
  row-major (z, y, x). This is handled internally, but n5 metadata (shape, chunk
  shape, chunk ids) is stored reversed relative to z5's in-memory order.
- The zarr format support here is little-endian and C-order only.
- There is no thread / process synchronization: writing to the same chunk
  concurrently is undefined behavior.
