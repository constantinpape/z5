#pragma once

#include "z5/filesystem/store.hxx"
#include "z5/generic/sharded_dataset.hxx"


namespace z5 {
namespace filesystem {

    // zarr v3 sharded dataset over the filesystem chunk store; the format logic
    // lives in z5/generic/sharded_dataset.hxx
    template<typename T>
    using ShardedDataset = z5::generic::ShardedDataset<T, ChunkStore>;

}
}
