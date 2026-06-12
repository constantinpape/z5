#pragma once

#include "z5/s3/store.hxx"
#include "z5/generic/sharded_dataset.hxx"


namespace z5 {
namespace s3 {

    // zarr v3 sharded dataset over S3: the on-store object is the shard, GET/PUT
    // as a whole; the format logic lives in z5/generic/sharded_dataset.hxx
    template<typename T>
    using ShardedDataset = z5::generic::ShardedDataset<T, ChunkStore>;

}
}
