#pragma once

#include "z5/filesystem/store.hxx"
#include "z5/generic/dataset.hxx"


namespace z5 {
namespace filesystem {

    // plain (one file per chunk) dataset over the filesystem chunk store;
    // the format logic lives in z5/generic/dataset.hxx
    template<typename T>
    using Dataset = z5::generic::Dataset<T, ChunkStore>;

}
}
