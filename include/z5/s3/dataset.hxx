#pragma once

#include "z5/s3/store.hxx"
#include "z5/generic/dataset.hxx"


namespace z5 {
namespace s3 {

    // plain (one object per chunk) dataset over S3;
    // the format logic lives in z5/generic/dataset.hxx
    template<typename T>
    using Dataset = z5::generic::Dataset<T, ChunkStore>;

}
}
