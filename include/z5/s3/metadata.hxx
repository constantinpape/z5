#pragma once

#include "z5/s3/handle.hxx"


namespace z5 {
namespace s3 {

    // TODO implement for s3

    template<class GROUP>
    inline void writeMetadata(const z5::handle::File<GROUP> & handle, const Metadata & metadata) {
    }


    template<class GROUP>
    inline void writeMetadata(const z5::handle::Group<GROUP> & handle, const Metadata & metadata) {
    }


    inline void writeMetadata(const handle::Dataset & handle, const DatasetMetadata & metadata) {
    }


    inline void readMetadata(const handle::Dataset & handle, DatasetMetadata & metadata) {
    }


}
}
