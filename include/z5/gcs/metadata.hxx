#pragma once

#include "z5/gcs/handle.hxx"


namespace z5 {
namespace gcs {

    // TODO implement for gcs

    template<class GROUP>
    inline void writeMetadata(const z5::handle::File<GROUP> & handle, const Metadata & metadata) {
        detail::notImplemented();
    }


    template<class GROUP>
    inline void writeMetadata(const z5::handle::Group<GROUP> & handle, const Metadata & metadata) {
        detail::notImplemented();
    }


    inline void writeMetadata(const handle::Dataset & handle, const DatasetMetadata & metadata) {
        detail::notImplemented();
    }


    inline void readMetadata(const handle::Dataset & handle, DatasetMetadata & metadata) {
        detail::notImplemented();
    }


}
}
