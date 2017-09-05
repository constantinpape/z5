#pragma once

#include "zarr/array.hxx"
#include "zarr/metadata.hxx"
#include "zarr/handle/handle.hxx"

namespace zarr {

    // factory function to open an existing zarr-array
    std::unique_ptr<ZarrArray> openZarrArray(const std::string &  path) {

        // read the data type from the metadata

        // make the array handle

        // make the ptr to the ZarrArrayTyped of appropriate dtype

        //return;
    }


    // TODO all relevant params
    std::unique_ptr<ZarrArray> createZarrArray(
        const std::string & path
    ) {

        // make metadata

        // make array handle

        // make the ptr to the ZarrArrayTyped of appropriate dtype

        //return;
    }

}
