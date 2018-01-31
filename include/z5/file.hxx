#pragma once

#include "z5/handle/handle.hxx"
#include "z5/metadata.hxx"
#include "z5/attributes.hxx"

namespace z5 {

    inline void createFile(const handle::File & file, const bool isZarr=true) {
        file.createDir();
        if(isZarr) {
            Metadata fmeta;
            writeMetadata(file, fmeta);
        } else {
            nlohmann::json n5v;
            std::string version = "2.0.0";
            n5v["n5"] = version;
            writeAttributes(file, n5v);
        }
    }

}
