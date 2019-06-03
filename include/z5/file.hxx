#pragma once

#include "z5/handle/handle.hxx"
#include "z5/metadata.hxx"
#include "z5/attributes.hxx"

namespace z5 {

    inline void createFile(const handle::File & file, const bool isZarr=true) {
        file.createDir();
	Metadata fmeta(isZarr);
	writeMetadata(file, fmeta);
    }
}
