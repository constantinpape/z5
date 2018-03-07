#pragma once

#include "z5/handle/handle.hxx"
#include "z5/metadata.hxx"

namespace z5 {

    inline void createGroup(const handle::Group & group, const bool isZarr=true) {
        if(!group.mode().canCreate()) {
            const std::string err = "Cannot create new group in file mode " + group.mode().printMode();
            throw std::runtime_error(err.c_str());
        }
        group.createDir();
        if(isZarr) {
            Metadata gmeta;
            writeMetadata(group, gmeta);
        }
    }

    inline void createGroup(const handle::Group & group, const std::string & key, const bool isZarr=true) {
       auto path = group.path();
       path /= key;
       handle::Group subGroup(path.string());
       createGroup(subGroup, isZarr);
    }

}
