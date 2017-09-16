#pragma once

#include "z5/handle/handle.hxx"
#include "z5/metadata.hxx"

namespace z5 {

    void createGroup(const handle::Group & group, const bool isZarr=true) {
        group.createDir();
        if(isZarr) {
            Metadata gmeta;
            writeMetadata(group, gmeta);
        }
    }

    void createGroup(const handle::Group & group, const std::string & key, const bool isZarr=true) {
       auto path = group.path();
       path /= key;
       handle::Group subGroup(path);
       createGroup(subGroup, isZarr);
    }

}
