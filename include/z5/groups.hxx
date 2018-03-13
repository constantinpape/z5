#pragma once

#include "z5/handle/handle.hxx"
#include "z5/metadata.hxx"

namespace z5 {

    inline void createGroup(const handle::Group & group, const bool isZarr=true) {
        if(!group.mode().canWrite()) {
            const std::string err = "Cannot create new group in file mode " + group.mode().printMode();
            throw std::invalid_argument(err.c_str());
        }
        group.createDir();
        if(isZarr) {
            Metadata gmeta;
            writeMetadata(group, gmeta);
        }
    }

    inline void createGroup(const handle::Group & group,
                            const std::string & key,
                            const bool isZarr=true,
                            const FileMode::modes mode=FileMode::a) {
       auto path = group.path();
       path /= key;
       handle::Group subGroup(path.string(), mode);
       createGroup(subGroup, isZarr);
    }

}
