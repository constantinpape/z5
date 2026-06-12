#pragma once

#include <fstream>
#include <iomanip>

#include "z5/filesystem/handle.hxx"
#include "z5/generic/attributes.hxx"


namespace z5 {
namespace filesystem {

namespace attrs_detail {

    // per-operation JSON IO rooted at a node (group / dataset) directory;
    // see z5/generic/attributes.hxx for the logic implemented on top of it
    class JsonIO {
    public:
        explicit JsonIO(const fs::path & base) : base_(base) {}

        // a cheap existence check for child nodes is available on the filesystem
        static constexpr bool hasNodeExists = true;

        inline bool exists(const std::string & name) const {
            return fs::exists(base_ / name);
        }

        inline bool nodeExists(const std::string & name) const {
            return fs::exists(base_ / name);
        }

        // read the JSON document 'name'; returns false (leaving j untouched) if absent
        inline bool read(const std::string & name, nlohmann::json & j) const {
            const auto path = base_ / name;
            if(!fs::exists(path)) {
                return false;
            }
            std::ifstream file(path);
            file >> j;
            file.close();
            return true;
        }

        // compact serialization (zarr v2 '.zattrs', n5 'attributes.json')
        inline void write(const std::string & name, const nlohmann::json & j) const {
            std::ofstream file(base_ / name);
            file << j;
            file.close();
        }

        // pretty serialization (zarr.json and metadata files)
        inline void writePretty(const std::string & name, const nlohmann::json & j) const {
            std::ofstream file(base_ / name);
            file << std::setw(4) << j << std::endl;
            file.close();
        }

    private:
        fs::path base_;
    };

}  // namespace attrs_detail


    template<class GROUP>
    inline void readAttributes(const z5::handle::Group<GROUP> & group, nlohmann::json & j) {
        attrs_detail::JsonIO io(group.path());
        generic::readAttributes(io, group.isZarr(), j);
    }

    template<class GROUP>
    inline void writeAttributes(const z5::handle::Group<GROUP> & group, const nlohmann::json & j) {
        attrs_detail::JsonIO io(group.path());
        generic::writeAttributes(io, group.isZarr(), j);
    }

    template<class GROUP>
    inline void removeAttribute(const z5::handle::Group<GROUP> & group, const std::string & key) {
        attrs_detail::JsonIO io(group.path());
        generic::removeAttribute(io, group.isZarr(), key);
    }


    template<class DATASET>
    inline void readAttributes(const z5::handle::Dataset<DATASET> & ds, nlohmann::json & j) {
        attrs_detail::JsonIO io(ds.path());
        generic::readAttributes(io, ds.isZarr(), j);
    }

    template<class DATASET>
    inline void writeAttributes(const z5::handle::Dataset<DATASET> & ds, const nlohmann::json & j) {
        attrs_detail::JsonIO io(ds.path());
        generic::writeAttributes(io, ds.isZarr(), j);
    }

    template<class DATASET>
    inline void removeAttribute(const z5::handle::Dataset<DATASET> & ds, const std::string & key) {
        attrs_detail::JsonIO io(ds.path());
        generic::removeAttribute(io, ds.isZarr(), key);
    }


    template<class GROUP>
    inline bool isSubGroup(const z5::handle::Group<GROUP> & group, const std::string & key){
        attrs_detail::JsonIO io(group.path());
        return generic::isSubGroup(io, group.isZarr(), key);
    }

}
}
