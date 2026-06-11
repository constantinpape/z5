#pragma once

#include "z5/s3/handle.hxx"
#include "z5/generic/attributes.hxx"


namespace z5 {
namespace s3 {

namespace attrs_detail {

    // read a JSON object stored at 'key'; leaves 'j' untouched if the object is absent
    inline bool readJson(Aws::S3::S3Client & client, const std::string & bucket,
                         const std::string & key, nlohmann::json & j) {
        std::string content;
        if(!detail::getObjectString(client, bucket, key, content)) {
            return false;
        }
        if(content.empty()) {
            j = nlohmann::json::object();
        } else {
            j = nlohmann::json::parse(content);
        }
        return true;
    }

    inline void writeJson(Aws::S3::S3Client & client, const std::string & bucket,
                          const std::string & key, const nlohmann::json & j) {
        detail::putObjectString(client, bucket, key, j.dump(4));
    }


    // per-operation JSON IO rooted at a node (group / dataset) prefix; holds ONE
    // client for the whole operation (the v3 probe and the actual reads / writes).
    // See z5/generic/attributes.hxx for the logic implemented on top of it.
    class JsonIO {
    public:
        template<class HANDLE>
        explicit JsonIO(const HANDLE & handle) : client_(detail::makeClient(handle)),
                                                 bucket_(handle.bucketName()),
                                                 base_(handle.nameInBucket()) {}

        // there is no cheap "node exists" check on s3 (a prefix has no marker object)
        static constexpr bool hasNodeExists = false;

        inline bool exists(const std::string & name) const {
            return detail::objectExists(*client_, bucket_, detail::joinKey(base_, name));
        }

        inline bool read(const std::string & name, nlohmann::json & j) const {
            return readJson(*client_, bucket_, detail::joinKey(base_, name), j);
        }

        inline void write(const std::string & name, const nlohmann::json & j) const {
            writeJson(*client_, bucket_, detail::joinKey(base_, name), j);
        }

        inline void writePretty(const std::string & name, const nlohmann::json & j) const {
            write(name, j);
        }

    private:
        std::shared_ptr<Aws::S3::S3Client> client_;
        std::string bucket_;
        std::string base_;
    };

}  // namespace attrs_detail


    template<class GROUP>
    inline void readAttributes(const z5::handle::Group<GROUP> & group, nlohmann::json & j) {
        attrs_detail::JsonIO io(group);
        generic::readAttributes(io, group.isZarr(), j);
    }

    template<class GROUP>
    inline void writeAttributes(const z5::handle::Group<GROUP> & group, const nlohmann::json & j) {
        attrs_detail::JsonIO io(group);
        generic::writeAttributes(io, group.isZarr(), j);
    }

    template<class GROUP>
    inline void removeAttribute(const z5::handle::Group<GROUP> & group, const std::string & key) {
        attrs_detail::JsonIO io(group);
        generic::removeAttribute(io, group.isZarr(), key);
    }


    template<class DATASET>
    inline void readAttributes(const z5::handle::Dataset<DATASET> & ds, nlohmann::json & j) {
        attrs_detail::JsonIO io(ds);
        generic::readAttributes(io, ds.isZarr(), j);
    }

    template<class DATASET>
    inline void writeAttributes(const z5::handle::Dataset<DATASET> & ds, const nlohmann::json & j) {
        attrs_detail::JsonIO io(ds);
        generic::writeAttributes(io, ds.isZarr(), j);
    }

    template<class DATASET>
    inline void removeAttribute(const z5::handle::Dataset<DATASET> & ds, const std::string & key) {
        attrs_detail::JsonIO io(ds);
        generic::removeAttribute(io, ds.isZarr(), key);
    }


    template<class GROUP>
    inline bool isSubGroup(const z5::handle::Group<GROUP> & group, const std::string & key){
        attrs_detail::JsonIO io(group);
        return generic::isSubGroup(io, group.isZarr(), key);
    }

}
}
