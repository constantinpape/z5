#pragma once

#include "z5/handle.hxx"


namespace z5 {
namespace generic {

    // Backend-independent attribute logic, parameterized on a per-call JSON-IO
    // object (see the JsonIO classes in the backends' attrs_detail namespaces).
    // The IO object is rooted at the node (group / dataset) the operation acts
    // on and is constructed once per public operation, so backends can reuse
    // expensive per-operation state (e.g. the s3 client) across the v3 probe
    // and the actual reads / writes.
    //
    // zarr v3 stores user attributes inline in zarr.json under the "attributes"
    // key; zarr v2 and n5 store them in a separate file (".zattrs" /
    // "attributes.json"). n5 keeps dataset metadata in the same file, which is
    // why attribute writes always merge into the existing document.

namespace attrs_impl {

    template<class IO>
    inline void readV3Attributes(const IO & io, nlohmann::json & j) {
        nlohmann::json full;
        if(!io.read("zarr.json", full)) {
            return;
        }
        if(full.contains("attributes")) {
            j = full["attributes"];
        }
    }

    template<class IO>
    inline void writeV3Attributes(const IO & io, const nlohmann::json & j) {
        nlohmann::json full = nlohmann::json::object();
        io.read("zarr.json", full);
        nlohmann::json attrs = full.contains("attributes") ? full["attributes"] : nlohmann::json::object();
        for(auto jIt = j.begin(); jIt != j.end(); ++jIt) {
            attrs[jIt.key()] = jIt.value();
        }
        full["attributes"] = attrs;
        io.writePretty("zarr.json", full);
    }

    template<class IO>
    inline void removeV3Attribute(const IO & io, const std::string & key) {
        nlohmann::json full;
        if(!io.read("zarr.json", full)) {
            return;
        }
        if(full.contains("attributes")) {
            full["attributes"].erase(key);
        }
        io.writePretty("zarr.json", full);
    }

    // attribute file of a zarr v2 / n5 node
    inline std::string attributesName(const bool isZarr) {
        return isZarr ? ".zattrs" : "attributes.json";
    }

}  // namespace attrs_impl


    template<class IO>
    inline void readAttributes(const IO & io, const bool isZarr, nlohmann::json & j) {
        if(isZarr && io.exists("zarr.json")) {
            attrs_impl::readV3Attributes(io, j);
            return;
        }
        // leaves j untouched if there are no attributes
        io.read(attrs_impl::attributesName(isZarr), j);
    }

    template<class IO>
    inline void writeAttributes(const IO & io, const bool isZarr, const nlohmann::json & j) {
        if(isZarr && io.exists("zarr.json")) {
            attrs_impl::writeV3Attributes(io, j);
            return;
        }
        // read the existing attributes (if any), merge in the new ones, write back
        const auto name = attrs_impl::attributesName(isZarr);
        nlohmann::json jOut = nlohmann::json::object();
        io.read(name, jOut);
        for(auto jIt = j.begin(); jIt != j.end(); ++jIt) {
            jOut[jIt.key()] = jIt.value();
        }
        io.write(name, jOut);
    }

    template<class IO>
    inline void removeAttribute(const IO & io, const bool isZarr, const std::string & key) {
        if(isZarr && io.exists("zarr.json")) {
            attrs_impl::removeV3Attribute(io, key);
            return;
        }
        const auto name = attrs_impl::attributesName(isZarr);
        nlohmann::json jOut;
        if(!io.read(name, jOut)) {
            return;
        }
        jOut.erase(key);
        io.write(name, jOut);
    }

    // is the child node 'key' of the group a sub-group (as opposed to a dataset)?
    template<class IO>
    inline bool isSubGroup(const IO & io, const bool isZarr, const std::string & key) {
        if constexpr(IO::hasNodeExists) {
            if(!io.nodeExists(key)) {
                return false;
            }
        }
        if(isZarr) {
            // zarr v3: zarr.json distinguishes group vs array via node_type
            const std::string v3Name = key + "/zarr.json";
            if(io.exists(v3Name)) {
                nlohmann::json j;
                io.read(v3Name, j);
                return j.value("node_type", std::string("group")) == "group";
            }
            // zarr v2: a group has a .zgroup file / object
            return io.exists(key + "/.zgroup");
        } else {
            // n5 has no group markers: everything without the full set of
            // dataset metadata attributes is a group
            nlohmann::json j;
            if(!io.read(key + "/attributes.json", j)) {
                return true;
            }
            return !z5::handle::hasAllN5DatasetAttributes(j);
        }
    }

}
}
