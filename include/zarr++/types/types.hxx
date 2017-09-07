#pragma once

#include <vector>
#include <string>
#include <map>

namespace zarr {
namespace types {

    // TODO implement class that inherits froms std::vector and overloads useful operators (+, -, etc.)
    // TODO rename to coordinate type
    // type for array shapes
    typedef std::vector<size_t> ShapeType;

    // TODO consider alternative:
    // datatypes via X-macros
    // cf. https://stackoverflow.com/questions/147267/easy-way-to-use-variables-of-enum-types-as-string-in-%20%20%20c#202511

    // TODO add bool ?
    // dtype enum and map
    enum Datatypes {
        int8, int16, int32, int64,
        uint8, uint16, uint32, uint64,
        float32, float64
    };

    std::map<std::string, Datatypes> stringToDtype({
        { {"<i1", int8}, {"<i2", int16}, {"<i4", int32}, {"<i8", int64},
          {"<u1", uint8}, {"<u2", uint16}, {"<u4", uint32}, {"<u8", uint64},
          {"<f4", float32}, {"<f8", float64}
        }
    });

    std::string parseDtype(const std::string & dtype) {

        char endian, type, bytes;
        size_t strLen = dtype.size();

        if(strLen == 2) {

            endian = '<';
            type = dtype[0];
            bytes = dtype[1];

        } else if(strLen == 3) {

            endian = dtype[0];
            type = dtype[1];
            bytes = dtype[2];

            if(endian != '<') {
                throw std::runtime_error(
                    "Invalid dtype, Zarr++ only supports little endian"
                );
            }

        } else {

            throw std::runtime_error(
                "Invalid dtype, Zarr++ dtypes must be encoded as 'i8' or '<i8'"
            );

        }

        // TODO is bool supported ?
        if(!(type == 'f' || type == 'u' || type == 'i' /*|| type == 'b'*/)) {
            throw std::runtime_error(
                "Invalid dtype, Zarr++ only supports float, integer and unsigned"
            );
        }

        if(!(bytes == '1' || bytes == '2' || bytes == '4' || bytes == '8')) {
            throw std::runtime_error(
                "Invalid dtype, Zarr++ only supports 1, 2, 4 and 8 bytes"
            );
        }

        std::string ret;
        ret.push_back(endian);
        ret.push_back(type);
        ret.push_back(bytes);
        return ret;
    }


    bool isRealType(const std::string & dtype) {
        return dtype[1] == 'f';
    }

    bool isUnsignedType(const std::string & dtype) {
        return dtype[1] == 'u';
    }

} // namespace::types
} // namespace::zarr
