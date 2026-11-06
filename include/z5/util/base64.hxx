#pragma once

#include <stdexcept>
#include <string>

namespace z5 {
namespace util {

    // Standard base64 (RFC 4648, with padding), as zarr-python uses for bytes
    // fill values in the metadata.

    inline std::string base64Encode(const std::string & in) {
        static const char * alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        std::string out;
        out.reserve(4 * ((in.size() + 2) / 3));
        std::size_t i = 0;
        for(; i + 2 < in.size(); i += 3) {
            const unsigned v = (static_cast<unsigned char>(in[i]) << 16) |
                               (static_cast<unsigned char>(in[i + 1]) << 8) |
                               static_cast<unsigned char>(in[i + 2]);
            out += alphabet[(v >> 18) & 63];
            out += alphabet[(v >> 12) & 63];
            out += alphabet[(v >> 6) & 63];
            out += alphabet[v & 63];
        }
        const std::size_t rest = in.size() - i;
        if(rest > 0) {
            unsigned v = static_cast<unsigned char>(in[i]) << 16;
            if(rest == 2) {
                v |= static_cast<unsigned char>(in[i + 1]) << 8;
            }
            out += alphabet[(v >> 18) & 63];
            out += alphabet[(v >> 12) & 63];
            out += rest == 2 ? alphabet[(v >> 6) & 63] : '=';
            out += '=';
        }
        return out;
    }

    // strict decoding: the length must be a multiple of 4, with at most two
    // padding characters at the end
    inline std::string base64Decode(const std::string & in) {
        auto value = [](const char c) -> int {
            if(c >= 'A' && c <= 'Z') return c - 'A';
            if(c >= 'a' && c <= 'z') return c - 'a' + 26;
            if(c >= '0' && c <= '9') return c - '0' + 52;
            if(c == '+') return 62;
            if(c == '/') return 63;
            return -1;
        };
        if(in.size() % 4 != 0) {
            throw std::runtime_error("z5: invalid base64 string: length is not a multiple of 4");
        }
        std::string out;
        out.reserve(3 * (in.size() / 4));
        for(std::size_t i = 0; i < in.size(); i += 4) {
            const bool last = i + 4 == in.size();
            const int pad = last ? (in[i + 3] == '=') + (in[i + 2] == '=') : 0;
            if(pad == 1 && in[i + 2] == '=') {
                throw std::runtime_error("z5: invalid base64 string: bad padding");
            }
            unsigned v = 0;
            for(int k = 0; k < 4; ++k) {
                const int d = k >= 4 - pad ? 0 : value(in[i + k]);
                if(d < 0) {
                    throw std::runtime_error("z5: invalid base64 string: unexpected character");
                }
                v = (v << 6) | static_cast<unsigned>(d);
            }
            out += static_cast<char>((v >> 16) & 0xFF);
            if(pad < 2) {
                out += static_cast<char>((v >> 8) & 0xFF);
            }
            if(pad < 1) {
                out += static_cast<char>(v & 0xFF);
            }
        }
        return out;
    }

}
}
