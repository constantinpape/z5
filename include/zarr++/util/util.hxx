#pragma once

#include <string>

namespace zarr {
namespace util {

    template<typename ITER>
    void join(const ITER & begin, const ITER & end, std::string & out, const std::string & delimiter) {
        for(ITER it = begin; it != end; ++it) {
            if(!out.empty()) {
                out.append(delimiter);
            }
            out.append(std::to_string(*it));
        }
    }


    // FIXME types are ugly hardcoded...
    void split(const std::string & in, const std::string & delimiter, std::vector<size_t> & out) {
        auto start = 0U;
        auto end = in.find(delimiter);
        while(end != std::string::npos) {
            out.push_back(
                static_cast<size_t>( std::stoul(in.substr(start, end - start)) )
            );
        }
        out.push_back(
            static_cast<size_t>( std::stoul(in.substr(start, end - start)) )
        );
    }

}
}
