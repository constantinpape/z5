#pragma once

#include <string>

#include "z5/types/types.hxx"

namespace z5 {
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


    // FIXME this is relatively ugly...
    // would be nicer to do this in a dimension independent way
    void makeRegularGrid(const types::ShapeType & minCoords, const types::ShapeType & maxCoords, std::vector<types::ShapeType> & grid) {
        size_t nDim = minCoords.size();
        if(nDim == 1) {
            for(size_t x = minCoords[0]; x <= maxCoords[0]; ++x) {
                grid.emplace_back(types::ShapeType({x}));
            }
        }

        else if(nDim == 2) {
            for(size_t x = minCoords[0]; x <= maxCoords[0]; ++x) {
                for(size_t y = minCoords[1]; y <= maxCoords[1]; ++y) {
                    grid.emplace_back(types::ShapeType({x, y}));
                }
            }
        }

        else if(nDim == 3) {
            for(size_t x = minCoords[0]; x <= maxCoords[0]; ++x) {
                for(size_t y = minCoords[1]; y <= maxCoords[1]; ++y) {
                    for(size_t z = minCoords[2]; z <= maxCoords[2]; ++z) {
                        grid.emplace_back(types::ShapeType({x, y, z}));
                    }
                }
            }
        }

        else if(nDim == 4) {
            for(size_t x = minCoords[0]; x <= maxCoords[0]; ++x) {
                for(size_t y = minCoords[1]; y <= maxCoords[1]; ++y) {
                    for(size_t z = minCoords[2]; z <= maxCoords[2]; ++z) {
                        for(size_t t = minCoords[3]; t <= maxCoords[3]; ++t) {
                            grid.emplace_back(types::ShapeType({x, y, z, t}));
                        }
                    }
                }
            }
        }

        else if(nDim == 5) {
            for(size_t x = minCoords[0]; x <= maxCoords[0]; ++x) {
                for(size_t y = minCoords[1]; y <= maxCoords[1]; ++y) {
                    for(size_t z = minCoords[2]; z <= maxCoords[2]; ++z) {
                        for(size_t t = minCoords[3]; t <= maxCoords[3]; ++t) {
                            for(size_t c = minCoords[4]; t <= maxCoords[4]; ++t) {
                                grid.emplace_back(types::ShapeType({x, y, z, t, c}));
                            }
                        }
                    }
                }
            }
        }

        else {
            throw std::runtime_error("More than 5D currently not supported");
        }
    }


    // reverse endianness for all values in the vector
    // boost endian would be nice, but it doesn't support floats...
    template<typename T>
    inline void reverseEndiannessInplace(std::vector<T> & values) {
        int typeLen = sizeof(T);
        int typeMax = typeLen - 1;
        T ret;
        char * valP;
        char * retP = (char *) & ret;
        for(auto & val : values) {
            valP = (char*) &val;
            for(int ii = 0; ii < typeLen; ++ii) {
                retP[ii] = valP[typeMax - ii];
            }
            val = ret;
        }
    }

    // reverse endianness for as single value
    template<typename T>
    inline void reverseEndiannessInplace(T & val) {
        int typeLen = sizeof(T);
        int typeMax = typeLen - 1;
        T ret;
        char * valP = (char *) &val;
        char * retP = (char *) &ret;
        for(int ii = 0; ii < typeLen; ++ii) {
            retP[ii] = valP[typeMax - ii];
        }
        val = ret;
    }

    // reverse endianness for all values in the vector
    // boost endian would be nice, but it doesn't support floats...
    //template<typename T>
    //inline void reverseEndianness(const std::vector<T> & values, std::vector<T> valuesOut) {
    //    int typeLen = sizeof(T);
    //    int typeMax = typeLen - 1;
    //    char * valP;
    //    char * retP;
    //    for(size_t i = 0; i < values.size(); ++i) {
    //        valP = (char*) values[i];
    //        retP = (char*) valuesOut[i];
    //        for(int ii = 0; ii < typeLen; ++ii) {
    //            retP[ii] = valP[typeMax - ii];
    //        }
    //    }
    //}
}
}
