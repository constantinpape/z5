#pragma once

#include <string>

#include "z5/types/types.hxx"

namespace z5 {
namespace util {

    template<typename ITER>
    inline void join(const ITER & begin, const ITER & end, std::string & out, const std::string & delimiter) {
        for(ITER it = begin; it != end; ++it) {
            if(!out.empty()) {
                out.append(delimiter);
            }
            out.append(std::to_string(*it));
        }
    }


    // FIXME this is relatively ugly...
    // can use imglib trick !!!
    // would be nicer to do this in a dimension independent way
    inline void makeRegularGrid(const types::ShapeType & minCoords, const types::ShapeType & maxCoords, std::vector<types::ShapeType> & grid) {
        std::size_t nDim = minCoords.size();
        if(nDim == 1) {
            for(std::size_t x = minCoords[0]; x <= maxCoords[0]; ++x) {
                grid.emplace_back(types::ShapeType({x}));
            }
        }

        else if(nDim == 2) {
            for(std::size_t x = minCoords[0]; x <= maxCoords[0]; ++x) {
                for(std::size_t y = minCoords[1]; y <= maxCoords[1]; ++y) {
                    grid.emplace_back(types::ShapeType({x, y}));
                }
            }
        }

        else if(nDim == 3) {
            for(std::size_t x = minCoords[0]; x <= maxCoords[0]; ++x) {
                for(std::size_t y = minCoords[1]; y <= maxCoords[1]; ++y) {
                    for(std::size_t z = minCoords[2]; z <= maxCoords[2]; ++z) {
                        grid.emplace_back(types::ShapeType({x, y, z}));
                    }
                }
            }
        }

        else if(nDim == 4) {
            for(std::size_t x = minCoords[0]; x <= maxCoords[0]; ++x) {
                for(std::size_t y = minCoords[1]; y <= maxCoords[1]; ++y) {
                    for(std::size_t z = minCoords[2]; z <= maxCoords[2]; ++z) {
                        for(std::size_t t = minCoords[3]; t <= maxCoords[3]; ++t) {
                            grid.emplace_back(types::ShapeType({x, y, z, t}));
                        }
                    }
                }
            }
        }

        else if(nDim == 5) {
            for(std::size_t x = minCoords[0]; x <= maxCoords[0]; ++x) {
                for(std::size_t y = minCoords[1]; y <= maxCoords[1]; ++y) {
                    for(std::size_t z = minCoords[2]; z <= maxCoords[2]; ++z) {
                        for(std::size_t t = minCoords[3]; t <= maxCoords[3]; ++t) {
                            for(std::size_t c = minCoords[4]; c <= maxCoords[4]; ++t) {
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


    // TODO in the long run this should be implemented as a filter for iostreams
    // reverse endianness for all values in the iterator range
    // boost endian would be nice, but it doesn't support floats...
    template<typename T, typename ITER>
    inline void reverseEndiannessInplace(ITER begin, ITER end) {
        int typeLen = sizeof(T);
        int typeMax = typeLen - 1;
        T ret;
        char * valP;
        char * retP = (char *) & ret;
        for(ITER it = begin; it != end; ++it) {
            valP = (char*) &(*it);
            for(int ii = 0; ii < typeLen; ++ii) {
                retP[ii] = valP[typeMax - ii];
            }
            *it = ret;
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
}
}
