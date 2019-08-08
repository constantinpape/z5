#pragma once

#include <fstream>

// include boost::filesystem or std::filesystem header
// and define the namespace fs
#ifdef WITH_BOOST_FS
    #ifndef BOOST_FILESYSTEM_NO_DEPERECATED
        #define BOOST_FILESYSTEM_NO_DEPERECATED
    #endif
    #include <boost/filesystem.hpp>
    #include <boost/filesystem/fstream.hpp>
    namespace fs = boost::filesystem;
#else
    #if __GCC__ > 7
        #include <filesystem>
        namespace fs = std::filesystem;
    #else
        #include <experimental/filesystem>
        namespace fs = std::experimental::filesystem;
    #endif
#endif


namespace z5 {
}
