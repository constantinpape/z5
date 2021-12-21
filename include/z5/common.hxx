#pragma once

#include <fstream>

// helpful summary of compiler versions
// https://blog.kowalczyk.info/article/j/guide-to-predefined-macros-in-c-compilers-gcc-clang-msvc-etc..html

// include boost::filesystem or std::filesystem header
// and define the namespace fs
#ifdef WITH_BOOST_FS
    #ifndef BOOST_FILESYSTEM_NO_DEPRECATED
        #define BOOST_FILESYSTEM_NO_DEPRECATED
    #endif
    #include <boost/filesystem.hpp>
    #include <boost/filesystem/fstream.hpp>
    namespace fs = boost::filesystem;

    // relative path in boost filesystem behaves unexpectedly, so we use the
    // same implementation as below
	inline fs::path relativeImpl(const fs::path & from, const fs::path & to) {
	   fs::path::const_iterator fromIter = from.begin();
	   fs::path::const_iterator toIter = to.begin();

	   while (fromIter != from.end() && toIter != to.end() && (*toIter) == (*fromIter)){
	      ++toIter;
	      ++fromIter;
	   }

	   fs::path finalPath;
	   while (fromIter != from.end()){
	      finalPath /= "..";
	      ++fromIter;
	   }

	   while (toIter != to.end()){
	      finalPath /= *toIter;
	      ++toIter;
	   }

	   return finalPath;
	}
#else
    // macos behaves very weird here, I can't get it to build on
    // osx < 10.15 right now. For now the workaround is to use boost filesystem ...
    // TODO MSVC check ?
    #if (defined(__GNUC__) && (__GNUC__ > 7)) || defined(__clang__) || (defined(_MSC_VER) && _MSC_VER > 1900)
        #include <filesystem>
        namespace fs = std::filesystem;

        // need to be consistent with the other implementations
        inline fs::path relativeImpl(const fs::path & from, const fs::path & to){
            return fs::relative(to, from);
        }

    #else
        #include <experimental/filesystem>
        namespace fs = std::experimental::filesystem;

        // experimental::filesystem does not have relative yet, so
        // we re-implement it following:
        // https://stackoverflow.com/questions/10167382/boostfilesystem-get-relative-path/37715252#37715252
		inline fs::path relativeImpl(const fs::path & from, const fs::path & to){
		   // Start at the root path and while they are the same then do nothing then when they first
		   // diverge take the entire from path, swap it with '..' segments, and then append the remainder of the to path.
		   fs::path::const_iterator fromIter = from.begin();
		   fs::path::const_iterator toIter = to.begin();

		   // Loop through both while they are the same to find nearest common directory
		   while (fromIter != from.end() && toIter != to.end() && (*toIter) == (*fromIter)){
		      ++toIter;
		      ++fromIter;
		   }

		   // Replace from path segments with '..' (from => nearest common directory)
		   fs::path finalPath;
		   while (fromIter != from.end()){
		      finalPath /= "..";
		      ++fromIter;
		   }

		   // Append the remainder of the to path (nearest common directory => to)
		   while (toIter != to.end()){
		      finalPath /= *toIter;
		      ++toIter;
		   }

		   return finalPath;
		}
    #endif
#endif

namespace z5 {

    // NOTE we can't just define variables, because that would not work as header-only include
    //
    // get booleans for preprocessor flags
    //

    inline std::map<std::string, bool> getAvailableCodecs() {
        std::map<std::string, bool> flags;

        flags["raw"] = true;

        flags["blosc"] = false;
        #ifdef WITH_BLOSC
        flags["blosc"] = true;
        #endif

        flags["bzip2"] = false;
        #ifdef WITH_BZIP2
        flags["bzip2"] = true;
        #endif

        flags["lz4"] = false;
        #ifdef WITH_LZ4
        flags["lz4"] = true;
        #endif

        flags["xz"] = false;
        #ifdef WITH_XZ
        flags["xz"] = true;
        #endif

        flags["zlib"] = false;
        flags["gzip"] = false;
        #ifdef WITH_ZLIB
        flags["zlib"] = true;
        flags["gzip"] = true;
        #endif

        return flags;
    }


    inline std::map<std::string, bool> getAvailableBackends() {
        std::map<std::string, bool> flags;

        flags["filesystem"] = true;

        flags["gcs"] = false;
        #ifdef WITH_GCS
        flags["gcs"] = true;
        #endif
        flags["s3"] = false;
        #ifdef WITH_S3
        flags["s3"] = true;
        #endif

        return flags;
    }
}
