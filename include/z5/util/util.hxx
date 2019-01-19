#pragma once

#include <string>

#include "z5/types/types.hxx"

#include <cstdio>  // declaration of ::fileno
#include <fstream>  // for basic_filebuf template
#include <cerrno>

#if defined(__GLIBCXX__) || (defined(__GLIBCPP__) && __GLIBCPP__>=20020514)  // GCC >= 3.1.0
# include <ext/stdio_filebuf.h>
#endif
#if defined(__GLIBCXX__) // GCC >= 3.4.0
# include <ext/stdio_sync_filebuf.h>
#endif


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


    // make regular grid betwee `minCoords` and `maxCoords` with step size 1.
    // uses imglib trick for ND code.
    inline void makeRegularGrid(const types::ShapeType & minCoords,
                                const types::ShapeType & maxCoords,
                                std::vector<types::ShapeType> & grid) {
        // get the number of dims and initialize the positions
        // at the min coordinates
        const int nDim = minCoords.size();
        types::ShapeType positions = minCoords;

        // start iteration in highest dimension
        for(int d = nDim - 1; d >= 0;) {
            // write out the coordinates
            grid.emplace_back(positions);
            // decrease the dimension
            for(d = nDim - 1; d >= 0; --d) {
                // increase position in the given dimension
                ++positions[d];
                // continue in this dimension if we have not reached
                // the max coord yet, otherwise reset the position to
                // the minimum coord and go to next lower dimension
                if(positions[d] <= maxCoords[d]) {
                    break;
                } else {
                    positions[d] = minCoords[d];
                }
            }
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

    // copied from:
    // https://ginac.de/~kreckel/fileno/

	//! Similar to fileno(3), but taking a C++ stream as argument instead of a
	//! FILE*.  Note that there is no way for the library to track what you do with
	//! the descriptor, so be careful.
	//! \return  The integer file descriptor associated with the stream, or -1 if
	//!   that stream is invalid.  In the latter case, for the sake of keeping the
	//!   code as similar to fileno(3), errno is set to EBADF.
	//! \see  The <A HREF="https://www.ginac.de/~kreckel/fileno/">upstream page at
	//!   https://www.ginac.de/~kreckel/fileno/</A> of this code provides more
	//!   detailed information.
	template <typename charT, typename traits>
	inline int
	fileno_hack(const std::basic_ios<charT, traits>& stream)
	{
	    // Some C++ runtime libraries shipped with ancient GCC, Sun Pro,
	    // Sun WS/Forte 5/6, Compaq C++ supported non-standard file descriptor
	    // access basic_filebuf<>::fd().  Alas, starting from GCC 3.1, the GNU C++
	    // runtime removes all non-standard std::filebuf methods and provides an
	    // extension template class __gnu_cxx::stdio_filebuf on all systems where
	    // that appears to make sense (i.e. at least all Unix systems).  Starting
	    // from GCC 3.4, there is an __gnu_cxx::stdio_sync_filebuf, in addition.
	    // Sorry, darling, I must get brutal to fetch the darn file descriptor!
	    // Please complain to your compiler/libstdc++ vendor...
	#if defined(__GLIBCXX__) || defined(__GLIBCPP__)
	    // OK, stop reading here, because it's getting obscene.  Cross fingers!
	# if defined(__GLIBCXX__)  // >= GCC 3.4.0
	    // This applies to cin, cout and cerr when not synced with stdio:
	    typedef __gnu_cxx::stdio_filebuf<charT, traits> unix_filebuf_t;
	    unix_filebuf_t* fbuf = dynamic_cast<unix_filebuf_t*>(stream.rdbuf());
	    if (fbuf != NULL) {
	        return fbuf->fd();
	    }

	    // This applies to filestreams:
	    typedef std::basic_filebuf<charT, traits> filebuf_t;
	    filebuf_t* bbuf = dynamic_cast<filebuf_t*>(stream.rdbuf());
	    if (bbuf != NULL) {
	        // This subclass is only there for accessing the FILE*.  Ouuwww, sucks!
	        struct my_filebuf : public std::basic_filebuf<charT, traits> {
	            int fd() { return this->_M_file.fd(); }
	        };
	        return static_cast<my_filebuf*>(bbuf)->fd();
	    }

	    // This applies to cin, cout and cerr when synced with stdio:
	    typedef __gnu_cxx::stdio_sync_filebuf<charT, traits> sync_filebuf_t;
	    sync_filebuf_t* sbuf = dynamic_cast<sync_filebuf_t*>(stream.rdbuf());
	    if (sbuf != NULL) {
	#  if (__GLIBCXX__<20040906) // GCC < 3.4.2
	        // This subclass is only there for accessing the FILE*.
	        // See GCC PR#14600 and PR#16411.
	        struct my_filebuf : public sync_filebuf_t {
	            my_filebuf();  // Dummy ctor keeps the compiler happy.
	            // Note: stdio_sync_filebuf has a FILE* as its first (but private)
	            // member variable.  However, it is derived from basic_streambuf<>
	            // and the FILE* is the first non-inherited member variable.
	            FILE* c_file() {
	                return *(FILE**)((char*)this + sizeof(std::basic_streambuf<charT, traits>));
	            }
	        };
	        return ::fileno(static_cast<my_filebuf*>(sbuf)->c_file());
	#  else
	        return ::fileno(sbuf->file());
	#  endif
	    }
	# else  // GCC < 3.4.0 used __GLIBCPP__
	#  if (__GLIBCPP__>=20020514)  // GCC >= 3.1.0
	    // This applies to cin, cout and cerr:
	    typedef __gnu_cxx::stdio_filebuf<charT, traits> unix_filebuf_t;
	    unix_filebuf_t* buf = dynamic_cast<unix_filebuf_t*>(stream.rdbuf());
	    if (buf != NULL) {
	        return buf->fd();
	    }

	    // This applies to filestreams:
	    typedef std::basic_filebuf<charT, traits> filebuf_t;
	    filebuf_t* bbuf = dynamic_cast<filebuf_t*>(stream.rdbuf());
	    if (bbuf != NULL) {
	        // This subclass is only there for accessing the FILE*.  Ouuwww, sucks!
	        struct my_filebuf : public std::basic_filebuf<charT, traits> {
	            // Note: _M_file is of type __basic_file<char> which has a
	            // FILE* as its first (but private) member variable.
	            FILE* c_file() { return *(FILE**)(&this->_M_file); }
	        };
	        FILE* c_file = static_cast<my_filebuf*>(bbuf)->c_file();
	        if (c_file != NULL) {  // Could be NULL for failed ifstreams.
	            return ::fileno(c_file);
	        }
	    }
	#  else  // GCC 3.0.x
	    typedef std::basic_filebuf<charT, traits> filebuf_t;
	    filebuf_t* fbuf = dynamic_cast<filebuf_t*>(stream.rdbuf());
	    if (fbuf != NULL) {
	        struct my_filebuf : public filebuf_t {
	            // Note: basic_filebuf<charT, traits> has a __basic_file<charT>* as
	            // its first (but private) member variable.  Since it is derived
	            // from basic_streambuf<charT, traits> we can guess its offset.
	            // __basic_file<charT> in turn has a FILE* as its first (but
	            // private) member variable.  Get it by brute force.  Oh, geez!
	            FILE* c_file() {
	                std::__basic_file<charT>* ptr_M_file = *(std::__basic_file<charT>**)((char*)this + sizeof(std::basic_streambuf<charT, traits>));
	#  if _GLIBCPP_BASIC_FILE_INHERITANCE
	                // __basic_file<charT> inherits from __basic_file_base<charT>
	                return *(FILE**)((char*)ptr_M_file + sizeof(std::__basic_file_base<charT>));
	#  else
	                // __basic_file<charT> is base class, but with vptr.
	                return *(FILE**)((char*)ptr_M_file + sizeof(void*));
	#  endif
	            }
	        };
	        return ::fileno(static_cast<my_filebuf*>(fbuf)->c_file());
	    }
	#  endif
	# endif
	#else
	#  error "Does anybody know how to fetch the bloody file descriptor?"
	    return stream.rdbuf()->fd();  // Maybe a good start?
	#endif
	    errno = EBADF;
	    return -1;
	}

	//! 8-Bit character instantiation: fileno(ios).
	// template <>
	inline int fileno(const std::ios& stream)
	{
	    return fileno_hack(stream);
	}

    /*
	#if !(defined(__GLIBCXX__) || defined(__GLIBCPP__)) || (defined(_GLIBCPP_USE_WCHAR_T) || defined(_GLIBCXX_USE_WCHAR_T))
	//! Wide character instantiation: fileno(wios).
	template <>
	int fileno<wchar_t>(const std::wios& stream)
	{
	    return fileno_hack(stream);
	}
	#endif
    */

}
}
