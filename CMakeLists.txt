cmake_minimum_required(VERSION 3.1)

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

project(z5)


##############################
# Check and enable C++ 14
##############################

include(CheckCXXCompilerFlag)
# make sure the compiler supports c++14
# FIXME I think this won't work for MSVC
CHECK_CXX_COMPILER_FLAG("-std=c++14" HAS_CPP14_FLAG)
if(NOT HAS_CPP14_FLAG)
    message(FATAL_ERROR "Unsupported compiler -- z5 requires C++14 support!")
endif()
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")


# set default build type
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to 'Release' as none was specified.")
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
        "MinSizeRel" "RelWithDebInfo")
endif()
string(TOUPPER "${CMAKE_BUILD_TYPE}" U_CMAKE_BUILD_TYPE)

# Old ABI to be compatible with boost
add_definitions(-D_GLIBCXX_USE_CXX11_ABI=0)


##############################
# Build options
##############################
# Compression Libraries
option(WITH_BLOSC ON)
option(WITH_ZLIB ON)
option(WITH_BZIP2 ON)
# marray multiarray
option(WITH_MARRAY OFF)

# adaptions to travis build
option(WITHIN_TRAVIS OFF)


###############################
# Include git submodules
###############################

# add gtest external project and include the directories
add_subdirectory(external/googletest/googletest)
include_directories(${gtest_SOURCE_DIR/include} ${gtest_SOURCE_DIR})

# include json external library
include_directories(${CMAKE_SOURCE_DIR}/external/json/include)


###############################
# Set up conda env
###############################

# FIXME this is not consistent between conda versions ;(
# This does not work within travis for some reason
if(NOT WITHIN_TRAVIS)
    # Find the current conda env and set it as CMAKE_PREFIX_PATH
    execute_process(
        COMMAND bash -c "conda info | grep 'active env location' | awk '{print $5}'"
        OUTPUT_VARIABLE CMAKE_PREFIX_PATH
    )
    string(REGEX REPLACE "\n$" "" CMAKE_PREFIX_PATH "${CMAKE_PREFIX_PATH}")
endif()

# Set CMAKE_PREFIX_PATH to the conda env, but allow changing it
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} CACHE PATH "")

MESSAGE(STATUS "Setting cmake prefix path to ${CMAKE_PREFIX_PATH}")
include_directories("${CMAKE_PREFIX_PATH}/include")


###############################
# Include system / conda libs
###############################

# TODO I don't really see why we need libthreads here
# find libraries - pthread
find_package(Threads)
# TODO do we need RT ?!

# boost
SET(BOOST_ROOT "${CMAKE_PREFIX_PATH}")
SET(BOOST_LIBRARYDIR "${CMAKE_PREFIX_PATH}/lib")
SET(Boost_NO_SYSTEM_PATHS ON)
find_package(Boost 1.63.0 COMPONENTS system filesystem REQUIRED)
include_directories(${Boost_INCLUDE_DIR})


###############################
# Include compression libs
###############################

SET(COMPRESSION_LIBRARIES "")

if(WITH_BLOSC)
    find_package(BLOSC REQUIRED)
    include_directories(${BLOSC_INCLUDE_DIR})
    add_definitions(-DWITH_BLOSC)
    SET(COMPRESSION_LIBRARIES "${COMPRESSION_LIBRARIES};${BLOSC_LIBRARIES}")
endif()

if(WITH_ZLIB)
    find_package(ZLIB REQUIRED)
    include_directories(ZLIB_INCLUDE_DIRS)
    add_definitions(-DWITH_ZLIB)
    SET(COMPRESSION_LIBRARIES "${COMPRESSION_LIBRARIES};${ZLIB_LIBRARIES}")
endif()

if(WITH_BZIP2)
    find_package(BZip2 REQUIRED)
    include_directories(BZIP2_INCLUDE_DIRS)
    add_definitions(-DWITH_BZIP2)
    SET(COMPRESSION_LIBRARIES "${COMPRESSION_LIBRARIES};${BZIP2_LIBRARIES}")
endif()


###############################
# Include marray (optional)
###############################

if(WITH_MARRAY)
    set(MARRAY_INCLUDE_DIR ${CMAKE_PREFIX_PATH} CACHE PATH "")
    add_definitions(-DWITH_MARRAY)
    include_directories(${MARRAY_INCLUDE_DIR})
endif()


###############################
# Python-bindings
###############################

list(APPEND CMAKE_MODULE_PATH "${CMAKE_PREFIX_PATH}/share/cmake/pybind11")
include(pybind11Tools)

# numpy
set(PYBIND11_CPP_STANDARD -std=c++14)
find_package(NumPy REQUIRED)
include_directories(${NUMPY_INCLUDE_DIRS})
message(STATUS "Using numpy from ${NUMPY_INCLUDE_DIRS}")


###############################
# Set-up and install
###############################

# find global headers
file(GLOB_RECURSE headers include/*.hxx)
file(GLOB_RECURSE headers ${CMAKE_INSTALL_PREFIX}/include/*.hxx)
include_directories(include)

# add subdirectories
add_subdirectory(src)

# install the headers
install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/include/z5"
  DESTINATION include
  FILES_MATCHING
  PATTERN "*.hxx"
  PATTERN "*.hpp"
  PATTERN "*.h"
)
