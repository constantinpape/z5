cmake_minimum_required(VERSION 3.1)

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

project(zarr++)

include(CheckCXXCompilerFlag)


# make sure the compiler supports c++11
# FIXME I think this won't work for MSVC
CHECK_CXX_COMPILER_FLAG("-std=c++11" HAS_CPP11_FLAG)
if(NOT HAS_CPP11_FLAG)
    message(FATAL_ERROR "Unsupported compiler -- zarr++ requires C++11 support!")
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

option(BUILD_Z5_PYTHON OFF)

option(WITH_BLOSC ON)
option(WITH_ZLIB ON)
option(WITH_BZIP2 ON)


# find libraries - pthread
find_package(Threads)
# TODO do we need RT ?!


# find libraries - boost
find_package(Boost 1.63.0 COMPONENTS system filesystem REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})
MESSAGE(STATUS ${Boost_INCLUDE_DIRS})
include_directories(${Boost_INCLUDE_DIR})
MESSAGE(STATUS ${Boost_LIBRARIES})


SET(COMPRESSION_LIBRARIES "")

# find libraries - blosc
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


# find global headers
file(GLOB_RECURSE headers include/*.hxx)
file(GLOB_RECURSE headers ${CMAKE_INSTALL_PREFIX}/include/*.hxx)
include_directories(include)


# add gtest external project and include the directories
add_subdirectory(external/googletest/googletest)
include_directories(${gtest_SOURCE_DIR/include} ${gtest_SOURCE_DIR})


# include json external library
include_directories(${CMAKE_SOURCE_DIR}/external/json/src)

# include marray
include_directories(${CMAKE_SOURCE_DIR}/external/marray/include)

# add pybind
if(BUILD_Z5_PYTHON)
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/external/pybind11/include)
    add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/external/pybind11)
endif()

# FIXME xtensor-python only works if pybind11 is included, so we need 
# to make these includes depend on each other
# include xtensor
include_directories(${CMAKE_SOURCE_DIR}/external/xtensor/include)
include_directories(${CMAKE_SOURCE_DIR}/external/xtensor-python/include)
include_directories(${CMAKE_SOURCE_DIR}/external/xtl/include)

# add subdirectories
add_subdirectory(src)

# install the headers
install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/include/zarr++"
  DESTINATION include
  FILES_MATCHING
  PATTERN "*.hxx"
  PATTERN "*.hpp"
  PATTERN "*.h"
)
