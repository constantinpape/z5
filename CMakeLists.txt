cmake_minimum_required(VERSION 3.1)

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

project(z5)


##############################
# Check and enable C++ 14
##############################

set(CMAKE_CXX_STANDARD 14)
# make sure the compiler supports c++14
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# Not sure if we should set this flag
# set(CMAKE_CXX_EXTENSIONS OFF)

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
option(WITH_XZ ON)
option(WITH_LZ4 ON)

# marray multiarray
option(WITH_MARRAY OFF)

# adaptions to travis build
option(WITHIN_TRAVIS OFF)

# build c++ tests
option(BUILD_TESTS OFF)


###############################
# Include git submodules
###############################

if(BUILD_TESTS)
    # add gtest external project and include the directories
    add_subdirectory(external/googletest/googletest)
    include_directories(${gtest_SOURCE_DIR/include} ${gtest_SOURCE_DIR})
endif()

# include json external library
include_directories(${CMAKE_SOURCE_DIR}/external/json/include)


###############################
# Set up conda env
###############################

# This does not work within travis or appveyor, because
# we set the CMAKE_PREFIX_PATH
# TODO rename this to USE_CONDA_PREFIX or similar
if(NOT WITHIN_TRAVIS)
    # Find the current conda env and set it as CMAKE_PREFIX_PATH
    execute_process(COMMAND bash -c "conda info | grep 'active env location' | awk '{print $5}'"
                    OUTPUT_VARIABLE CMAKE_PREFIX_PATH)

    # older conda versions don't work with the above command
    if ("${CMAKE_PREFIX_PATH}" STREQUAL "")
        execute_process(
            COMMAND bash -c "conda info | grep 'default environment' | awk '{print $4}'"
            OUTPUT_VARIABLE CMAKE_PREFIX_PATH
        )
    endif()

    string(REGEX REPLACE "\n$" "" CMAKE_PREFIX_PATH "${CMAKE_PREFIX_PATH}")
endif()

MESSAGE(STATUS "Setting cmake prefix path to ${CMAKE_PREFIX_PATH}")

# Set CMAKE_PREFIX_PATH to the conda env, but allow changing it
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} CACHE PATH "")

MESSAGE(STATUS "Setting cmake prefix path to ${CMAKE_PREFIX_PATH}")
if (MSVC)
    include_directories("${CMAKE_PREFIX_PATH}/Library/include")
else()
    include_directories("${CMAKE_PREFIX_PATH}/include")
endif()

###############################
# Include system / conda libs
###############################

# find libraries - pthread
find_package(Threads)

# boost
if (MSVC)
    SET(BOOST_ROOT "${CMAKE_PREFIX_PATH}/Library")
    SET(BOOST_LIBRARYDIR "${CMAKE_PREFIX_PATH}/Library/lib")
else()
    SET(BOOST_ROOT "${CMAKE_PREFIX_PATH}")
    SET(BOOST_LIBRARYDIR "${CMAKE_PREFIX_PATH}/lib")
endif()
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

if(WITH_XZ)
    find_package(LibLZMA REQUIRED)
    include_directories(LIBLZMA_INCLUDE_DIRS)
    add_definitions(-DWITH_XZ)
    SET(COMPRESSION_LIBRARIES "${COMPRESSION_LIBRARIES};${LIBLZMA_LIBRARIES}")
endif()

if(WITH_LZ4)
    find_package(LZ4 REQUIRED)
    include_directories(LZ4_INCLUDE_DIR)
    add_definitions(-DWITH_LZ4)
    SET(COMPRESSION_LIBRARIES "${COMPRESSION_LIBRARIES};${LZ4_LIBRARY}")
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
if(MSVC)
    set(PYBIND11_CPP_STANDARD /std:c++14)
else()
    set(PYBIND11_CPP_STANDARD -std=c++14)
endif()
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
