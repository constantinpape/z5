cmake_minimum_required(VERSION 3.1)

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

project(z5)


##############################
# C++ Standard
##############################

# check whether we have c++ 17

message(STATUS "CXX_FLAGS: ${CMAKE_CXX_FLAGS}")
set(CPP17 "${CMAKE_CXX_FLAGS}" MATCHES "-std=c\\+\\+17" CACHE INTERNAL "")
if (CPP17)
    message(STATUS "Using c++ 17")
    set(CMAKE_CXX_STANDARD 17)
else()
    message(STATUS "Using c++ 14")
    set(CMAKE_CXX_STANDARD 14)
endif()

# make sure the compiler supports the requested c++ standard
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# don't use gnu extensions
set(CMAKE_CXX_EXTENSIONS OFF)

# set default build type
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to 'Release' as none was specified.")
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
        "MinSizeRel" "RelWithDebInfo")
endif()
string(TOUPPER "${CMAKE_BUILD_TYPE}" U_CMAKE_BUILD_TYPE)

# NOTE whether we need to enable the old ABI depends
# on which ABI boost is compiled with.
# If you get boost linker errors try to enable / disable the old ABI
option(USE_OLD_ABI OFF)
if(USE_OLD_ABI)
    message(STATUS "Using old GCC ABI")
    add_definitions(-D_GLIBCXX_USE_CXX11_ABI=0)
endif()

if(MSVC)
    add_definitions(/DNOMINMAX)
endif()


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
# Include gtest submodule
###############################

# NOTE I tried to replace thhis with the conda package and
# use find_pacakge(GTest), but although CMake could find the
# libraries, this led to horrible linker errors
if(BUILD_TESTS)
    # add gtest external project and include the directories
    add_subdirectory(external/googletest/googletest)
    include_directories(${gtest_SOURCE_DIR/include} ${gtest_SOURCE_DIR})
endif()


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


###############################
# Boost
###############################

# If WITH_BOOST_FS is set, we always use the boost filesystem library
# otherwise, we check if we have c++17 support. If so, we use std::filesystem,
# otherwise we fall back to boost filesystem.
if(CPP17)
    SET(WITH_BOOST_FS FALSE CACHE BOOL "")
else()
    SET(WITH_BOOST_FS TRUE CACHE BOOL "")
endif()

if (MSVC)
    SET(BOOST_ROOT "${CMAKE_PREFIX_PATH}/Library")
    SET(BOOST_LIBRARYDIR "${CMAKE_PREFIX_PATH}/Library/lib")
else()
    SET(BOOST_ROOT "${CMAKE_PREFIX_PATH}")
    SET(BOOST_LIBRARYDIR "${CMAKE_PREFIX_PATH}/lib")
endif()
SET(Boost_NO_SYSTEM_PATHS ON)

if(WITH_BOOST_FS)
    message(STATUS "With boost filesystem")
    find_package(Boost 1.63.0 COMPONENTS system filesystem REQUIRED)
    add_definitions(-DWITH_BOOST_FS)
    SET(FILESYSTEM_LIBRARIES "${Boost_FILESYSTEM_LIBRARY};${Boost_SYSTEM_LIBRARY}")
else()
    message(STATUS "With std filesystem")
    find_package(Boost 1.63.0 REQUIRED)

    # see this issue for discussions about the filesystem lib in CMake
    # https://gitlab.kitware.com/cmake/cmake/issues/17834
    if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        SET(FILESYSTEM_LIBRARIES "stdc++fs")
        SET(FILESYSTEM_LIBRARIES "")
    endif()

    # on clang, we need to enable libc++experimental, see
    # https://stackoverflow.com/a/45332844
    if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        set(LIBCXX_ENABLE_EXPERIMENTAL_LIBRARY ON)
    endif()
endif()
include_directories(${Boost_INCLUDE_DIR})


###################
# xtensor libraries
###################

find_package(xtensor REQUIRED)
include_directories(xtensor_INCLUDE_DIRS)

find_package(xtensor-python REQUIRED)
include_directories(xtensor-python_INCLUDE_DIRS)

find_package(xsimd)
if(xsimd_FOUND)
    message(STATUS "Found xsimd, compiling with XTENSOR_USE_XSIMD")
    include_directories(xsimd_INCLUDE_DIRS)
    add_definitions(-DXTENSOR_USE_XSIMD)
else()
    message(STATUS "xsimd not found, compiling without vector instructions")
endif()


###########################
# nlohmann_json library
###########################
find_package(nlohmann_json REQUIRED)
include_directories(${nlohmann_json_INCLUDE_DIRS})


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

find_package(pybind11 REQUIRED)
if (CPP17)
    message(STATUS "Using c++ 17 for pybind")
    if(MSVC)
        set(PYBIND11_CPP_STANDARD /std:c++17)
    else()
        set(PYBIND11_CPP_STANDARD -std=c++17)
    endif()
else()
    message(STATUS "Using c++ 14 for pybind")
    if(MSVC)
        set(PYBIND11_CPP_STANDARD /std:c++14)
    else()
        set(PYBIND11_CPP_STANDARD -std=c++14)
    endif()
endif()

# numpy
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
  DESTINATION "${CMAKE_INSTALL_PREFIX}/include"
  FILES_MATCHING
  PATTERN "*.hxx"
  PATTERN "*.hpp"
  PATTERN "*.h"
)
