cmake_minimum_required(VERSION 3.1)

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

project(z5)

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

# TODO make marray optional and remove it from external
# include marray
include_directories(${CMAKE_SOURCE_DIR}/external/marray/include)


#######################
# Setup python bindings
#######################

#  
# Find the current conda env and add it's include path
execute_process(
    COMMAND bash -c "conda info | grep 'default environment' | awk '{print $4}'"
    OUTPUT_VARIABLE CMAKE_PREFIX_PATH
)
string(REGEX REPLACE "\n$" "" CMAKE_PREFIX_PATH "${CMAKE_PREFIX_PATH}")

# FIXME this is stuarts version, which doesn't work for me
# (cmake version ?!)
#set(CMAKE_PREFIX_PATH $ENV{CONDA_PREFIX} CACHE PATH "")
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} CACHE PATH "")

MESSAGE(STATUS "Setting cmake prefix path to ${CMAKE_PREFIX_PATH}")
list(APPEND CMAKE_MODULE_PATH "${CMAKE_PREFIX_PATH}/share/cmake/pybind11")

include(pybind11Tools)
include_directories("${CMAKE_PREFIX_PATH}/include")

# numpy
set(PYBIND11_CPP_STANDARD -std=c++14)
find_package(NumPy REQUIRED)
include_directories(${NUMPY_INCLUDE_DIRS})
message(STATUS "Using numpy from ${NUMPY_INCLUDE_DIRS}")

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
