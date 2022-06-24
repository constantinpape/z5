cmake_minimum_required(VERSION 3.1)

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

project(z5)

set(Z5_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)


##############################
# Versioning
##############################

file(STRINGS "${Z5_INCLUDE_DIR}/z5/z5.hxx" z5_version_defines
     REGEX "#define Z5_VERSION_(MAJOR|MINOR|PATCH)")
foreach(ver ${z5_version_defines})
    if(ver MATCHES "#define Z5_VERSION_(MAJOR|MINOR|PATCH) +([^ ]+)$")
        set(Z5_VERSION_${CMAKE_MATCH_1} "${CMAKE_MATCH_2}" CACHE INTERNAL "")
    endif()
endforeach()
set(${PROJECT_NAME}_VERSION
    ${Z5_VERSION_MAJOR}.${Z5_VERSION_MINOR}.${Z5_VERSION_PATCH})
message(STATUS "Building z5 v${${PROJECT_NAME}_VERSION}")


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
option(USE_OLD_ABI "Use the old GCC ABI to be compatible with old GLIBC versions" OFF)
if(USE_OLD_ABI)
    message(STATUS "Using old GCC ABI")
    add_definitions(-D_GLIBCXX_USE_CXX11_ABI=0)
endif()

if(MSVC)
    add_definitions(/DNOMINMAX)
endif()


include(CMakePackageConfigHelpers)


##############################
# Build options
##############################

# Compression Libraries
option(WITH_BLOSC "Build with blosc compression" OFF)
option(WITH_ZLIB "Build with zlib compression" ON)
option(WITH_BZIP2 "Build with bzip2 compression" OFF)
option(WITH_XZ "Build with xz compression" OFF)
option(WITH_LZ4 "Build with lz4 compression" OFF)

# build with amazon s3 storage
option(WITH_S3 "Build with AWS S3 support" OFF)

# build with google cloud storage
option(WITH_GCS "Build with google cloud storage support" OFF)

# marray multiarray
option(WITH_MARRAY "Build with c++ marray multiarray" OFF)

# adaptions to travis build
option(WITHIN_TRAVIS "Flag for builds within travis" OFF)

# build c++ tests
option(BUILD_TESTS "Build c++ tests" OFF)

# do we build python bindings?
# a.k.a z5py
option(BUILD_Z5PY "Build z5 python bindings" ON)


###############################
# Include gtest submodule
###############################

# NOTE I tried to replace this with the conda package and
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
    endif()

    # on clang, we need to enable libc++experimental, see
    # https://stackoverflow.com/a/45332844
    if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        SET(FILESYSTEM_LIBRARIES "c++experimental")
    endif()
endif()
include_directories(${Boost_INCLUDE_DIR})


###################
# xtensor libraries
###################

find_package(xtensor REQUIRED)
include_directories(xtensor_INCLUDE_DIRS)

if(BUILD_Z5PY)
    find_package(xtensor-python REQUIRED)
    include_directories(xtensor-python_INCLUDE_DIRS)
endif()

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
    set(Z5_DEFINES "${Z5_DEFINES};-DWITH_BLOSC")
    SET(COMPRESSION_LIBRARIES "${COMPRESSION_LIBRARIES};${BLOSC_LIBRARIES}")
endif()

if(WITH_ZLIB)
    find_package(ZLIB REQUIRED)
    include_directories(ZLIB_INCLUDE_DIRS)
    set(Z5_DEFINES "${Z5_DEFINES};-DWITH_ZLIB")
    SET(COMPRESSION_LIBRARIES "${COMPRESSION_LIBRARIES};${ZLIB_LIBRARIES}")
endif()

if(WITH_BZIP2)
    find_package(BZip2 REQUIRED)
    include_directories(BZIP2_INCLUDE_DIRS)
    set(Z5_DEFINES "${Z5_DEFINES};-DWITH_BZIP2")
    SET(COMPRESSION_LIBRARIES "${COMPRESSION_LIBRARIES};${BZIP2_LIBRARIES}")
endif()

if(WITH_XZ)
    find_package(LibLZMA REQUIRED)
    include_directories(LIBLZMA_INCLUDE_DIRS)
    set(Z5_DEFINES "${Z5_DEFINES};-DWITH_XZ")
    SET(COMPRESSION_LIBRARIES "${COMPRESSION_LIBRARIES};${LIBLZMA_LIBRARIES}")
endif()

# findig lz4 does not work reliably on windows, so we don't make it required
if(WITH_LZ4)
    find_package(LZ4)
    if(LZ4_FOUND)
        include_directories(LZ4_INCLUDE_DIR)
        set(Z5_DEFINES "${Z5_DEFINES};-DWITH_LZ4")
        SET(COMPRESSION_LIBRARIES "${COMPRESSION_LIBRARIES};${LZ4_LIBRARY}")
    endif()
endif()


###############################
# Cloud storage
###############################

SET(CLOUD_LIBRARIES "")

if(WITH_S3)
    # Do we need any additional components?
    find_package(AWSSDK REQUIRED COMPONENTS core s3)
    set(Z5_DEFINES "${Z5_DEFINES};-DWITH_S3")
    SET(CLOUD_LIBRARIES "${CLOUD_LIBRARIES};${AWSSDK_LINK_LIBRARIES}")
endif()

if(WITH_GCS)
    find_package(CURL REQUIRED)
    find_package(storage_client REQUIRED)
    set(Z5_DEFINES "${Z5_DEFINES};-DWITH_GCS")
    # SET(CLOUD_LIBRARIES "${CLOUD_LIBRARIES};${AWSSDK_LINK_LIBRARIES}")
endif()
message(STATUS "CLOUD LIBS: ${CLOUD_LIBRARIES}")


###############################
# Include marray (optional)
###############################

if(WITH_MARRAY)
    set(MARRAY_INCLUDE_DIR ${CMAKE_PREFIX_PATH} CACHE PATH "")
    set(Z5_DEFINES "${Z5_DEFINES};-DWITH_MARRAY")
    include_directories(${MARRAY_INCLUDE_DIR})
endif()


###############################
# Python-bindings
###############################


if(BUILD_Z5PY)
    # find pybind11, which is required to build the python bindings
    # the find package command will find python and set the python variables
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

    # pybind11 does not include numpy, so we must search for it in addition here
    find_package(NumPy REQUIRED)
    include_directories(${NUMPY_INCLUDE_DIRS})
    message(STATUS "Using numpy from ${NUMPY_INCLUDE_DIRS}")
endif()

add_library(z5 INTERFACE)

if(DEFINED Z5_DEFINES)
  add_definitions(${Z5_DEFINES})
  target_compile_definitions(z5 INTERFACE ${Z5_DEFINES})
endif()

target_link_libraries(z5 INTERFACE ${COMPRESSION_LIBRARIES})
target_include_directories(z5 INTERFACE
    $<BUILD_INTERFACE:${Z5_INCLUDE_DIR}>
    $<INSTALL_INTERFACE:include>)


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

install(TARGETS z5
        EXPORT ${PROJECT_NAME}-targets)

set(Z5_CMAKECONFIG_INSTALL_DIR "${CMAKE_INSTALL_PREFIX}/lib/cmake/${PROJECT_NAME}" CACHE
    STRING "install path for z5-config.cmake")

configure_package_config_file("${PROJECT_NAME}-config.cmake"
                              "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}-config.cmake"
                              INSTALL_DESTINATION ${Z5_CMAKECONFIG_INSTALL_DIR})

write_basic_package_version_file("${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}-config-version.cmake"
                                 VERSION ${${PROJECT_NAME}_VERSION}
                                 COMPATIBILITY AnyNewerVersion)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}-config.cmake
              ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}-config-version.cmake
        DESTINATION ${Z5_CMAKECONFIG_INSTALL_DIR})

install(EXPORT ${PROJECT_NAME}-targets
        FILE ${PROJECT_NAME}-targets.cmake
        DESTINATION ${Z5_CMAKECONFIG_INSTALL_DIR})
