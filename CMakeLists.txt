cmake_minimum_required(VERSION 3.15)

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

set(CMAKE_CXX_STANDARD 20)

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


if(MSVC)
    add_definitions(/DNOMINMAX)
endif()


include(CMakePackageConfigHelpers)


##############################
# Build options
##############################

# Compression Libraries
option(WITH_BLOSC "Build with blosc compression" OFF)
option(WITH_ZLIB "Build with zlib/gzip compression" ON)
# Use libdeflate (a faster, SIMD-accelerated gzip/zlib implementation) as the
# backend for the gzip/zlib codec instead of stock zlib. On-disk format is
# unchanged; set to OFF to fall back to the stock zlib code path.
option(USE_LIBDEFLATE "Use libdeflate as the gzip/zlib backend instead of zlib" ON)
option(WITH_BZIP2 "Build with bzip2 compression" OFF)
option(WITH_XZ "Build with xz compression" OFF)
option(WITH_LZ4 "Build with lz4 compression" OFF)
option(WITH_ZSTD "Build with zstd compression" OFF)

# build with amazon s3 storage
option(WITH_S3 "Build with AWS S3 support" OFF)

# build with google cloud storage
option(WITH_GCS "Build with google cloud storage support" OFF)

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
    # add gtest external project and include the directories.
    # we enter the inner googletest dir directly, so we must provide
    # GOOGLETEST_VERSION (normally set by the outer CMakeLists) ourselves.
    if(NOT DEFINED GOOGLETEST_VERSION)
        set(GOOGLETEST_VERSION 1.15.0)
    endif()
    add_subdirectory(external/googletest/googletest)
    include_directories(${gtest_SOURCE_DIR/include} ${gtest_SOURCE_DIR})
endif()


###############################
# Set up conda env
###############################

# This does not work within travis or appveyor, because
# we set the CMAKE_PREFIX_PATH
# It is also skipped under scikit-build-core (pip wheel builds), where the
# dependencies come from the vcpkg toolchain instead of a conda env.
# TODO rename this to USE_CONDA_PREFIX or similar
if(NOT WITHIN_TRAVIS AND NOT SKBUILD)
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

# Only add the prefix include dir when we actually have a prefix (conda/dev
# builds). Under scikit-build-core the prefix is empty and deps are found via
# the vcpkg toolchain.
if(CMAKE_PREFIX_PATH)
    if (MSVC)
        include_directories("${CMAKE_PREFIX_PATH}/Library/include")
    else()
        include_directories("${CMAKE_PREFIX_PATH}/include")
    endif()
endif()

###############################
# Include system / conda libs
###############################

# find libraries - pthread
find_package(Threads)

# add C++ filesystem
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    SET(FILESYSTEM_LIBRARIES "stdc++fs")
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

    # vcpkg's static libblosc is built with snappy and references its symbols;
    # z5's FindBLOSC only returns the blosc lib, not its transitive deps, so add
    # snappy explicitly for the wheel (SKBUILD) build. Conda ships a shared
    # libblosc that carries its own deps, so this is skipped there.
    if(SKBUILD)
        find_library(SNAPPY_LIBRARY NAMES snappy)
        if(SNAPPY_LIBRARY)
            SET(COMPRESSION_LIBRARIES "${COMPRESSION_LIBRARIES};${SNAPPY_LIBRARY}")
        endif()

        # libblosc's built-in zlib codec (zlib_wrap_compress/_decompress) references
        # zlib's compress2/uncompress. The static wheel libblosc does not carry zlib,
        # and the WITH_ZLIB block now links libdeflate (not zlib), so link zlib
        # explicitly here (same rationale as the snappy handling above). Conda's
        # shared libblosc carries its own zlib, so this SKBUILD-only block is skipped
        # there.
        find_package(ZLIB REQUIRED)
        SET(COMPRESSION_LIBRARIES "${COMPRESSION_LIBRARIES};${ZLIB_LIBRARIES}")
    endif()
endif()

if(WITH_ZLIB)
    set(Z5_DEFINES "${Z5_DEFINES};-DWITH_ZLIB")
    if(USE_LIBDEFLATE)
        find_package(libdeflate CONFIG REQUIRED)
        set(Z5_DEFINES "${Z5_DEFINES};-DWITH_LIBDEFLATE")
        # the imported target name varies by packaging: conda-forge ships
        # libdeflate::libdeflate_shared, vcpkg ships libdeflate::libdeflate_static,
        # and some installs provide a plain libdeflate::libdeflate alias.
        if(TARGET libdeflate::libdeflate)
            set(LIBDEFLATE_TARGET libdeflate::libdeflate)
        elseif(TARGET libdeflate::libdeflate_shared)
            set(LIBDEFLATE_TARGET libdeflate::libdeflate_shared)
        elseif(TARGET libdeflate::libdeflate_static)
            set(LIBDEFLATE_TARGET libdeflate::libdeflate_static)
        else()
            message(FATAL_ERROR "libdeflate was found but exposes no known imported target "
                                "(libdeflate::libdeflate[_shared|_static])")
        endif()
        message(STATUS "Using libdeflate target ${LIBDEFLATE_TARGET} as the gzip/zlib backend")
        SET(COMPRESSION_LIBRARIES "${COMPRESSION_LIBRARIES};${LIBDEFLATE_TARGET}")
    else()
        find_package(ZLIB REQUIRED)
        include_directories(${ZLIB_INCLUDE_DIRS})
        SET(COMPRESSION_LIBRARIES "${COMPRESSION_LIBRARIES};${ZLIB_LIBRARIES}")
    endif()
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

if(WITH_ZSTD)
    find_package(ZSTD REQUIRED)
    include_directories(${ZSTD_INCLUDE_DIR})
    set(Z5_DEFINES "${Z5_DEFINES};-DWITH_ZSTD")
    SET(COMPRESSION_LIBRARIES "${COMPRESSION_LIBRARIES};${ZSTD_LIBRARY}")
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
# Python-bindings
###############################


if(BUILD_Z5PY)
    # find Python (needed by nanobind to build the extension module)
    if(CMAKE_VERSION VERSION_LESS 3.18)
        set(DEV_MODULE Development)
    else()
        set(DEV_MODULE Development.Module)
    endif()
    find_package(Python 3.9 COMPONENTS Interpreter ${DEV_MODULE} REQUIRED)

    # locate nanobind's cmake config via the installed package
    # (works for both pip and conda installs). We point nanobind_DIR directly
    # at the config directory so this works regardless of the CMP0074 policy.
    if(NOT nanobind_DIR)
        execute_process(
            COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
            OUTPUT_STRIP_TRAILING_WHITESPACE
            OUTPUT_VARIABLE nanobind_DIR)
    endif()
    find_package(nanobind CONFIG REQUIRED)
    message(STATUS "Using nanobind from ${nanobind_DIR}")
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

# Install the header-only C++ library and its CMake package config.
# Skipped under scikit-build-core: pip wheels only ship the z5py extension,
# not the C++ headers / CMake config.
if(NOT SKBUILD)
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
endif()
